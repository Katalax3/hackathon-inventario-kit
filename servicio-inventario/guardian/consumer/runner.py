"""Hilo que lee Kafka.

Fase 1: lee `shop.catalog` hasta el final que tenía al arrancar (regla 22). Así, al
reconstruir, las compras encuentran sus piezas en lugar de irse a revisión manual.
Fase 2: lee todos los tópicos del taller y, para reconstruir, `inventory.events`.

La posición de lectura vive en la base (`consumer_offsets`), no en el grupo de Kafka: se
asignan las particiones a mano y se busca la posición guardada. Base vacía = leer desde
el principio.
"""

from __future__ import annotations

import logging
import threading
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from ..config import CATALOG_TOPIC, INPUT_TOPICS, OUTPUT_TOPIC, Settings
from ..db import Database
from .processor import EventProcessor, load_offsets

log = logging.getLogger(__name__)


class ConsumerWorker(threading.Thread):
    def __init__(self, settings: Settings, db: Database, processor: EventProcessor, stop: threading.Event) -> None:
        super().__init__(name="kafka-consumer", daemon=True)
        self.settings = settings
        self.db = db
        self.processor = processor
        self.stop_event = stop
        self.catalog_loaded = threading.Event()
        self.consumed = 0
        self._probe: Consumer | None = None
        self._probe_lock = threading.Lock()
        self.topics = list(INPUT_TOPICS) + ([OUTPUT_TOPIC] if settings.replay_api_journal else [])

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._run_once()
            except Exception:
                log.exception("el consumidor se detuvo; se reinicia desde la última posición guardada")
                self.stop_event.wait(5)
        with self._probe_lock:
            if self._probe is not None:
                self._probe.close()

    def _run_once(self) -> None:
        consumer = Consumer({
            "bootstrap.servers": self.settings.broker,
            "group.id": self.settings.consumer_group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        try:
            partitions = self._wait_for_topics(consumer)
            if not self.catalog_loaded.is_set():
                started = time.monotonic()
                self._consume(consumer, [tp for tp in partitions if tp[0] == CATALOG_TOPIC], until_end=True)
                if self.stop_event.is_set():
                    return
                self.processor.retry_pending(force=True)
                self.catalog_loaded.set()
                log.info("catálogo cargado en %.1f s; leyendo todos los tópicos", time.monotonic() - started)
            self._consume(consumer, partitions, until_end=False)
        finally:
            self.processor.flush_offsets()
            consumer.close()

    def _wait_for_topics(self, consumer: Consumer) -> list[tuple[str, int]]:
        while not self.stop_event.is_set():
            try:
                metadata = consumer.list_topics(timeout=10)
                missing = [t for t in self.topics if t not in metadata.topics]
                if not missing:
                    return [(t, p) for t in self.topics for p in sorted(metadata.topics[t].partitions)]
                log.info("esperando a que existan los tópicos %s", missing)
            except Exception as exc:
                log.info("esperando al broker %s: %s", self.settings.broker, exc)
            self.stop_event.wait(3)
        return []

    def _consume(self, consumer: Consumer, partitions: list[tuple[str, int]], *, until_end: bool) -> None:
        stored = self.db.run(load_offsets)
        consumer.assign([TopicPartition(t, p, stored.get((t, p), OFFSET_BEGINNING)) for t, p in partitions])

        # Fase 1: hasta dónde hay que leer en cada partición para "llegar al final".
        targets: dict[tuple[str, int], int] = {}
        if until_end:
            for t, p in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10)
                if high > stored.get((t, p), low):
                    targets[(t, p)] = high

        last_retry = last_report = time.monotonic()
        consumed_at_report = self.consumed
        while not self.stop_event.is_set():
            if until_end and not targets:
                return
            msg = consumer.poll(0.5)
            now = time.monotonic()
            if now - last_retry >= self.settings.pending_retry_interval:
                self.processor.retry_pending()
                last_retry = now
            if now - last_report >= 5:
                if self.consumed != consumed_at_report:
                    log.info("leídos %s mensajes (%.0f/s)", self.consumed,
                             (self.consumed - consumed_at_report) / (now - last_report))
                last_report, consumed_at_report = now, self.consumed
            if msg is None:
                self.processor.flush_offsets()  # ocioso: guardar posiciones de mensajes ignorados
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("error del broker: %s", msg.error())
                continue
            tp = (msg.topic(), msg.partition())
            key = msg.key().decode("utf-8", "replace") if msg.key() else None
            self.processor.handle(msg.topic(), msg.value(), key, (tp[0], tp[1], msg.offset() + 1))
            self.consumed += 1
            if until_end and tp in targets and msg.offset() + 1 >= targets[tp]:
                del targets[tp]

    def lag(self) -> int | None:
        """Mensajes por leer: marca alta del broker menos la posición guardada en la base.
        0 = al día (p. ej., terminó una reconstrucción). Usa un cliente aparte que no lee
        mensajes: las consultas del consumidor activo se forman detrás de sus lecturas."""
        try:
            with self._probe_lock:
                if self._probe is None:
                    self._probe = Consumer({"bootstrap.servers": self.settings.broker,
                                            "group.id": f"{self.settings.consumer_group}-probe"})
                metadata = self._probe.list_topics(timeout=3)
                stored = self.db.run(load_offsets)
                total = 0
                for topic in self.topics:
                    for partition in metadata.topics[topic].partitions:
                        _, high = self._probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=3)
                        total += max(high - stored.get((topic, partition), 0), 0)
                return total
        except Exception:
            return None
