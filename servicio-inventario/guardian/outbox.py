"""Publicador de la bandeja de salida: lleva `outbox` a Kafka.

Lee en orden de inserción, publica con productor idempotente (acks=all), espera la
confirmación del broker y solo entonces marca `published_at`. Si algo falla, la
transacción se revierte y el lote se reintenta: entrega al menos una vez, con el mismo
event_id, como el resto del contrato.
"""

from __future__ import annotations

import json
import logging
import threading

from confluent_kafka import Producer

from .config import Settings
from .db import Database

log = logging.getLogger(__name__)

BATCH_SIZE = 200
_wakeup = threading.Event()


def wake() -> None:
    """Avisa que hay eventos nuevos tras un commit, para no esperar al siguiente sondeo."""
    _wakeup.set()


class OutboxPublisher(threading.Thread):
    def __init__(self, settings: Settings, db: Database, stop: threading.Event) -> None:
        super().__init__(name="outbox-publisher", daemon=True)
        self.settings = settings
        self.db = db
        self.stop_event = stop

    def run(self) -> None:
        producer = Producer({
            "bootstrap.servers": self.settings.broker,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        })
        log.info("publicador de outbox activo")
        while not self.stop_event.is_set():
            _wakeup.wait(self.settings.outbox_poll_interval)
            _wakeup.clear()
            try:
                while self._publish_batch(producer) == BATCH_SIZE and not self.stop_event.is_set():
                    pass
            except Exception:
                log.exception("no se pudo publicar la bandeja de salida; se reintenta")
                self.stop_event.wait(2)
        producer.flush(10)

    def _publish_batch(self, producer: Producer) -> int:
        def publish(cur) -> int:
            cur.execute(
                "SELECT id, topic, event_key, payload, headers FROM outbox WHERE published_at IS NULL"
                " ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
                (BATCH_SIZE,),
            )
            rows = cur.fetchall()
            if not rows:
                return 0
            failures: list[str] = []

            def on_delivery(err, _msg) -> None:
                if err is not None:
                    failures.append(str(err))

            for row in rows:
                headers = [(k, str(v).encode()) for k, v in (row["headers"] or {}).items()]
                producer.produce(
                    row["topic"],
                    key=row["event_key"].encode(),
                    value=json.dumps(row["payload"], ensure_ascii=False).encode(),
                    headers=headers or None,
                    on_delivery=on_delivery,
                )
            remaining = producer.flush(15)
            if remaining or failures:
                raise RuntimeError(f"el broker no confirmó {remaining} mensajes: {failures[:3]}")
            cur.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)", ([r["id"] for r in rows],))
            return len(rows)

        return self.db.run(publish)
