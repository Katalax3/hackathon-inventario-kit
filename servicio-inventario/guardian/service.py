"""Ensamble del servicio: base de datos, procesador de eventos y hilos de fondo."""

from __future__ import annotations

import logging
import threading

from .config import Settings
from .consumer.processor import EventProcessor
from .consumer.runner import ConsumerWorker
from .consumer.validation import ContractValidator
from .db import SERVICE_APP_NAME, Database
from .outbox import OutboxPublisher

log = logging.getLogger(__name__)


class Guardian:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.database_url, settings.db_pool_size, SERVICE_APP_NAME)
        self.stop_event = threading.Event()
        self.processor = EventProcessor(self.db, settings, ContractValidator(settings.contract_dir))
        self.consumer: ConsumerWorker | None = None
        self.publisher: OutboxPublisher | None = None

    @property
    def catalog_loaded(self) -> bool:
        return self.consumer is not None and self.consumer.catalog_loaded.is_set()

    def open(self) -> None:
        """Conecta y deja el esquema al día, sin arrancar hilos."""
        self.db.open()
        applied = self.db.migrate()
        if applied:
            log.info("migraciones aplicadas: %s", ", ".join(applied))

    def start(self) -> None:
        self.open()
        if self.settings.run_publisher:
            self.publisher = OutboxPublisher(self.settings, self.db, self.stop_event)
            self.publisher.start()
        if self.settings.run_consumer:
            self.consumer = ConsumerWorker(self.settings, self.db, self.processor, self.stop_event)
            self.consumer.start()
        log.info("guardian activo · broker %s", self.settings.broker)

    def shutdown(self) -> None:
        self.stop_event.set()
        for worker in (self.consumer, self.publisher):
            if worker is not None:
                worker.join(timeout=15)
        self.db.close()
