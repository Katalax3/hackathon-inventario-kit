"""Configuración del servicio, leída de variables de entorno."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = SERVICE_DIR / "sql"

# Tópicos del contrato (contract/README.md §2).
CATALOG_TOPIC = "shop.catalog"
INPUT_TOPICS = ("shop.catalog", "shop.work_orders", "shop.inspections", "shop.purchasing")
OUTPUT_TOPIC = "inventory.events"
DLQ_TOPIC = "inventory.dlq"

RECEIVING_LOCATION_ID = 100  # U-100 (regla 11)


def _default_contract_dir() -> str:
    candidate = SERVICE_DIR.parent / "contract" / "schemas"
    return str(candidate) if candidate.is_dir() else ""


def _bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "si", "sí")


@dataclass(frozen=True)
class Settings:
    database_url: str
    broker: str
    consumer_group: str
    api_host: str
    api_port: int
    contract_dir: str
    db_pool_size: int
    # Reintentos de un evento que falla por un error inesperado antes de ir a la DLQ.
    max_processing_attempts: int
    # Un evento que espera algo que no llega (pieza, orden...) va a la DLQ tras este tiempo.
    pending_max_age_seconds: float
    pending_retry_interval: float
    outbox_poll_interval: float
    # Releer las operaciones de la API (salidas, transferencias, conteos, recepciones
    # resueltas) desde inventory.events al reconstruir.
    replay_api_journal: bool
    run_consumer: bool
    run_publisher: bool
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        return cls(
            database_url=env("DATABASE_URL", "postgresql://guardian:guardian@localhost:5432/inventario"),
            broker=env("BROKER", "localhost:19092"),
            consumer_group=env("CONSUMER_GROUP", "inventory-guardian"),
            api_host=env("API_HOST", "0.0.0.0"),
            api_port=int(env("API_PORT", "8000")),
            contract_dir=env("CONTRACT_DIR", _default_contract_dir()),
            db_pool_size=int(env("DB_POOL_SIZE", "10")),
            max_processing_attempts=int(env("MAX_PROCESSING_ATTEMPTS", "3")),
            pending_max_age_seconds=float(env("PENDING_MAX_AGE_SECONDS", str(6 * 3600))),
            pending_retry_interval=float(env("PENDING_RETRY_INTERVAL", "5")),
            outbox_poll_interval=float(env("OUTBOX_POLL_INTERVAL", "0.5")),
            replay_api_journal=_bool("REPLAY_API_JOURNAL", True),
            run_consumer=_bool("RUN_CONSUMER", True),
            run_publisher=_bool("RUN_PUBLISHER", True),
            log_level=env("LOG_LEVEL", "INFO"),
        )
