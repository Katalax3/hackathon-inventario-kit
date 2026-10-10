"""Unidad de trabajo: una transacción, el hecho que la originó y los eventos que produce.

Todo cambio de inventario ocurre dentro de una `UnitOfWork`. Los eventos de salida se
escriben en `outbox` en la misma transacción (patrón outbox): si la transacción se
confirma, el evento se publicará; si se revierte, nunca existió.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from psycopg import Cursor
from psycopg.types.json import Jsonb

from .config import OUTPUT_TOPIC
from .timeutil import iso, utcnow

# Espacio para los event_id deterministas de las salidas derivadas de un evento de entrada:
# reconstruir desde cero vuelve a producir los mismos event_id y quien consuma puede deduplicar.
OUTPUT_NAMESPACE = uuid.UUID("6f1c2a52-8d3e-4b7a-9c41-2e5d7f0a9b13")

# Marca en `data` de las operaciones originadas en la API. Al reconstruir, el consumidor
# relee inventory.events y vuelve a aplicar solo estas (ver consumer/journal.py).
API_ORIGIN = "api"


class UnitOfWork:
    def __init__(self, cur: Cursor, *, occurred_at: datetime | None = None,
                 source_event_id: str | None = None) -> None:
        self.cur = cur
        self.occurred_at = occurred_at or utcnow()
        self.source_event_id = source_event_id
        self.dirty_parts: set[int] = set()  # piezas cuya sugerencia de compra hay que recalcular
        self.freed_parts: set[int] = set()  # piezas con reservas liberadas, para surtir a otros
        self.emitted: list[dict] = []
        self._seq = 0

    def touch(self, part_id: int | None) -> None:
        if part_id is not None:
            self.dirty_parts.add(part_id)

    def emit(self, event_type: str, key: str, data: dict, *, journal: bool = False) -> dict:
        """Deja un evento `stock.*` en la bandeja de salida.

        `journal=True` marca una operación de la API: se registra como ya procesada para
        que el consumidor la ignore al leerla de vuelta, y se reaplica al reconstruir.
        """
        self._seq += 1
        if self.source_event_id and not journal:
            event_id = str(uuid.uuid5(OUTPUT_NAMESPACE, f"{self.source_event_id}:{self._seq}"))
        else:
            event_id = str(uuid.uuid4())
        if journal:
            data = {**data, "origin": API_ORIGIN}
        envelope = {
            "event_id": event_id,
            "event_type": event_type,
            "event_version": 1,
            "occurred_at": iso(self.occurred_at),
            "source": "inventory",
            "key": key,
            "data": data,
        }
        self.cur.execute(
            "INSERT INTO outbox (event_id, event_type, event_key, payload, topic)"
            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING",
            (event_id, event_type, key, Jsonb(envelope), OUTPUT_TOPIC),
        )
        if journal:
            self.cur.execute(
                "INSERT INTO processed_events (event_id, event_type, topic) VALUES (%s, %s, %s)"
                " ON CONFLICT (event_id) DO NOTHING",
                (event_id, event_type, OUTPUT_TOPIC),
            )
        self.emitted.append(envelope)
        return envelope
