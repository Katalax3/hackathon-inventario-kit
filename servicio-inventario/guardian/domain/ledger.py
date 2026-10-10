"""Libro mayor: movimientos inmutables, saldos calculados y candados por pieza.

La existencia nunca se guarda como número: es la suma de `movements`. Toda operación que
lee un saldo para decidir (reservar, dar salida, transferir) primero toma el candado de
la pieza, así dos operaciones simultáneas no se llevan la misma última unidad (regla 8).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal

from psycopg import Cursor

from ..timeutil import iso

PART_LOCK_CLASS = 1  # primer argumento de pg_advisory_xact_lock(int, int)


def lock_parts(cur: Cursor, part_ids: Iterable[int | None]) -> None:
    """Candado transaccional por pieza, siempre en orden ascendente para evitar deadlocks."""
    for part_id in sorted({p for p in part_ids if p is not None}):
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (PART_LOCK_CLASS, part_id))


def on_hand(cur: Cursor, part_id: int, location_id: int | None = None) -> int:
    if location_id is None:
        cur.execute("SELECT COALESCE(SUM(quantity), 0)::int AS q FROM movements WHERE part_id = %s", (part_id,))
    else:
        cur.execute(
            "SELECT COALESCE(SUM(quantity), 0)::int AS q FROM movements WHERE part_id = %s AND location_id = %s",
            (part_id, location_id),
        )
    return cur.fetchone()["q"]


def reserved(cur: Cursor, part_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(SUM(quantity - issued_qty), 0)::int AS q FROM reservations"
        " WHERE part_id = %s AND status = 'active'",
        (part_id,),
    )
    return cur.fetchone()["q"]


def available(cur: Cursor, part_id: int) -> int:
    return on_hand(cur, part_id) - reserved(cur, part_id)


def record(cur: Cursor, *, part_id: int, location_id: int, quantity: int, type: str,
           occurred_at: datetime, reference: str | None = None, work_order_id: int | None = None,
           purchase_line_id: int | None = None, unit_cost: Decimal | str | None = None,
           currency: str | None = None, issued_by: str | None = None, transfer_id: str | None = None,
           source_event_id: str | None = None) -> dict:
    """Inserta un movimiento con signo y lo devuelve."""
    cur.execute(
        """
        INSERT INTO movements (part_id, location_id, quantity, type, reference, work_order_id,
                               purchase_line_id, unit_cost, currency, issued_by, transfer_id,
                               source_event_id, occurred_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (part_id, location_id, quantity, type, reference, work_order_id, purchase_line_id,
         unit_cost, currency, issued_by, transfer_id, source_event_id, occurred_at),
    )
    return cur.fetchone()


def receipt_exists(cur: Cursor, purchase_line_id: int) -> bool:
    cur.execute("SELECT 1 FROM movements WHERE purchase_line_id = %s AND type = 'receipt'", (purchase_line_id,))
    return cur.fetchone() is not None


def movement_view(row: dict) -> dict:
    """Forma `Movement` de la API (contract/openapi.yaml)."""
    view = {
        "movement_id": row["movement_id"],
        "type": row["type"],
        "part_id": row["part_id"],
        "location_id": row["location_id"],
        "quantity": row["quantity"],
        "occurred_at": iso(row["occurred_at"]),
        "reference": row.get("reference"),
    }
    for extra in ("work_order_id", "purchase_line_id", "issued_by", "currency"):
        if row.get(extra) is not None:
            view[extra] = row[extra]
    if row.get("unit_cost") is not None:
        view["unit_cost"] = f"{row['unit_cost']:.2f}"
    if row.get("transfer_id") is not None:
        view["transfer_id"] = str(row["transfer_id"])
    return view
