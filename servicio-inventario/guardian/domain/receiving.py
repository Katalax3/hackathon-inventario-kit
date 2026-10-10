"""Recepciones de compra (tópico shop.purchasing) y revisión manual de las que no se relacionan."""

from __future__ import annotations

from decimal import Decimal

from psycopg import Cursor

from ..config import RECEIVING_LOCATION_ID
from ..errors import Conflict, MissingDependency, NotFound
from ..timeutil import iso, parse_iso
from ..uow import UnitOfWork
from . import allocation, ledger, lookups
from .matching import could_match, match_part


def purchase_received(uow: UnitOfWork, data: dict) -> None:
    if data["purpose"] == "customer_order":
        return  # material para revender: no entra ni surte faltantes (regla 9)
    cur = uow.cur
    line_id = data["line_id"]
    # Segundo candado de idempotencia: una línea de compra entra una sola vez.
    if ledger.receipt_exists(cur, line_id) or _unmatched_by_line(cur, line_id):
        return

    match = match_part(cur, data["part_number"], data["description"])
    if match.part_id is None:
        _register_unmatched(uow, data, match.candidates)
        return
    receive(uow, part_id=match.part_id, location_id=RECEIVING_LOCATION_ID, purchase=data,
            match_rule=match.rule)


def receive(uow: UnitOfWork, *, part_id: int, location_id: int, purchase: dict,
            match_rule: str | None = None, journal: bool = False) -> dict:
    """Registra la entrada (regla 11), emite `stock.received` y surte faltantes (regla 12)."""
    cur = uow.cur
    ledger.lock_parts(cur, [part_id])
    movement = ledger.record(
        cur, part_id=part_id, location_id=location_id, quantity=purchase["quantity"], type="receipt",
        occurred_at=parse_iso(purchase["received_at"]), reference=f"PR-{purchase['request_number']}",
        purchase_line_id=purchase["line_id"], unit_cost=Decimal(purchase["unit_price"]),
        currency=purchase["currency"], source_event_id=uow.source_event_id,
    )
    data = {
        "part_id": part_id,
        "location_id": location_id,
        "quantity": purchase["quantity"],
        "unit_cost": f"{Decimal(purchase['unit_price']):.2f}",
        "currency": purchase["currency"],
        "purchase_line_id": purchase["line_id"],
        "movement_id": movement["movement_id"],
        "request_number": purchase["request_number"],
        "work_order_code": purchase.get("work_order_code"),
        "match_rule": match_rule,
    }
    uow.emit("stock.received", f"part:{part_id}", data, journal=journal)
    allocation.fill_shortages(uow, part_id, purchase.get("work_order_code"))
    uow.touch(part_id)
    return movement


# --- revisión manual (regla 10) -----------------------------------------------------------
def _unmatched_by_line(cur: Cursor, line_id: int) -> dict | None:
    cur.execute("SELECT * FROM unmatched_receipts WHERE purchase_line_id = %s", (line_id,))
    return cur.fetchone()


def _register_unmatched(uow: UnitOfWork, data: dict, candidates: list[int]) -> None:
    uow.cur.execute(
        """
        INSERT INTO unmatched_receipts (purchase_line_id, purchase_request_id, request_number, part_number,
                                        description, quantity, unit_price, currency, work_order_code,
                                        received_at, candidate_part_ids)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """,
        (data["line_id"], data["purchase_request_id"], data["request_number"], data["part_number"],
         data["description"], data["quantity"], Decimal(data["unit_price"]), data["currency"],
         data["work_order_code"], parse_iso(data["received_at"]), candidates or None),
    )
    receipt_id = uow.cur.fetchone()["id"]
    uow.emit("stock.unmatched_receipt", f"purchase_line:{data['line_id']}", {
        "purchase_line_id": data["line_id"],
        "part_number": data["part_number"],
        "description": data["description"],
        "quantity": data["quantity"],
        "unmatched_receipt_id": receipt_id,
        "candidate_part_ids": candidates,
        "work_order_code": data["work_order_code"],
    })


def _as_purchase(row: dict) -> dict:
    """Fila de unmatched_receipts → forma del `data` de purchase.item_received."""
    return {
        "purchase_request_id": row["purchase_request_id"],
        "request_number": row["request_number"],
        "line_id": row["purchase_line_id"],
        "part_number": row["part_number"],
        "description": row["description"],
        "quantity": row["quantity"],
        "unit_price": f"{row['unit_price'] or 0:.2f}",
        "currency": row["currency"] or "MXN",
        "received_at": iso(row["received_at"]),
        "work_order_code": row["work_order_code"],
    }


def _mark_resolved(uow: UnitOfWork, receipt_id: int, part_id: int, movement_id: int, resolution: str) -> None:
    uow.cur.execute(
        """
        UPDATE unmatched_receipts SET status = 'resolved', resolved_part_id = %s, resolved_at = %s,
               resolution = %s, movement_id = %s
        WHERE id = %s
        """,
        (part_id, uow.occurred_at, resolution, movement_id, receipt_id),
    )


def resolve_unmatched(uow: UnitOfWork, receipt_id: int, part_id: int, location_id: int | None) -> dict:
    """POST /unmatched-receipts/{id}/resolve: entra como si hubiera coincidido desde el principio."""
    cur = uow.cur
    cur.execute("SELECT * FROM unmatched_receipts WHERE id = %s FOR UPDATE", (receipt_id,))
    row = cur.fetchone()
    if row is None:
        raise NotFound(f"no existe la recepción pendiente {receipt_id}")
    if row["status"] != "pending":
        raise Conflict(f"la recepción {receipt_id} ya se resolvió")
    if not lookups.part_exists(cur, part_id):
        raise NotFound(f"no existe la pieza {part_id}")
    location_id = location_id or RECEIVING_LOCATION_ID
    if not lookups.location_exists(cur, location_id):
        raise NotFound(f"no existe la ubicación {location_id}")
    movement = receive(uow, part_id=part_id, location_id=location_id, purchase=_as_purchase(row),
                       match_rule="manual", journal=True)
    _mark_resolved(uow, receipt_id, part_id, movement["movement_id"], "manual")
    return movement


def auto_resolve_for_part(uow: UnitOfWork, part_id: int, sku: str | None) -> None:
    """Al llegar una pieza o cambiar su sku, relaciona las recepciones pendientes que ahora
    coinciden sin ambigüedad con ella."""
    cur = uow.cur
    cur.execute("SELECT * FROM unmatched_receipts WHERE status = 'pending' ORDER BY id")
    for row in cur.fetchall():
        if not could_match(sku, part_id, row["part_number"], row["description"]):
            continue
        match = match_part(cur, row["part_number"], row["description"])
        if match.part_id != part_id:
            continue
        movement = receive(uow, part_id=part_id, location_id=RECEIVING_LOCATION_ID,
                           purchase=_as_purchase(row), match_rule=match.rule)
        _mark_resolved(uow, row["id"], part_id, movement["movement_id"], "automatic")


def replay_resolution(uow: UnitOfWork, data: dict) -> None:
    """Reconstrucción: reaplica una resolución manual leída de inventory.events."""
    cur = uow.cur
    line_id = data["purchase_line_id"]
    if ledger.receipt_exists(cur, line_id):
        return  # al releer ya coincidió sola (p. ej. con el catálogo completo)
    row = _unmatched_by_line(cur, line_id)
    if row is None:
        raise MissingDependency(f"purchase_line:{line_id}")
    for ref, check in (("part", lookups.part_exists), ("location", lookups.location_exists)):
        ident = data["part_id"] if ref == "part" else data["location_id"]
        if not check(cur, ident):
            raise MissingDependency(f"{ref}:{ident}")
    movement = receive(uow, part_id=data["part_id"], location_id=data["location_id"],
                       purchase=_as_purchase(row), match_rule="manual")
    _mark_resolved(uow, row["id"], data["part_id"], movement["movement_id"], "manual")
