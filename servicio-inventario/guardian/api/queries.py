"""Consultas de lectura de la API. No modifican nada."""

from __future__ import annotations

from psycopg import Cursor

from ..domain import lookups
from ..domain.ledger import movement_view
from ..errors import NotFound
from ..timeutil import iso


def _require_part(cur: Cursor, part_id: int) -> dict:
    part = lookups.part(cur, part_id)
    if part is None:
        raise NotFound(f"no existe la pieza {part_id}")
    return part


def availability(cur: Cursor, part_id: int) -> dict:
    part = _require_part(cur, part_id)
    cur.execute("SELECT on_hand, reserved, available FROM v_availability WHERE part_id = %s", (part_id,))
    totals = cur.fetchone()
    cur.execute(
        "SELECT location_id, code, on_hand FROM v_stock_by_location WHERE part_id = %s ORDER BY location_id",
        (part_id,),
    )
    # Las reservas son a nivel pieza: por ubicación se reporta reserved = 0 (ver openapi.yaml).
    locations = [{"location_id": r["location_id"], "code": r["code"], "on_hand": r["on_hand"],
                  "reserved": 0, "available": r["on_hand"]} for r in cur.fetchall()]
    return {"part_id": part_id, "sku": part["sku"], "name": part["name"], "on_hand": totals["on_hand"],
            "reserved": totals["reserved"], "available": totals["available"], "locations": locations}


def ledger(cur: Cursor, part_id: int, limit: int) -> dict:
    _require_part(cur, part_id)
    cur.execute(
        """
        SELECT l.*, loc.code AS location_code FROM v_ledger l JOIN locations loc USING (location_id)
        WHERE l.part_id = %s ORDER BY l.occurred_at DESC, l.movement_id DESC LIMIT %s
        """,
        (part_id, limit),
    )
    entries = [{**movement_view(r), "location_code": r["location_code"], "balance": r["balance"]}
               for r in cur.fetchall()]
    return {"part_id": part_id, "entries": entries}


def materials(cur: Cursor, code: str) -> dict:
    work_order = lookups.work_order_by_code(cur, code)
    if work_order is None:
        raise NotFound(f"no existe la orden {code}")
    cur.execute(
        """
        SELECT n.*, b.group_name, b.sort_order,
               COALESCE(r.remaining, 0)::int AS reserved, COALESCE(r.issued, 0)::int AS issued,
               s.shortage_id, s.missing_quantity
        FROM needs n
        LEFT JOIN bom_lines b USING (bom_line_id)
        LEFT JOIN LATERAL (
            SELECT SUM(quantity - issued_qty) FILTER (WHERE status = 'active') AS remaining,
                   SUM(issued_qty) AS issued
            FROM reservations r WHERE r.work_order_id = n.work_order_id AND r.bom_line_id = n.bom_line_id
        ) r ON true
        LEFT JOIN shortages s ON s.work_order_id = n.work_order_id AND s.bom_line_id = n.bom_line_id
                             AND s.status = 'open'
        WHERE n.work_order_id = %s AND n.active
        ORDER BY b.sort_order NULLS LAST, n.bom_line_id
        """,
        (work_order["work_order_id"],),
    )
    lines = [{
        "inspection_item_id": r["inspection_item_id"],
        "bom_line_id": r["bom_line_id"],
        "part_id": r["part_id"],
        "name": r["name"],
        "group_name": r["group_name"],
        "required": r["required_qty"],
        "reserved": r["reserved"],
        "issued": r["issued"],
        "missing": r["missing_quantity"] if r["shortage_id"] is not None else 0,
        "inspection_id": r["inspection_id"],
    } for r in cur.fetchall()]
    return {"work_order_id": work_order["work_order_id"], "code": work_order["code"],
            "model_id": work_order["model_id"], "stage": work_order["stage"],
            "deleted": work_order["deleted_at"] is not None, "lines": lines}


def shortages(cur: Cursor, work_order_code: str | None, part_id: int | None) -> dict:
    cur.execute(
        """
        SELECT s.*, w.code FROM shortages s JOIN work_orders w USING (work_order_id)
        WHERE s.status = 'open'
          AND (%(code)s::text IS NULL OR w.code = %(code)s)
          AND (%(part)s::int IS NULL OR s.part_id = %(part)s)
        ORDER BY s.opened_at, s.shortage_id
        """,
        {"code": work_order_code, "part": part_id},
    )
    return {"items": [{
        "work_order_id": r["work_order_id"],
        "work_order_code": r["code"],
        "part_id": r["part_id"],
        "name": r["name"],
        "missing_quantity": r["missing_quantity"],
        "inspection_item_id": r["inspection_item_id"],
        "bom_line_id": r["bom_line_id"],
        "shortage_id": r["shortage_id"],
        "opened_at": iso(r["opened_at"]),
    } for r in cur.fetchall()]}


def reorder_suggestions(cur: Cursor) -> dict:
    cur.execute(
        """
        SELECT r.*, p.sku, p.name FROM reorder_suggestions r JOIN parts p USING (part_id)
        ORDER BY r.updated_at DESC, r.part_id
        """
    )
    return {"items": [{
        "part_id": r["part_id"],
        "sku": r["sku"],
        "name": r["name"],
        "suggested_quantity": r["suggested_quantity"],
        "work_order_ids": list(r["work_order_ids"]),
        "updated_at": iso(r["updated_at"]),
    } for r in cur.fetchall()]}


def unmatched_receipts(cur: Cursor) -> dict:
    cur.execute("SELECT * FROM unmatched_receipts WHERE status = 'pending' ORDER BY received_at, id")
    return {"items": [{
        "id": r["id"],
        "purchase_line_id": r["purchase_line_id"],
        "request_number": r["request_number"],
        "part_number": r["part_number"],
        "description": r["description"],
        "quantity": r["quantity"],
        "unit_price": f"{r['unit_price']:.2f}" if r["unit_price"] is not None else None,
        "currency": r["currency"],
        "received_at": iso(r["received_at"]),
        "work_order_code": r["work_order_code"],
        "candidate_part_ids": list(r["candidate_part_ids"] or []),
    } for r in cur.fetchall()]}


def scan(cur: Cursor, code: str) -> dict:
    """Interpreta un código escaneado (§5 del contrato): ubicación, pieza por sku o P-<id>.
    Si un sku pertenece a varias piezas, devuelve todas para que el usuario elija."""
    cur.execute(
        "SELECT location_id, code, name, active FROM locations WHERE code = upper(btrim(%s))"
        " ORDER BY active DESC, updated_at DESC",
        (code,),
    )
    locations = cur.fetchall()
    cur.execute(
        """
        SELECT part_id, sku, name, active FROM parts
        WHERE sku = %(code)s OR sku_norm = upper(btrim(%(code)s))
           OR part_id = CASE WHEN upper(btrim(%(code)s)) ~ '^P-[0-9]{1,9}$'
                             THEN substring(upper(btrim(%(code)s)) from 3)::int END
        ORDER BY (sku = %(code)s) DESC NULLS LAST, active DESC, part_id
        """,
        {"code": code},
    )
    parts = cur.fetchall()
    return {"code": code, "locations": locations, "parts": parts,
            "needs_choice": len(parts) > 1 or len(locations) > 1}


def health(cur: Cursor) -> dict:
    cur.execute(
        """
        SELECT (SELECT count(*) FROM pending_events)::int AS pending_events,
               (SELECT count(*) FROM outbox WHERE published_at IS NULL)::int AS outbox_backlog,
               (SELECT count(*) FROM outbox WHERE topic = 'inventory.dlq')::int AS dead_letters,
               (SELECT count(*) FROM processed_events)::int AS processed_events
        """
    )
    return cur.fetchone()
