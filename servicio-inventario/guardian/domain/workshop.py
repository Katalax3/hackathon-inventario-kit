"""Órdenes de trabajo e inspecciones (tópicos shop.work_orders y shop.inspections)."""

from __future__ import annotations

from psycopg.types.json import Jsonb

from ..errors import MissingDependency
from ..timeutil import parse_iso
from ..uow import UnitOfWork
from . import allocation, ledger, lookups


# --- órdenes ----------------------------------------------------------------------------
def work_order_opened(uow: UnitOfWork, data: dict) -> None:
    # Si la baja llegó primero, deleted_at se conserva.
    uow.cur.execute(
        """
        INSERT INTO work_orders (work_order_id, code, model_id, department, stage, received_at)
        VALUES (%s, %s, %s, %s, 'awaiting_quick_inspection', %s)
        ON CONFLICT (work_order_id) DO UPDATE SET
            code = EXCLUDED.code, model_id = EXCLUDED.model_id, department = EXCLUDED.department,
            received_at = EXCLUDED.received_at,
            stage = COALESCE(work_orders.stage, EXCLUDED.stage)
        """,
        (data["work_order_id"], data["code"], data["model_id"], data["department"], parse_iso(data["received_at"])),
    )


def work_order_stage_changed(uow: UnitOfWork, data: dict) -> None:
    uow.cur.execute("UPDATE work_orders SET stage = %s WHERE work_order_id = %s",
                    (data["to_stage"], data["work_order_id"]))
    if uow.cur.rowcount == 0:
        raise MissingDependency(f"work_order:{data['work_order_id']}")


def work_order_deleted(uow: UnitOfWork, data: dict) -> None:
    """Libera todas las reservas activas de la orden y cierra sus faltantes (regla 14)."""
    cur = uow.cur
    cur.execute(
        """
        INSERT INTO work_orders (work_order_id, code, deleted_at) VALUES (%s, %s, %s)
        ON CONFLICT (work_order_id) DO UPDATE SET deleted_at = COALESCE(work_orders.deleted_at, EXCLUDED.deleted_at)
        """,
        (data["work_order_id"], data["code"], uow.occurred_at),
    )
    _release_needs(uow, data["work_order_id"])


def _release_needs(uow: UnitOfWork, work_order_id: int, inspection_id: int | None = None) -> None:
    cur = uow.cur
    cur.execute(
        """
        SELECT n.bom_line_id, n.part_id FROM needs n
        WHERE n.work_order_id = %s AND n.active AND (%s::int IS NULL OR n.inspection_id = %s)
        """,
        (work_order_id, inspection_id, inspection_id),
    )
    needs = cur.fetchall()
    ledger.lock_parts(cur, [n["part_id"] for n in needs])
    for need in needs:
        allocation.release_line(uow, work_order_id, need["bom_line_id"])
    allocation.redistribute(uow)


# --- inspecciones -----------------------------------------------------------------------
def _upsert_inspection(uow: UnitOfWork, data: dict, status: str) -> None:
    uow.cur.execute(
        """
        INSERT INTO inspections (inspection_id, work_order_id, kind, status, approved_by, approved_at, items)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (inspection_id) DO UPDATE SET
            kind = EXCLUDED.kind, items = EXCLUDED.items, approved_by = EXCLUDED.approved_by,
            approved_at = EXCLUDED.approved_at,
            status = CASE WHEN inspections.status IN ('approved', 'voided') AND EXCLUDED.status = 'submitted'
                          THEN inspections.status ELSE EXCLUDED.status END
        """,
        (data["inspection_id"], data["work_order_id"], data["kind"], status, data.get("approved_by"),
         parse_iso(data["approved_at"]) if data.get("approved_at") else None, Jsonb(data["items"])),
    )


def _set_status(uow: UnitOfWork, data: dict, status: str) -> dict | None:
    uow.cur.execute(
        "UPDATE inspections SET status = %s, reason = %s WHERE inspection_id = %s RETURNING *",
        (status, data.get("reason"), data["inspection_id"]),
    )
    return uow.cur.fetchone()


def inspection_submitted(uow: UnitOfWork, data: dict) -> None:
    _upsert_inspection(uow, data, "submitted")  # no mueve inventario (regla 1)


def inspection_rejected(uow: UnitOfWork, data: dict) -> None:
    _set_status(uow, data, "rejected")


def inspection_discarded(uow: UnitOfWork, data: dict) -> None:
    _set_status(uow, data, "discarded")


def inspection_voided(uow: UnitOfWork, data: dict) -> None:
    """Libera reservas y cierra faltantes de las líneas cuya necesidad vigente venía de esta
    inspección (regla 13)."""
    _set_status(uow, data, "voided")
    _release_needs(uow, data["work_order_id"], data["inspection_id"])


def inspection_approved(uow: UnitOfWork, data: dict) -> None:
    """Solo la aprobación crea necesidad (regla 1)."""
    cur = uow.cur
    work_order = lookups.work_order_by_id(cur, data["work_order_id"])
    if work_order is None:
        raise MissingDependency(f"work_order:{data['work_order_id']}")
    lines = _approved_lines(cur, data["items"])  # puede pedir reintento: antes de escribir nada
    _upsert_inspection(uow, data, "approved")
    if work_order["deleted_at"] is not None:
        return  # orden dada de baja: la aprobación ya no aparta material

    cur.execute("SELECT part_id FROM needs WHERE work_order_id = %s AND active", (work_order["work_order_id"],))
    ledger.lock_parts(cur, [r["part_id"] for r in cur.fetchall()] + [l["part_id"] for l in lines])
    for line in lines:
        allocation.set_need(uow, work_order, inspection_id=data["inspection_id"], **line)
    allocation.redistribute(uow)


def _approved_lines(cur, items: list[dict]) -> list[dict]:
    """Agrupa los renglones por bom_line_id y resuelve la cantidad de cada uno.

    - condition ok o action repair: sin necesidad (regla 4).
    - quantity null con buy: qty_per_unit de la línea vigente; si no hay, None (regla 5).
    - Si un mismo renglón aparece dos veces en la aprobación, las cantidades se suman.
    """
    by_line: dict[int, dict] = {}
    for item in items:
        buy = item["condition"] != "ok" and item["action"] == "buy"
        quantity = item["quantity"]
        if buy:
            if item["part_id"] is not None and not lookups.part_exists(cur, item["part_id"]):
                raise MissingDependency(f"part:{item['part_id']}")
            if quantity is None:
                bom = lookups.bom_line(cur, item["bom_line_id"])
                if bom is None:
                    raise MissingDependency(f"bom_line:{item['bom_line_id']}")
                quantity = bom["qty_per_unit"] if bom["active"] else None
        line = by_line.get(item["bom_line_id"])
        if line is None or not line["buy"]:
            by_line[item["bom_line_id"]] = {
                "bom_line_id": item["bom_line_id"],
                "inspection_item_id": item["inspection_item_id"],
                "part_id": item["part_id"],
                "name": item["name"],
                "required": quantity if buy else None,
                "buy": buy,
            }
        elif buy:
            line["required"] = None if line["required"] is None or quantity is None else line["required"] + quantity
    return list(by_line.values())
