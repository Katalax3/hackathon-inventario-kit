"""Sugerencias de compra (regla 18) y políticas de reabasto.

Una pieza tiene sugerencia vigente si tiene faltantes abiertos o, con política, si lo
disponible queda bajo su mínimo:

    por faltantes = suma de lo que falta (los faltantes de cantidad desconocida cuentan 1)
    por política  = si disponible < mínimo: max(reorder_qty, (máximo o mínimo) - disponible)
    sugerida      = por faltantes + por política

Se emite `stock.reorder_suggested` cuando la sugerencia nace o cambia.
"""

from __future__ import annotations

from ..uow import UnitOfWork
from . import ledger


def compute(cur, part_id: int) -> tuple[int, int, list[int]]:
    """(por faltantes, por política, órdenes que la piden)."""
    cur.execute(
        "SELECT work_order_id, missing_quantity FROM shortages WHERE part_id = %s AND status = 'open'",
        (part_id,),
    )
    rows = cur.fetchall()
    from_shortages = sum(r["missing_quantity"] if r["missing_quantity"] is not None else 1 for r in rows)
    work_order_ids = sorted({r["work_order_id"] for r in rows})

    from_policy = 0
    cur.execute("SELECT * FROM stock_policies WHERE part_id = %s", (part_id,))
    policy = cur.fetchone()
    if policy and policy["min_qty"] > 0:
        projected = ledger.available(cur, part_id)
        if projected < policy["min_qty"]:
            target = policy["max_qty"] or policy["min_qty"]
            from_policy = max(policy["reorder_qty"] or 0, target - projected)
    return from_shortages, from_policy, work_order_ids


def refresh(uow: UnitOfWork) -> None:
    """Recalcula la sugerencia de cada pieza tocada en esta transacción."""
    cur = uow.cur
    for part_id in sorted(uow.dirty_parts):
        from_shortages, from_policy, work_order_ids = compute(cur, part_id)
        suggested = from_shortages + from_policy
        cur.execute("SELECT * FROM reorder_suggestions WHERE part_id = %s FOR UPDATE", (part_id,))
        current = cur.fetchone()

        if suggested <= 0:
            if current:
                cur.execute("DELETE FROM reorder_suggestions WHERE part_id = %s", (part_id,))
            continue
        if current and current["suggested_quantity"] == suggested and list(current["work_order_ids"]) == work_order_ids:
            continue

        cur.execute(
            """
            INSERT INTO reorder_suggestions (part_id, suggested_quantity, work_order_ids, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (part_id) DO UPDATE SET suggested_quantity = EXCLUDED.suggested_quantity,
                work_order_ids = EXCLUDED.work_order_ids, updated_at = EXCLUDED.updated_at
            """,
            (part_id, suggested, work_order_ids, uow.occurred_at),
        )
        cur.execute("SELECT sku, name FROM parts WHERE part_id = %s", (part_id,))
        part = cur.fetchone() or {}
        uow.emit("stock.reorder_suggested", f"part:{part_id}", {
            "part_id": part_id,
            "suggested_quantity": suggested,
            "work_order_ids": work_order_ids,
            "sku": part.get("sku"),
            "name": part.get("name"),
            "shortage_quantity": from_shortages,
            "policy_quantity": from_policy,
        })
    uow.dirty_parts.clear()


def set_policy(uow: UnitOfWork, part_id: int, *, min_qty: int, max_qty: int | None, reorder_qty: int | None,
               lead_time_days: int | None) -> dict:
    uow.cur.execute(
        """
        INSERT INTO stock_policies (part_id, min_qty, max_qty, reorder_qty, lead_time_days)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (part_id) DO UPDATE SET min_qty = EXCLUDED.min_qty, max_qty = EXCLUDED.max_qty,
            reorder_qty = EXCLUDED.reorder_qty, lead_time_days = EXCLUDED.lead_time_days
        RETURNING *
        """,
        (part_id, min_qty, max_qty, reorder_qty, lead_time_days),
    )
    policy = uow.cur.fetchone()
    uow.touch(part_id)
    return policy
