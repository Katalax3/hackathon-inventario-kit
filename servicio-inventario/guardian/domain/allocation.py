"""Necesidades, reservas y faltantes.

Modelo:
  - La necesidad vive por (orden, bom_line_id) en `needs`. Una aprobación posterior que trae
    la misma línea la REEMPLAZA (regla 2); una línea no mencionada la conserva (regla 3).
  - Las reservas son a nivel pieza (sin ubicación). Lo reservado activo de una línea es
    `quantity - issued_qty` de sus reservas `active`.
  - Para cada línea: objetivo = necesidad - ya entregado. Si lo reservado supera el objetivo
    se libera el excedente; si no alcanza, se reserva `min(disponible, lo que falta)` y el
    resto es faltante (regla 7). Hay a lo más un faltante abierto por línea.
  - Liberar no emite evento (regla 15). Lo liberado se ofrece a los faltantes más antiguos
    de esa pieza (`redistribute`): el material no se queda ocioso mientras alguien lo espera.
"""

from __future__ import annotations

from psycopg import Cursor

from ..uow import UnitOfWork
from . import ledger


# --- reservas -------------------------------------------------------------------------
def line_totals(cur: Cursor, work_order_id: int, bom_line_id: int) -> tuple[int, int]:
    """(reservado activo, entregado) de una línea."""
    cur.execute(
        """
        SELECT COALESCE(SUM(quantity - issued_qty) FILTER (WHERE status = 'active'), 0)::int AS remaining,
               COALESCE(SUM(issued_qty), 0)::int AS issued
        FROM reservations WHERE work_order_id = %s AND bom_line_id = %s
        """,
        (work_order_id, bom_line_id),
    )
    row = cur.fetchone()
    return row["remaining"], row["issued"]


def create_reservation(uow: UnitOfWork, work_order: dict, *, bom_line_id: int, inspection_item_id: int,
                       part_id: int, quantity: int) -> dict:
    uow.cur.execute(
        """
        INSERT INTO reservations (work_order_id, bom_line_id, inspection_item_id, part_id, quantity,
                                  created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *
        """,
        (work_order["work_order_id"], bom_line_id, inspection_item_id, part_id, quantity,
         uow.occurred_at, uow.occurred_at),
    )
    reservation = uow.cur.fetchone()
    uow.emit("stock.reserved", work_order["code"], {
        "reservation_id": reservation["reservation_id"],
        "work_order_id": work_order["work_order_id"],
        "part_id": part_id,
        "quantity": quantity,
        "inspection_item_id": inspection_item_id,
        "bom_line_id": bom_line_id,
    })
    uow.touch(part_id)
    return reservation


def shrink_line(uow: UnitOfWork, work_order_id: int, bom_line_id: int, excess: int | None = None) -> int:
    """Libera `excess` unidades reservadas de la línea (todas si es None), de la más nueva a
    la más antigua. Devuelve lo liberado."""
    cur = uow.cur
    cur.execute(
        """
        SELECT * FROM reservations
        WHERE work_order_id = %s AND bom_line_id = %s AND status = 'active'
        ORDER BY created_at DESC, reservation_id DESC FOR UPDATE
        """,
        (work_order_id, bom_line_id),
    )
    left = excess
    freed = 0
    for r in cur.fetchall():
        if left is not None and left <= 0:
            break
        remaining = r["quantity"] - r["issued_qty"]
        take = remaining if left is None else min(remaining, left)
        if take == remaining:
            # Se libera todo lo pendiente: queda cumplida por lo entregado, o cancelada.
            if r["issued_qty"] > 0:
                cur.execute(
                    "UPDATE reservations SET quantity = issued_qty, status = 'fulfilled', updated_at = %s"
                    " WHERE reservation_id = %s",
                    (uow.occurred_at, r["reservation_id"]),
                )
            else:
                cur.execute(
                    "UPDATE reservations SET status = 'cancelled', updated_at = %s WHERE reservation_id = %s",
                    (uow.occurred_at, r["reservation_id"]),
                )
        else:
            cur.execute(
                "UPDATE reservations SET quantity = quantity - %s, updated_at = %s WHERE reservation_id = %s",
                (take, uow.occurred_at, r["reservation_id"]),
            )
        freed += take
        if left is not None:
            left -= take
        uow.touch(r["part_id"])
        uow.freed_parts.add(r["part_id"])
    return freed


def consume_for_issue(uow: UnitOfWork, work_order_id: int, part_id: int, quantity: int) -> int:
    """Una salida consume primero las reservas de esa orden para esa pieza (regla 16).
    Devuelve cuánto salió de reservas."""
    cur = uow.cur
    cur.execute(
        """
        SELECT * FROM reservations
        WHERE work_order_id = %s AND part_id = %s AND status = 'active'
        ORDER BY created_at, reservation_id FOR UPDATE
        """,
        (work_order_id, part_id),
    )
    left = quantity
    for r in cur.fetchall():
        if left <= 0:
            break
        take = min(r["quantity"] - r["issued_qty"], left)
        issued = r["issued_qty"] + take
        status = "fulfilled" if issued == r["quantity"] else "active"
        cur.execute(
            "UPDATE reservations SET issued_qty = %s, status = %s, updated_at = %s WHERE reservation_id = %s",
            (issued, status, uow.occurred_at, r["reservation_id"]),
        )
        left -= take
    uow.touch(part_id)
    return quantity - left


# --- faltantes ------------------------------------------------------------------------
def _open_shortage(cur: Cursor, work_order_id: int, bom_line_id: int) -> dict | None:
    cur.execute(
        "SELECT * FROM shortages WHERE work_order_id = %s AND bom_line_id = %s AND status = 'open' FOR UPDATE",
        (work_order_id, bom_line_id),
    )
    return cur.fetchone()


def close_shortage(uow: UnitOfWork, work_order_id: int, bom_line_id: int, status: str = "cancelled") -> None:
    shortage = _open_shortage(uow.cur, work_order_id, bom_line_id)
    if shortage:
        uow.cur.execute(
            "UPDATE shortages SET status = %s, closed_at = %s WHERE shortage_id = %s",
            (status, uow.occurred_at, shortage["shortage_id"]),
        )
        uow.touch(shortage["part_id"])


def set_shortage(uow: UnitOfWork, work_order: dict, need: dict, missing: int | None) -> None:
    """Deja el faltante abierto de la línea en `missing` (None = cantidad desconocida; 0 = sin
    faltante). Emite `stock.shortage_detected` cuando nace o cambia."""
    cur = uow.cur
    wo_id, line_id = work_order["work_order_id"], need["bom_line_id"]
    current = _open_shortage(cur, wo_id, line_id)
    if missing == 0:
        if current:
            close_shortage(uow, wo_id, line_id, "cancelled")
        return

    if current:
        changed = current["missing_quantity"] != missing or current["part_id"] != need["part_id"]
        if current["part_id"] != need["part_id"]:
            uow.touch(current["part_id"])
        cur.execute(
            "UPDATE shortages SET missing_quantity = %s, part_id = %s, name = %s, inspection_item_id = %s"
            " WHERE shortage_id = %s",
            (missing, need["part_id"], need["name"], need["inspection_item_id"], current["shortage_id"]),
        )
        shortage_id = current["shortage_id"]
        if not changed:
            return
    else:
        cur.execute(
            """
            INSERT INTO shortages (work_order_id, bom_line_id, inspection_item_id, part_id, name,
                                   missing_quantity, opened_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING shortage_id
            """,
            (wo_id, line_id, need["inspection_item_id"], need["part_id"], need["name"], missing, uow.occurred_at),
        )
        shortage_id = cur.fetchone()["shortage_id"]

    uow.emit("stock.shortage_detected", work_order["code"], {
        "work_order_id": wo_id,
        "part_id": need["part_id"],
        "name": need["name"],
        "missing_quantity": missing,
        "inspection_item_id": need["inspection_item_id"],
        "bom_line_id": line_id,
        "shortage_id": shortage_id,
    })
    uow.touch(need["part_id"])


# --- necesidades ----------------------------------------------------------------------
def reconcile_line(uow: UnitOfWork, work_order: dict, need: dict) -> None:
    """Ajusta reservas y faltante de una línea a su necesidad vigente."""
    cur = uow.cur
    part_id, required = need["part_id"], need["required_qty"]
    wo_id, line_id = work_order["work_order_id"], need["bom_line_id"]

    if part_id is None:  # pieza sin identificar: faltante, nunca reserva (regla 6)
        set_shortage(uow, work_order, need, required)
        return
    if required is None:  # sin cantidad ni qty_per_unit: faltante desconocido (regla 5)
        shrink_line(uow, wo_id, line_id)
        set_shortage(uow, work_order, need, None)
        return

    remaining, issued = line_totals(cur, wo_id, line_id)
    target = max(required - issued, 0)
    if remaining > target:
        shrink_line(uow, wo_id, line_id, remaining - target)
        remaining = target
    elif remaining < target:
        got = min(max(ledger.available(cur, part_id), 0), target - remaining)
        if got > 0:
            create_reservation(uow, work_order, bom_line_id=line_id,
                               inspection_item_id=need["inspection_item_id"], part_id=part_id, quantity=got)
            remaining += got
    set_shortage(uow, work_order, need, target - remaining)


def release_line(uow: UnitOfWork, work_order_id: int, bom_line_id: int) -> None:
    """Libera todo lo reservado de la línea, cierra su faltante y desactiva la necesidad."""
    shrink_line(uow, work_order_id, bom_line_id)
    close_shortage(uow, work_order_id, bom_line_id, "cancelled")
    uow.cur.execute(
        "UPDATE needs SET active = false, updated_at = %s WHERE work_order_id = %s AND bom_line_id = %s",
        (uow.occurred_at, work_order_id, bom_line_id),
    )


def set_need(uow: UnitOfWork, work_order: dict, *, bom_line_id: int, inspection_id: int,
             inspection_item_id: int, part_id: int | None, name: str, required: int | None,
             buy: bool) -> None:
    """Fija la necesidad de una línea con lo que trae una aprobación.

    `buy=False` (repair u ok) significa que la línea ya no requiere material: si tenía una
    necesidad, se libera. Es la lectura de "reemplaza" para una línea que pasa de comprar a
    reparar entre la inspección rápida y la completa.
    """
    cur = uow.cur
    wo_id = work_order["work_order_id"]
    cur.execute("SELECT * FROM needs WHERE work_order_id = %s AND bom_line_id = %s FOR UPDATE", (wo_id, bom_line_id))
    old = cur.fetchone()

    if not buy:
        if old and old["active"]:
            release_line(uow, wo_id, bom_line_id)
        return

    if old and old["active"] and old["part_id"] != part_id:
        release_line(uow, wo_id, bom_line_id)  # cambió la pieza: lo reservado ya no aplica

    cur.execute(
        """
        INSERT INTO needs (work_order_id, bom_line_id, inspection_id, inspection_item_id, part_id, name,
                           required_qty, active, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, true, %s)
        ON CONFLICT (work_order_id, bom_line_id) DO UPDATE SET
            inspection_id = EXCLUDED.inspection_id, inspection_item_id = EXCLUDED.inspection_item_id,
            part_id = EXCLUDED.part_id, name = EXCLUDED.name, required_qty = EXCLUDED.required_qty,
            active = true, updated_at = EXCLUDED.updated_at
        RETURNING *
        """,
        (wo_id, bom_line_id, inspection_id, inspection_item_id, part_id, name, required, uow.occurred_at),
    )
    reconcile_line(uow, work_order, cur.fetchone())


# --- surtido de faltantes ---------------------------------------------------------------
def fill_shortages(uow: UnitOfWork, part_id: int, preferred_work_order_code: str | None = None) -> None:
    """Surte faltantes abiertos de la pieza con lo disponible (regla 12): primero los de la
    orden indicada (si sigue viva) y después del más antiguo al más nuevo. Cada surtido emite
    `stock.reserved` + `stock.shortage_resolved`."""
    cur = uow.cur
    available = ledger.available(cur, part_id)
    if available <= 0:
        return
    cur.execute(
        """
        SELECT s.*, w.code
        FROM shortages s JOIN work_orders w USING (work_order_id)
        WHERE s.part_id = %s AND s.status = 'open' AND s.missing_quantity IS NOT NULL
          AND w.deleted_at IS NULL
        ORDER BY COALESCE(w.code = %s, false) DESC, s.opened_at, s.shortage_id
        FOR UPDATE OF s
        """,
        (part_id, preferred_work_order_code),
    )
    for shortage in cur.fetchall():
        if available <= 0:
            break
        give = min(available, shortage["missing_quantity"])
        work_order = {"work_order_id": shortage["work_order_id"], "code": shortage["code"]}
        create_reservation(uow, work_order, bom_line_id=shortage["bom_line_id"],
                           inspection_item_id=shortage["inspection_item_id"], part_id=part_id, quantity=give)
        left = shortage["missing_quantity"] - give
        if left == 0:
            cur.execute(
                "UPDATE shortages SET missing_quantity = 0, status = 'resolved', closed_at = %s WHERE shortage_id = %s",
                (uow.occurred_at, shortage["shortage_id"]),
            )
        else:
            cur.execute("UPDATE shortages SET missing_quantity = %s WHERE shortage_id = %s",
                        (left, shortage["shortage_id"]))
        uow.emit("stock.shortage_resolved", shortage["code"], {
            "work_order_id": shortage["work_order_id"],
            "part_id": part_id,
            "quantity": give,
            "inspection_item_id": shortage["inspection_item_id"],
            "bom_line_id": shortage["bom_line_id"],
            "shortage_id": shortage["shortage_id"],
            "remaining_missing": left,
        })
        available -= give


def redistribute(uow: UnitOfWork) -> None:
    """Ofrece lo liberado en esta transacción a los faltantes que esperan esa pieza."""
    while uow.freed_parts:
        fill_shortages(uow, uow.freed_parts.pop())
