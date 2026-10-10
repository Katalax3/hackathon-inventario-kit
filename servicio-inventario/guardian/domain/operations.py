"""Operaciones de piso que llegan por la API: salidas, transferencias y conteos.

Cada una emite su evento con `journal=True`: queda en inventory.events como registro
durable y, al reconstruir desde cero, `replay_*` la vuelve a aplicar (ver consumer/journal.py).
"""

from __future__ import annotations

import uuid

from ..errors import Conflict, InvalidRequest, MissingDependency, NotFound
from ..timeutil import iso, parse_iso
from ..uow import UnitOfWork
from . import allocation, ledger, lookups


def _require_part(cur, part_id: int) -> None:
    if not lookups.part_exists(cur, part_id):
        raise NotFound(f"no existe la pieza {part_id}")


def _require_location(cur, location_id: int) -> None:
    if not lookups.location_exists(cur, location_id):
        raise NotFound(f"no existe la ubicación {location_id}")


# --- salida ------------------------------------------------------------------------------
def issue(uow: UnitOfWork, *, work_order_code: str, part_id: int, location_id: int, quantity: int,
          issued_by: int | str) -> dict:
    """Baja el saldo de la ubicación y consume primero la reserva de la orden (regla 16).

    409 si la ubicación no tiene la cantidad, o si lo que no cubre la reserva de la orden
    excede lo disponible: tomarlo dejaría a otras órdenes sin lo que tienen apartado.
    """
    cur = uow.cur
    work_order = lookups.work_order_by_code(cur, work_order_code)
    if work_order is None:
        raise NotFound(f"no existe la orden {work_order_code}")
    _require_part(cur, part_id)
    _require_location(cur, location_id)
    if work_order["deleted_at"] is not None:
        raise Conflict(f"la orden {work_order_code} está dada de baja")

    ledger.lock_parts(cur, [part_id])
    at_location = ledger.on_hand(cur, part_id, location_id)
    if at_location < quantity:
        raise Conflict(f"no alcanza el saldo en la ubicación {location_id}: hay {at_location}, se piden {quantity}")
    cur.execute(
        "SELECT COALESCE(SUM(quantity - issued_qty), 0)::int AS q FROM reservations"
        " WHERE work_order_id = %s AND part_id = %s AND status = 'active'",
        (work_order["work_order_id"], part_id),
    )
    own_reserved = cur.fetchone()["q"]
    unreserved_needed = max(quantity - own_reserved, 0)
    free = ledger.available(cur, part_id)
    if unreserved_needed > free:
        raise Conflict(f"la orden tiene {own_reserved} reservadas y solo hay {max(free, 0)} disponibles;"
                       f" el resto está apartado para otras órdenes")

    movement = ledger.record(cur, part_id=part_id, location_id=location_id, quantity=-quantity, type="issue",
                             occurred_at=uow.occurred_at, reference=work_order_code,
                             work_order_id=work_order["work_order_id"], issued_by=str(issued_by))
    allocation.consume_for_issue(uow, work_order["work_order_id"], part_id, quantity)
    uow.emit("stock.issued", work_order_code, {
        "work_order_id": work_order["work_order_id"],
        "part_id": part_id,
        "location_id": location_id,
        "quantity": quantity,
        "issued_by": issued_by,
        "movement_id": movement["movement_id"],
        "occurred_at": iso(movement["occurred_at"]),
    }, journal=True)
    return movement


# --- transferencia -------------------------------------------------------------------------
def transfer(uow: UnitOfWork, *, part_id: int, from_location_id: int, to_location_id: int,
             quantity: int) -> dict:
    cur = uow.cur
    if from_location_id == to_location_id:
        raise InvalidRequest("el origen y el destino son la misma ubicación")
    _require_part(cur, part_id)
    _require_location(cur, from_location_id)
    _require_location(cur, to_location_id)

    ledger.lock_parts(cur, [part_id])
    at_origin = ledger.on_hand(cur, part_id, from_location_id)
    if at_origin < quantity:
        raise Conflict(f"no alcanza el saldo en la ubicación {from_location_id}: hay {at_origin}, se piden {quantity}")

    transfer_id = str(uuid.uuid4())
    out = _transfer_movements(uow, part_id, from_location_id, to_location_id, quantity, transfer_id, uow.occurred_at)
    uow.emit("stock.transferred", f"part:{part_id}", {
        "part_id": part_id,
        "from_location_id": from_location_id,
        "to_location_id": to_location_id,
        "quantity": quantity,
        "transfer_id": transfer_id,
        "occurred_at": iso(out["occurred_at"]),
    }, journal=True)
    return {**out, "to_location_id": to_location_id}


def _transfer_movements(uow: UnitOfWork, part_id: int, from_id: int, to_id: int, quantity: int,
                        transfer_id: str, occurred_at) -> dict:
    reference = f"transferencia {transfer_id[:8]}"
    out = ledger.record(uow.cur, part_id=part_id, location_id=from_id, quantity=-quantity, type="transfer_out",
                        occurred_at=occurred_at, reference=reference, transfer_id=transfer_id)
    ledger.record(uow.cur, part_id=part_id, location_id=to_id, quantity=quantity, type="transfer_in",
                  occurred_at=occurred_at, reference=reference, transfer_id=transfer_id)
    return out


# --- conteo cíclico ---------------------------------------------------------------------------
def count(uow: UnitOfWork, *, location_id: int, reason: str, lines: list[dict]) -> list[dict]:
    """Un ajuste por cada diferencia contra el saldo (regla 17). Única vía para dejar
    `available` negativo. Una pieza que no se lista no se toca."""
    cur = uow.cur
    reason = (reason or "").strip()
    if not reason:
        raise InvalidRequest("el motivo del conteo es obligatorio")
    part_ids = [line["part_id"] for line in lines]
    if len(set(part_ids)) != len(part_ids):
        raise InvalidRequest("una pieza aparece más de una vez en el conteo")
    _require_location(cur, location_id)
    for part_id in part_ids:
        _require_part(cur, part_id)

    ledger.lock_parts(cur, part_ids)
    adjustments = []
    for line in lines:
        part_id = line["part_id"]
        delta = line["counted_quantity"] - ledger.on_hand(cur, part_id, location_id)
        if delta == 0:
            continue
        movement = ledger.record(cur, part_id=part_id, location_id=location_id, quantity=delta, type="adjustment",
                                 occurred_at=uow.occurred_at, reference=reason)
        uow.emit("stock.adjusted", f"part:{part_id}", {
            "part_id": part_id,
            "location_id": location_id,
            "delta": delta,
            "reason": reason,
            "counted_quantity": line["counted_quantity"],
            "movement_id": movement["movement_id"],
            "occurred_at": iso(movement["occurred_at"]),
        }, journal=True)
        uow.touch(part_id)
        if delta > 0:
            uow.freed_parts.add(part_id)  # apareció material: puede surtir faltantes
        adjustments.append({"part_id": part_id, "delta": delta})
    allocation.redistribute(uow)
    return adjustments


# --- reconstrucción: reaplicar lo registrado en inventory.events ------------------------------
def _require_known(cur, part_id: int, *location_ids: int) -> None:
    if not lookups.part_exists(cur, part_id):
        raise MissingDependency(f"part:{part_id}")
    for location_id in location_ids:
        if not lookups.location_exists(cur, location_id):
            raise MissingDependency(f"location:{location_id}")


def replay_issue(uow: UnitOfWork, data: dict) -> None:
    cur = uow.cur
    _require_known(cur, data["part_id"], data["location_id"])
    ledger.lock_parts(cur, [data["part_id"]])
    occurred_at = parse_iso(data.get("occurred_at") or iso(uow.occurred_at))
    work_order = lookups.work_order_by_id(cur, data["work_order_id"])
    ledger.record(cur, part_id=data["part_id"], location_id=data["location_id"], quantity=-data["quantity"],
                  type="issue", occurred_at=occurred_at, reference=work_order["code"] if work_order else None,
                  work_order_id=data["work_order_id"], issued_by=str(data["issued_by"]),
                  source_event_id=uow.source_event_id)
    allocation.consume_for_issue(uow, data["work_order_id"], data["part_id"], data["quantity"])


def replay_transfer(uow: UnitOfWork, data: dict) -> None:
    _require_known(uow.cur, data["part_id"], data["from_location_id"], data["to_location_id"])
    ledger.lock_parts(uow.cur, [data["part_id"]])
    occurred_at = parse_iso(data.get("occurred_at") or iso(uow.occurred_at))
    _transfer_movements(uow, data["part_id"], data["from_location_id"], data["to_location_id"], data["quantity"],
                        data.get("transfer_id") or str(uuid.uuid4()), occurred_at)


def replay_adjustment(uow: UnitOfWork, data: dict) -> None:
    _require_known(uow.cur, data["part_id"], data["location_id"])
    ledger.lock_parts(uow.cur, [data["part_id"]])
    occurred_at = parse_iso(data.get("occurred_at") or iso(uow.occurred_at))
    ledger.record(uow.cur, part_id=data["part_id"], location_id=data["location_id"], quantity=data["delta"],
                  type="adjustment", occurred_at=occurred_at, reference=data["reason"],
                  source_event_id=uow.source_event_id)
    uow.touch(data["part_id"])
