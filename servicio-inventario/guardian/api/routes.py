"""Rutas HTTP. Las de lectura consultan; las de escritura abren una unidad de trabajo."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from fastapi import APIRouter, Query, Request

from .. import outbox
from ..domain import ledger as ledger_domain
from ..domain import lookups, operations, receiving, suggestions
from ..domain.finish import finish
from ..errors import NotFound
from ..uow import UnitOfWork
from . import queries
from .models import CountRequest, IssueRequest, PolicyRequest, ResolveRequest, TransferRequest

router = APIRouter()
T = TypeVar("T")


def _read(request: Request, fn: Callable[..., T], *args) -> T:
    return request.app.state.guardian.db.run(lambda cur: fn(cur, *args))


def _command(request: Request, fn: Callable[[UnitOfWork], T]) -> T:
    """Ejecuta una operación en una transacción, con el cierre común y aviso al publicador."""
    def tx(cur) -> T:
        uow = UnitOfWork(cur)
        result = fn(uow)
        finish(uow)
        return result

    result = request.app.state.guardian.db.run(tx)
    outbox.wake()
    return result


# --- consultas del contrato ----------------------------------------------------------------
@router.get("/parts/{part_id}/availability")
def get_availability(request: Request, part_id: int):
    return _read(request, queries.availability, part_id)


@router.get("/parts/{part_id}/ledger")
def get_ledger(request: Request, part_id: int, limit: int = Query(500, ge=1, le=5000)):
    return _read(request, queries.ledger, part_id, limit)


@router.get("/work-orders/{code}/materials")
def get_materials(request: Request, code: str):
    return _read(request, queries.materials, code)


@router.get("/shortages")
def get_shortages(request: Request, work_order_code: str | None = None, part_id: int | None = None):
    return _read(request, queries.shortages, work_order_code, part_id)


@router.get("/reorder-suggestions")
def get_reorder_suggestions(request: Request):
    return _read(request, queries.reorder_suggestions)


@router.get("/unmatched-receipts")
def get_unmatched_receipts(request: Request):
    return _read(request, queries.unmatched_receipts)


# --- operaciones del contrato ----------------------------------------------------------------
@router.post("/unmatched-receipts/{receipt_id}/resolve")
def post_resolve(request: Request, receipt_id: str, body: ResolveRequest):
    try:
        rid = int(receipt_id)
    except ValueError:
        raise NotFound(f"no existe la recepción pendiente {receipt_id}") from None
    movement = _command(request, lambda uow: receiving.resolve_unmatched(uow, rid, body.part_id, body.location_id))
    return ledger_domain.movement_view(movement)


@router.post("/issues", status_code=201)
def post_issue(request: Request, body: IssueRequest):
    movement = _command(request, lambda uow: operations.issue(uow, **body.model_dump()))
    return ledger_domain.movement_view(movement)


@router.post("/transfers", status_code=201)
def post_transfer(request: Request, body: TransferRequest):
    movement = _command(request, lambda uow: operations.transfer(uow, **body.model_dump()))
    return {**ledger_domain.movement_view(movement), "to_location_id": movement["to_location_id"]}


@router.post("/counts", status_code=201)
def post_count(request: Request, body: CountRequest):
    adjustments = _command(request, lambda uow: operations.count(
        uow, location_id=body.location_id, reason=body.reason, lines=[l.model_dump() for l in body.lines]))
    return {"adjustments": adjustments}


# --- apoyo para el piso y la operación -----------------------------------------------------------
@router.put("/parts/{part_id}/policy")
def put_policy(request: Request, part_id: int, body: PolicyRequest):
    def apply(uow: UnitOfWork) -> dict:
        if not lookups.part_exists(uow.cur, part_id):
            raise NotFound(f"no existe la pieza {part_id}")
        return suggestions.set_policy(uow, part_id, **body.model_dump())
    return _command(request, apply)


@router.get("/scan/{code:path}")
def get_scan(request: Request, code: str):
    """Resuelve un código Code 128 escaneado a ubicación o pieza(s)."""
    return _read(request, queries.scan, code)


@router.get("/health")
def get_health(request: Request):
    guardian = request.app.state.guardian
    status = _read(request, queries.health)
    lag = guardian.consumer.lag() if guardian.consumer else None
    return {"status": "ok", "catalog_loaded": guardian.catalog_loaded, "lag": lag, **status}
