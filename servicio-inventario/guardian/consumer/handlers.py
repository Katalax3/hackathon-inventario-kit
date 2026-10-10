"""Qué función del dominio atiende cada tipo de evento."""

from __future__ import annotations

from collections.abc import Callable

from ..domain import catalog, operations, receiving, workshop
from ..uow import UnitOfWork

Handler = Callable[[UnitOfWork, dict], None]

# Eventos del taller (shop.*).
INPUT_HANDLERS: dict[str, Handler] = {
    "part.upserted": catalog.upsert_part,
    "location.upserted": catalog.upsert_location,
    "bom.upserted": catalog.upsert_bom,
    "work_order.opened": workshop.work_order_opened,
    "work_order.stage_changed": workshop.work_order_stage_changed,
    "work_order.deleted": workshop.work_order_deleted,
    "inspection.submitted": workshop.inspection_submitted,
    "inspection.approved": workshop.inspection_approved,
    "inspection.rejected": workshop.inspection_rejected,
    "inspection.discarded": workshop.inspection_discarded,
    "inspection.voided": workshop.inspection_voided,
    "purchase.item_received": receiving.purchase_received,
}

# Operaciones de la API registradas en inventory.events, para reconstruir.
JOURNAL_HANDLERS: dict[str, Handler] = {
    "stock.issued": operations.replay_issue,
    "stock.transferred": operations.replay_transfer,
    "stock.adjusted": operations.replay_adjustment,
    "stock.received": receiving.replay_resolution,
}

def provides(event: dict) -> list[str]:
    """Qué dependencias satisface un evento ya aplicado, con la misma forma que
    `MissingDependency.waiting_for`. Sirve para reintentar solo lo que destraba."""
    data, event_type = event["data"], event["event_type"]
    if event_type == "part.upserted":
        return [f"part:{data['part_id']}"]
    if event_type == "location.upserted":
        return [f"location:{data['location_id']}"]
    if event_type == "bom.upserted":
        return [f"bom_line:{line['bom_line_id']}" for line in data["lines"]]
    if event_type == "work_order.opened":
        return [f"work_order:{data['work_order_id']}"]
    if event_type == "purchase.item_received":
        return [f"purchase_line:{data['line_id']}"]
    return []
