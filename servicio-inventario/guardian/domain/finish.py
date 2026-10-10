"""Cierre común de toda unidad de trabajo, antes del commit."""

from __future__ import annotations

from ..uow import UnitOfWork
from . import allocation, suggestions


def finish(uow: UnitOfWork) -> None:
    allocation.redistribute(uow)  # lo liberado surte a quien espera
    suggestions.refresh(uow)      # sugerencias de las piezas tocadas
