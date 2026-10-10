"""Errores del dominio. La API los traduce a códigos HTTP; el consumidor, a reintentos o DLQ."""

from __future__ import annotations


class GuardianError(Exception):
    """Base de los errores del servicio."""

    status_code = 500

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class NotFound(GuardianError):
    status_code = 404


class Conflict(GuardianError):
    """La operación choca con el estado actual: saldo insuficiente, ya resuelto..."""

    status_code = 409


class InvalidRequest(GuardianError):
    status_code = 422


class InvalidEvent(GuardianError):
    """El evento no cumple el contrato. No tiene caso reintentarlo: va directo a la DLQ."""


class MissingDependency(GuardianError):
    """El evento referencia algo que aún no llega (regla 20). Se encola y se reintenta.

    `waiting_for` identifica lo que falta, ej. "part:412" o "work_order:1203".
    """

    def __init__(self, waiting_for: str) -> None:
        super().__init__(f"falta {waiting_for}")
        self.waiting_for = waiting_for
