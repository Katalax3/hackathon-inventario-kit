"""Fechas en UTC con el formato del contrato: ISO 8601 terminado en Z, a segundos."""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value: datetime | None) -> str | None:
    """A segundos: así el texto ordena igual que la fecha (sin mezclar '05Z' con '05.3Z')."""
    if value is None:
        return None
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
