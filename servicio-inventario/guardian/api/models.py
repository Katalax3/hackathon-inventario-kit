"""Cuerpos de las peticiones."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class IssueRequest(BaseModel):
    work_order_code: str = Field(min_length=1)
    part_id: int
    location_id: int
    quantity: int = Field(ge=1)
    issued_by: int | str

    @model_validator(mode="after")
    def _issued_by_not_empty(self) -> "IssueRequest":
        if isinstance(self.issued_by, str) and not self.issued_by.strip():
            raise ValueError("issued_by no puede estar vacío")
        return self


class TransferRequest(BaseModel):
    part_id: int
    from_location_id: int
    to_location_id: int
    quantity: int = Field(ge=1)


class CountLine(BaseModel):
    part_id: int
    counted_quantity: int = Field(ge=0)


class CountRequest(BaseModel):
    location_id: int
    reason: str
    lines: list[CountLine]


class ResolveRequest(BaseModel):
    part_id: int
    location_id: int | None = None


class PolicyRequest(BaseModel):
    min_qty: int = Field(default=0, ge=0)
    max_qty: int | None = Field(default=None, ge=0)
    reorder_qty: int | None = Field(default=None, ge=1)
    lead_time_days: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _max_over_min(self) -> "PolicyRequest":
        if self.max_qty is not None and self.max_qty < self.min_qty:
            raise ValueError("max_qty debe ser mayor o igual a min_qty")
        return self
