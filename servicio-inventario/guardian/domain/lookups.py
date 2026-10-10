"""Consultas de existencia compartidas por los módulos del dominio."""

from __future__ import annotations

from psycopg import Cursor


def part(cur: Cursor, part_id: int) -> dict | None:
    cur.execute("SELECT * FROM parts WHERE part_id = %s", (part_id,))
    return cur.fetchone()


def part_exists(cur: Cursor, part_id: int) -> bool:
    cur.execute("SELECT 1 FROM parts WHERE part_id = %s", (part_id,))
    return cur.fetchone() is not None


def location_exists(cur: Cursor, location_id: int) -> bool:
    cur.execute("SELECT 1 FROM locations WHERE location_id = %s", (location_id,))
    return cur.fetchone() is not None


def bom_line(cur: Cursor, bom_line_id: int) -> dict | None:
    cur.execute("SELECT * FROM bom_lines WHERE bom_line_id = %s", (bom_line_id,))
    return cur.fetchone()


def work_order_by_id(cur: Cursor, work_order_id: int) -> dict | None:
    cur.execute("SELECT * FROM work_orders WHERE work_order_id = %s", (work_order_id,))
    return cur.fetchone()


def work_order_by_code(cur: Cursor, code: str) -> dict | None:
    cur.execute("SELECT * FROM work_orders WHERE code = %s", (code,))
    return cur.fetchone()
