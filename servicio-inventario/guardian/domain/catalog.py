"""Catálogo: piezas, ubicaciones y listas de partes (tópico shop.catalog)."""

from __future__ import annotations

from ..uow import UnitOfWork
from . import receiving
from .matching import compact


def upsert_part(uow: UnitOfWork, data: dict) -> None:
    cur = uow.cur
    cur.execute("SELECT sku FROM parts WHERE part_id = %s", (data["part_id"],))
    previous = cur.fetchone()
    unit = data.get("unit") or {}
    cur.execute(
        """
        INSERT INTO parts (part_id, sku, sku_compact, name, family, "group", subgroup,
                           unit_id, unit_name, image_url, active, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (part_id) DO UPDATE SET
            sku = EXCLUDED.sku, sku_compact = EXCLUDED.sku_compact, name = EXCLUDED.name,
            family = EXCLUDED.family, "group" = EXCLUDED."group", subgroup = EXCLUDED.subgroup,
            unit_id = EXCLUDED.unit_id, unit_name = EXCLUDED.unit_name,
            image_url = EXCLUDED.image_url, active = EXCLUDED.active, updated_at = EXCLUDED.updated_at
        """,
        (data["part_id"], data["sku"], compact(data["sku"]), data["name"], data.get("family"),
         data.get("group"), data.get("subgroup"), unit.get("id"), unit.get("name"),
         data.get("image_url"), data["active"], uow.occurred_at),
    )
    # Pieza nueva o sku nuevo: puede relacionar recepciones que esperaban revisión.
    # Así, una compra que llegó antes que el sku de su pieza entra igual que al reconstruir.
    if previous is None or previous["sku"] != data["sku"]:
        receiving.auto_resolve_for_part(uow, data["part_id"], data["sku"])


def upsert_location(uow: UnitOfWork, data: dict) -> None:
    uow.cur.execute(
        """
        INSERT INTO locations (location_id, code, name, is_workbench, active, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id) DO UPDATE SET
            code = EXCLUDED.code, name = EXCLUDED.name, is_workbench = EXCLUDED.is_workbench,
            active = EXCLUDED.active, updated_at = EXCLUDED.updated_at
        """,
        (data["location_id"], data["code"], data["name"], data["is_workbench"], data["active"], uow.occurred_at),
    )


def upsert_bom(uow: UnitOfWork, data: dict) -> None:
    """La lista llega completa y reemplaza a la anterior. Las líneas que ya no vienen se
    desactivan (no se borran: necesidades existentes pueden apuntar a ellas)."""
    cur = uow.cur
    cur.execute(
        """
        INSERT INTO models (model_id, model_name, brand, updated_at) VALUES (%s, %s, %s, %s)
        ON CONFLICT (model_id) DO UPDATE SET
            model_name = EXCLUDED.model_name, brand = EXCLUDED.brand, updated_at = EXCLUDED.updated_at
        """,
        (data["model_id"], data["model_name"], data["brand"], uow.occurred_at),
    )
    line_ids = []
    for line in data["lines"]:
        unit = line.get("unit") or {}
        cur.execute(
            """
            INSERT INTO bom_lines (bom_line_id, model_id, part_id, name, group_name, unit,
                                   qty_per_unit, sort_order, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true)
            ON CONFLICT (bom_line_id) DO UPDATE SET
                model_id = EXCLUDED.model_id, part_id = EXCLUDED.part_id, name = EXCLUDED.name,
                group_name = EXCLUDED.group_name, unit = EXCLUDED.unit,
                qty_per_unit = EXCLUDED.qty_per_unit, sort_order = EXCLUDED.sort_order, active = true
            """,
            (line["bom_line_id"], data["model_id"], line["part_id"], line["name"], line["group_name"],
             unit.get("name"), line["qty_per_unit"], line["sort_order"]),
        )
        line_ids.append(line["bom_line_id"])
    cur.execute(
        "UPDATE bom_lines SET active = false WHERE model_id = %s AND active AND NOT (bom_line_id = ANY(%s))",
        (data["model_id"], line_ids),
    )
