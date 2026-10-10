"""Relación de un número de parte de compra con una pieza del catálogo (regla 10).

Se prueban reglas de la más estricta a la más flexible. En cada regla:
  - una sola pieza  → coincide;
  - varias piezas   → ambiguo: se detiene y va a revisión manual (nunca se adivina);
  - ninguna         → se prueba la siguiente regla.

Reglas, observadas en las compras reales del dataset:
  1. exacto        sku igual ignorando mayúsculas y espacios al inicio y al final.
  2. prefijo NP    "NP: 000.606.452" → "000.606.452".
  3. etiqueta      "P-24" → pieza 24 (formato de etiqueta de piezas sin sku, §5 del contrato).
  4. compacto      sin espacios ni signos: "  MN70 - 2 - 88 " ↔ "MN70-2-88", "KO007" ↔ "KO-007".
  5. descripción   la descripción es el sku exacto (compras con número de proveedor
                   "PROV-99888" y descripción "SEPARADOR5-004").

Se buscan todas las piezas, también las inactivas: así reconstruir con el catálogo final
relaciona igual que en vivo.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from psycopg import Cursor

_NP_PREFIX = re.compile(r"^\s*N\.?\s*P\.?\s*[:#]\s*", re.IGNORECASE)
_LABEL = re.compile(r"^\s*P-(\d+)\s*$", re.IGNORECASE)
_NON_ALNUM = re.compile(r"[\W_]+", re.UNICODE)


def compact(value: str | None) -> str | None:
    """Mayúsculas sin espacios ni signos. None si no queda nada."""
    if value is None:
        return None
    result = _NON_ALNUM.sub("", value.upper())
    return result or None


@dataclass
class MatchResult:
    part_id: int | None
    rule: str | None = None
    candidates: list[int] = field(default_factory=list)

    @property
    def ambiguous(self) -> bool:
        return self.part_id is None and len(self.candidates) > 1


def _exact(cur: Cursor, text: str) -> list[int]:
    if not text.strip():
        return []
    cur.execute("SELECT part_id FROM parts WHERE sku_norm = upper(btrim(%s)) ORDER BY part_id", (text,))
    return [r["part_id"] for r in cur.fetchall()]


def _compact(cur: Cursor, text: str) -> list[int]:
    key = compact(text)
    if key is None:
        return []
    cur.execute("SELECT part_id FROM parts WHERE sku_compact = %s ORDER BY part_id", (key,))
    return [r["part_id"] for r in cur.fetchall()]


def _label(cur: Cursor, text: str) -> list[int]:
    m = _LABEL.match(text)
    if not m:
        return []
    cur.execute("SELECT part_id FROM parts WHERE part_id = %s", (int(m.group(1)),))
    return [r["part_id"] for r in cur.fetchall()]


def match_part(cur: Cursor, part_number: str, description: str | None = None) -> MatchResult:
    stripped = _NP_PREFIX.sub("", part_number)
    rules = [("exact", _exact, part_number)]
    if stripped != part_number:
        rules.append(("np_prefix", _exact, stripped))
    rules += [("label", _label, stripped), ("compact", _compact, stripped)]
    if description:
        rules.append(("description", _exact, description))

    for name, finder, text in rules:
        found = finder(cur, text)
        if len(found) == 1:
            return MatchResult(found[0], name, found)
        if len(found) > 1:
            return MatchResult(None, name, found)
    return MatchResult(None)


def could_match(part_sku: str | None, part_id: int, part_number: str, description: str | None) -> bool:
    """Filtro rápido, sin base: ¿alguna regla podría relacionar esta compra con esta pieza?"""
    stripped = _NP_PREFIX.sub("", part_number)
    texts = [part_number, stripped] + ([description] if description else [])
    m = _LABEL.match(stripped)
    if m and int(m.group(1)) == part_id:
        return True
    if not part_sku:
        return False
    sku_norm, sku_compact = part_sku.strip().upper(), compact(part_sku)
    if any(t.strip().upper() == sku_norm for t in texts):
        return True
    return sku_compact is not None and compact(stripped) == sku_compact
