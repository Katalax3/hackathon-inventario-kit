"""Reglas de relación número de parte → pieza que no necesitan base de datos.

    .venv/bin/python -m pytest tests
"""

import pytest

from guardian.domain.matching import _NP_PREFIX, compact, could_match


@pytest.mark.parametrize("written, sku", [
    ("  MN70 - 2 - 88 ", "MN70-2-88"),
    ("KO007", "KO-007"),
    ("000613324", "000.613.324"),
    ("3/8X246G10L/12L", "3/8X24 6G-10L/12L"),
    ("BORNERA#33", "BORNERA #33"),
])
def test_compact_iguala_escrituras_del_dataset(written, sku):
    assert compact(written) == compact(sku)


def test_compact_vacio_es_none():
    assert compact(" - / ") is None
    assert compact(None) is None


@pytest.mark.parametrize("text, expected", [
    ("NP: 000.606.452", "000.606.452"),
    ("np:RRS007-D", "RRS007-D"),
    ("N.P.: TOPE-004", "TOPE-004"),
    ("PROV-15211", "PROV-15211"),
    ("S/N FLECHA", "S/N FLECHA"),
])
def test_prefijo_np(text, expected):
    assert _NP_PREFIX.sub("", text) == expected


def test_could_match_por_sku_exacto_compacto_y_descripcion():
    assert could_match("FLECHA-923", 23, "  flecha-923 ", None)
    assert could_match("FLECHA-923", 23, "NP: FLECHA 923", None)
    assert could_match("SEPARADOR5-004", 98, "PROV-99888", "SEPARADOR5-004")
    assert not could_match("FLECHA-923", 23, "PROV-1", "otra cosa")


def test_could_match_por_etiqueta_de_pieza_sin_sku():
    assert could_match(None, 24, "P-24", None)
    assert not could_match(None, 24, "P-25", None)
