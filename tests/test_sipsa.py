"""Tests for SIPSA import helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from pyimport.main import main
from pyimport.sipsa import (
    Period,
    candidate_urls,
    extract_food_rows,
    parse_period_arg,
    parse_period_from_filename,
    write_csv,
)

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
LEGACY_SAMPLE = ROOT / "data" / "anex_mensual_dic_2016.xls"


def test_parse_period_from_filename_legacy() -> None:
    period = parse_period_from_filename("anex_mensual_dic_2016.xls")
    assert period == Period(year=2016, month=12)
    assert period.month_str == "12"
    assert period.month_abbr == "dic"


def test_parse_period_from_filename_mayoristas() -> None:
    period = parse_period_from_filename("anexo_mensual_SIPSA_mayoristas_dic_2021.xlsx")
    assert period == Period(2021, 12)


def test_parse_period_from_filename_operaciones() -> None:
    period = parse_period_from_filename("anex-SIPSAMensual-dic2024.xlsx")
    assert period == Period(2024, 12)
    assert period.month_str == "12"


def test_month_mapping_january_is_01() -> None:
    period = parse_period_from_filename("anex_mensual_ene_2017.xlsx")
    assert period.month_str == "01"


def test_candidate_urls_include_all_schemes() -> None:
    urls = candidate_urls(Period(2021, 12))
    assert urls[0].endswith("anex_mensual_dic_2021.xls")
    assert urls[1].endswith("anex_mensual_dic_2021.xlsx")
    assert urls[2].endswith("anexo_mensual_SIPSA_mayoristas_dic_2021.xlsx")
    assert urls[3].endswith("anex-SIPSAMensual-dic2024.xlsx".replace("2024", "2021"))
    assert "operaciones/SIPSA" in urls[3]


def test_parse_period_arg() -> None:
    assert parse_period_arg("2016-12") == Period(2016, 12)
    with pytest.raises(ValueError):
        parse_period_arg("2016")


def test_extract_from_sample_xls() -> None:
    sample = LEGACY_SAMPLE if LEGACY_SAMPLE.exists() else RAW / "anex_mensual_dic_2016.xls"
    if not sample.exists():
        pytest.skip("sample Excel not present")
    rows = extract_food_rows(sample)
    assert rows
    assert rows[0].year == "2016"
    assert rows[0].month == "12"
    assert rows[0].alimento == "Ahuyama"
    assert rows[0].valor == "614"
    names = {r.alimento for r in rows}
    assert "Hortalizas y verduras" not in names
    assert "Fuente: DANE" not in names


def test_extract_from_mayoristas_xlsx() -> None:
    sample = RAW / "anexo_mensual_SIPSA_mayoristas_dic_2021.xlsx"
    if not sample.exists():
        pytest.skip("mayoristas sample not present")
    rows = extract_food_rows(sample)
    assert rows
    assert rows[0].year == "2021"
    assert rows[0].month == "12"
    assert rows[0].alimento == "Ahuyama"
    assert rows[0].valor == "1237"


def test_extract_from_operaciones_xlsx_uses_sheet_1() -> None:
    sample = RAW / "anex-SIPSAMensual-dic2024.xlsx"
    if not sample.exists():
        pytest.skip("operaciones sample not present")
    rows = extract_food_rows(sample)
    assert rows
    assert rows[0].year == "2024"
    assert rows[0].month == "12"
    assert rows[0].alimento == "Ahuyama"
    assert rows[0].valor == "1241"


def test_write_csv(tmp_path: Path) -> None:
    sample = LEGACY_SAMPLE if LEGACY_SAMPLE.exists() else RAW / "anex_mensual_dic_2016.xls"
    if not sample.exists():
        pytest.skip("sample Excel not present")
    rows = extract_food_rows(sample)
    out = tmp_path / "out.csv"
    count = write_csv(rows, out)
    assert count == len(rows)
    text = out.read_text(encoding="utf-8")
    assert text.startswith("anio,mes,alimento,valor\n")
    assert "2016,12,Ahuyama,614\n" in text


def test_main_help_returns_zero() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
