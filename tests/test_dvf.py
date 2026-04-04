"""
Tests unitaires — ingestion DVF.
"""

import pytest
import polars as pl

from ingestion.prix.dvf import (
    _detect_encoding,
    _detect_separator,
    _extract_quarter,
    _extract_year,
    _resolve_column,
    apply_filters,
    parse_csv,
)


def _make_csv(sep: str, columns: list[str], rows: list[list]) -> bytes:
    header = sep.join(columns)
    lines = [header] + [sep.join(str(v) for v in row) for row in rows]
    return "\n".join(lines).encode("utf-8")


class TestDetectSeparator:
    def test_semicolon(self):
        assert _detect_separator(b"a;b;c\n1;2;3") == ";"

    def test_comma(self):
        assert _detect_separator(b"a,b,c\n1,2,3") == ","


class TestExtractYearAndQuarter:
    def test_extract_year_from_string(self):
        assert _extract_year("DVF 2024 trimestre 1") == 2024

    def test_extract_quarter_from_string(self):
        assert _extract_quarter("DVF 2024 T2") == 2
        assert _extract_quarter("DVF 2024 2ème trimestre") == 2
        assert _extract_quarter("DVF 2024 Q3") == 3


class TestResolveColumn:
    def test_alias_match(self):
        assert _resolve_column(["DATE_MUTATION", "VALEUR_FONCIERE"], "date_mutation") == "DATE_MUTATION"
        assert _resolve_column(["code_departement"], "code_departement") == "code_departement"


class TestParseCSV:
    def test_parse_csv_normalizes_fields(self):
        csv = _make_csv(
            ";",
            ["date_mutation", "valeur_fonciere", "code_departement", "latitude", "longitude"],
            [["2024-03-01", "125000", "72", "47.8", "-0.9"]],
        )
        df = parse_csv(csv, year=2024, trimestre=1)
        assert df["annee"][0] == 2024
        assert df["trimestre"][0] == 1
        assert pytest.approx(df["valeur_fonciere"][0]) == 125000.0
        assert df["code_departement"][0] == "72"

    def test_parse_csv_accepts_comma_decimals(self):
        csv = _make_csv(
            ";",
            ["date_mutation", "valeur_fonciere", "code_departement"],
            [["2024-03-01", "125,000", "72"]],
        )
        df = parse_csv(csv, year=2024, trimestre=1)
        assert pytest.approx(df["valeur_fonciere"][0]) == 125000.0


class TestApplyFilters:
    def setup_method(self):
        self.df = pl.DataFrame(
            {
                "annee": [2024, 2024],
                "code_departement": ["72", "53"],
                "latitude": [47.85, 48.1],
                "longitude": [-0.94, 0.1],
            }
        )

    def test_filter_departement(self):
        result = apply_filters(self.df, departements=["72"])
        assert len(result) == 1
        assert result["code_departement"][0] == "72"

    def test_filter_bbox(self):
        result = apply_filters(self.df, departements=None, lat=47.85, lon=-0.94, rayon_km=10)
        assert len(result) == 1
        assert result["longitude"][0] == pytest.approx(-0.94)
