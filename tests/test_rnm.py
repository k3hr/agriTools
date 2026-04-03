"""
Tests unitaires — ingestion RNM.
"""
import io
import pytest
import polars as pl

from ingestion.prix.rnm import (
    _detect_separator,
    _detect_encoding,
    _extract_year,
    _resolve_column,
    normalize,
    parse_csv,
    apply_filters,
    COL_ALIASES,
    TARGET_SCHEMA,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _make_csv(sep: str, columns: list[str], rows: list[list]) -> bytes:
    header = sep.join(columns)
    lines = [header] + [sep.join(str(v) for v in row) for row in rows]
    return "\n".join(lines).encode("utf-8")


SAMPLE_CSV_SEMICOLON = _make_csv(
    ";",
    ["annee", "semaine", "produit_libelle", "marche_libelle", "stade_libelle",
     "categorie", "calibre", "variete", "origine", "unite",
     "prix_min", "prix_max", "prix_moyen"],
    [
        [2023, 1, "TOMATE RONDE", "RUNGIS", "GROS", "I", "57/67", "ROUGE", "FRANCE", "KG", "0.80", "1.20", "1.00"],
        [2023, 1, "COURGETTE", "NANTES SAINT LUCE", "GROS", "I", "15/20", "", "FRANCE", "KG", "0.60", "0.90", "0.75"],
        [2023, 2, "CAROTTE", "RUNGIS", "GROS", "I", "30/40", "", "FRANCE", "KG", "0.40", "0.70", "0.55"],
    ],
)

SAMPLE_CSV_COMMA_OLD_NAMES = _make_csv(
    ",",
    ["Annee", "Semaine", "Produit", "Marche", "Stade",
     "Categorie", "Calibre", "Variete", "Origine", "Unite",
     "Prix Min", "Prix Max", "Prix Moyen"],
    [
        [2022, 5, "SALADE BATAVIA", "RUNGIS", "GROS", "I", "", "", "FRANCE", "PIECE", "0.30", "0.60", "0.45"],
    ],
)


# ---------------------------------------------------------------------------
class TestDetectSeparator:
    def test_semicolon(self):
        assert _detect_separator(b"a;b;c\n1;2;3") == ";"

    def test_comma(self):
        assert _detect_separator(b"a,b,c\n1,2,3") == ","

    def test_prefers_semicolon_when_equal_or_more(self):
        assert _detect_separator(b"a;b,c\n1;2,3") == ";"


class TestDetectEncoding:
    def test_utf8(self):
        assert _detect_encoding("héllo".encode("utf-8")) == "utf-8"

    def test_latin1(self):
        assert _detect_encoding("héllo".encode("latin-1")) == "latin-1"


class TestExtractYear:
    def test_in_title(self):
        assert _extract_year("Cotations RNM 2023") == 2023

    def test_in_url(self):
        assert _extract_year("https://example.com/rnm_2022.csv") == 2022

    def test_not_found(self):
        assert _extract_year("cotations RNM") is None


class TestResolveColumn:
    def test_exact_match(self):
        assert _resolve_column(["produit_libelle", "marche"], "produit") == "produit_libelle"

    def test_old_name(self):
        assert _resolve_column(["Produit", "Marche"], "produit") == "Produit"

    def test_not_found(self):
        assert _resolve_column(["foo", "bar"], "produit") is None

    def test_case_insensitive(self):
        assert _resolve_column(["PRODUIT_LIBELLE"], "produit") == "PRODUIT_LIBELLE"


class TestNormalize:
    def test_target_columns_present(self):
        df = parse_csv(SAMPLE_CSV_SEMICOLON, 2023)
        assert set(TARGET_SCHEMA.keys()).issubset(set(df.columns))

    def test_old_column_names_resolved(self):
        df = parse_csv(SAMPLE_CSV_COMMA_OLD_NAMES, 2022)
        assert "produit" in df.columns
        assert "marche" in df.columns
        assert "prix_min" in df.columns

    def test_price_as_float(self):
        df = parse_csv(SAMPLE_CSV_SEMICOLON, 2023)
        assert df["prix_min"].dtype == pl.Float64
        assert df["prix_moyen"][0] == pytest.approx(1.0)

    def test_row_count(self):
        df = parse_csv(SAMPLE_CSV_SEMICOLON, 2023)
        assert len(df) == 3

    def test_year_injected_when_missing(self):
        # Crée un CSV sans colonne annee
        csv = _make_csv(
            ";",
            ["produit_libelle", "marche_libelle", "stade_libelle", "unite", "prix_min", "prix_max", "prix_moyen"],
            [["TOMATE", "RUNGIS", "GROS", "KG", "0.8", "1.2", "1.0"]],
        )
        df = parse_csv(csv, 2021)
        assert df["annee"][0] == 2021

    def test_comma_decimal_parsed(self):
        csv = _make_csv(
            ";",
            ["annee", "produit_libelle", "marche_libelle", "stade_libelle",
             "unite", "prix_min", "prix_max", "prix_moyen"],
            [[2023, "TOMATE", "RUNGIS", "GROS", "KG", "0,80", "1,20", "1,00"]],
        )
        df = parse_csv(csv, 2023)
        assert df["prix_moyen"][0] == pytest.approx(1.0)


class TestApplyFilters:
    def setup_method(self):
        self.df = parse_csv(SAMPLE_CSV_SEMICOLON, 2023)

    def test_filter_marche(self):
        filtered = apply_filters(self.df, marches=["RUNGIS"])
        assert all(r == "RUNGIS" for r in filtered["marche"].to_list())

    def test_filter_marche_case_insensitive(self):
        filtered = apply_filters(self.df, marches=["rungis"])
        assert len(filtered) > 0

    def test_filter_stade(self):
        filtered = apply_filters(self.df, stades=["GROS"])
        assert len(filtered) == len(self.df)  # tout est GROS dans le sample

    def test_no_filter_keeps_all(self):
        filtered = apply_filters(self.df)
        assert len(filtered) == len(self.df)

    def test_unknown_marche_returns_empty(self):
        filtered = apply_filters(self.df, marches=["MARCHE_INCONNU"])
        assert len(filtered) == 0
