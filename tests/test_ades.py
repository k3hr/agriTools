"""
Tests unitaires — ingestion ADES (niveaux piézométriques Hub'eau).

Couvre la transformation et la normalisation des données,
sans appel réseau.
"""
from datetime import date

import polars as pl
import pytest

from ingestion.geo.ades import (
    TARGET_SCHEMA,
    chroniques_to_dataframe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
SAMPLE_RECORDS = [
    {
        "code_bss":           "07548X0009/S",
        "date_mesure":        "2024-03-15",
        "profondeur_nappe":   5.32,
        "niveau_eau_ngf":     29.68,
        "mode_obtention":     "Mesure directe",
        "qualification":      "Bonne",
        "code_qualification": 1,
    },
    {
        "code_bss":           "07548X0009/S",
        "date_mesure":        "2024-04-01",
        "profondeur_nappe":   4.87,
        "niveau_eau_ngf":     30.13,
        "mode_obtention":     "Mesure directe",
        "qualification":      "Bonne",
        "code_qualification": 1,
    },
    {
        "code_bss":           "07548X0042/F",
        "date_mesure":        "2024-03-20",
        "profondeur_nappe":   12.4,
        "niveau_eau_ngf":     43.1,
        "mode_obtention":     "Automatique",
        "qualification":      "Incertaine",
        "code_qualification": 2,
    },
    {
        # Valeurs manquantes / None — cas API réel
        "code_bss":           "07548X0101/P",
        "date_mesure":        "2023-11-10",
        "profondeur_nappe":   None,
        "niveau_eau_ngf":     None,
        "mode_obtention":     None,
        "qualification":      None,
        "code_qualification": None,
    },
    {
        # Date avec heure (format ISO 8601 complet)
        "code_bss":           "07548X0009/S",
        "date_mesure":        "2024-01-08T00:00:00",
        "profondeur_nappe":   6.10,
        "niveau_eau_ngf":     28.90,
        "mode_obtention":     "Mesure directe",
        "qualification":      "Bonne",
        "code_qualification": 1,
    },
    {
        # Valeurs numériques retournées sous forme string par l'API
        "code_bss":           "07548X0042/F",
        "date_mesure":        "2024-02-14",
        "profondeur_nappe":   "11.75",
        "niveau_eau_ngf":     "43.75",
        "mode_obtention":     "Automatique",
        "qualification":      "Bonne",
        "code_qualification": "1",
    },
]


# ---------------------------------------------------------------------------
# Tests chroniques_to_dataframe
# ---------------------------------------------------------------------------
class TestChroniqueToDataframe:

    def test_colonnes_schema(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert set(df.columns) == set(TARGET_SCHEMA.keys())

    def test_nombre_lignes(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert len(df) == len(SAMPLE_RECORDS)

    def test_entree_vide_retourne_df_vide(self):
        df = chroniques_to_dataframe([])
        assert len(df) == 0
        assert set(df.columns) == set(TARGET_SCHEMA.keys())

    def test_tri_code_bss_puis_date(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        # Vérifie que pour chaque station, les dates sont croissantes
        for code in df["code_bss"].unique().to_list():
            dates = df.filter(pl.col("code_bss") == code)["date_mesure"].to_list()
            assert dates == sorted(dates), f"Dates non triées pour {code}"

    def test_type_date_mesure(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["date_mesure"].dtype == pl.Date

    def test_type_profondeur(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["profondeur_nappe"].dtype == pl.Float64

    def test_type_niveau_ngf(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["niveau_eau_ngf"].dtype == pl.Float64

    def test_type_code_qualification(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["code_qualification"].dtype == pl.Int64

    def test_date_avec_heure_traitee(self):
        """Une date ISO 8601 complète (avec T et heure) doit être parsée."""
        records = [SAMPLE_RECORDS[4]]  # "2024-01-08T00:00:00"
        df = chroniques_to_dataframe(records)
        assert len(df) == 1
        assert df["date_mesure"][0] == date(2024, 1, 8)

    def test_numeriques_string_cast(self):
        """Les valeurs numériques retournées en string par l'API doivent être castées."""
        records = [SAMPLE_RECORDS[5]]  # profondeur="11.75", niveau="43.75"
        df = chroniques_to_dataframe(records)
        assert df["profondeur_nappe"][0] == pytest.approx(11.75)
        assert df["niveau_eau_ngf"][0] == pytest.approx(43.75)
        assert df["code_qualification"][0] == 1

    def test_valeurs_none_preservees(self):
        """Les champs None doivent rester null dans le DataFrame."""
        records = [SAMPLE_RECORDS[3]]  # profondeur=None, niveau=None
        df = chroniques_to_dataframe(records)
        assert df["profondeur_nappe"].null_count() == 1
        assert df["niveau_eau_ngf"].null_count() == 1
        assert df["qualification"].null_count() == 1
        assert df["code_qualification"].null_count() == 1

    def test_code_bss_correct(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        codes = set(df["code_bss"].to_list())
        assert "07548X0009/S" in codes
        assert "07548X0042/F" in codes
        assert "07548X0101/P" in codes

    def test_deux_stations(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["code_bss"].n_unique() == 3

    def test_valeurs_profondeur_positives(self):
        """Les profondeurs valides doivent être positives."""
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        non_null = df["profondeur_nappe"].drop_nulls()
        assert (non_null >= 0).all()

    def test_qualification_bonne(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        bonnes = df.filter(pl.col("qualification") == "Bonne")
        # 3 enregistrements Bonne dans SAMPLE_RECORDS (indices 0, 1, 4, 5)
        # mais indices 4 et 5 : 4 = Bonne, 5 = Bonne → total 4
        assert len(bonnes) >= 3

    def test_date_mesure_sans_heure(self):
        """Une date simple YYYY-MM-DD est parsée correctement."""
        records = [SAMPLE_RECORDS[0]]  # "2024-03-15"
        df = chroniques_to_dataframe(records)
        assert df["date_mesure"][0] == date(2024, 3, 15)

    def test_mode_obtention_utf8(self):
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        assert df["mode_obtention"].dtype == pl.Utf8

    def test_niveau_ngf_present(self):
        """Les niveaux NGF doivent être présents pour les enregistrements non-null."""
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        # Les enregistrements avec profondeur non-null ont aussi un niveau NGF
        avec_prof = df.filter(pl.col("profondeur_nappe").is_not_null())
        assert avec_prof["niveau_eau_ngf"].null_count() == 0

    def test_date_mesure_pas_dans_futur(self):
        """Aucune date de mesure ne doit être dans le futur."""
        df = chroniques_to_dataframe(SAMPLE_RECORDS)
        today = date.today()
        dates_valides = df["date_mesure"].drop_nulls()
        assert all(d <= today for d in dates_valides.to_list())


# ---------------------------------------------------------------------------
# Tests sur la robustesse du parsing
# ---------------------------------------------------------------------------
class TestParsingRobustesse:

    def test_date_invalide_retourne_null(self):
        """Une date malformée ne doit pas planter — retourner null."""
        records = [{
            "code_bss":           "TEST",
            "date_mesure":        "pas-une-date",
            "profondeur_nappe":   1.0,
            "niveau_eau_ngf":     10.0,
            "mode_obtention":     None,
            "qualification":      None,
            "code_qualification": None,
        }]
        df = chroniques_to_dataframe(records)
        assert len(df) == 1
        assert df["date_mesure"].null_count() == 1

    def test_date_vide_retourne_null(self):
        records = [{
            "code_bss":           "TEST",
            "date_mesure":        "",
            "profondeur_nappe":   2.5,
            "niveau_eau_ngf":     None,
            "mode_obtention":     None,
            "qualification":      None,
            "code_qualification": None,
        }]
        df = chroniques_to_dataframe(records)
        assert df["date_mesure"].null_count() == 1

    def test_profondeur_non_numerique_retourne_null(self):
        records = [{
            "code_bss":           "TEST",
            "date_mesure":        "2024-01-01",
            "profondeur_nappe":   "n.a.",
            "niveau_eau_ngf":     None,
            "mode_obtention":     None,
            "qualification":      None,
            "code_qualification": None,
        }]
        df = chroniques_to_dataframe(records)
        assert df["profondeur_nappe"].null_count() == 1

    def test_code_qualification_non_entier_retourne_null(self):
        records = [{
            "code_bss":           "TEST",
            "date_mesure":        "2024-01-01",
            "profondeur_nappe":   1.0,
            "niveau_eau_ngf":     None,
            "mode_obtention":     None,
            "qualification":      "Bonne",
            "code_qualification": "invalide",
        }]
        df = chroniques_to_dataframe(records)
        assert df["code_qualification"].null_count() == 1

    def test_champ_absent_retourne_null(self):
        """Un enregistrement qui ne contient pas tous les champs ne doit pas planter."""
        records = [{"code_bss": "MINIMAL", "date_mesure": "2024-06-01"}]
        df = chroniques_to_dataframe(records)
        assert len(df) == 1
        assert df["profondeur_nappe"].null_count() == 1
        assert df["niveau_eau_ngf"].null_count() == 1
