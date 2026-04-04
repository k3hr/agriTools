"""
Tests unitaires — ingestion heures de travail.
Couvre le parsing durée/date et la logique CSV sans I/O réel.
"""
from datetime import date, timedelta

import pytest

from ingestion.perso.heures import parse_duree, parse_date, JOURS_FR


# ---------------------------------------------------------------------------
# parse_duree
# ---------------------------------------------------------------------------
class TestParseDuree:
    def test_heures_minutes(self):
        assert parse_duree("2h30") == pytest.approx(2.5)

    def test_heures_minutes_avec_m(self):
        assert parse_duree("2h30m") == pytest.approx(2.5)

    def test_heures_decimales(self):
        assert parse_duree("1.5h") == pytest.approx(1.5)

    def test_heures_entiere(self):
        assert parse_duree("3h") == pytest.approx(3.0)

    def test_minutes_min(self):
        assert parse_duree("90min") == pytest.approx(1.5)

    def test_minutes_m(self):
        assert parse_duree("45m") == pytest.approx(0.75)

    def test_demi_heure_decimale(self):
        assert parse_duree("0.5h") == pytest.approx(0.5)

    def test_une_heure_pile(self):
        assert parse_duree("1h") == pytest.approx(1.0)

    def test_minutes_exactes(self):
        assert parse_duree("60min") == pytest.approx(1.0)

    def test_format_inconnu_retourne_none(self):
        assert parse_duree("deux heures") is None

    def test_format_vide_retourne_none(self):
        assert parse_duree("") is None

    def test_chiffre_seul_retourne_none(self):
        assert parse_duree("2") is None

    def test_insensible_casse(self):
        assert parse_duree("2H30") == pytest.approx(2.5)
        assert parse_duree("90MIN") == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# parse_date
# ---------------------------------------------------------------------------
class TestParseDate:
    def test_aujourd_hui(self):
        assert parse_date("aujourd'hui") == date.today()

    def test_auj(self):
        assert parse_date("auj") == date.today()

    def test_hier(self):
        assert parse_date("hier") == date.today() - timedelta(days=1)

    def test_iso(self):
        assert parse_date("2026-04-01") == date(2026, 4, 1)

    def test_format_fr(self):
        assert parse_date("01/04/2026") == date(2026, 4, 1)

    def test_inconnu_retourne_none(self):
        assert parse_date("demain") is None

    def test_vide_retourne_none(self):
        assert parse_date("") is None

    @pytest.mark.parametrize("jour", JOURS_FR.keys())
    def test_jour_semaine_dans_le_passe(self, jour):
        d = parse_date(jour)
        assert d is not None
        assert d < date.today() or d == date.today()
        assert d >= date.today() - timedelta(days=7)

    @pytest.mark.parametrize("jour", JOURS_FR.keys())
    def test_jour_semaine_bon_weekday(self, jour):
        d = parse_date(jour)
        assert d.weekday() == JOURS_FR[jour]

    def test_lundi_majuscule(self):
        # insensible à la casse
        d1 = parse_date("lundi")
        d2 = parse_date("Lundi")
        assert d1 == d2
