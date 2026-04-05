from datetime import datetime
from unittest.mock import Mock

from app.components.parcelle import render_parcelle_preview
from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import AxisScore, ParcelleScore


def test_render_parcelle_preview_with_complete_parcelle():
    parcelle = Parcelle(
        id="parcelle_complete_001",
        nom="Prairie Complète",
        surface_ha=4.2,
        commune="Sablé-sur-Sarthe",
        departement="72",
        coords_centroid=(47.8474, -0.9416),
        prix_achat=15000.0,
        prix_location_annuel=320.0,
        acces_eau="forage",
        debit_estime_m3h=3.4,
        distance_cours_eau_m=120.0,
        pente_pct=2.5,
        exposition="S",
        altitude_m=54.0,
        risque_gel_tardif=False,
        distance_marche_km=11.5,
        distance_agglo_km=8.0,
        acces_vehicule="facile",
        notes="Terrain plat, forage actif, acces simple.",
        statut="visite",
        date_creation=datetime(2026, 4, 1, 9, 30, 0),
        date_modification=datetime(2026, 4, 2, 18, 15, 0),
        meteo_precip_annuelle_mm=710.0,
        meteo_jours_gel=18,
        meteo_etp_annuelle_mm=640.0,
        prix_comparable_eur_ha=6200.0,
        forages_brgm_count=3,
    )
    fake_st = Mock()

    payload = render_parcelle_preview(parcelle, fake_st)

    fake_st.subheader.assert_called_once_with("Prairie Complète (parcelle_complete_001)")
    fake_st.caption.assert_called_once_with("Sablé-sur-Sarthe (72) • 4.2 ha")
    fake_st.json.assert_called_once_with(payload)

    assert payload["Identite"] == {
        "id": "parcelle_complete_001",
        "nom": "Prairie Complète",
        "surface_ha": 4.2,
        "commune": "Sablé-sur-Sarthe",
        "departement": "72",
        "coords_centroid": [47.8474, -0.9416],
    }
    assert payload["Economique"] == {
        "prix_achat": 15000.0,
        "prix_location_annuel": 320.0,
        "prix_comparable_eur_ha": 6200.0,
    }
    assert payload["Eau_irrigation"] == {
        "acces_eau": "forage",
        "debit_estime_m3h": 3.4,
        "distance_cours_eau_m": 120.0,
        "forages_brgm_count": 3,
    }
    assert payload["Topographie_logistique"] == {
        "pente_pct": 2.5,
        "exposition": "S",
        "altitude_m": 54.0,
        "risque_gel_tardif": False,
        "distance_marche_km": 11.5,
        "distance_agglo_km": 8.0,
        "acces_vehicule": "facile",
    }
    assert payload["Enrichissements"] == {
        "meteo_precip_annuelle_mm": 710.0,
        "meteo_jours_gel": 18,
        "meteo_etp_annuelle_mm": 640.0,
    }
    assert payload["Suivi"] == {
        "statut": "visite",
        "notes": "Terrain plat, forage actif, acces simple.",
        "date_creation": "2026-04-01T09:30:00",
        "date_modification": "2026-04-02T18:15:00",
    }


def test_render_parcelle_preview_with_minimal_parcelle():
    parcelle = Parcelle(
        id="parcelle_min_001",
        nom="Parcelle Simple",
        surface_ha=1.8,
        commune="Louailles",
        departement="72",
        coords_centroid=(47.996, -0.153),
    )
    fake_st = Mock()

    payload = render_parcelle_preview(parcelle, fake_st)

    fake_st.subheader.assert_called_once_with("Parcelle Simple (parcelle_min_001)")
    fake_st.caption.assert_called_once_with("Louailles (72) • 1.8 ha")
    fake_st.json.assert_called_once_with(payload)

    assert payload["Economique"] == {
        "prix_achat": None,
        "prix_location_annuel": None,
        "prix_comparable_eur_ha": None,
    }
    assert payload["Eau_irrigation"] == {
        "acces_eau": "inconnu",
        "debit_estime_m3h": None,
        "distance_cours_eau_m": None,
        "forages_brgm_count": None,
    }
    assert payload["Topographie_logistique"] == {
        "pente_pct": None,
        "exposition": "plat",
        "altitude_m": None,
        "risque_gel_tardif": None,
        "distance_marche_km": None,
        "distance_agglo_km": None,
        "acces_vehicule": "facile",
    }
    assert payload["Enrichissements"] == {
        "meteo_precip_annuelle_mm": None,
        "meteo_jours_gel": None,
        "meteo_etp_annuelle_mm": None,
    }
    assert payload["Suivi"]["statut"] == "prospect"
    assert payload["Suivi"]["notes"] == ""
    assert isinstance(payload["Suivi"]["date_creation"], str)
    assert isinstance(payload["Suivi"]["date_modification"], str)


def test_render_parcelle_preview_when_score_is_none():
    parcelle = Parcelle(
        id="parcelle_no_score",
        nom="Parcelle Sans Score",
        surface_ha=2.1,
        commune="Précigné",
        departement="72",
        coords_centroid=(47.768, -0.321),
    )
    fake_st = Mock()

    payload = render_parcelle_preview(parcelle, fake_st, score=None)

    fake_st.subheader.assert_called_once_with("Parcelle Sans Score (parcelle_no_score)")
    fake_st.caption.assert_called_once_with("Précigné (72) • 2.1 ha")
    fake_st.info.assert_called_once_with("Score non calcule pour le moment.")
    fake_st.metric.assert_not_called()
    fake_st.json.assert_called_once_with(payload)


def test_render_parcelle_preview_when_score_is_zero():
    parcelle = Parcelle(
        id="parcelle_zero_score",
        nom="Parcelle Score Zero",
        surface_ha=3.0,
        commune="Avoise",
        departement="72",
        coords_centroid=(47.851, -0.209),
    )
    fake_st = Mock()

    payload = render_parcelle_preview(parcelle, fake_st, score=0)

    fake_st.subheader.assert_called_once_with("Parcelle Score Zero (parcelle_zero_score)")
    fake_st.caption.assert_called_once_with("Avoise (72) • 3.0 ha")
    fake_st.info.assert_not_called()
    fake_st.metric.assert_called_once_with("Score global", "0/100")
    fake_st.json.assert_called_once_with(payload)


def test_render_parcelle_preview_when_score_is_hundred():
    parcelle = Parcelle(
        id="parcelle_top_score",
        nom="Parcelle Parfaite",
        surface_ha=2.7,
        commune="Juigné-sur-Sarthe",
        departement="72",
        coords_centroid=(47.889, -0.286),
    )
    fake_st = Mock()

    payload = render_parcelle_preview(parcelle, fake_st, score=100)

    fake_st.subheader.assert_called_once_with("Parcelle Parfaite (parcelle_top_score)")
    fake_st.caption.assert_called_once_with("Juigné-sur-Sarthe (72) • 2.7 ha")
    fake_st.info.assert_not_called()
    fake_st.metric.assert_called_once_with("Score global", "100/100")
    fake_st.json.assert_called_once_with(payload)


def test_render_parcelle_preview_displays_three_axes_when_present():
    parcelle = Parcelle(
        id="parcelle_axes",
        nom="Parcelle Axes",
        surface_ha=3.6,
        commune="Parcé-sur-Sarthe",
        departement="72",
        coords_centroid=(47.834, -0.201),
    )
    score = ParcelleScore(
        parcelle_id=parcelle.id,
        parcelle_nom=parcelle.nom,
        global_score=78,
        score_economique_logistique=AxisScore(
            name="Économique & Logistique",
            score=81,
            weight=0.35,
            criteria={"prix_achat": 80},
        ),
        score_eau_irrigation=AxisScore(
            name="Eau & Irrigation",
            score=74,
            weight=0.35,
            criteria={"acces_eau": 70},
        ),
        score_topographie_exposition=AxisScore(
            name="Topographie & Exposition",
            score=79,
            weight=0.30,
            criteria={"pente": 82},
        ),
    )
    fake_st = Mock()

    render_parcelle_preview(parcelle, fake_st, score=score)

    assert fake_st.metric.call_count == 4
    fake_st.metric.assert_any_call("Score global", "78/100")
    fake_st.metric.assert_any_call("Axe Economique", "81/100")
    fake_st.metric.assert_any_call("Axe Eau", "74/100")
    fake_st.metric.assert_any_call("Axe Topographie", "79/100")
