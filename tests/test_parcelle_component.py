from datetime import datetime
from unittest.mock import Mock

from app.components.parcelle import render_parcelle_preview
from implantation.models.parcelle import Parcelle


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
