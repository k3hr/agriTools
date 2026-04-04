from __future__ import annotations

from datetime import date

import polars as pl

from implantation.enrichment.service import ParcelleEnricher
from implantation.models.parcelle import Parcelle


def _base_parcelle() -> Parcelle:
    return Parcelle(
        id="72181_2026_0001",
        nom="Parcelle test",
        surface_ha=2.0,
        commune="Sable-sur-Sarthe",
        departement="72",
        coords_centroid=(47.85, -0.94),
    )


class TestParcelleEnricher:
    def test_enrich_populates_weather_dvf_and_bss(self, tmp_path):
        meteo_path = tmp_path / "meteo.parquet"
        dvf_path = tmp_path / "prix_dvf_2025.parquet"
        bss_path = tmp_path / "bss_stations.parquet"

        pl.DataFrame(
            {
                "date": [date(2024, 1, 1), date(2024, 1, 2), date(2025, 1, 1), date(2025, 1, 2)],
                "latitude": [47.85, 47.85, 47.85, 47.85],
                "longitude": [-0.94, -0.94, -0.94, -0.94],
                "precipitation_sum": [10.0, 20.0, 30.0, 40.0],
                "et0_fao_evapotranspiration": [1.0, 2.0, 3.0, 4.0],
                "temperature_2m_min": [-2.0, 2.0, -1.0, 5.0],
            }
        ).write_parquet(meteo_path)

        pl.DataFrame(
            {
                "valeur_fonciere": [12000.0, 18000.0],
                "surface_terrain": [10000.0, 15000.0],
                "latitude": [47.8505, 48.20],
                "longitude": [-0.9405, 0.10],
            }
        ).write_parquet(dvf_path)

        pl.DataFrame(
            {
                "latitude": [47.851, 48.5],
                "longitude": [-0.941, 0.2],
            }
        ).write_parquet(bss_path)

        enricher = ParcelleEnricher(
            meteo_pattern=str(meteo_path),
            dvf_pattern=str(dvf_path),
            bss_pattern=str(bss_path),
            dvf_radius_km=10.0,
            bss_radius_km=5.0,
        )

        enriched, diagnostics = enricher.enrich(_base_parcelle())

        assert enriched.meteo_precip_annuelle_mm == 50.0
        assert enriched.meteo_etp_annuelle_mm == 5.0
        assert enriched.meteo_jours_gel == 1
        assert enriched.prix_comparable_eur_ha == 12000.0
        assert enriched.forages_brgm_count == 1
        assert diagnostics.weather_years == 2
        assert diagnostics.dvf_transactions == 1
        assert diagnostics.bss_stations == 1
        assert diagnostics.nearest_bss_km is not None
        assert diagnostics.warnings == []

    def test_enrich_warns_when_dvf_surface_is_missing(self, tmp_path):
        meteo_path = tmp_path / "meteo.parquet"
        dvf_path = tmp_path / "prix_dvf_2025.parquet"
        bss_path = tmp_path / "bss_stations.parquet"

        pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "latitude": [47.85],
                "longitude": [-0.94],
                "precipitation_sum": [10.0],
                "et0_fao_evapotranspiration": [1.0],
                "temperature_2m_min": [1.0],
            }
        ).write_parquet(meteo_path)

        pl.DataFrame(
            {
                "valeur_fonciere": [12000.0],
                "latitude": [47.8505],
                "longitude": [-0.9405],
            }
        ).write_parquet(dvf_path)

        pl.DataFrame({"latitude": [47.851], "longitude": [-0.941]}).write_parquet(bss_path)

        enricher = ParcelleEnricher(
            meteo_pattern=str(meteo_path),
            dvf_pattern=str(dvf_path),
            bss_pattern=str(bss_path),
        )

        enriched, diagnostics = enricher.enrich(_base_parcelle())

        assert enriched.prix_comparable_eur_ha is None
        assert any("surface_terrain absente" in warning for warning in diagnostics.warnings)
