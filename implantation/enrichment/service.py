"""
Enrichissement automatique des parcelles depuis le datalake local.

Cette couche centralise les requêtes DuckDB nécessaires pour injecter
des indicateurs contextuels (météo, DVF, BSS) dans le modèle Parcelle.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from implantation.models.parcelle import Parcelle
from ingestion._config import load_config


@dataclass
class EnrichmentDiagnostics:
    """Retourne des informations utiles à l'UI après enrichissement."""

    weather_years: int = 0
    weather_point: tuple[float, float] | None = None
    dvf_transactions: int = 0
    bss_stations: int = 0
    nearest_bss_km: float | None = None
    warnings: list[str] = field(default_factory=list)


class ParcelleEnricher:
    """Service d'enrichissement d'une parcelle candidate."""

    def __init__(
        self,
        meteo_pattern: str | None = None,
        dvf_pattern: str | None = None,
        bss_pattern: str | None = None,
        dvf_radius_km: float = 20.0,
        bss_radius_km: float = 5.0,
    ) -> None:
        cfg = load_config()
        processed_root = Path(cfg["paths"]["processed"])
        self.meteo_pattern = meteo_pattern or str(processed_root / "meteo" / "*.parquet")
        self.dvf_pattern = dvf_pattern or str(processed_root / "prix" / "prix_dvf_*.parquet")
        self.bss_pattern = bss_pattern or str(processed_root / "geo" / "bss_stations.parquet")
        self.dvf_radius_km = dvf_radius_km
        self.bss_radius_km = bss_radius_km

    def enrich(self, parcelle: Parcelle, overwrite: bool = False) -> tuple[Parcelle, EnrichmentDiagnostics]:
        """
        Enrichit une parcelle avec des indicateurs dérivés du datalake.

        Les champs calculés ne sont écrasés que si `overwrite=True` ou si le champ
        courant est vide (`None`).
        """
        diagnostics = EnrichmentDiagnostics()
        lat, lon = parcelle.coords_centroid

        updates = {
            "meteo_precip_annuelle_mm": parcelle.meteo_precip_annuelle_mm,
            "meteo_jours_gel": parcelle.meteo_jours_gel,
            "meteo_etp_annuelle_mm": parcelle.meteo_etp_annuelle_mm,
            "prix_comparable_eur_ha": parcelle.prix_comparable_eur_ha,
            "forages_brgm_count": parcelle.forages_brgm_count,
        }

        weather = self._query_weather(lat, lon, diagnostics)
        if weather:
            self._assign_if_needed(updates, "meteo_precip_annuelle_mm", weather["precip_mm"], overwrite)
            self._assign_if_needed(updates, "meteo_jours_gel", weather["freeze_days"], overwrite)
            self._assign_if_needed(updates, "meteo_etp_annuelle_mm", weather["etp_mm"], overwrite)

        dvf = self._query_dvf(lat, lon, diagnostics)
        if dvf is not None:
            self._assign_if_needed(updates, "prix_comparable_eur_ha", dvf, overwrite)

        bss = self._query_bss(lat, lon, diagnostics)
        if bss is not None:
            self._assign_if_needed(updates, "forages_brgm_count", bss["count"], overwrite)

        return parcelle.model_copy(update=updates), diagnostics

    @staticmethod
    def _assign_if_needed(target: dict, key: str, value: object, overwrite: bool) -> None:
        if value is None:
            return
        if overwrite or target.get(key) is None:
            target[key] = value

    @staticmethod
    def _parquet_exists(pattern: str) -> bool:
        if "*" in pattern:
            return bool(list(Path(pattern).parent.glob(Path(pattern).name)))
        return Path(pattern).exists()

    def _query_weather(self, lat: float, lon: float, diagnostics: EnrichmentDiagnostics) -> dict[str, float | int] | None:
        if not self._parquet_exists(self.meteo_pattern):
            diagnostics.warnings.append("Meteo non enrichie: aucun parquet meteo disponible.")
            return None

        query = f"""
            WITH nearest_point AS (
                SELECT DISTINCT latitude, longitude
                FROM read_parquet('{self.meteo_pattern}')
                ORDER BY POW(latitude - ?, 2) + POW(longitude - ?, 2)
                LIMIT 1
            ),
            annual AS (
                SELECT
                    EXTRACT(YEAR FROM date) AS year,
                    SUM(precipitation_sum) AS annual_precip_mm,
                    SUM(et0_fao_evapotranspiration) AS annual_etp_mm,
                    SUM(CASE WHEN temperature_2m_min < 0 THEN 1 ELSE 0 END) AS annual_freeze_days
                FROM read_parquet('{self.meteo_pattern}')
                WHERE (latitude, longitude) IN (SELECT latitude, longitude FROM nearest_point)
                GROUP BY 1
            )
            SELECT
                ROUND(AVG(annual_precip_mm), 1) AS precip_mm,
                CAST(ROUND(AVG(annual_freeze_days), 0) AS INTEGER) AS freeze_days,
                ROUND(AVG(annual_etp_mm), 1) AS etp_mm,
                COUNT(*) AS n_years,
                (SELECT latitude FROM nearest_point) AS ref_latitude,
                (SELECT longitude FROM nearest_point) AS ref_longitude
            FROM annual
        """
        row = duckdb.execute(query, [lat, lon]).fetchone()
        if not row or row[0] is None:
            diagnostics.warnings.append("Meteo non enrichie: donnees locales introuvables.")
            return None

        diagnostics.weather_years = int(row[3] or 0)
        diagnostics.weather_point = (float(row[4]), float(row[5])) if row[4] is not None and row[5] is not None else None
        return {
            "precip_mm": float(row[0]) if row[0] is not None else None,
            "freeze_days": int(row[1]) if row[1] is not None else None,
            "etp_mm": float(row[2]) if row[2] is not None else None,
        }

    def _query_dvf(self, lat: float, lon: float, diagnostics: EnrichmentDiagnostics) -> float | None:
        if not self._parquet_exists(self.dvf_pattern):
            diagnostics.warnings.append("DVF non enrichi: aucun parquet prix_dvf disponible.")
            return None

        try:
            columns = duckdb.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{self.dvf_pattern}')"
            ).fetchall()
        except Exception as exc:
            diagnostics.warnings.append(f"DVF non enrichi: lecture impossible ({exc}).")
            return None

        column_names = {col[0] for col in columns}
        if "surface_terrain" not in column_names:
            diagnostics.warnings.append("DVF non enrichi: colonne surface_terrain absente, €/ha indisponible.")
            return None

        query = f"""
            WITH candidates AS (
                SELECT
                    valeur_fonciere,
                    surface_terrain,
                    6371.0 * 2 * ASIN(
                        SQRT(
                            POW(SIN(RADIANS(latitude - ?) / 2), 2)
                            + COS(RADIANS(?)) * COS(RADIANS(latitude))
                            * POW(SIN(RADIANS(longitude - ?) / 2), 2)
                        )
                    ) AS distance_km
                FROM read_parquet('{self.dvf_pattern}')
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND valeur_fonciere IS NOT NULL
                  AND surface_terrain IS NOT NULL
                  AND surface_terrain > 0
            )
            SELECT
                ROUND(MEDIAN(valeur_fonciere / (surface_terrain / 10000.0)), 0) AS prix_ha,
                COUNT(*) AS n
            FROM candidates
            WHERE distance_km <= ?
        """
        row = duckdb.execute(query, [lat, lat, lon, self.dvf_radius_km]).fetchone()
        diagnostics.dvf_transactions = int(row[1] or 0) if row else 0

        if not row or row[0] is None:
            diagnostics.warnings.append("DVF non enrichi: aucune transaction exploitable dans le rayon local.")
            return None
        return float(row[0])

    def _query_bss(self, lat: float, lon: float, diagnostics: EnrichmentDiagnostics) -> dict[str, int | float | None] | None:
        if not self._parquet_exists(self.bss_pattern):
            diagnostics.warnings.append("BSS non enrichi: aucun parquet BSS disponible.")
            return None

        query = f"""
            WITH distances AS (
                SELECT
                    6371.0 * 2 * ASIN(
                        SQRT(
                            POW(SIN(RADIANS(latitude - ?) / 2), 2)
                            + COS(RADIANS(?)) * COS(RADIANS(latitude))
                            * POW(SIN(RADIANS(longitude - ?) / 2), 2)
                        )
                    ) AS distance_km
                FROM read_parquet('{self.bss_pattern}')
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
            )
            SELECT COUNT(*) AS n, MIN(distance_km) AS nearest_km
            FROM distances
            WHERE distance_km <= ?
        """
        row = duckdb.execute(query, [lat, lat, lon, self.bss_radius_km]).fetchone()
        if not row:
            diagnostics.warnings.append("BSS non enrichi: aucune station lisible.")
            return None

        diagnostics.bss_stations = int(row[0] or 0)
        diagnostics.nearest_bss_km = float(row[1]) if row[1] is not None else None
        return {"count": diagnostics.bss_stations, "nearest_km": diagnostics.nearest_bss_km}
