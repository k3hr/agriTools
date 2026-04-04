"""
Couche d'accès aux données pour les pages Streamlit.
Toutes les requêtes DuckDB passent par ici — les pages ne touchent pas DuckDB directement.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import streamlit as st

from ingestion._config import load_config


def _processed_dir(source: str) -> str:
    cfg = load_config()
    return str(Path(cfg["paths"]["processed"]) / source / "*.parquet")


@st.cache_data(ttl=3600, show_spinner=False)
def load_meteo(start: date | None = None, end: date | None = None) -> pl.DataFrame:
    """
    Charge les données météo Parquet et filtre sur la période demandée.
    Mise en cache 1 heure (ttl=3600).
    """
    pattern = _processed_dir("meteo")
    where_clauses: list[str] = []
    if start:
        where_clauses.append(f"date >= DATE '{start}'")
    if end:
        where_clauses.append(f"date <= DATE '{end}'")
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    df = duckdb.sql(f"""
        SELECT
            date,
            temperature_2m_max   AS t_max,
            temperature_2m_min   AS t_min,
            temperature_2m_mean  AS t_moy,
            precipitation_sum    AS pluie_mm,
            et0_fao_evapotranspiration AS etp_mm,
            wind_speed_10m_max   AS vent_kmh,
            shortwave_radiation_sum AS rayonnement_mj
        FROM read_parquet('{pattern}')
        {where}
        ORDER BY date
    """).pl()
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_prix() -> pl.DataFrame:
    """
    Charge toutes les cotations RNM, parse les dates et extrait la famille produit.
    Mise en cache 1 heure.
    """
    pattern = _processed_dir("prix")
    df = duckdb.sql(f"""
        SELECT
            annee,
            TRY_STRPTIME(date, '%d/%m/%Y')::DATE AS date_cot,
            produit,
            marche,
            stade,
            unite,
            prix_min,
            prix_moyen,
            prix_max
        FROM read_parquet('{pattern}')
        WHERE prix_moyen IS NOT NULL
        ORDER BY date_cot
    """).pl()

    # Famille = premier mot du libellé produit
    df = df.with_columns(
        pl.col("produit").str.split(" ").list.first().alias("famille")
    )
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def meteo_date_range() -> tuple[date, date]:
    """Retourne (date_min, date_max) du datalake météo."""
    pattern = _processed_dir("meteo")
    row = duckdb.sql(f"""
        SELECT MIN(date)::TEXT, MAX(date)::TEXT
        FROM read_parquet('{pattern}')
    """).fetchone()
    return date.fromisoformat(row[0]), date.fromisoformat(row[1])
