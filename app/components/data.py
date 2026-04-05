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


def _meteo_france_dir() -> Path:
    cfg = load_config()
    return Path(cfg["paths"]["processed"]) / "meteo_france"


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


@st.cache_data(ttl=3600, show_spinner=False)
def load_meteo_france(start: date | None = None, end: date | None = None) -> pl.DataFrame:
    """
    Charge les données Météo-France (DPObs) et les aligne sur le même schéma
    que load_meteo() pour permettre la réutilisation des graphiques.

    Colonne supplémentaire vs Open-Meteo : ensoleillement_min (INST).
    Retourne un DataFrame vide si aucun Parquet Météo-France n'est disponible.
    """
    mf_dir = _meteo_france_dir()
    if not mf_dir.exists() or not list(mf_dir.glob("*.parquet")):
        return pl.DataFrame()

    pattern = str(mf_dir / "*.parquet")
    where_clauses: list[str] = []
    if start:
        where_clauses.append(f"date >= DATE '{start}'")
    if end:
        where_clauses.append(f"date <= DATE '{end}'")
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    return duckdb.sql(f"""
        SELECT
            date,
            temperature_2m_max          AS t_max,
            temperature_2m_min          AS t_min,
            temperature_2m_mean         AS t_moy,
            precipitation_sum           AS pluie_mm,
            et0_fao_evapotranspiration  AS etp_mm,
            wind_speed_10m_max          AS vent_kmh,
            sunshine_duration_min       AS ensoleillement_min
        FROM read_parquet('{pattern}')
        {where}
        ORDER BY date
    """).pl()


def meteo_france_date_range() -> tuple[date, date] | None:
    """
    Retourne (date_min, date_max) du datalake Météo-France.
    Retourne None si aucun Parquet n'est disponible (non ingéré).
    Non mis en cache : appelé une fois par session pour vérifier la disponibilité.
    """
    mf_dir = _meteo_france_dir()
    if not mf_dir.exists() or not list(mf_dir.glob("*.parquet")):
        return None

    pattern = str(mf_dir / "*.parquet")
    row = duckdb.sql(f"""
        SELECT MIN(date)::TEXT, MAX(date)::TEXT
        FROM read_parquet('{pattern}')
    """).fetchone()
    if not row or not row[0]:
        return None
    return date.fromisoformat(row[0]), date.fromisoformat(row[1])


# =============================================================================
# Fonctions de vérification du datalake (Tableau de bord)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def datalake_status() -> dict:
    """
    Vérifie la fraîcheur et l'état de chaque source du datalake.
    Retourne un dict avec last_update, row_count, et status pour chaque source.
    Cache 5 minutes.
    """
    from datetime import datetime, timedelta
    
    cfg = load_config()
    processed_root = cfg["paths"]["processed"]
    
    status = {}
    
    # --- Météo ---
    try:
        pattern_meteo = _processed_dir("meteo")
        row = duckdb.sql(f"""
            SELECT MAX(date)::TEXT, COUNT(*) as n
            FROM read_parquet('{pattern_meteo}')
        """).fetchone()
        last_date = date.fromisoformat(row[0]) if row[0] else None
        count = row[1] if row else 0
        
        days_old = (date.today() - last_date).days if last_date else None
        is_fresh = days_old is not None and days_old <= 3
        
        status["meteo"] = {
            "last_update": last_date,
            "days_old": days_old,
            "row_count": count,
            "status": "✅ À jour" if is_fresh else ("⚠️ Ancien" if days_old is not None else "❌ Erreur"),
            "alert": None if is_fresh else f"Pas d'update depuis {days_old} jours"
        }
    except Exception as e:
        status["meteo"] = {
            "last_update": None,
            "days_old": None,
            "row_count": 0,
            "status": "❌ Erreur",
            "alert": str(e)[:50]
        }
    
    # --- Prix RNM ---
    try:
        pattern_prix = _processed_dir("prix")
        row = duckdb.sql(rf"""
            SELECT MAX(
                CASE 
                    WHEN date ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN TRY_STRPTIME(date, '%d/%m/%Y')::DATE
                    ELSE TRY_STRPTIME(date, '%Y-%m-%d')::DATE
                END
            )::TEXT, COUNT(*) as n
            FROM read_parquet('{pattern_prix}')
        """).fetchone()
        last_date = date.fromisoformat(row[0]) if row[0] else None
        count = row[1] if row else 0
        
        days_old = (date.today() - last_date).days if last_date else None
        is_fresh = days_old is not None and days_old <= 10
        
        status["prix"] = {
            "last_update": last_date,
            "days_old": days_old,
            "row_count": count,
            "status": "✅ À jour" if is_fresh else ("⚠️ Ancien" if days_old is not None else "❌ Erreur"),
            "alert": None if is_fresh else f"Pas d'update depuis {days_old} jours"
        }
    except Exception as e:
        status["prix"] = {
            "last_update": None,
            "days_old": None,
            "row_count": 0,
            "status": "❌ Erreur",
            "alert": str(e)[:50]
        }
    
    # --- RPG ---
    try:
        pattern_rpg = _processed_dir("geo")
        # Le fichier RPG vient du catalog — récupérer la date du fichier
        rpg_files = list(Path(processed_root).glob("geo/*.parquet"))
        last_mtime = max([f.stat().st_mtime for f in rpg_files]) if rpg_files else None
        last_date = date.fromtimestamp(last_mtime) if last_mtime else None
        
        count = duckdb.sql(f"""
            SELECT COUNT(*) as n
            FROM read_parquet('{pattern_rpg}')
        """).fetchone()[0]
        
        days_old = (date.today() - last_date).days if last_date else None
        is_fresh = days_old is not None and days_old <= 365  # RPG annuel
        
        status["rpg"] = {
            "last_update": last_date,
            "days_old": days_old,
            "row_count": count,
            "status": "✅ À jour" if is_fresh else ("⚠️ Ancien" if days_old is not None else "❌ Erreur"),
            "alert": None if is_fresh else f"Pas d'update depuis {days_old} jours"
        }
    except Exception as e:
        status["rpg"] = {
            "last_update": None,
            "days_old": None,
            "row_count": 0,
            "status": "❌ Erreur",
            "alert": str(e)[:50]
        }
    
    return status
