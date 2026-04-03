"""
Ingestion Open-Meteo — données météo historiques.

API gratuite, sans clé, sans limite d'usage raisonnable.
Documentation : https://open-meteo.com/en/docs/historical-weather-api

Usage :
    python -m ingestion.meteo.open_meteo            # backfill complet depuis config
    python -m ingestion.meteo.open_meteo --verify   # vérifie les Parquet existants
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl
import requests
from rich.logging import RichHandler

from ingestion._config import load_config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)
log = logging.getLogger("open_meteo")

# ---------------------------------------------------------------------------
# Constantes API
# ---------------------------------------------------------------------------
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
# Open-Meteo archive a un lag d'environ 2 jours
ARCHIVE_LAG_DAYS = 3


# ---------------------------------------------------------------------------
# Couche API
# ---------------------------------------------------------------------------
def fetch_historical(
    lat: float,
    lon: float,
    start: date,
    end: date,
    variables: list[str],
    timezone: str = "Europe/Paris",
) -> dict:
    """
    Appel API Open-Meteo archive.
    Retourne le JSON brut.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(variables),
        "timezone": timezone,
    }
    log.debug(f"GET {ARCHIVE_URL} params={params}")
    resp = requests.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
def response_to_dataframe(data: dict) -> pl.DataFrame:
    """
    Convertit la réponse JSON Open-Meteo en DataFrame Polars typé.

    Colonnes produites :
        date (Date), latitude (f64), longitude (f64), + variables demandées
    """
    daily = data.get("daily", {})
    if not daily or "time" not in daily:
        raise ValueError("Réponse Open-Meteo invalide : clé 'daily.time' manquante")

    # strict=False pour gérer les valeurs null/None de l'API (données manquantes)
    df = pl.DataFrame(daily, strict=False)
    df = df.rename({"time": "date"})
    df = df.with_columns(pl.col("date").str.to_date(format="%Y-%m-%d"))
    df = df.with_columns(
        [
            pl.lit(data["latitude"]).cast(pl.Float64).alias("latitude"),
            pl.lit(data["longitude"]).cast(pl.Float64).alias("longitude"),
        ]
    )

    # Réordonner : date, lat, lon, puis le reste
    fixed = ["date", "latitude", "longitude"]
    other = [c for c in df.columns if c not in fixed]
    return df.select(fixed + other)


# ---------------------------------------------------------------------------
# Stockage Parquet
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, output_dir: Path) -> list[Path]:
    """
    Sauvegarde en Parquet partitionné par année.
    Si un fichier existe déjà pour une année, il est remplacé.
    Retourne la liste des fichiers écrits.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    years = df["date"].dt.year().unique().sort().to_list()
    written = []

    for year in years:
        year_df = df.filter(pl.col("date").dt.year() == year)
        path = output_dir / f"meteo_{year}.parquet"
        year_df.write_parquet(path, compression="zstd")
        log.info(f"  ✓ {path.name}  ({len(year_df)} jours)")
        written.append(path)

    return written


# ---------------------------------------------------------------------------
# Vérification DuckDB
# ---------------------------------------------------------------------------
def verify(processed_dir: Path) -> None:
    """
    Interroge les Parquet via DuckDB et affiche un résumé.
    Sert à valider que les fichiers sont lisibles et cohérents.
    """
    pattern = str(processed_dir / "*.parquet")
    try:
        result = duckdb.sql(f"""
            SELECT
                MIN(date)::TEXT                          AS premiere_date,
                MAX(date)::TEXT                          AS derniere_date,
                COUNT(*)                                 AS n_jours,
                ROUND(AVG(temperature_2m_max), 1)        AS tmax_moy_c,
                ROUND(AVG(temperature_2m_min), 1)        AS tmin_moy_c,
                ROUND(SUM(precipitation_sum), 0)         AS pluie_totale_mm
            FROM read_parquet('{pattern}')
        """).df()
        print("\n── Vérification datalake météo ──────────────────────────")
        print(result.to_string(index=False))
        print("─────────────────────────────────────────────────────────\n")
    except Exception as e:
        log.error(f"Erreur lors de la vérification DuckDB : {e}")
        raise


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def run(backfill_years: "int | None" = None, verify_only: bool = False) -> "pl.DataFrame | None":
    cfg = load_config()
    lat = cfg["farm"]["latitude"]
    lon = cfg["farm"]["longitude"]
    tz = cfg["farm"]["timezone"]
    variables = cfg["meteo"]["variables"]
    years = backfill_years or cfg["meteo"]["historical_years"]
    processed_dir = Path(cfg["paths"]["processed"]) / "meteo"

    if verify_only:
        verify(processed_dir)
        return None

    end = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    start = date(end.year - years, 1, 1)

    log.info(f"Open-Meteo backfill : {lat}°N {lon}°E | {start} → {end}")
    log.info(f"Variables : {', '.join(variables)}")

    data = fetch_historical(lat, lon, start, end, variables, tz)
    df = response_to_dataframe(data)

    log.info(f"  {len(df)} jours récupérés")
    save_parquet(df, processed_dir)

    log.info("Ingestion Open-Meteo terminée ✓")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion Open-Meteo")
    parser.add_argument(
        "--years", type=int, default=None, help="Nombre d'années d'historique (défaut: config.toml)"
    )
    parser.add_argument(
        "--verify", action="store_true", help="Vérifier les Parquet existants sans re-télécharger"
    )
    args = parser.parse_args()
    run(backfill_years=args.years, verify_only=args.verify)


if __name__ == "__main__":
    main()
