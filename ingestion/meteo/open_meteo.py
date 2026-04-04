"""
Ingestion Open-Meteo — données météo historiques.

API gratuite, sans clé, sans limite d'usage raisonnable.
Documentation : https://open-meteo.com/en/docs/historical-weather-api

Usage :
    python -m ingestion.meteo.open_meteo                       # backfill initial ou refresh incrémental
    python -m ingestion.meteo.open_meteo --full-refresh        # reconstitue tout l'historique
    python -m ingestion.meteo.open_meteo --verify              # vérifie les Parquet existants
    python -m ingestion.meteo.open_meteo --schedule            # lance le scheduler quotidien
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
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
DEFAULT_REFRESH_LOOKBACK_DAYS = 30
DEFAULT_SCHEDULE_HOUR = 6
DEFAULT_SCHEDULE_MINUTE = 0


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


def compute_backfill_start(end: date, years: int) -> date:
    """
    Calcule une fenêtre de backfill exprimée en années calendaires inclusives.
    Exemple : end=2026-04-04 et years=5 -> 2022-01-01.
    """
    if years < 1:
        raise ValueError("Le backfill doit couvrir au moins 1 an")
    return date(end.year - years + 1, 1, 1)


def load_existing_data(processed_dir: Path) -> pl.DataFrame | None:
    """Charge les Parquet météo existants s'ils sont présents."""
    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        return None
    return pl.read_parquet(files).sort("date")


def merge_weather_data(existing_df: pl.DataFrame | None, new_df: pl.DataFrame) -> pl.DataFrame:
    """
    Fusionne les données existantes et nouvelles en gardant la version la plus récente
    pour chaque date, puis trie chronologiquement.
    """
    if existing_df is None or existing_df.is_empty():
        return new_df.sort("date")

    return (
        pl.concat([existing_df, new_df], how="diagonal_relaxed")
        .unique(subset=["date"], keep="last")
        .sort("date")
    )


def determine_fetch_start(
    existing_df: pl.DataFrame | None,
    end: date,
    backfill_years: int,
    refresh_lookback_days: int,
    force_full_refresh: bool,
) -> tuple[date, str]:
    """Détermine la borne de début à télécharger selon l'état local."""
    backfill_start = compute_backfill_start(end, backfill_years)

    if force_full_refresh or existing_df is None or existing_df.is_empty():
        return backfill_start, "full"

    last_date = existing_df["date"].max()
    refresh_start = max(backfill_start, last_date - timedelta(days=refresh_lookback_days))
    return refresh_start, "incremental"


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


def scheduled_refresh(backfill_years: int | None = None, refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS) -> None:
    """Job APScheduler : relance un refresh incrémental de la météo."""
    run(
        backfill_years=backfill_years,
        refresh_lookback_days=refresh_lookback_days,
        verify_only=False,
        force_full_refresh=False,
    )


def start_scheduler(
    hour: int = DEFAULT_SCHEDULE_HOUR,
    minute: int = DEFAULT_SCHEDULE_MINUTE,
    backfill_years: int | None = None,
    refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS,
    run_immediately: bool = True,
) -> None:
    """Lance un scheduler quotidien local pour la météo."""
    cfg = load_config()
    timezone = cfg["farm"]["timezone"]
    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        scheduled_refresh,
        CronTrigger(hour=hour, minute=minute, timezone=timezone),
        kwargs={
            "backfill_years": backfill_years,
            "refresh_lookback_days": refresh_lookback_days,
        },
        id="open_meteo_daily_refresh",
        replace_existing=True,
    )

    log.info(
        "Scheduler Open-Meteo actif : refresh quotidien à %02d:%02d (%s)",
        hour,
        minute,
        timezone,
    )

    if run_immediately:
        log.info("Exécution immédiate du refresh météo avant planification")
        scheduled_refresh(
            backfill_years=backfill_years,
            refresh_lookback_days=refresh_lookback_days,
        )

    scheduler.start()


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def run(
    backfill_years: int | None = None,
    refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS,
    verify_only: bool = False,
    force_full_refresh: bool = False,
) -> pl.DataFrame | None:
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
    existing_df = load_existing_data(processed_dir)
    start, mode = determine_fetch_start(
        existing_df=existing_df,
        end=end,
        backfill_years=years,
        refresh_lookback_days=refresh_lookback_days,
        force_full_refresh=force_full_refresh,
    )

    if start > end:
        log.info("Aucune nouvelle fenêtre météo à récupérer")
        return existing_df

    action = "backfill complet" if mode == "full" else "refresh incrémental"
    log.info(f"Open-Meteo {action} : {lat}°N {lon}°E | {start} → {end}")
    log.info(f"Variables : {', '.join(variables)}")

    data = fetch_historical(lat, lon, start, end, variables, tz)
    df = response_to_dataframe(data)
    merged_df = merge_weather_data(existing_df, df)

    log.info(f"  {len(df)} jours récupérés ({len(merged_df)} jours cumulés)")
    save_parquet(merged_df, processed_dir)

    log.info("Ingestion Open-Meteo terminée ✓")
    return merged_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion Open-Meteo")
    parser.add_argument(
        "--years", type=int, default=None, help="Nombre d'années d'historique (défaut: config.toml)"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_REFRESH_LOOKBACK_DAYS,
        help="Fenêtre de recouvrement pour le refresh incrémental",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-télécharger tout l'historique configuré même si des Parquet existent déjà",
    )
    parser.add_argument(
        "--verify", action="store_true", help="Vérifier les Parquet existants sans re-télécharger"
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Lancer un scheduler quotidien local pour le refresh météo",
    )
    parser.add_argument("--hour", type=int, default=DEFAULT_SCHEDULE_HOUR, help="Heure du refresh quotidien")
    parser.add_argument(
        "--minute",
        type=int,
        default=DEFAULT_SCHEDULE_MINUTE,
        help="Minute du refresh quotidien",
    )
    parser.add_argument(
        "--no-run-immediately",
        action="store_true",
        help="Ne pas lancer de refresh immédiat lors du démarrage du scheduler",
    )
    args = parser.parse_args()
    if args.schedule:
        start_scheduler(
            hour=args.hour,
            minute=args.minute,
            backfill_years=args.years,
            refresh_lookback_days=args.lookback_days,
            run_immediately=not args.no_run_immediately,
        )
        return

    run(
        backfill_years=args.years,
        refresh_lookback_days=args.lookback_days,
        verify_only=args.verify,
        force_full_refresh=args.full_refresh,
    )


if __name__ == "__main__":
    main()
