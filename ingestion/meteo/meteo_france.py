"""
Ingestion Météo-France — données officielles d'observation quotidienne.

Source : Portail API Météo-France — DPObs (Données Publiques Observations)
Doc API : https://portail-api.meteofrance.fr/web/fr/api/DPObs
Champs  : https://portail-api.meteofrance.fr/web/fr/api/DPObs/documentation

Clé API : https://portail-api.meteofrance.fr → Mon Compte → Applications
          Puis dans config.local.toml :
              [meteo_france]
              api_key = "votre-cle-ici"
          Ou via variable d'environnement : METEOFRANCE_API_KEY

Différences vs Open-Meteo :
  - Stations d'observation officielles du réseau Météo-France
  - DPObs couvre environ 48 mois glissants ; au-delà → DPClim (commandes async)
  - Unités brutes : températures en 1/10 °C, précipitations en 1/10 mm
  - Le module sélectionne automatiquement la station la plus proche de la ferme

Usage :
    python -m ingestion.meteo.meteo_france                   # backfill ou refresh incrémental
    python -m ingestion.meteo.meteo_france --full-refresh    # reconstituer tout l'historique
    python -m ingestion.meteo.meteo_france --verify          # vérifier les Parquet existants
    python -m ingestion.meteo.meteo_france --list-stations   # lister les stations proches
    python -m ingestion.meteo.meteo_france --schedule        # lancer le scheduler quotidien
"""
from __future__ import annotations

import argparse
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
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
log = logging.getLogger("meteo_france")

# ---------------------------------------------------------------------------
# Constantes API
# ---------------------------------------------------------------------------
BASE_URL = "https://public-api.meteofrance.fr/public/DPObs/v1"
STATIONS_ENDPOINT = f"{BASE_URL}/liste-stations/quotidien"
DAILY_ENDPOINT = f"{BASE_URL}/station/quotidien"

# Lag observationnel : DPObs est disponible avec ~1 jour de délai
ARCHIVE_LAG_DAYS = 2
DEFAULT_REFRESH_LOOKBACK_DAYS = 14
DEFAULT_SCHEDULE_HOUR = 6
DEFAULT_SCHEDULE_MINUTE = 30

# Nombre maximum de stations proches affichées par --list-stations
MAX_NEARBY_STATIONS = 10

# ---------------------------------------------------------------------------
# Correspondance champs Météo-France → schéma projet
#
# L'API DPObs retourne les valeurs brutes en 1/10 d'unité pour la plupart
# des variables. Les conversions sont appliquées dans response_to_dataframe().
#
# Champ MF  | Unité brute       | Colonne projet              | Unité projet
# ----------|-------------------|-----------------------------|-------------
# TX        | 1/10 °C           | temperature_2m_max          | °C
# TN        | 1/10 °C           | temperature_2m_min          | °C
# TM        | 1/10 °C           | temperature_2m_mean         | °C
# RR        | 1/10 mm           | precipitation_sum           | mm
# FFM       | 1/10 m/s (moy.)   | wind_speed_10m_max          | km/h  (approx)
# ETP       | 1/10 mm           | et0_fao_evapotranspiration  | mm
# INST      | minutes           | sunshine_duration_min       | min   (supplément)
# ---------------------------------------------------------------------------
MF_TO_PROJECT: dict[str, tuple[str, float]] = {
    # champ_mf : (colonne_projet, facteur_conversion)
    "TX":   ("temperature_2m_max",          0.1),
    "TN":   ("temperature_2m_min",          0.1),
    "TM":   ("temperature_2m_mean",         0.1),
    "RR":   ("precipitation_sum",           0.1),
    "FFM":  ("wind_speed_10m_max",          0.36),   # 1/10 m/s → km/h : × 0.1 × 3.6
    "ETP":  ("et0_fao_evapotranspiration",  0.1),
    "INST": ("sunshine_duration_min",       1.0),    # minutes, pas de conversion
}


# ---------------------------------------------------------------------------
# Résolution de la clé API
# ---------------------------------------------------------------------------
def _resolve_api_key(cfg: dict) -> str:
    """
    Cherche la clé API dans l'ordre :
    1. Variable d'environnement METEOFRANCE_API_KEY
    2. config.local.toml → [meteo_france] api_key

    Lève RuntimeError si introuvable.
    """
    key = os.environ.get("METEOFRANCE_API_KEY", "").strip()
    if key:
        return key

    key = cfg.get("meteo_france", {}).get("api_key", "").strip()
    if key:
        return key

    raise RuntimeError(
        "Clé API Météo-France introuvable.\n"
        "  → Définir METEOFRANCE_API_KEY=<cle> dans l'environnement\n"
        "  → Ou ajouter dans config.local.toml :\n"
        "      [meteo_france]\n"
        "      api_key = \"votre-cle-ici\""
    )


# ---------------------------------------------------------------------------
# Couche API
# ---------------------------------------------------------------------------
def _auth_params(api_key: str) -> dict[str, str]:
    return {"apikey": api_key}


def fetch_stations(api_key: str, timeout: int = 30) -> list[dict]:
    """
    Récupère la liste de toutes les stations Météo-France ayant des données
    quotidiennes disponibles via DPObs.

    Retourne une liste de dicts avec au moins :
        id, nom, lat, lon, altitude, departement
    """
    resp = requests.get(STATIONS_ENDPOINT, params=_auth_params(api_key), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_daily(
    station_id: str,
    start: date,
    end: date,
    api_key: str,
    timeout: int = 60,
) -> list[dict]:
    """
    Récupère les observations quotidiennes d'une station via DPObs.

    L'API attend des dates au format ISO 8601 UTC : 'YYYY-MM-DDTHH:MM:SSZ'.
    La fenêtre maximale est d'environ 48 mois glissants.

    Retourne la liste brute des enregistrements JSON.
    """
    def _to_api_date(d: date) -> str:
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    params = {
        **_auth_params(api_key),
        "id-station": station_id,
        "date-deb-periode": _to_api_date(start),
        "date-fin-periode": _to_api_date(end),
        "format": "json",
    }
    log.debug("GET %s  params=%s", DAILY_ENDPOINT, {k: v for k, v in params.items() if k != "apikey"})
    resp = requests.get(DAILY_ENDPOINT, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Sélection de station
# ---------------------------------------------------------------------------
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance haversine en km entre deux points GPS."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def find_nearest_station(stations: list[dict], lat: float, lon: float) -> dict:
    """
    Retourne la station la plus proche des coordonnées de la ferme.

    Les stations sont attendues avec les champs 'lat' et 'lon'
    (ou 'latitude' / 'longitude' — les deux sont gérés).
    """
    if not stations:
        raise ValueError("Liste de stations vide")

    def _lat(s: dict) -> float:
        return s.get("lat") or s.get("latitude") or s["Latitude"]

    def _lon(s: dict) -> float:
        return s.get("lon") or s.get("longitude") or s["Longitude"]

    return min(stations, key=lambda s: _haversine_km(lat, lon, _lat(s), _lon(s)))


def rank_stations(stations: list[dict], lat: float, lon: float, n: int = MAX_NEARBY_STATIONS) -> list[tuple[float, dict]]:
    """Retourne les n stations les plus proches avec leur distance en km."""
    def _lat(s: dict) -> float:
        return s.get("lat") or s.get("latitude") or s["Latitude"]

    def _lon(s: dict) -> float:
        return s.get("lon") or s.get("longitude") or s["Longitude"]

    ranked = sorted(stations, key=lambda s: _haversine_km(lat, lon, _lat(s), _lon(s)))
    return [(round(_haversine_km(lat, lon, _lat(s), _lon(s)), 1), s) for s in ranked[:n]]


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
def response_to_dataframe(records: list[dict], station_lat: float, station_lon: float) -> pl.DataFrame:
    """
    Convertit les enregistrements JSON DPObs quotidien en DataFrame Polars
    normalisé au schéma projet.

    Les champs bruts (TX, TN, RR…) sont convertis selon MF_TO_PROJECT.
    Les valeurs manquantes (-9999 ou null) sont remplacées par null.

    Colonnes produites :
        date (Date), latitude (f64), longitude (f64),
        temperature_2m_max, temperature_2m_min, temperature_2m_mean,
        precipitation_sum, wind_speed_10m_max, et0_fao_evapotranspiration,
        sunshine_duration_min
    """
    if not records:
        raise ValueError("Aucun enregistrement DPObs à convertir")

    rows = []
    for rec in records:
        # La date peut être sous 'reference_time' (ISO 8601) ou 'DATE' (YYYY-MM-DD)
        raw_date = rec.get("reference_time") or rec.get("DATE") or rec.get("date")
        if not raw_date:
            continue
        # Normalise vers date Python
        if "T" in str(raw_date):
            parsed_date = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00")).date()
        else:
            parsed_date = date.fromisoformat(str(raw_date)[:10])

        row: dict = {"date": parsed_date}
        for mf_field, (proj_col, factor) in MF_TO_PROJECT.items():
            raw_val = rec.get(mf_field)
            if raw_val is None or raw_val == -9999 or raw_val == "-9999":
                row[proj_col] = None
            else:
                row[proj_col] = round(float(raw_val) * factor, 2)
        rows.append(row)

    if not rows:
        raise ValueError("Aucune ligne valide après parsing des enregistrements DPObs")

    df = pl.DataFrame(rows, infer_schema_length=len(rows))
    df = df.with_columns(pl.col("date").cast(pl.Date))
    df = df.with_columns(
        pl.lit(station_lat).cast(pl.Float64).alias("latitude"),
        pl.lit(station_lon).cast(pl.Float64).alias("longitude"),
    )

    # Réordonner : date, lat, lon, puis métriques
    fixed = ["date", "latitude", "longitude"]
    other = [c for c in df.columns if c not in fixed]
    return df.select(fixed + other).sort("date")


# ---------------------------------------------------------------------------
# Pipeline (mêmes fonctions que open_meteo.py)
# ---------------------------------------------------------------------------
def compute_backfill_start(end: date, years: int) -> date:
    """Borne de début du backfill : 1er janvier de (end.year - years + 1)."""
    if years < 1:
        raise ValueError("Le backfill doit couvrir au moins 1 an")
    return date(end.year - years + 1, 1, 1)


def load_existing_data(processed_dir: Path) -> pl.DataFrame | None:
    """Charge les Parquet Météo-France existants s'ils sont présents."""
    files = sorted(processed_dir.glob("*.parquet"))
    if not files:
        return None
    return pl.read_parquet(files).sort("date")


def merge_weather_data(existing_df: pl.DataFrame | None, new_df: pl.DataFrame) -> pl.DataFrame:
    """Fusionne en gardant la version la plus récente par date."""
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
    """Détermine la borne de début à récupérer selon l'état local."""
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
    """Sauvegarde en Parquet partitionné par année (remplace si existant)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    years = df["date"].dt.year().unique().sort().to_list()
    written = []
    for year in years:
        year_df = df.filter(pl.col("date").dt.year() == year)
        path = output_dir / f"meteo_france_{year}.parquet"
        year_df.write_parquet(path, compression="zstd")
        log.info("  ✓ %s  (%d jours)", path.name, len(year_df))
        written.append(path)
    return written


# ---------------------------------------------------------------------------
# Vérification DuckDB
# ---------------------------------------------------------------------------
def verify(processed_dir: Path) -> None:
    """Interroge les Parquet via DuckDB et affiche un résumé."""
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
        print("\n── Vérification datalake Météo-France ───────────────────")
        print(result.to_string(index=False))
        print("─────────────────────────────────────────────────────────\n")
    except Exception as e:
        log.error("Erreur DuckDB : %s", e)
        raise


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
def scheduled_refresh(
    station_id: str,
    station_lat: float,
    station_lon: float,
    backfill_years: int | None = None,
    refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS,
) -> None:
    """Job APScheduler : refresh incrémental Météo-France."""
    run(
        station_id=station_id,
        station_lat=station_lat,
        station_lon=station_lon,
        backfill_years=backfill_years,
        refresh_lookback_days=refresh_lookback_days,
        verify_only=False,
        force_full_refresh=False,
    )


def start_scheduler(
    station_id: str,
    station_lat: float,
    station_lon: float,
    hour: int = DEFAULT_SCHEDULE_HOUR,
    minute: int = DEFAULT_SCHEDULE_MINUTE,
    backfill_years: int | None = None,
    refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS,
    run_immediately: bool = True,
) -> None:
    """Lance un scheduler quotidien local pour Météo-France."""
    cfg = load_config()
    timezone_str = cfg["farm"]["timezone"]
    scheduler = BlockingScheduler(timezone=timezone_str)
    scheduler.add_job(
        scheduled_refresh,
        CronTrigger(hour=hour, minute=minute, timezone=timezone_str),
        kwargs={
            "station_id": station_id,
            "station_lat": station_lat,
            "station_lon": station_lon,
            "backfill_years": backfill_years,
            "refresh_lookback_days": refresh_lookback_days,
        },
        id="meteo_france_daily_refresh",
        replace_existing=True,
    )
    log.info(
        "Scheduler Météo-France actif : refresh quotidien à %02d:%02d (%s)",
        hour, minute, timezone_str,
    )
    if run_immediately:
        log.info("Exécution immédiate avant planification")
        scheduled_refresh(
            station_id=station_id,
            station_lat=station_lat,
            station_lon=station_lon,
            backfill_years=backfill_years,
            refresh_lookback_days=refresh_lookback_days,
        )
    scheduler.start()


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------
def run(
    station_id: str | None = None,
    station_lat: float | None = None,
    station_lon: float | None = None,
    backfill_years: int | None = None,
    refresh_lookback_days: int = DEFAULT_REFRESH_LOOKBACK_DAYS,
    verify_only: bool = False,
    force_full_refresh: bool = False,
) -> pl.DataFrame | None:
    """
    Orchestre le backfill ou le refresh incrémental Météo-France.

    Si station_id est None, sélectionne automatiquement la station la plus
    proche des coordonnées de la ferme dans config.toml.
    """
    cfg = load_config()
    api_key = _resolve_api_key(cfg)
    farm_lat: float = cfg["farm"]["latitude"]
    farm_lon: float = cfg["farm"]["longitude"]
    years: int = backfill_years or cfg.get("meteo_france", {}).get("historical_years") or cfg["meteo"]["historical_years"]
    processed_dir = Path(cfg["paths"]["processed"]) / "meteo_france"

    if verify_only:
        verify(processed_dir)
        return None

    # Sélection de station si non fournie
    if station_id is None:
        log.info("Récupération de la liste des stations DPObs…")
        stations = fetch_stations(api_key)
        nearest = find_nearest_station(stations, farm_lat, farm_lon)

        # Extraction des coords de la station (clés variables selon l'API)
        station_id = str(nearest.get("id") or nearest.get("ID") or nearest["id_station"])
        station_lat = float(nearest.get("lat") or nearest.get("latitude") or nearest["Latitude"])
        station_lon = float(nearest.get("lon") or nearest.get("longitude") or nearest["Longitude"])
        station_name = nearest.get("nom") or nearest.get("name") or nearest.get("Nom", station_id)
        dist_km = round(_haversine_km(farm_lat, farm_lon, station_lat, station_lon), 1)
        log.info("Station sélectionnée : %s (id=%s) — à %.1f km", station_name, station_id, dist_km)
    else:
        station_lat = station_lat or farm_lat
        station_lon = station_lon or farm_lon

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
        log.info("Aucune nouvelle fenêtre à récupérer (données à jour)")
        return existing_df

    action = "backfill complet" if mode == "full" else "refresh incrémental"
    log.info("Météo-France %s : station %s | %s → %s", action, station_id, start, end)

    records = fetch_daily(station_id, start, end, api_key)
    df = response_to_dataframe(records, station_lat, station_lon)
    merged_df = merge_weather_data(existing_df, df)

    log.info("  %d jours récupérés (%d jours cumulés)", len(df), len(merged_df))
    save_parquet(merged_df, processed_dir)
    log.info("Ingestion Météo-France terminée ✓")
    return merged_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingestion Météo-France — observations quotidiennes officielles"
    )
    parser.add_argument(
        "--years", type=int, default=None,
        help="Nombre d'années d'historique (défaut : config.toml → meteo_france.historical_years)",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_REFRESH_LOOKBACK_DAYS,
        help="Fenêtre de recouvrement pour le refresh incrémental (défaut : %(default)s j)",
    )
    parser.add_argument(
        "--station", type=str, default=None, metavar="ID",
        help="ID de station Météo-France à utiliser (défaut : station la plus proche de la ferme)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Re-télécharger tout l'historique même si des Parquet existent déjà",
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Vérifier les Parquet existants sans re-télécharger",
    )
    parser.add_argument(
        "--list-stations", action="store_true",
        help=f"Lister les {MAX_NEARBY_STATIONS} stations DPObs les plus proches de la ferme",
    )
    parser.add_argument(
        "--schedule", action="store_true",
        help="Lancer un scheduler quotidien local pour le refresh",
    )
    parser.add_argument(
        "--hour", type=int, default=DEFAULT_SCHEDULE_HOUR,
        help="Heure du refresh quotidien (défaut : %(default)s)",
    )
    parser.add_argument(
        "--minute", type=int, default=DEFAULT_SCHEDULE_MINUTE,
        help="Minute du refresh quotidien (défaut : %(default)s)",
    )
    parser.add_argument(
        "--no-run-immediately", action="store_true",
        help="Ne pas lancer de refresh immédiat au démarrage du scheduler",
    )
    args = parser.parse_args()

    cfg = load_config()
    api_key = _resolve_api_key(cfg)
    farm_lat: float = cfg["farm"]["latitude"]
    farm_lon: float = cfg["farm"]["longitude"]

    if args.list_stations:
        log.info("Récupération de la liste des stations DPObs…")
        stations = fetch_stations(api_key)
        ranked = rank_stations(stations, farm_lat, farm_lon)
        print(f"\n── {MAX_NEARBY_STATIONS} stations les plus proches ({farm_lat}°N, {farm_lon}°E) ──")
        for dist_km, s in ranked:
            sid = s.get("id") or s.get("ID") or s.get("id_station", "?")
            nom = s.get("nom") or s.get("name") or s.get("Nom", sid)
            dept = s.get("departement") or s.get("dep", "")
            alt = s.get("altitude") or s.get("Altitude", "?")
            print(f"  {dist_km:6.1f} km  |  {sid:<12}  |  {nom:<30}  |  dept={dept}  |  alt={alt}m")
        print()
        return

    if args.schedule:
        # Sélectionner la station avant de lancer le scheduler
        log.info("Sélection de la station pour le scheduler…")
        station_id = args.station
        station_lat, station_lon = farm_lat, farm_lon
        if station_id is None:
            stations = fetch_stations(api_key)
            nearest = find_nearest_station(stations, farm_lat, farm_lon)
            station_id = str(nearest.get("id") or nearest.get("ID") or nearest["id_station"])
            station_lat = float(nearest.get("lat") or nearest.get("latitude") or nearest["Latitude"])
            station_lon = float(nearest.get("lon") or nearest.get("longitude") or nearest["Longitude"])

        start_scheduler(
            station_id=station_id,
            station_lat=station_lat,
            station_lon=station_lon,
            hour=args.hour,
            minute=args.minute,
            backfill_years=args.years,
            refresh_lookback_days=args.lookback_days,
            run_immediately=not args.no_run_immediately,
        )
        return

    run(
        station_id=args.station,
        backfill_years=args.years,
        refresh_lookback_days=args.lookback_days,
        verify_only=args.verify,
        force_full_refresh=args.full_refresh,
    )


if __name__ == "__main__":
    main()
