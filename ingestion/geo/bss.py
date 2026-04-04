"""
Ingestion BRGM BSS (Banque du Sous-Sol) — Hub'eau API

Récupère les stations piézométriques et forages référencés dans un rayon
autour des coordonnées de la ferme. Données utiles pour évaluer l'accès
à l'eau souterraine sur une parcelle candidate.

API Hub'eau — Niveaux nappes / Eaux souterraines :
    Endpoint : https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations
    Auth     : aucune (open data)
    Docs     : https://hubeau.eaufrance.fr/page/api-niveaux-nappes

Usage :
    python -m ingestion.geo.bss                     # config.toml (coords + rayon)
    python -m ingestion.geo.bss --radius 15         # rayon 15 km
    python -m ingestion.geo.bss --verify            # vérifie le Parquet existant
    python -m ingestion.geo.bss --show              # affiche les stations les plus proches
"""
from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path

import duckdb
import polars as pl
import requests
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn
from rich.table import Table
from rich.console import Console

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
log = logging.getLogger("bss")

# ---------------------------------------------------------------------------
# Constantes API Hub'eau
# ---------------------------------------------------------------------------
HUBEAU_BASE = "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes"
STATIONS_ENDPOINT = f"{HUBEAU_BASE}/stations"
PAGE_SIZE = 2000   # raisonnable par requête
MAX_OFFSET = 20000  # Hub'eau v1 : page × size ≤ 20 000

# Champs retournés par Hub'eau qu'on conserve
FIELDS = ",".join([
    "code_bss",
    "bss_id",
    "libelle_pe",
    "nom_commune",
    "code_commune_insee",
    "code_departement",
    "nom_departement",
    "x",                        # longitude WGS84 (nommé "x" dans Hub'eau)
    "y",                        # latitude  WGS84 (nommé "y" dans Hub'eau)
    "altitude_station",
    "profondeur_investigation",
    "date_debut_mesure",
    "date_fin_mesure",
    "nb_mesures_piezo",
    "codes_bdlisa",             # liste → on prend le premier
    "noms_masse_eau_edl",       # masse d'eau souterraine
])

# Schéma cible normalisé
TARGET_SCHEMA: dict[str, type] = {
    "code_bss":              pl.Utf8,
    "bss_id":                pl.Utf8,
    "libelle_pe":            pl.Utf8,
    "nom_commune":           pl.Utf8,
    "code_commune_insee":    pl.Utf8,
    "code_departement":      pl.Utf8,
    "nom_departement":       pl.Utf8,
    "longitude":             pl.Float64,  # champ "x" dans l'API, renommé
    "latitude":              pl.Float64,  # champ "y" dans l'API, renommé
    "altitude_station":      pl.Float64,  # string dans l'API → castée
    "profondeur_investigation": pl.Float64,
    "date_debut_mesure":     pl.Utf8,
    "date_fin_mesure":       pl.Utf8,
    "nb_mesures_piezo":      pl.Int64,
    "code_bdlisa":           pl.Utf8,     # codes_bdlisa[0]
    "nom_masse_eau":         pl.Utf8,     # noms_masse_eau_edl[0]
    "distance_km":           pl.Float64,  # calculé localement
}


# ---------------------------------------------------------------------------
# Géométrie
# ---------------------------------------------------------------------------
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance haversine en km entre deux points WGS84."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _bbox(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """
    Bounding box WGS84 autour d'un point.
    Retourne (lon_min, lat_min, lon_max, lat_max) — format Hub'eau.
    """
    dlat = radius_km / 111.32
    dlon = radius_km / (111.32 * math.cos(math.radians(lat)))
    return lon - dlon, lat - dlat, lon + dlon, lat + dlat


# ---------------------------------------------------------------------------
# Appel API Hub'eau
# ---------------------------------------------------------------------------
def fetch_stations(
    lat: float,
    lon: float,
    radius_km: float,
    timeout: int = 30,
) -> list[dict]:
    """
    Récupère les stations BSS dans un rayon autour du point via filtre bbox.

    Notes Hub'eau v1 :
    - Le paramètre `distance` n'est pas supporté sur /niveaux_nappes/stations.
      On utilise un filtre bbox calculé depuis lat/lon/radius.
    - Offset max : page × size ≤ 20 000. On s'arrête proprement à cette limite
      et on filtre ensuite par distance réelle (haversine).
    """
    lon_min, lat_min, lon_max, lat_max = _bbox(lat, lon, radius_km)
    bbox_str = f"{lon_min:.6f},{lat_min:.6f},{lon_max:.6f},{lat_max:.6f}"

    params = {
        "bbox":   bbox_str,
        "fields": FIELDS,
        "size":   PAGE_SIZE,
        "page":   1,
        "format": "json",
    }

    all_stations: list[dict] = []
    truncated = False

    with Progress(SpinnerColumn(), "[progress.description]{task.description}", BarColumn()) as progress:
        task = progress.add_task("Requête Hub'eau BSS...", total=None)

        while True:
            try:
                resp = requests.get(STATIONS_ENDPOINT, params=params, timeout=timeout)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise RuntimeError(f"Hub'eau API erreur {resp.status_code}: {e}") from e
            except requests.exceptions.ConnectionError as e:
                raise RuntimeError(
                    "Impossible de joindre Hub'eau (hubeau.eaufrance.fr). "
                    "Vérifie ta connexion internet."
                ) from e

            data = resp.json()
            stations = data.get("data", [])
            all_stations.extend(stations)

            total = data.get("count", 0)
            progress.update(
                task,
                description=f"Hub'eau BSS — {len(all_stations)}/{total} stations dans la bbox...",
                total=total,
                completed=len(all_stations),
            )

            fetched = len(all_stations)
            if fetched >= total or len(stations) < PAGE_SIZE:
                break

            # Limite Hub'eau : page × size ≤ 20 000
            if fetched + PAGE_SIZE > MAX_OFFSET:
                truncated = True
                break

            params["page"] += 1

    # Filtre haversine post-fetch : ne garder que les stations dans le rayon réel
    # Hub'eau nomme les coordonnées "x" (longitude) et "y" (latitude)
    before = len(all_stations)
    all_stations = [
        s for s in all_stations
        if s.get("y") is not None and s.get("x") is not None
        and _haversine_km(lat, lon, float(s["y"]), float(s["x"])) <= radius_km
    ]

    log.info(
        f"  bbox → {before} stations | filtre {radius_km} km → {len(all_stations)} retenues"
    )
    if truncated:
        log.warning(
            f"  ⚠ Offset Hub'eau plafonné à {MAX_OFFSET}. "
            f"Certaines stations dans la bbox ont pu être omises. "
            f"Réduis le rayon si la zone est dense."
        )

    return all_stations


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
def stations_to_dataframe(stations: list[dict], farm_lat: float, farm_lon: float) -> pl.DataFrame:
    """
    Convertit la liste de stations Hub'eau en DataFrame Polars normalisé.
    Ajoute la distance calculée par rapport aux coordonnées de la ferme.
    """
    if not stations:
        return pl.DataFrame(schema=TARGET_SCHEMA)

    rows = []
    for s in stations:
        raw_lon = s.get("x")   # Hub'eau : x = longitude
        raw_lat = s.get("y")   # Hub'eau : y = latitude
        lon_f = float(raw_lon) if raw_lon is not None else None
        lat_f = float(raw_lat) if raw_lat is not None else None
        dist = _haversine_km(farm_lat, farm_lon, lat_f, lon_f) if (lat_f and lon_f) else None

        # codes_bdlisa et noms_masse_eau_edl sont des listes → premier élément
        codes_bdlisa = s.get("codes_bdlisa") or []
        noms_eau = s.get("noms_masse_eau_edl") or []

        # altitude_station est retournée comme string par l'API
        alt = s.get("altitude_station")
        try:
            alt_f = float(alt) if alt is not None else None
        except (ValueError, TypeError):
            alt_f = None

        rows.append({
            "code_bss":                 s.get("code_bss"),
            "bss_id":                   s.get("bss_id"),
            "libelle_pe":               s.get("libelle_pe"),
            "nom_commune":              s.get("nom_commune"),
            "code_commune_insee":       s.get("code_commune_insee"),
            "code_departement":         s.get("code_departement"),
            "nom_departement":          s.get("nom_departement"),
            "longitude":                lon_f,
            "latitude":                 lat_f,
            "altitude_station":         alt_f,
            "profondeur_investigation": s.get("profondeur_investigation"),
            "date_debut_mesure":        s.get("date_debut_mesure"),
            "date_fin_mesure":          s.get("date_fin_mesure"),
            "nb_mesures_piezo":         s.get("nb_mesures_piezo"),
            "code_bdlisa":              codes_bdlisa[0] if codes_bdlisa else None,
            "nom_masse_eau":            noms_eau[0] if noms_eau else None,
            "distance_km":              round(dist, 2) if dist is not None else None,
        })

    df = pl.DataFrame(rows, schema=TARGET_SCHEMA)
    return df.sort("distance_km")


# ---------------------------------------------------------------------------
# Sauvegarde
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, processed_dir: Path) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / "bss_stations.parquet"
    df.write_parquet(path, compression="zstd")
    log.info(f"  ✓ {path.name}  ({len(df):,} stations)")
    return path


# ---------------------------------------------------------------------------
# Vérification
# ---------------------------------------------------------------------------
def verify(processed_dir: Path) -> None:
    path = str(processed_dir / "bss_stations.parquet")
    try:
        result = duckdb.sql(f"""
            SELECT
                COUNT(*)                               AS n_stations,
                COUNT(DISTINCT code_departement)       AS n_depts,
                ROUND(MIN(distance_km), 1)             AS dist_min_km,
                ROUND(MAX(distance_km), 1)             AS dist_max_km,
                ROUND(AVG(profondeur_investigation), 1) AS prof_moy_m,
                COUNT(DISTINCT code_bdlisa)            AS n_masses_eau
            FROM read_parquet('{path}')
        """).fetchone()
        if result:
            print("\n── Vérification BSS ─────────────────────────────────────")
            print(f"  Stations          : {result[0]:,}")
            print(f"  Départements      : {result[1]}")
            print(f"  Distance          : {result[2]} → {result[3]} km")
            print(f"  Profondeur moy.   : {result[4]} m")
            print(f"  Masses d'eau      : {result[5]}")
            print("─────────────────────────────────────────────────────────\n")
    except Exception as e:
        log.error(f"Erreur vérification : {e}")


def show_nearest(processed_dir: Path, n: int = 15) -> None:
    """Affiche les N stations les plus proches avec Rich."""
    path = str(processed_dir / "bss_stations.parquet")
    rows = duckdb.sql(f"""
        SELECT
            code_bss,
            nom_commune,
            libelle_pe,
            ROUND(distance_km, 1)          AS dist_km,
            profondeur_investigation       AS prof_m,
            nb_mesures_piezo               AS n_mesures,
            date_fin_mesure
        FROM read_parquet('{path}')
        ORDER BY distance_km
        LIMIT {n}
    """).fetchall()

    console = Console()
    table = Table(title=f"BSS — {n} stations les plus proches")
    for col in ["Code BSS", "Commune", "Libellé", "Dist. (km)", "Prof. (m)", "N mesures", "Dernière MAJ"]:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(v) if v is not None else "—" for v in row])
    console.print(table)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(radius_km: float | None = None, verify_only: bool = False, show: bool = False) -> pl.DataFrame | None:
    cfg = load_config()
    lat = cfg["farm"]["latitude"]
    lon = cfg["farm"]["longitude"]
    r = radius_km if radius_km is not None else cfg.get("geo", {}).get("rayon_km", 25)
    processed_dir = Path(cfg["paths"]["processed"]) / "geo"

    if verify_only:
        verify(processed_dir)
        return None

    if show:
        show_nearest(processed_dir)
        return None

    log.info(f"Ingestion BSS — {lat}°N {lon}°E, rayon {r} km")

    stations = fetch_stations(lat, lon, r)

    if not stations:
        log.warning("Aucune station BSS trouvée dans la zone.")
        return pl.DataFrame(schema=TARGET_SCHEMA)

    df = stations_to_dataframe(stations, lat, lon)

    # Résumé par département
    summary = (
        df.group_by("nom_departement")
        .agg(pl.len().alias("n"), pl.col("profondeur_investigation").mean().round(1).alias("prof_moy_m"))
        .sort("n", descending=True)
    )
    log.info("\n  Stations par département :")
    for row in summary.iter_rows(named=True):
        log.info(f"    {(row['nom_departement'] or 'Inconnu'):30s} {row['n']:4d}  prof. moy. {row['prof_moy_m'] or '—'} m")

    save_parquet(df, processed_dir)
    log.info("Ingestion BSS terminée ✓")
    return df


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion BRGM BSS — Hub'eau API")
    parser.add_argument("--radius", type=float, default=None, help="Rayon km (défaut: config.toml)")
    parser.add_argument("--verify", action="store_true", help="Vérifie le Parquet existant")
    parser.add_argument("--show", action="store_true", help="Affiche les stations les plus proches")
    args = parser.parse_args()
    run(radius_km=args.radius, verify_only=args.verify, show=args.show)


if __name__ == "__main__":
    main()
