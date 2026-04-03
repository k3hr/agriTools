"""
Ingestion RPG (Registre Parcellaire Graphique) — IGN Géoplateforme

Le RPG recense les îlots culturaux déclarés à la PAC (surface, culture principale).
Mis à jour annuellement, décalage d'environ 1 an.

Source principale : https://data.geopf.fr/telechargement/download/RPG/
Source secondaire : https://geoservices.ign.fr/rpg

Le script utilise DuckDB Spatial pour lire les GeoPackage/GeoJSON sans dépendance
GDAL/geopandas — DuckDB télécharge l'extension spatial automatiquement au premier run.

Usage :
    python -m ingestion.geo.rpg                          # depuis config.toml
    python -m ingestion.geo.rpg --dept 72                # Sarthe
    python -m ingestion.geo.rpg --dept 72 53             # Sarthe + Mayenne
    python -m ingestion.geo.rpg --year 2023              # année spécifique
    python -m ingestion.geo.rpg --from-file file.gpkg    # fichier local déjà téléchargé
    python -m ingestion.geo.rpg --list-years             # années disponibles sur IGN
"""
from __future__ import annotations

import argparse
import logging
import math
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import polars as pl
import requests
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, DownloadColumn, TransferSpeedColumn, BarColumn, TimeRemainingColumn

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
log = logging.getLogger("rpg")

# ---------------------------------------------------------------------------
# Constantes IGN Géoplateforme
# ---------------------------------------------------------------------------
# API catalogue pour découvrir les fichiers disponibles
GEOPF_CATALOG_URL = "https://data.geopf.fr/telechargement/collections/RPG/items"

# Pattern URL de téléchargement direct (fallback si API indisponible)
# Format IGN : RPG_2-0__GPKG_LAMB93_D{dept}_{year}-01-01
GEOPF_DOWNLOAD_PATTERN = (
    "https://data.geopf.fr/telechargement/download/RPG/"
    "RPG_2-0__GPKG_LAMB93_D{dept}_{year}-01-01/"
    "RPG_2-0__GPKG_LAMB93_D{dept}_{year}-01-01.7z"
)

# Codes culture PAC → libellés lisibles
CODES_CULTURE = {
    "1":  "Blé tendre", "2":  "Blé dur", "3":  "Orge", "4": "Autres céréales",
    "5":  "Maïs grain et ensilage", "6": "Oléagineux", "7": "Protéagineux",
    "8":  "Légumineuses à grains", "11": "Prairies permanentes",
    "12": "Prairies temporaires", "13": "Estives et landes",
    "14": "Gel", "15": "Légumes ou fleurs",
    "16": "Arboriculture", "17": "Viticulture",
    "18": "Vergers", "19": "Fruits à coque",
    "20": "Autres cultures industrielles",
    "23": "Chanvre", "24": "Lin",
    "25": "Divers", "28": "Semences",
}


# ---------------------------------------------------------------------------
# Découverte des fichiers disponibles
# ---------------------------------------------------------------------------
def list_available_years(dept: str, timeout: int = 15) -> list[int]:
    """
    Interroge l'API catalogue IGN pour lister les années disponibles
    pour un département donné.
    Retourne une liste triée décroissante (plus récent en premier).
    """
    try:
        params = {
            "limit": 50,
            "filter": f"id LIKE '%D{dept.zfill(3)}%'",
        }
        resp = requests.get(GEOPF_CATALOG_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        items = resp.json().get("features", [])
        years = []
        for item in items:
            # ID format attendu : RPG_2-0__GPKG_LAMB93_D072_2023-01-01
            item_id = item.get("id", "")
            for part in item_id.split("_"):
                if part.startswith("20") and len(part) == 4 and part.isdigit():
                    years.append(int(part))
                    break
        return sorted(set(years), reverse=True)
    except Exception as e:
        log.warning(f"API catalogue IGN inaccessible ({e}). Fallback sur années connues.")
        return [2023, 2022, 2021]


def resolve_download_url(dept: str, year: int | None = None, timeout: int = 15) -> tuple[str, int]:
    """
    Retourne (url, annee) pour le téléchargement d'un département RPG.
    Si year=None, utilise la dernière année disponible.
    """
    dept_padded = dept.zfill(3)
    years = list_available_years(dept, timeout)

    if year is not None:
        if year not in years:
            log.warning(f"Année {year} peut ne pas être disponible pour le dept {dept}. Tentative quand même.")
        target_year = year
    else:
        target_year = years[0] if years else 2023

    url = GEOPF_DOWNLOAD_PATTERN.format(dept=dept_padded, year=target_year)
    return url, target_year


# ---------------------------------------------------------------------------
# Téléchargement
# ---------------------------------------------------------------------------
def download_file(url: str, dest: Path, timeout: int = 300) -> Path:
    """
    Télécharge url → dest avec barre de progression.
    Supporte les redirections.
    """
    log.info(f"Téléchargement : {url}")
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(dest.name, total=total or None)
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)
                    progress.advance(task, len(chunk))
    log.info(f"  Téléchargé → {dest} ({dest.stat().st_size / 1e6:.1f} Mo)")
    return dest


def extract_archive(archive: Path, dest_dir: Path) -> Path:
    """
    Extrait l'archive (ZIP ou 7z) dans dest_dir.
    Retourne le premier fichier GeoPackage (.gpkg) trouvé.
    Nécessite 7z installé sur le système pour les archives .7z.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    if archive.suffix == ".zip":
        log.info("Extraction ZIP...")
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(dest_dir)

    elif archive.suffix == ".7z":
        log.info("Extraction 7z...")
        # Tente d'abord py7zr (pip), puis 7z système
        try:
            import py7zr
            with py7zr.SevenZipFile(archive, mode="r") as zf:
                zf.extractall(dest_dir)
        except ImportError:
            import subprocess
            result = subprocess.run(
                ["7z", "x", str(archive), f"-o{dest_dir}", "-y"],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    "Impossible d'extraire le .7z. Installe py7zr (`uv pip install py7zr`) "
                    "ou 7-Zip (https://www.7-zip.org/).\n"
                    f"Erreur : {result.stderr}"
                )
    else:
        raise ValueError(f"Format d'archive non supporté : {archive.suffix}")

    # Trouve le GeoPackage
    gpkg_files = list(dest_dir.rglob("*.gpkg"))
    if not gpkg_files:
        raise FileNotFoundError(f"Aucun fichier .gpkg trouvé dans {dest_dir}")

    log.info(f"  GeoPackage trouvé : {gpkg_files[0].name}")
    return gpkg_files[0]


# ---------------------------------------------------------------------------
# Lecture & filtrage spatial via DuckDB Spatial
# ---------------------------------------------------------------------------
def _bbox_from_center(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """
    Calcule une bounding box WGS84 autour d'un point (lat, lon) avec un rayon en km.
    Retourne (min_lon, min_lat, max_lon, max_lat).
    """
    # 1° de latitude ≈ 111.32 km
    delta_lat = radius_km / 111.32
    # 1° de longitude ≈ 111.32 * cos(lat) km
    delta_lon = radius_km / (111.32 * math.cos(math.radians(lat)))
    return (
        lon - delta_lon,
        lat - delta_lat,
        lon + delta_lon,
        lat + delta_lat,
    )


def read_and_filter_gpkg(
    gpkg_path: Path,
    lat: float,
    lon: float,
    radius_km: float,
) -> pl.DataFrame:
    """
    Lit un GeoPackage RPG via DuckDB Spatial, filtre spatialement,
    et retourne un DataFrame Polars avec géométrie en WKT (EPSG:4326).

    DuckDB Spatial est installé automatiquement au premier appel.
    Le GeoPackage IGN est en Lambert 93 (EPSG:2154) — transformation vers WGS84 incluse.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    if radius_km > 0:
        min_lon, min_lat, max_lon, max_lat = _bbox_from_center(lat, lon, radius_km)
        log.info(f"  Filtre spatial : rayon {radius_km}km autour de {lat:.4f}°N {lon:.4f}°E")
        log.info(f"  BBox WGS84 : lon [{min_lon:.4f}, {max_lon:.4f}] lat [{min_lat:.4f}, {max_lat:.4f}]")

        # Transforme la bbox en Lambert 93 pour le filtre
        # puis reprojette la géométrie en WGS84 pour le stockage
        query = f"""
            SELECT
                CAST(ID_PARCEL AS VARCHAR)   AS id_parcel,
                CODE_CULTU                   AS code_culture,
                SURF_PARC                    AS surface_ha,
                ST_AsText(
                    ST_Transform(geom, 'EPSG:2154', 'EPSG:4326')
                )                            AS geometry_wkt
            FROM ST_Read('{gpkg_path}')
            WHERE ST_Intersects(
                ST_Transform(geom, 'EPSG:2154', 'EPSG:4326'),
                ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat})
            )
        """
    else:
        log.info("  Pas de filtre spatial — département complet")
        query = f"""
            SELECT
                CAST(ID_PARCEL AS VARCHAR)   AS id_parcel,
                CODE_CULTU                   AS code_culture,
                SURF_PARC                    AS surface_ha,
                ST_AsText(
                    ST_Transform(geom, 'EPSG:2154', 'EPSG:4326')
                )                            AS geometry_wkt
            FROM ST_Read('{gpkg_path}')
        """

    log.info("  Lecture GeoPackage via DuckDB Spatial...")
    result = conn.execute(query).fetchall()
    conn.close()

    if not result:
        log.warning("  ⚠ Aucune parcelle dans la zone. Vérifie les coordonnées et le rayon.")
        return pl.DataFrame(schema={
            "id_parcel": pl.Utf8,
            "code_culture": pl.Utf8,
            "surface_ha": pl.Float64,
            "geometry_wkt": pl.Utf8,
        })

    df = pl.DataFrame(
        {
            "id_parcel":    [r[0] for r in result],
            "code_culture": [r[1] for r in result],
            "surface_ha":   [r[2] for r in result],
            "geometry_wkt": [r[3] for r in result],
        }
    )

    # Enrichit avec le libellé culture
    df = df.with_columns(
        pl.col("code_culture")
        .map_elements(lambda c: CODES_CULTURE.get(str(c), "Inconnu"), return_dtype=pl.Utf8)
        .alias("libelle_culture")
    )

    return df


# ---------------------------------------------------------------------------
# Sauvegarde Parquet
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, output_dir: Path, dept: str, year: int) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"rpg_D{dept.zfill(3)}_{year}.parquet"
    df.write_parquet(path, compression="zstd")
    log.info(f"  ✓ {path.name}  ({len(df):,} parcelles, {df['surface_ha'].sum():.0f} ha)")
    return path


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(
    depts: list[str] | None = None,
    year: int | None = None,
    from_file: Path | None = None,
) -> dict[str, pl.DataFrame]:
    cfg = load_config()
    lat = cfg["farm"]["latitude"]
    lon = cfg["farm"]["longitude"]
    radius_km = cfg.get("geo", {}).get("rayon_km", 25)
    cfg_depts = cfg.get("geo", {}).get("departements", ["72"])
    cfg_year = cfg.get("geo", {}).get("annee_rpg", None)

    target_depts = depts or cfg_depts
    target_year = year or cfg_year  # None = auto-détecté

    raw_dir = Path(cfg["paths"]["raw"]) / "geo" / "rpg"
    processed_dir = Path(cfg["paths"]["processed"]) / "geo"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    for dept in target_depts:
        log.info(f"\n── Département {dept} ──────────────────────────────────────")

        if from_file:
            gpkg_path = from_file
            resolved_year = target_year or 2023
        else:
            url, resolved_year = resolve_download_url(dept, target_year)
            archive_name = url.split("/")[-1]
            archive_path = raw_dir / archive_name

            if not archive_path.exists():
                download_file(url, archive_path)
            else:
                log.info(f"  Archive déjà présente : {archive_path.name}")

            extract_dir = raw_dir / archive_path.stem
            gpkg_path = extract_archive(archive_path, extract_dir)

        df = read_and_filter_gpkg(gpkg_path, lat, lon, radius_km)
        log.info(f"  {len(df):,} parcelles extraites")

        path = save_parquet(df, processed_dir, dept, resolved_year)
        results[dept] = df

        # Résumé par culture
        if len(df) > 0:
            summary = (
                df.group_by("libelle_culture")
                .agg(
                    pl.len().alias("n_parcelles"),
                    pl.col("surface_ha").sum().alias("surface_ha"),
                )
                .sort("surface_ha", descending=True)
                .head(8)
            )
            log.info("  Top cultures dans la zone :")
            for row in summary.iter_rows(named=True):
                log.info(f"    {row['libelle_culture']:30s} {row['n_parcelles']:4d} parcelles  {row['surface_ha']:8.1f} ha")

    log.info("\nIngestion RPG terminée ✓")
    return results


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion RPG — IGN Géoplateforme")
    parser.add_argument("--dept", nargs="+", help="Code(s) département (ex: 72 53)")
    parser.add_argument("--year", type=int, help="Année du RPG (défaut: dernière disponible)")
    parser.add_argument("--from-file", type=Path, help="Fichier GeoPackage local (skip téléchargement)")
    parser.add_argument("--list-years", action="store_true", help="Liste les années disponibles")
    args = parser.parse_args()

    if args.list_years:
        cfg = load_config()
        depts = args.dept or cfg.get("geo", {}).get("departements", ["72"])
        for dept in depts:
            years = list_available_years(dept)
            print(f"Département {dept} : {years}")
        return

    run(
        depts=args.dept,
        year=args.year,
        from_file=args.from_file,
    )


if __name__ == "__main__":
    main()
