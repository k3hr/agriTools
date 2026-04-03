"""
Ingestion RPG (Registre Parcellaire Graphique) — IGN Géoplateforme WFS

Le RPG recense les îlots culturaux déclarés à la PAC (surface, culture principale).
Mis à jour annuellement (décalage ~1 an). Dernière version disponible : 2023.

Stratégie : WFS paginé avec filtre BBOX → ne télécharge que les parcelles
autour de la ferme. Pas de téléchargement de la France entière (plusieurs GB).

WFS Géoplateforme IGN :
    Endpoint   : https://data.geopf.fr/wfs/ows
    Layer 2023 : RPG.2023:parcelles_graphiques
    Layer 2022 : RPG.2022:parcelles_graphiques

Usage :
    python -m ingestion.geo.rpg                    # config.toml (année + bbox)
    python -m ingestion.geo.rpg --year 2022        # année spécifique
    python -m ingestion.geo.rpg --radius 50        # rayon 50 km
    python -m ingestion.geo.rpg --verify           # vérifie le Parquet existant
    python -m ingestion.geo.rpg --list-layers      # liste les couches RPG disponibles
    python -m ingestion.geo.rpg --from-file f.gpkg # fichier local (fallback)
"""
from __future__ import annotations

import argparse
import logging
import math
import zipfile
from pathlib import Path

import duckdb
import polars as pl
import requests
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn

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
# Constantes WFS Géoplateforme IGN
# ---------------------------------------------------------------------------
WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
WFS_LAYER_PATTERN = "RPG.{year}:parcelles_graphiques"
WFS_PAGE_SIZE = 1000          # nombre de features par requête WFS
LATEST_KNOWN_YEAR = 2023      # dernière année confirmée disponible

# Codes culture PAC → libellés
CODES_CULTURE: dict[str, str] = {
    "1":  "Blé tendre",       "2":  "Blé dur",          "3":  "Orge",
    "4":  "Autres céréales",  "5":  "Maïs",              "6":  "Oléagineux",
    "7":  "Protéagineux",     "8":  "Légumineuses",      "11": "Prairies permanentes",
    "12": "Prairies temporaires", "13": "Estives et landes",
    "14": "Gel",              "15": "Légumes ou fleurs", "16": "Arboriculture",
    "17": "Viticulture",      "18": "Vergers",           "19": "Fruits à coque",
    "20": "Cultures industrielles", "23": "Chanvre",     "24": "Lin",
    "25": "Divers",           "28": "Semences",
}


# ---------------------------------------------------------------------------
# Géométrie
# ---------------------------------------------------------------------------
def _bbox(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """
    Bounding box WGS84 autour d'un point.
    Retourne (min_lon, min_lat, max_lon, max_lat).
    """
    dlat = radius_km / 111.32
    dlon = radius_km / (111.32 * math.cos(math.radians(lat)))
    return lon - dlon, lat - dlat, lon + dlon, lat + dlat


# ---------------------------------------------------------------------------
# WFS — liste des couches disponibles
# ---------------------------------------------------------------------------
def list_rpg_layers(timeout: int = 15) -> list[str]:
    """
    Interroge GetCapabilities pour lister les couches RPG disponibles.
    """
    try:
        resp = requests.get(
            WFS_ENDPOINT,
            params={"SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetCapabilities"},
            timeout=timeout,
        )
        resp.raise_for_status()
        # Parse minimal : cherche les noms de layers RPG dans le XML brut
        layers = [
            line.strip().replace("<Name>", "").replace("</Name>", "")
            for line in resp.text.splitlines()
            if "<Name>RPG." in line
        ]
        return sorted(set(layers))
    except Exception as e:
        log.warning(f"GetCapabilities inaccessible ({e})")
        return []


# ---------------------------------------------------------------------------
# WFS — téléchargement paginé
# ---------------------------------------------------------------------------
def _wfs_page(
    layer: str,
    bbox: tuple[float, float, float, float],
    start_index: int,
    timeout: int = 60,
) -> dict:
    """
    Récupère une page de features WFS en GeoJSON.
    BBOX au format CRS:84 (lon/lat).
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "SERVICE":      "WFS",
        "VERSION":      "2.0.0",
        "REQUEST":      "GetFeature",
        "TYPENAMES":    layer,
        "SRSNAME":      "CRS:84",
        "BBOX":         f"{min_lon},{min_lat},{max_lon},{max_lat},CRS:84",
        "outputFormat": "application/json",
        "count":        WFS_PAGE_SIZE,
        "startindex":   start_index,
    }
    resp = requests.get(WFS_ENDPOINT, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_wfs(
    year: int,
    lat: float,
    lon: float,
    radius_km: float,
) -> pl.DataFrame:
    """
    Télécharge toutes les parcelles RPG dans un rayon autour du point,
    via WFS paginé.
    """
    layer = WFS_LAYER_PATTERN.format(year=year)
    bbox = _bbox(lat, lon, radius_km)
    min_lon, min_lat, max_lon, max_lat = bbox
    log.info(f"  Layer WFS : {layer}")
    log.info(f"  Rayon     : {radius_km} km → bbox [{min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}]")

    all_features: list[dict] = []
    start = 0

    with Progress(SpinnerColumn(), "[progress.description]{task.description}", BarColumn()) as progress:
        task = progress.add_task("Téléchargement WFS...", total=None)

        while True:
            data = _wfs_page(layer, bbox, start)
            features = data.get("features", [])
            if not features:
                break
            all_features.extend(features)
            progress.update(task, description=f"WFS — {len(all_features)} parcelles...")
            start += len(features)
            if len(features) < WFS_PAGE_SIZE:
                break  # dernière page

    log.info(f"  {len(all_features)} parcelles téléchargées")

    if not all_features:
        log.warning("Aucune parcelle dans la zone. Vérifie les coordonnées et le rayon.")
        return pl.DataFrame(schema={
            "id_parcel": pl.Utf8, "code_culture": pl.Utf8,
            "surface_ha": pl.Float64, "libelle_culture": pl.Utf8,
            "geometry_wkt": pl.Utf8,
        })

    return _features_to_dataframe(all_features)


# ---------------------------------------------------------------------------
# Transformation GeoJSON → DataFrame
# ---------------------------------------------------------------------------
def _geojson_geom_to_wkt(geom: dict) -> str:
    """
    Conversion GeoJSON geometry → WKT (Polygon/MultiPolygon).
    Implémentation minimale sans dépendance externe.
    """
    def ring_to_wkt(ring: list) -> str:
        return "(" + ",".join(f"{x} {y}" for x, y in ring) + ")"

    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if gtype == "Polygon":
        rings = ",".join(ring_to_wkt(r) for r in coords)
        return f"POLYGON({rings})"
    elif gtype == "MultiPolygon":
        polys = ",".join(
            "(" + ",".join(ring_to_wkt(r) for r in poly) + ")"
            for poly in coords
        )
        return f"MULTIPOLYGON({polys})"
    else:
        return ""


def _features_to_dataframe(features: list[dict]) -> pl.DataFrame:
    rows = []
    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry") or {}
        # Les noms de champs WFS peuvent varier selon la version
        code = str(props.get("code_cultu") or props.get("CODE_CULTU") or "")
        rows.append({
            "id_parcel":     str(props.get("id_parcel") or props.get("ID_PARCEL") or ""),
            "code_culture":  code,
            "surface_ha":    float(props.get("surf_parc") or props.get("SURF_PARC") or 0.0),
            "libelle_culture": CODES_CULTURE.get(code, "Inconnu"),
            "geometry_wkt":  _geojson_geom_to_wkt(geom),
        })
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Fallback : lecture d'un GeoPackage local via DuckDB Spatial
# ---------------------------------------------------------------------------
def read_gpkg(gpkg_path: Path, lat: float, lon: float, radius_km: float) -> pl.DataFrame:
    """
    Lit un GeoPackage local (téléchargé manuellement) via DuckDB Spatial.
    Filtre spatialement si radius_km > 0.
    La projection Lambert 93 (EPSG:2154) est reprojetée en WGS84 automatiquement.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    if radius_km > 0:
        min_lon, min_lat, max_lon, max_lat = _bbox(lat, lon, radius_km)
        where = f"""
            WHERE ST_Intersects(
                ST_Transform(geom, 'EPSG:2154', 'EPSG:4326'),
                ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat})
            )
        """
    else:
        where = ""

    query = f"""
        SELECT
            CAST(ID_PARCEL AS VARCHAR)    AS id_parcel,
            CODE_CULTU                    AS code_culture,
            SURF_PARC                     AS surface_ha,
            ST_AsText(
                ST_Transform(geom, 'EPSG:2154', 'EPSG:4326')
            )                             AS geometry_wkt
        FROM ST_Read('{gpkg_path}')
        {where}
    """
    rows = conn.execute(query).fetchall()
    conn.close()

    df = pl.DataFrame({
        "id_parcel":    [r[0] for r in rows],
        "code_culture": [r[1] for r in rows],
        "surface_ha":   [r[2] for r in rows],
        "geometry_wkt": [r[3] for r in rows],
    })
    df = df.with_columns(
        pl.col("code_culture")
        .map_elements(lambda c: CODES_CULTURE.get(str(c), "Inconnu"), return_dtype=pl.Utf8)
        .alias("libelle_culture")
    )
    return df


# ---------------------------------------------------------------------------
# Vérification du Parquet existant
# ---------------------------------------------------------------------------
def verify(processed_dir: Path, year: int) -> None:
    pattern = str(processed_dir / f"rpg_{year}.parquet")
    result = duckdb.sql(f"""
        SELECT
            COUNT(*)                      AS n_parcelles,
            ROUND(SUM(surface_ha), 0)     AS surface_totale_ha,
            COUNT(DISTINCT code_culture)  AS n_cultures
        FROM read_parquet('{pattern}')
    """).fetchone()
    if result:
        print(f"\n── Vérification RPG {year} ────────────────────────────")
        print(f"  Parcelles      : {result[0]:,}")
        print(f"  Surface totale : {result[1]:,.0f} ha")
        print(f"  Cultures diff. : {result[2]}")
        print("─────────────────────────────────────────────────────\n")


# ---------------------------------------------------------------------------
# Sauvegarde
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, processed_dir: Path, year: int) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / f"rpg_{year}.parquet"
    df.write_parquet(path, compression="zstd")
    log.info(f"  ✓ {path.name}  ({len(df):,} parcelles  {df['surface_ha'].sum():.0f} ha)")
    return path


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(
    year: int | None = None,
    radius_km: float | None = None,
    from_file: Path | None = None,
    verify_only: bool = False,
) -> pl.DataFrame | None:
    cfg = load_config()
    lat = cfg["farm"]["latitude"]
    lon = cfg["farm"]["longitude"]
    r = radius_km if radius_km is not None else cfg.get("geo", {}).get("rayon_km", 25)
    y = year or cfg.get("geo", {}).get("annee_rpg", None) or LATEST_KNOWN_YEAR

    processed_dir = Path(cfg["paths"]["processed"]) / "geo"

    if verify_only:
        verify(processed_dir, y)
        return None

    if from_file:
        log.info(f"Lecture GeoPackage local : {from_file}")
        df = read_gpkg(from_file, lat, lon, r)
    else:
        df = fetch_wfs(y, lat, lon, r)

    if len(df) == 0:
        return df

    save_parquet(df, processed_dir, y)

    # Résumé par culture
    summary = (
        df.group_by("libelle_culture")
        .agg(
            pl.len().alias("n"),
            pl.col("surface_ha").sum().round(1).alias("ha"),
        )
        .sort("ha", descending=True)
        .head(10)
    )
    log.info("\n  Top cultures dans la zone :")
    for row in summary.iter_rows(named=True):
        log.info(f"    {row['libelle_culture']:30s} {row['n']:4d} parcelles  {row['ha']:8.1f} ha")

    log.info("\nIngestion RPG terminée ✓")
    return df


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion RPG — WFS IGN Géoplateforme")
    parser.add_argument("--year",      type=int,   default=None, help=f"Année RPG (défaut: {LATEST_KNOWN_YEAR})")
    parser.add_argument("--radius",    type=float, default=None, help="Rayon km (défaut: config.toml)")
    parser.add_argument("--from-file", type=Path,  default=None, help="GeoPackage local (bypass WFS)")
    parser.add_argument("--verify",    action="store_true",      help="Vérifie le Parquet existant")
    parser.add_argument("--list-layers", action="store_true",    help="Liste les couches RPG WFS disponibles")
    args = parser.parse_args()

    if args.list_layers:
        layers = list_rpg_layers()
        if layers:
            print("Couches RPG disponibles sur le WFS :")
            for layer in layers:
                print(f"  {layer}")
        else:
            print("Impossible de récupérer la liste (GetCapabilities inaccessible).")
        return

    run(
        year=args.year,
        radius_km=args.radius,
        from_file=args.from_file,
        verify_only=args.verify,
    )


if __name__ == "__main__":
    main()
