"""
Ingestion DVF (Demandes de valeurs foncières) — data.gouv.fr.

Pipeline:
    1. Découverte des ressources CSV via l'API data.gouv.fr
    2. Téléchargement des fichiers trimestriels
    3. Normalisation du schéma minimal
    4. Filtre spatial/administratif selon la zone de la ferme
    5. Sauvegarde en Parquet partitionné par année

Usage:
    python -m ingestion.prix.dvf                  # backfill des années configurées
    python -m ingestion.prix.dvf --years 2023 2024
    python -m ingestion.prix.dvf --verify
    python -m ingestion.prix.dvf --list-resources
    python -m ingestion.prix.dvf --from-file ./demandes-de-valeurs-foncieres-2024.csv
"""
from __future__ import annotations

import argparse
import io
import logging
import math
import re
import zipfile
from datetime import date
from pathlib import Path

import polars as pl
import requests
from rich.logging import RichHandler
from rich.progress import BarColumn, DownloadColumn, Progress, SpinnerColumn, TransferSpeedColumn

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
log = logging.getLogger("dvf")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATAGOUV_API = "https://www.data.gouv.fr/api/1/datasets"
DATASET_SLUG = "demandes-de-valeurs-foncieres"
SUPPORTED_FORMATS = {"CSV", "ZIP", "XLS", "XLSX"}

COL_ALIASES: dict[str, list[str]] = {
    "date_mutation": ["date_mutation", "DATE_MUTATION", "date mutation", "date"],
    "valeur_fonciere": ["valeur_fonciere", "VALEUR_FONCIERE", "valeur fonciere", "valeur"] ,
    "code_departement": ["code_departement", "CODE_DEPARTEMENT", "departement", "code_dept"],
    "code_commune": ["code_commune", "CODE_COMMUNE", "commune"],
    "code_postal": ["code_postal", "CODE_POSTAL", "postal"],
    "type_local": ["type_local", "TYPE_LOCAL"],
    "surface_reelle_bati": ["surface_reelle_bati", "SURFACE_REELLE_BATI", "surface_bati", "surface_reelle"],
    "nombre_pieces_principales": ["nombre_pieces_principales", "NOMBRE_PIECES_PRINCIPALES", "nombre_pieces"],
    "longitude": ["longitude", "LONGITUDE", "lon"],
    "latitude": ["latitude", "LATITUDE", "lat"],
    "id_parcelle": ["id_parcelle", "ID_PARCELLE", "id_parcel"],
    "nature_mutation": ["nature_mutation", "NATURE_MUTATION", "nature"],
    "numero_voie": ["numero_voie", "NUMERO_VOIE"],
    "nom_voie": ["nom_voie", "NOM_VOIE"],
}

TARGET_SCHEMA: dict[str, pl.DataType] = {
    "annee": pl.Int32,
    "trimestre": pl.Int32,
    "date_mutation": pl.Utf8,
    "valeur_fonciere": pl.Float64,
    "code_departement": pl.Utf8,
    "code_commune": pl.Utf8,
    "code_postal": pl.Utf8,
    "type_local": pl.Utf8,
    "surface_reelle_bati": pl.Float64,
    "nombre_pieces_principales": pl.Int32,
    "longitude": pl.Float64,
    "latitude": pl.Float64,
    "id_parcelle": pl.Utf8,
    "nature_mutation": pl.Utf8,
    "numero_voie": pl.Utf8,
    "nom_voie": pl.Utf8,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_root() -> Path:
    return Path(load_config()["paths"]["raw"]).resolve().parents[1]


def _dvf_raw_dir() -> Path:
    path = Path(load_config()["paths"]["raw"]) / "prix" / "dvf"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _dvf_processed_dir() -> Path:
    path = Path(load_config()["paths"]["processed"]) / "prix"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _make_raw_filename(year: int, trimestre: int, suffix: str) -> str:
    return f"dvf_{year}_T{trimestre}.{suffix}"


def _make_processed_filename(year: int) -> str:
    return f"prix_dvf_{year}.parquet"


def _detect_separator(raw: bytes) -> str:
    if raw.count(b";") >= raw.count(b",") and raw.count(b";") > 0:
        return ";"
    return ","


def _detect_encoding(raw: bytes) -> str:
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            raw.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _extract_year(text: str) -> int | None:
    match = re.search(r"(?<![0-9])(20\d{2})(?![0-9])", text)
    return int(match.group(1)) if match else None


def _extract_quarter(text: str) -> int | None:
    patterns = [r"[Tt]([1-4])", r"[Qq]([1-4])", r"([1-4])[eè]me trimestre", r"([1-4])er trimestre"]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def _resolve_column(columns: list[str], target: str) -> str | None:
    candidates = [c for c in columns if c.lower() == target.lower()]
    if candidates:
        return candidates[0]
    aliases = COL_ALIASES.get(target, [])
    for alias in aliases:
        for c in columns:
            if c.lower() == alias.lower():
                return c
    return None


_THOUSANDS_SEP_RE = re.compile(r"^\d{1,3}(,\d{3})+$")


def _normalize_french_float(value: str | None) -> str | None:
    """Normalise un nombre français vers un float parseable.

    - Séparateur de milliers (virgule avant groupes de 3 chiffres) : supprimé.
      Ex. "125,000" → "125000", "1,234,567" → "1234567"
    - Séparateur décimal (virgule hors du pattern milliers) : remplacé par '.'.
      Ex. "125,50" → "125.50", "125000,5" → "125000.5"
    """
    if value is None:
        return None
    value = value.strip().replace(" ", "")
    if _THOUSANDS_SEP_RE.match(value):
        return value.replace(",", "")
    return value.replace(",", ".")


def _normalize_column(df: pl.DataFrame, name: str, dtype: pl.DataType) -> pl.Series:
    col_name = _resolve_column(df.columns, name)
    if col_name is None:
        return pl.Series(name, [None] * len(df)).cast(dtype)
    series = df[col_name]
    if dtype == pl.Float64 and series.dtype == pl.Utf8:
        series = series.map_elements(_normalize_french_float, return_dtype=pl.Utf8)
    return series.cast(dtype, strict=False).alias(name)


def _clean_departement(code: str | int | None) -> str | None:
    if code is None:
        return None
    text = str(code).strip()
    return text.lstrip("0")


def _bbox(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    dlat = radius_km / 111.32
    dlon = radius_km / (111.32 * abs(math.cos(math.radians(lat))) or 1.0)
    return lon - dlon, lat - dlat, lon + dlon, lat + dlat


def list_resources(timeout: int = 20) -> list[dict[str, str | int | None]]:
    url = f"{DATAGOUV_API}/{DATASET_SLUG}/"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()

    resources: list[dict[str, str | int | None]] = []
    for resource in payload.get("resources", []):
        fmt = resource.get("format", "").upper()
        if fmt not in SUPPORTED_FORMATS:
            continue
        title = resource.get("title", "")
        url = resource.get("url", "")
        if not url:
            continue
        year = _extract_year(title) or _extract_year(url)
        trimestre = _extract_quarter(title) or _extract_quarter(url)
        resources.append({
            "title": title,
            "url": url,
            "format": fmt,
            "year": year,
            "trimestre": trimestre,
        })
    return resources


def resources_for_years(years: list[int], timeout: int = 20) -> list[dict[str, str | int | None]]:
    resources = list_resources(timeout)
    if not resources:
        raise RuntimeError("Aucune ressource DVF trouvée sur data.gouv.fr")
    matched = [r for r in resources if r["year"] in years]
    if not matched:
        available = sorted({r["year"] for r in resources if isinstance(r["year"], int)})
        raise ValueError(f"Années DVF disponibles : {available}")
    return matched


def find_local_csv_resources(project_root: Path) -> list[dict[str, str | int | None]]:
    results: list[dict[str, str | int | None]] = []
    for path in sorted(project_root.glob("**/*dvf*.csv")):
        year = _extract_year(path.name)
        trimestre = _extract_quarter(path.name)
        results.append({
            "title": path.name,
            "path": str(path),
            "format": "CSV",
            "year": year,
            "trimestre": trimestre,
        })
    return results


def normalize(df: pl.DataFrame, year: int | None = None, trimestre: int | None = None) -> pl.DataFrame:
    data: dict[str, pl.Series] = {}
    for name, dtype in TARGET_SCHEMA.items():
        if name == "annee" and year is not None:
            data[name] = pl.Series(name, [year] * len(df), dtype=dtype)
            continue
        if name == "trimestre" and trimestre is not None:
            data[name] = pl.Series(name, [trimestre] * len(df), dtype=dtype)
            continue
        data[name] = _normalize_column(df, name, dtype)

    normalized = pl.DataFrame(data)
    if normalized.height > 0 and "annee" in normalized.columns and normalized.schema["annee"] == pl.Int32 and year is None:
        if "date_mutation" in normalized.columns:
            normalized = normalized.with_columns(
                pl.col("date_mutation")
                .str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False)
                .dt.year()
                .cast(pl.Int32)
                .alias("annee")
            )
    if normalized.height > 0 and "trimestre" in normalized.columns and normalized.schema["trimestre"] == pl.Int32 and trimestre is None:
        if "date_mutation" in normalized.columns:
            normalized = normalized.with_columns(
                pl.col("date_mutation")
                .str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False)
                .dt.quarter()
                .cast(pl.Int32)
                .alias("trimestre")
            )
    return normalized


def parse_csv(raw: bytes, year: int | None = None, trimestre: int | None = None) -> pl.DataFrame:
    encoding = _detect_encoding(raw)
    sep = _detect_separator(raw)
    df = pl.read_csv(
        io.BytesIO(raw),
        separator=sep,
        encoding=encoding,
        ignore_errors=True,
        try_parse_dates=True,
    )
    return normalize(df, year=year, trimestre=trimestre)


def parse_zip(raw: bytes, year: int | None = None, trimestre: int | None = None) -> pl.DataFrame:
    members: list[bytes] = []
    with zipfile.ZipFile(io.BytesIO(raw), "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            lower = member.filename.lower()
            if lower.endswith(".csv"):
                members.append(archive.read(member))
    if not members:
        raise RuntimeError("Aucun fichier CSV trouvé dans l'archive ZIP DVF")
    frames = [parse_csv(content, year=year, trimestre=trimestre) for content in members]
    return pl.concat(frames, how="vertical")


def apply_filters(
    df: pl.DataFrame,
    departements: list[str] | None = None,
    lat: float | None = None,
    lon: float | None = None,
    rayon_km: float | None = None,
) -> pl.DataFrame:
    result = df
    if departements:
        normalized = [str(_clean_departement(d)) for d in departements if d is not None]
        normalized = [d for d in normalized if d]
        if normalized:
            result = result.filter(
                pl.col("code_departement")
                .cast(pl.Utf8)
                .str.strip_chars(" 0")
                .is_in(normalized)
            )
    if lat is not None and lon is not None and rayon_km and "latitude" in result.columns and "longitude" in result.columns:
        min_lon, min_lat, max_lon, max_lat = _bbox(lat, lon, rayon_km)
        result = result.filter(
            (pl.col("latitude") >= min_lat)
            & (pl.col("latitude") <= max_lat)
            & (pl.col("longitude") >= min_lon)
            & (pl.col("longitude") <= max_lon)
        )
    return result


def _download_resource(url: str, timeout: int = 60) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _save_raw_bytes(content: bytes, year: int, trimestre: int, fmt: str) -> Path:
    raw_dir = _dvf_raw_dir()
    suffix = fmt.lower()
    path = raw_dir / _make_raw_filename(year, trimestre, suffix)
    path.write_bytes(content)
    return path


def _write_parquet(df: pl.DataFrame, year: int) -> Path:
    out_path = _dvf_processed_dir() / _make_processed_filename(year)
    df.write_parquet(out_path)
    return out_path


def verify(years: list[int] | None = None) -> None:
    processed_dir = _dvf_processed_dir()
    parquet_files = list(processed_dir.glob("prix_dvf_*.parquet"))
    if not parquet_files:
        log.warning("Aucun fichier DVF traité trouvé dans %s", processed_dir)
        return
    for path in parquet_files:
        if years and int(re.search(r"(20\d{2})", path.name).group(1)) not in years:
            continue
        df = pl.read_parquet(path)
        log.info("%s — %d lignes", path.name, len(df))


def _load_geo_filter(cfg: dict) -> tuple[list[str], float | None, float | None, float | None]:
    departements = cfg.get("geo", {}).get("departements", [])
    rayon_km = cfg.get("geo", {}).get("rayon_km", 0)
    farm_lat = cfg.get("farm", {}).get("latitude")
    farm_lon = cfg.get("farm", {}).get("longitude")
    return departements, rayon_km, farm_lat, farm_lon


def _resource_key(resource: dict) -> tuple[int, int | int]:
    year = resource.get("year") or 0
    trimestre = resource.get("trimestre") or 0
    return year, trimestre


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion DVF — transactions foncières filtrées par zone")
    parser.add_argument("--years", nargs="+", type=int, help="Années à ingérer")
    parser.add_argument("--verify", action="store_true", help="Vérifie les fichiers Parquet traités")
    parser.add_argument("--list-resources", action="store_true", help="Liste les ressources DVF disponibles sur data.gouv.fr")
    parser.add_argument("--from-file", type=Path, help="Utilise un fichier local DVF CSV pour l'ingestion")
    parser.add_argument("--no-filter", action="store_true", help="Désactive le filtre géographique et départemental")
    args = parser.parse_args()

    cfg = load_config()
    if args.list_resources:
        resources = list_resources()
        for r in sorted(resources, key=_resource_key):
            log.info("%s — %s — %s — %s", r["year"], r["trimestre"], r["format"], r["title"])
        return

    if args.verify:
        verify(args.years)
        return

    years = args.years or []
    if not years:
        years = [date.today().year]

    departements, rayon_km, farm_lat, farm_lon = _load_geo_filter(cfg)
    if args.no_filter:
        departements = []
        rayon_km = None
        farm_lat = None
        farm_lon = None

    if args.from_file:
        raw_bytes = args.from_file.read_bytes()
        year = _extract_year(args.from_file.name) or years[0]
        quarter = _extract_quarter(args.from_file.name)
        if args.from_file.suffix.lower() == ".zip":
            df = parse_zip(raw_bytes, year=year, trimestre=quarter)
        else:
            df = parse_csv(raw_bytes, year=year, trimestre=quarter)
        df = apply_filters(df, departements=departements, lat=farm_lat, lon=farm_lon, rayon_km=rayon_km)
        if df.is_empty():
            log.warning("Aucune transaction retenue après filtrage")
            return
        _write_parquet(df, year)
        log.info("Traitement terminé : %s", _make_processed_filename(year))
        return

    resources = resources_for_years(years)
    by_year: dict[int, list[dict[str, str | int | None]]] = {}
    for resource in resources:
        year = resource.get("year")
        if year is None:
            continue
        by_year.setdefault(year, []).append(resource)

    for year, items in sorted(by_year.items()):
        frames: list[pl.DataFrame] = []
        for resource in sorted(items, key=_resource_key):
            fmt = resource.get("format")
            if fmt not in {"CSV", "ZIP"}:
                log.warning("Format non traité pour DVF : %s", fmt)
                continue
            content = _download_resource(resource["url"])  # type: ignore[arg-type]
            quarter = resource.get("trimestre")
            try:
                if fmt == "ZIP":
                    df = parse_zip(content, year=year, trimestre=quarter)
                else:
                    df = parse_csv(content, year=year, trimestre=quarter)
            except Exception as exc:
                log.warning("Échec du parsing de %s : %s", resource.get("title"), exc)
                continue
            if not args.no_filter:
                df = apply_filters(df, departements=departements, lat=farm_lat, lon=farm_lon, rayon_km=rayon_km)
            if df.is_empty():
                log.warning("Aucune transaction retenue pour %s", resource.get("title"))
                continue
            frames.append(df)
            if quarter:
                _save_raw_bytes(content, year, quarter, fmt.lower())
        if frames:
            combined = pl.concat(frames, how="vertical").unique()
            _write_parquet(combined, year)
            log.info("Année %d traitée — %d transactions conservées", year, len(combined))
        else:
            log.warning("Aucune ressource DVF retenue pour %d", year)


if __name__ == "__main__":
    main()
