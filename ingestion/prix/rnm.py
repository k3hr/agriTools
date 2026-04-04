"""
Ingestion RNM (Réseau des Nouvelles des Marchés) — FranceAgriMer / data.gouv.fr

Cotations hebdomadaires de fruits et légumes sur les marchés de gros (MIN).
Données publiées en open data, mises à jour chaque semaine.

Source : https://www.data.gouv.fr/datasets/cotations-du-reseau-des-nouvelles-des-marches

Pipeline :
    1. Découverte des ressources CSV via l'API data.gouv.fr
    2. Téléchargement des fichiers pour les années demandées
    3. Normalisation du schéma (les noms de colonnes varient selon les millésimes)
    4. Filtre par marché et stade de commercialisation (configurable)
    5. Sauvegarde en Parquet partitionné par année

Usage :
    python -m ingestion.prix.rnm                      # backfill config.toml
    python -m ingestion.prix.rnm --year 2023          # année spécifique
    python -m ingestion.prix.rnm --years 2021 2023    # plage d'années
    python -m ingestion.prix.rnm --all-marches        # sans filtre marché
    python -m ingestion.prix.rnm --verify             # résumé du Parquet existant
    python -m ingestion.prix.rnm --list-marches       # marchés présents dans les données
    python -m ingestion.prix.rnm --list-produits      # produits présents
"""
from __future__ import annotations

import argparse
import io
import logging
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
log = logging.getLogger("rnm")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DATAGOUV_API = "https://www.data.gouv.fr/api/1/datasets"
DATASET_SLUG = "cotations-du-reseau-des-nouvelles-des-marches"

# Normalisation des noms de colonnes selon les millésimes du dataset
# Les colonnes ont changé de nom au fil du temps
COL_ALIASES: dict[str, list[str]] = {
    "date":       ["date_cotation", "DateDebut", "date_debut", "date", "Date", "Semaine", "date cotation"],
    "annee":      ["annee", "Annee", "année", "year"],
    "semaine":    ["semaine", "Semaine", "num_semaine", "week"],
    "produit":    ["produit_libelle", "produit", "Produit", "libelle_produit", "Libelle_Produit"],
    "marche":     ["marche_libelle", "marche", "Marche", "libelle_marche", "Libelle_Marche", "marché"],
    "stade":      ["stade_libelle", "stade", "Stade"],
    "categorie":  ["categorie", "Categorie", "catégorie", "code produit"],
    "calibre":    ["calibre", "Calibre"],
    "variete":    ["variete", "Variete", "variété"],
    "origine":    ["origine", "Origine"],
    "unite":      ["unite", "Unite", "unité", "Unité"],
    "prix_min":   ["prix_min", "Prix Min", "PrixMin", "prix min", "min", "valeur en euro(s)"],
    "prix_max":   ["prix_max", "Prix Max", "PrixMax", "prix max", "max", "valeur en euro(s)"],
    "prix_moyen": ["prix_moyen", "Prix Moyen", "PrixMoyen", "prix moyen", "moyen", "Moyen", "valeur en euro(s)"],
}

# Schéma cible normalisé
TARGET_SCHEMA = {
    "annee":      pl.Int32,
    "semaine":    pl.Int32,
    "date":       pl.Utf8,    # conservé en string, variable selon les fichiers
    "produit":    pl.Utf8,
    "marche":     pl.Utf8,
    "stade":      pl.Utf8,
    "categorie":  pl.Utf8,
    "calibre":    pl.Utf8,
    "variete":    pl.Utf8,
    "origine":    pl.Utf8,
    "unite":      pl.Utf8,
    "prix_min":   pl.Float64,
    "prix_max":   pl.Float64,
    "prix_moyen": pl.Float64,
}


# ---------------------------------------------------------------------------
# Découverte des ressources via l'API data.gouv.fr
# ---------------------------------------------------------------------------
def list_resources(timeout: int = 20) -> list[dict]:
    """
    Interroge l'API data.gouv.fr pour lister les ressources CSV du dataset RNM.
    Retourne une liste de dicts {title, url, year}.
    """
    url = f"{DATAGOUV_API}/{DATASET_SLUG}/"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    resources = []
    for r in data.get("resources", []):
        if r.get("format", "").upper() not in ("CSV", "XLS", "XLSX"):
            continue
        title = r.get("title", "")
        dl_url = r.get("url", "")
        if not dl_url:
            continue
        # Tente de déduire l'année depuis le titre ou l'URL
        year = _extract_year(title) or _extract_year(dl_url)
        resources.append({
            "title": title,
            "url":   dl_url,
            "year":  year,
            "format": r.get("format", "").upper(),
        })
    return resources


def _extract_year(text: str) -> int | None:
    """Extrait une année 20xx depuis une chaîne."""
    import re
    m = re.search(r"(?<![0-9])(20\d{2})(?![0-9])", text)
    return int(m.group(1)) if m else None


def resources_for_years(target_years: list[int], timeout: int = 20) -> list[dict]:
    """
    Filtre les ressources disponibles pour les années demandées.
    Lève une RuntimeError si aucune ressource n'est trouvée.
    """
    all_resources = list_resources(timeout)
    if not all_resources:
        raise RuntimeError(
            "Aucune ressource trouvée. Vérifie ta connexion ou l'URL du dataset data.gouv.fr."
        )

    matched = [r for r in all_resources if r["year"] in target_years]
    if not matched:
        available = sorted({r["year"] for r in all_resources if r["year"]})
        raise ValueError(
            f"Années demandées {target_years} non trouvées. "
            f"Disponibles : {available}"
        )
    return matched


def _extract_year_from_filename(filename: str) -> int | None:
    """Déduit l'année depuis un nom de fichier RNM, par exemple A24 -> 2024."""
    year = _extract_year(filename)
    if year is not None:
        return year
    m = re.search(r"A(\d{2})(?!\d)", filename, re.IGNORECASE)
    if m:
        return 2000 + int(m.group(1))
    return None


def find_local_zip_resources(project_root: Path) -> list[dict]:
    """Recherche les archives RNM locales stockées à la racine du projet."""
    resources = []
    for path in sorted(project_root.glob("COT-MUL-prd_RNM-*.zip")):
        year = _extract_year_from_filename(path.name)
        if year is None:
            continue
        resources.append({
            "title": path.name,
            "path": path,
            "year": year,
            "format": "ZIP",
        })
    return resources


def _extract_zip_members(path: Path) -> list[tuple[str, bytes, str]]:
    """Retourne les fichiers supportés contenus dans l'archive ZIP."""
    results: list[tuple[str, bytes, str]] = []
    with zipfile.ZipFile(path, "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            name = member.filename
            lower = name.lower()
            if lower.endswith(".csv"):
                results.append(("csv", archive.read(member), name))
            elif lower.endswith(('.xls', '.xlsx')):
                results.append(("excel", archive.read(member), name))
    return results


def parse_excel(raw: bytes, year: int, source: str | None = None) -> pl.DataFrame:
    """Parse un fichier Excel en DataFrame normalisée."""
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("Pandas est requis pour parser les fichiers Excel RNM") from exc

    df = pd.read_excel(io.BytesIO(raw), engine=None)
    df = pl.from_pandas(df)
    df = normalize(df)

    if df["annee"].null_count() == len(df):
        df = df.with_columns(pl.lit(year).cast(pl.Int32).alias("annee"))
    return df


def parse_zip(path: Path, year: int) -> pl.DataFrame:
    """Parse un fichier ZIP local RNM et retourne une DataFrame unifiée."""
    members = _extract_zip_members(path)
    if not members:
        raise RuntimeError(f"Aucun fichier CSV ou Excel trouvé dans l'archive {path.name}")

    dfs: list[pl.DataFrame] = []
    for kind, raw, name in members:
        if kind == "csv":
            dfs.append(parse_csv(raw, year))
        else:
            dfs.append(parse_excel(raw, year, source=name))

    return pl.concat(dfs, how="vertical") if len(dfs) > 1 else dfs[0]


# ---------------------------------------------------------------------------
# Téléchargement
# ---------------------------------------------------------------------------
def download_bytes(url: str, timeout: int = 120) -> bytes:
    """Télécharge un fichier et retourne son contenu en bytes."""
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        chunks = []
        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
        ) as progress:
            task = progress.add_task(url.split("/")[-1][:60], total=total or None)
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                chunks.append(chunk)
                progress.advance(task, len(chunk))
    return b"".join(chunks)


# ---------------------------------------------------------------------------
# Parsing & normalisation
# ---------------------------------------------------------------------------
def _detect_separator(raw: bytes) -> str:
    """Détecte le séparateur CSV (;  ou ,) sur les 2048 premiers octets."""
    sample = raw[:2048].decode("utf-8", errors="replace")
    return ";" if sample.count(";") >= sample.count(",") else ","


def _detect_encoding(raw: bytes) -> str:
    """Détecte l'encodage (UTF-8 ou Latin-1 fréquents pour les données FR)."""
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _resolve_column(df_cols: list[str], target: str) -> str | None:
    """
    Retourne le nom de colonne réel pour un champ cible,
    en testant les aliases connus (insensible à la casse).
    """
    aliases = [a.lower() for a in COL_ALIASES.get(target, [target])]
    for col in df_cols:
        if col.lower() in aliases:
            return col
    return None


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    """
    Renomme et caste les colonnes vers le schéma cible normalisé.
    Les colonnes manquantes sont ajoutées avec des valeurs nulles.
    """
    # Strip whitespace from column names
    df = df.rename({col: col.strip() for col in df.columns})

    cols = df.columns

    # Construire le mapping renommage
    rename_map: dict[str, str] = {}
    for target in TARGET_SCHEMA:
        src = _resolve_column(cols, target)
        if src and src != target:
            rename_map[src] = target

    df = df.rename(rename_map)

    # Ajouter les colonnes manquantes
    for col, dtype in TARGET_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    # Garder uniquement les colonnes du schéma cible
    df = df.select(list(TARGET_SCHEMA.keys()))

    # Casting
    for col, dtype in TARGET_SCHEMA.items():
        try:
            if dtype in (pl.Float64,):
                # Nettoyage : remplace virgules décimales par points
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .str.replace(",", ".", literal=True)
                    .str.replace(r"\s+", "", literal=False)
                    .cast(dtype, strict=False)
                )
            elif dtype == pl.Int32:
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))
        except Exception:
            pass  # Laisser tel quel si le cast échoue

    return df


def parse_csv(raw: bytes, year: int) -> pl.DataFrame:
    """
    Parse un fichier CSV RNM brut en DataFrame Polars normalisé.
    Gère les variations d'encodage, séparateur et noms de colonnes.
    """
    sep = _detect_separator(raw)
    enc = _detect_encoding(raw)

    df = pl.read_csv(
        io.BytesIO(raw),
        separator=sep,
        encoding=enc,
        infer_schema_length=1000,
        ignore_errors=True,
        null_values=["", "NA", "N/A", "-", "nd", "ND"],
        truncate_ragged_lines=True,
    )

    log.debug(f"  Colonnes brutes ({year}) : {df.columns}")
    log.debug(f"  Types de colonnes: {df.dtypes}")
    df = normalize(df)

    # Injecter l'année si absente
    if df["annee"].null_count() == len(df):
        # Essayer d'extraire l'année depuis la colonne date
        if "date" in df.columns and df["date"].dtype == pl.Utf8:
            try:
                # Extraire l'année depuis des dates comme "08/01/2026" ou "2026-01-08"
                extracted_years = (
                    df["date"]
                    .str.extract(r"(\d{4})", 1)  # Capture le groupe 1 (l'année)
                    .cast(pl.Int32)
                )
                if extracted_years.null_count() < len(extracted_years):
                    df = df.with_columns(annee=extracted_years)
                    log.debug(f"  Année extraite depuis la colonne date")
                else:
                    df = df.with_columns(pl.lit(year).cast(pl.Int32).alias("annee"))
            except Exception:
                df = df.with_columns(pl.lit(year).cast(pl.Int32).alias("annee"))
        else:
            df = df.with_columns(pl.lit(year).cast(pl.Int32).alias("annee"))

    return df


# ---------------------------------------------------------------------------
# Filtrage
# ---------------------------------------------------------------------------
def apply_filters(
    df: pl.DataFrame,
    marches: list[str] | None = None,
    stades: list[str] | None = None,
) -> pl.DataFrame:
    """
    Filtre par marché(s) et stade(s) de commercialisation.
    Les comparaisons sont insensibles à la casse et aux espaces.
    """
    if marches:
        marches_upper = [m.upper().strip() for m in marches]
        df = df.filter(
            pl.col("marche").str.to_uppercase().str.strip_chars().is_in(marches_upper)
        )
    if stades:
        stades_upper = [s.upper().strip() for s in stades]
        df = df.filter(
            pl.col("stade").str.to_uppercase().str.strip_chars().is_in(stades_upper)
        )
    return df


# ---------------------------------------------------------------------------
# Sauvegarde
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, processed_dir: Path, year: int) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / f"prix_rnm_{year}.parquet"
    df.write_parquet(path, compression="zstd")
    log.info(f"  ✓ {path.name}  ({len(df):,} cotations)")
    return path


# ---------------------------------------------------------------------------
# Vérification
# ---------------------------------------------------------------------------
def verify(processed_dir: Path) -> None:
    import duckdb
    pattern = str(processed_dir / "prix_rnm_*.parquet")
    try:
        result = duckdb.sql(f"""
            SELECT
                MIN(annee)                     AS premiere_annee,
                MAX(annee)                     AS derniere_annee,
                COUNT(*)                       AS n_cotations,
                COUNT(DISTINCT produit)        AS n_produits,
                COUNT(DISTINCT marche)         AS n_marches,
                COUNT(DISTINCT origine)        AS n_origines
            FROM read_parquet('{pattern}')
        """).fetchone()
        if result:
            print("\n── Vérification RNM ────────────────────────────────────")
            print(f"  Période          : {result[0]} → {result[1]}")
            print(f"  Cotations        : {result[2]:,}")
            print(f"  Produits distincts : {result[3]}")
            print(f"  Marchés          : {result[4]}")
            print(f"  Origines         : {result[5]}")
            print("─────────────────────────────────────────────────────────\n")
    except Exception as e:
        log.error(f"Erreur verification : {e}")


def list_marches(processed_dir: Path) -> None:
    import duckdb
    pattern = str(processed_dir / "prix_rnm_*.parquet")
    rows = duckdb.sql(f"""
        SELECT marche, COUNT(*) AS n
        FROM read_parquet('{pattern}')
        GROUP BY marche ORDER BY n DESC
    """).fetchall()
    print("\nMarchés disponibles :")
    for marche, n in rows:
        print(f"  {marche or '(vide)':40s} {n:6,} cotations")


def list_produits(processed_dir: Path) -> None:
    import duckdb
    pattern = str(processed_dir / "prix_rnm_*.parquet")
    rows = duckdb.sql(f"""
        SELECT produit, COUNT(*) AS n
        FROM read_parquet('{pattern}')
        GROUP BY produit ORDER BY n DESC
        LIMIT 50
    """).fetchall()
    print("\nTop 50 produits (par fréquence de cotation) :")
    for produit, n in rows:
        print(f"  {produit or '(vide)':40s} {n:6,}")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(
    target_years: list[int] | None = None,
    all_marches: bool = False,
    verify_only: bool = False,
) -> list[pl.DataFrame]:
    cfg = load_config()
    marches = None if all_marches else cfg.get("prix", {}).get("marches", [])
    stades  = cfg.get("prix", {}).get("stades", ["GROS"])
    hist    = c