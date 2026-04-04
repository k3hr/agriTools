"""
Ingestion ADES — Niveaux piézométriques (Hub'eau)

Récupère les chroniques de niveaux de nappe pour les stations BSS
déjà ingérées (bss_stations.parquet).

Utile pour :
  - suivre la tendance du niveau de la nappe au fil des ans
  - identifier les périodes de stress hydrique souterrain
  - croiser avec les données météo (ETP, pluie) pour piloter l'irrigation

API Hub'eau — Niveaux nappes / Chroniques :
    Endpoint : https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques
    Auth     : aucune (open data)
    Docs     : https://hubeau.eaufrance.fr/page/api-niveaux-nappes

Usage :
    python -m ingestion.geo.ades                          # config.toml + BSS local
    python -m ingestion.geo.ades --since 2018             # depuis 2018
    python -m ingestion.geo.ades --stations 3             # 3 stations les + proches
    python -m ingestion.geo.ades --verify                 # résumé Parquet existant
    python -m ingestion.geo.ades --show                   # niveaux récents tabulés
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
import requests
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn
from rich.table import Table

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
log = logging.getLogger("ades")
console = Console()

# ---------------------------------------------------------------------------
# Constantes API Hub'eau
# ---------------------------------------------------------------------------
HUBEAU_BASE        = "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes"
CHRONIQUES_ENDPOINT = f"{HUBEAU_BASE}/chroniques"
PAGE_SIZE          = 2000   # max recommandé Hub'eau
MAX_OFFSET         = 20000  # page × size ≤ 20 000 (limite v1)
BATCH_SIZE         = 5      # nb stations par requête (évite les URLs trop longues)
DEFAULT_YEARS      = 10     # horizon par défaut (années glissantes)
RETRY_DELAY_S      = 2      # pause entre retries en cas de 429

# Champs demandés à l'API
FIELDS = ",".join([
    "code_bss",
    "date_mesure",
    "profondeur_nappe",
    "niveau_eau_ngf",
    "mode_obtention",
    "qualification",
    "code_qualification",
])

# Schéma Polars cible
TARGET_SCHEMA: dict[str, type] = {
    "code_bss":          pl.Utf8,
    "date_mesure":       pl.Date,
    "profondeur_nappe":  pl.Float64,   # m sous le sol (positif = plus profond)
    "niveau_eau_ngf":    pl.Float64,   # m NGF / altitude absolue
    "mode_obtention":    pl.Utf8,
    "qualification":     pl.Utf8,
    "code_qualification": pl.Int64,
}


# ---------------------------------------------------------------------------
# Lecture des stations BSS locales
# ---------------------------------------------------------------------------
def load_bss_stations(processed_dir: Path, n_stations: int | None = None) -> list[str]:
    """
    Lit les codes BSS depuis le Parquet local (trié par distance croissante).

    Args:
        processed_dir: dossier processed/geo/
        n_stations:    si fourni, limite aux N stations les plus proches

    Returns:
        liste de code_bss
    """
    path = processed_dir / "bss_stations.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier BSS introuvable : {path}\n"
            "Lance d'abord : python -m ingestion.geo.bss"
        )
    query = f"SELECT code_bss FROM read_parquet('{path}') ORDER BY distance_km"
    if n_stations:
        query += f" LIMIT {n_stations}"
    rows = duckdb.sql(query).fetchall()
    codes = [r[0] for r in rows if r[0]]
    log.info(f"  {len(codes)} station(s) BSS chargée(s) pour ingestion ADES")
    return codes


# ---------------------------------------------------------------------------
# Appel API Hub'eau — chroniques
# ---------------------------------------------------------------------------
def _fetch_batch(
    codes: list[str],
    since: date,
    until: date,
    timeout: int = 60,
) -> list[dict]:
    """
    Récupère les chroniques pour un lot de codes BSS avec pagination.

    Hub'eau accepte plusieurs code_bss dans la même requête (param répété).
    Limite offset : page × size ≤ 20 000 — on s'arrête proprement.
    """
    params: dict = {
        "code_bss":          ",".join(codes),
        "date_debut_mesure": since.isoformat(),
        "date_fin_mesure":   until.isoformat(),
        "fields":            FIELDS,
        "size":              PAGE_SIZE,
        "page":              1,
    }

    records: list[dict] = []
    truncated = False

    while True:
        for attempt in range(3):
            try:
                resp = requests.get(CHRONIQUES_ENDPOINT, params=params, timeout=timeout)
                if resp.status_code == 429:
                    log.warning(f"  ⚠ HTTP 429 — pause {RETRY_DELAY_S}s (tentative {attempt+1})")
                    time.sleep(RETRY_DELAY_S)
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.ConnectionError as exc:
                raise RuntimeError(
                    "Impossible de joindre Hub'eau (hubeau.eaufrance.fr). "
                    "Vérifie ta connexion."
                ) from exc
        else:
            raise RuntimeError("Hub'eau : trop de tentatives échouées (HTTP 429).")

        data    = resp.json()
        page_records = data.get("data", [])
        records.extend(page_records)

        total   = data.get("count", 0)
        fetched = len(records)

        if fetched >= total or len(page_records) < PAGE_SIZE:
            break

        if fetched + PAGE_SIZE > MAX_OFFSET:
            truncated = True
            break

        params["page"] += 1

    if truncated:
        log.warning(
            f"  ⚠ Offset Hub'eau plafonné ({MAX_OFFSET} entrées). "
            f"Réduis --since ou --stations si la zone est dense."
        )

    return records


def fetch_chroniques(
    codes: list[str],
    since: date,
    until: date,
    timeout: int = 60,
) -> list[dict]:
    """
    Récupère les chroniques pour toutes les stations, par lots de BATCH_SIZE.

    Affiche une barre de progression par lot.
    """
    all_records: list[dict] = []

    batches = [codes[i: i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    log.info(f"  Récupération chroniques — {len(codes)} stations, {len(batches)} lot(s)")

    with Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Hub'eau chroniques...", total=len(batches))

        for i, batch in enumerate(batches, 1):
            batch_records = _fetch_batch(batch, since, until, timeout=timeout)
            all_records.extend(batch_records)
            progress.update(
                task,
                advance=1,
                description=(
                    f"Lot {i}/{len(batches)} — "
                    f"{[c.split('/')[0] for c in batch]} "
                    f"→ {len(batch_records)} mesures"
                ),
            )

    log.info(f"  Total brut : {len(all_records):,} mesures récupérées")
    return all_records


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
def chroniques_to_dataframe(records: list[dict]) -> pl.DataFrame:
    """
    Convertit les enregistrements Hub'eau en DataFrame Polars normalisé.

    Nettoie les types, parse les dates, trie par station + date.
    """
    if not records:
        return pl.DataFrame(schema=TARGET_SCHEMA)

    # Extraction des champs, avec valeurs par défaut null
    rows = []
    for r in records:
        # date_mesure : "YYYY-MM-DD" ou "YYYY-MM-DDTHH:MM:SS"
        raw_date = r.get("date_mesure", "")
        parsed_date: date | None = None
        if raw_date:
            try:
                # Tronque éventuellement la partie heure
                parsed_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                pass

        # Numériques — l'API peut retourner None ou une string vide
        def _to_float(v) -> float | None:
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _to_int(v) -> int | None:
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        rows.append({
            "code_bss":           r.get("code_bss"),
            "date_mesure":        parsed_date,
            "profondeur_nappe":   _to_float(r.get("profondeur_nappe")),
            "niveau_eau_ngf":     _to_float(r.get("niveau_eau_ngf")),
            "mode_obtention":     r.get("mode_obtention"),
            "qualification":      r.get("qualification"),
            "code_qualification": _to_int(r.get("code_qualification")),
        })

    df = pl.DataFrame(rows, schema=TARGET_SCHEMA)
    return df.sort(["code_bss", "date_mesure"])


# ---------------------------------------------------------------------------
# Sauvegarde
# ---------------------------------------------------------------------------
def save_parquet(df: pl.DataFrame, processed_dir: Path) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    path = processed_dir / "ades_chroniques.parquet"
    df.write_parquet(path, compression="zstd")
    log.info(f"  ✓ {path.name}  ({len(df):,} mesures, {df['code_bss'].n_unique()} stations)")
    return path


# ---------------------------------------------------------------------------
# Vérification
# ---------------------------------------------------------------------------
def verify(processed_dir: Path) -> None:
    """Résumé statistique du Parquet existant."""
    path = str(processed_dir / "ades_chroniques.parquet")
    try:
        result = duckdb.sql(f"""
            SELECT
                COUNT(*)                            AS n_mesures,
                COUNT(DISTINCT code_bss)            AS n_stations,
                MIN(date_mesure)::TEXT              AS premiere_date,
                MAX(date_mesure)::TEXT              AS derniere_date,
                ROUND(AVG(profondeur_nappe), 2)     AS prof_moy_m,
                ROUND(MIN(profondeur_nappe), 2)     AS prof_min_m,
                ROUND(MAX(profondeur_nappe), 2)     AS prof_max_m
            FROM read_parquet('{path}')
        """).fetchone()

        rows_station = duckdb.sql(f"""
            SELECT
                code_bss,
                COUNT(*)                            AS n,
                MIN(date_mesure)::TEXT              AS debut,
                MAX(date_mesure)::TEXT              AS fin,
                ROUND(AVG(profondeur_nappe), 2)     AS prof_moy_m
            FROM read_parquet('{path}')
            GROUP BY code_bss
            ORDER BY n DESC
        """).fetchall()

        print("\n── Vérification ADES ─────────────────────────────────────")
        print(f"  Mesures           : {result[0]:,}")
        print(f"  Stations          : {result[1]}")
        print(f"  Période           : {result[2]} → {result[3]}")
        print(f"  Profondeur nappe  : moy {result[4]} m  (min {result[5]}  max {result[6]})")
        print("\n  Par station :")
        for code, n, debut, fin, prof in rows_station:
            print(f"    {(code or '?'):20s} {n:6,} mesures  {debut} → {fin}  moy {prof or '—'} m")
        print("─────────────────────────────────────────────────────────\n")

    except Exception as e:
        log.error(f"Erreur vérification : {e}")


# ---------------------------------------------------------------------------
# Affichage des niveaux récents
# ---------------------------------------------------------------------------
def show_recent(processed_dir: Path, n_days: int = 90) -> None:
    """Affiche les mesures les plus récentes par station (Rich table)."""
    path = str(processed_dir / "ades_chroniques.parquet")
    cutoff = (date.today() - timedelta(days=n_days)).isoformat()

    try:
        rows = duckdb.sql(f"""
            SELECT
                code_bss,
                date_mesure::TEXT                   AS date_m,
                ROUND(profondeur_nappe, 2)           AS prof_m,
                ROUND(niveau_eau_ngf, 2)             AS ngf_m,
                qualification
            FROM read_parquet('{path}')
            WHERE date_mesure >= '{cutoff}'
            ORDER BY code_bss, date_mesure DESC
            LIMIT 100
        """).fetchall()
    except Exception as e:
        log.error(f"Erreur lecture : {e}")
        return

    if not rows:
        console.print(f"[dim]Aucune mesure depuis {cutoff}.[/dim]")
        return

    table = Table(title=f"ADES — mesures depuis {cutoff} ({n_days} jours)")
    for col in ["Station BSS", "Date", "Prof. nappe (m)", "Niveau NGF (m)", "Qualification"]:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(v) if v is not None else "—" for v in row])
    console.print(table)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run(
    since_year: int | None     = None,
    n_stations: int | None     = None,
    verify_only: bool          = False,
    show: bool                 = False,
    show_days: int             = 90,
) -> pl.DataFrame | None:
    """
    Pipeline ADES complet :
    1. Charge les codes BSS (Parquet local)
    2. Récupère les chroniques Hub'eau
    3. Normalise et sauvegarde en Parquet
    """
    cfg = load_config()
    processed_dir = Path(cfg["paths"]["processed"]) / "geo"

    if verify_only:
        verify(processed_dir)
        return None

    if show:
        show_recent(processed_dir, n_days=show_days)
        return None

    # --- Horizon temporel ---
    until = date.today()
    if since_year:
        since = date(since_year, 1, 1)
    else:
        since = date(until.year - DEFAULT_YEARS, until.month, until.day)

    log.info(f"Ingestion ADES — période {since} → {until}")

    # --- Chargement stations BSS ---
    codes = load_bss_stations(processed_dir, n_stations=n_stations)
    if not codes:
        log.warning("Aucune station BSS disponible — lance d'abord ingestion.geo.bss")
        return pl.DataFrame(schema=TARGET_SCHEMA)

    # --- Fetch Hub'eau ---
    records = fetch_chroniques(codes, since=since, until=until)

    if not records:
        log.warning("Aucune mesure récupérée. Vérifie les codes BSS et la plage de dates.")
        return pl.DataFrame(schema=TARGET_SCHEMA)

    # --- Transform ---
    df = chroniques_to_dataframe(records)
    log.info(f"  → {len(df):,} mesures normalisées pour {df['code_bss'].n_unique()} station(s)")

    # Résumé par station
    summary = (
        df.group_by("code_bss")
        .agg([
            pl.len().alias("n"),
            pl.col("profondeur_nappe").mean().round(2).alias("prof_moy_m"),
            pl.col("date_mesure").min().alias("debut"),
            pl.col("date_mesure").max().alias("fin"),
        ])
        .sort("n", descending=True)
    )
    log.info("\n  Chroniques par station :")
    for row in summary.iter_rows(named=True):
        log.info(
            f"    {(row['code_bss'] or '?'):22s} {row['n']:6,} mesures  "
            f"{row['debut']} → {row['fin']}  moy {row['prof_moy_m'] or '—'} m"
        )

    # --- Sauvegarde ---
    save_parquet(df, processed_dir)
    log.info("Ingestion ADES terminée ✓")
    return df


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingestion ADES — niveaux piézométriques Hub'eau",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
exemples :
  python -m ingestion.geo.ades                  # 10 dernières années, toutes stations BSS
  python -m ingestion.geo.ades --since 2015     # depuis 2015
  python -m ingestion.geo.ades --stations 3     # 3 stations les plus proches
  python -m ingestion.geo.ades --verify         # résumé Parquet existant
  python -m ingestion.geo.ades --show           # niveaux des 90 derniers jours
  python -m ingestion.geo.ades --show --days 365
        """,
    )
    parser.add_argument(
        "--since",
        type=int,
        default=None,
        metavar="YYYY",
        help=f"Année de début (défaut : {date.today().year - DEFAULT_YEARS})",
    )
    parser.add_argument(
        "--stations",
        type=int,
        default=None,
        metavar="N",
        help="Limiter aux N stations BSS les plus proches",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Affiche un résumé du Parquet existant sans re-télécharger",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Affiche les mesures récentes (tableau Rich)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        metavar="N",
        help="Nb de jours pour --show (défaut: 90)",
    )
    args = parser.parse_args()

    run(
        since_year   = args.since,
        n_stations   = args.stations,
        verify_only  = args.verify,
        show         = args.show,
        show_days    = args.days,
    )


if __name__ == "__main__":
    main()
