"""
Suivi des heures de travail — CLI agriTools

Source de vérité : CSV dans datalake/raw/perso/heures/
Analytique      : Parquet dans datalake/processed/perso/

Modes de saisie :
  start  <poste> [notes]             Démarre un timer
  stop   [notes]                     Arrête le timer, calcule la durée
  add    <poste> <durée> [date] [notes]  Saisie a posteriori
  list   [--n N]                     Affiche les N dernières entrées
  ingest                             Convertit le CSV en Parquet analysable
  verify                             Résumé du Parquet existant

Formats durée acceptés : 2h30  2h30m  2.5h  150min  90m  2h
Dates relatives        : aujourd'hui (défaut)  hier  lundi..dimanche  YYYY-MM-DD

Exemples :
  python -m ingestion.perso.heures start désherbage
  python -m ingestion.perso.heures stop "planche nord"
  python -m ingestion.perso.heures add semis 2h30
  python -m ingestion.perso.heures add récolte 1.5h hier "tomates serre"
  python -m ingestion.perso.heures add commercialisation 3h lundi
  python -m ingestion.perso.heures list --n 20
  python -m ingestion.perso.heures ingest
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.logging import RichHandler
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
log = logging.getLogger("heures")
console = Console()

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
CSV_HEADER = ["id", "date", "debut", "fin", "duree_h", "poste", "notes", "source"]
TIMER_FILE = ".timer.json"

JOURS_FR = {
    "lundi": 0, "mardi": 1, "mercredi": 2, "jeudi": 3,
    "vendredi": 4, "samedi": 5, "dimanche": 6,
}


# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------
def _paths() -> tuple[Path, Path]:
    """Retourne (raw_dir, processed_dir)."""
    cfg = load_config()
    raw_dir = Path(cfg["paths"]["raw"]) / "perso" / "heures"
    proc_dir = Path(cfg["paths"]["processed"]) / "perso"
    raw_dir.mkdir(parents=True, exist_ok=True)
    proc_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir, proc_dir


def _csv_path() -> Path:
    raw_dir, _ = _paths()
    return raw_dir / "heures.csv"


def _timer_path() -> Path:
    raw_dir, _ = _paths()
    return raw_dir / TIMER_FILE


# ---------------------------------------------------------------------------
# Parser durée
# ---------------------------------------------------------------------------
def parse_duree(token: str) -> float | None:
    """
    Parse une durée en heures décimales.

    Formats supportés :
      2h30   2h30m   2.5h   150min   90m   2h   0.5h   30min

    Retourne None si non reconnu.
    """
    t = token.strip().lower()

    # Formes mixtes : 2h30  2h30m
    m = re.fullmatch(r"(\d+)h(\d+)m?", t)
    if m:
        return int(m.group(1)) + int(m.group(2)) / 60

    # Forme décimale : 2.5h  1.5h
    m = re.fullmatch(r"(\d+(?:\.\d+)?)h", t)
    if m:
        return float(m.group(1))

    # Minutes seules : 90min  45m  90min
    m = re.fullmatch(r"(\d+)(?:min|m)", t)
    if m:
        return int(m.group(1)) / 60

    return None


# ---------------------------------------------------------------------------
# Parser date relative
# ---------------------------------------------------------------------------
def parse_date(token: str) -> date | None:
    """
    Parse une date depuis un token.

    Reconnaît : aujourd'hui  hier  lundi..dimanche  YYYY-MM-DD  DD/MM/YYYY

    Retourne None si non reconnu.
    """
    t = token.strip().lower()
    today = date.today()

    if t in ("aujourd'hui", "auj", "today"):
        return today
    if t == "hier":
        return today - timedelta(days=1)

    if t in JOURS_FR:
        target_dow = JOURS_FR[t]
        delta = (today.weekday() - target_dow) % 7
        delta = delta if delta > 0 else 7  # toujours dans le passé
        return today - timedelta(days=delta)

    # ISO : YYYY-MM-DD
    try:
        return date.fromisoformat(token)
    except ValueError:
        pass

    # FR : DD/MM/YYYY
    try:
        return datetime.strptime(token, "%d/%m/%Y").date()
    except ValueError:
        pass

    return None


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def _ensure_csv(path: Path) -> None:
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADER)
        log.info(f"Fichier créé : {path}")


def _append_row(path: Path, row: dict) -> None:
    _ensure_csv(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writerow(row)


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Commandes
# ---------------------------------------------------------------------------
def cmd_start(poste: str, notes: str = "") -> None:
    """Démarre un timer pour un poste de travail."""
    timer = _timer_path()
    if timer.exists():
        state = json.loads(timer.read_text())
        console.print(
            f"[yellow]⚠ Timer déjà en cours : [bold]{state['poste']}[/bold] "
            f"(démarré à {state['started_at']}). Arrête-le d'abord avec [italic]stop[/italic].[/yellow]"
        )
        return

    now = datetime.now()
    state = {
        "poste": poste,
        "notes": notes,
        "started_at": now.isoformat(),
        "date": date.today().isoformat(),
    }
    timer.write_text(json.dumps(state, ensure_ascii=False))
    console.print(f"[green]▶ Timer démarré — [bold]{poste}[/bold] à {now.strftime('%H:%M')}[/green]")


def cmd_stop(notes: str = "") -> None:
    """Arrête le timer en cours et enregistre l'entrée."""
    timer = _timer_path()
    if not timer.exists():
        console.print("[red]Aucun timer en cours. Lance d'abord [italic]start <poste>[/italic].[/red]")
        return

    state = json.loads(timer.read_text())
    started = datetime.fromisoformat(state["started_at"])
    stopped = datetime.now()
    duree_h = round((stopped - started).total_seconds() / 3600, 4)

    row = {
        "id":      str(uuid.uuid4())[:8],
        "date":    state["date"],
        "debut":   started.strftime("%H:%M"),
        "fin":     stopped.strftime("%H:%M"),
        "duree_h": round(duree_h, 2),
        "poste":   state["poste"],
        "notes":   notes or state.get("notes", ""),
        "source":  "timer",
    }
    _append_row(_csv_path(), row)
    timer.unlink()

    h, m = divmod(int(duree_h * 60), 60)
    console.print(
        f"[green]■ Timer arrêté — [bold]{state['poste']}[/bold] "
        f"{started.strftime('%H:%M')} → {stopped.strftime('%H:%M')} "
        f"= [bold]{h}h{m:02d}[/bold][/green]"
    )


def cmd_add(poste: str, duree_raw: str, date_raw: str = "", notes: str = "") -> None:
    """Enregistre une entrée a posteriori."""
    duree_h = parse_duree(duree_raw)
    if duree_h is None:
        console.print(f"[red]Durée non reconnue : [bold]{duree_raw}[/bold]. "
                      f"Exemples valides : 2h30  1.5h  90min[/red]")
        return

    if date_raw:
        d = parse_date(date_raw)
        if d is None:
            console.print(f"[red]Date non reconnue : [bold]{date_raw}[/bold]. "
                          f"Exemples : hier  lundi  2026-04-01[/red]")
            return
    else:
        d = date.today()

    row = {
        "id":      str(uuid.uuid4())[:8],
        "date":    d.isoformat(),
        "debut":   "",
        "fin":     "",
        "duree_h": round(duree_h, 2),
        "poste":   poste,
        "notes":   notes,
        "source":  "manuel",
    }
    _append_row(_csv_path(), row)

    h, m = divmod(int(duree_h * 60), 60)
    console.print(
        f"[green]✓ Enregistré — [bold]{poste}[/bold] "
        f"{h}h{m:02d} le {d.strftime('%d/%m/%Y')}[/green]"
    )


def cmd_list(n: int = 15) -> None:
    """Affiche les N dernières entrées."""
    rows = _read_rows(_csv_path())
    if not rows:
        console.print("[dim]Aucune entrée enregistrée.[/dim]")
        return

    recent = rows[-n:][::-1]  # dernières en premier
    table = Table(title=f"Heures — {len(rows)} entrées au total")
    for col in ["Date", "Poste", "Durée", "Début", "Fin", "Notes", "Source"]:
        table.add_column(col)

    for r in recent:
        try:
            h, m = divmod(int(float(r["duree_h"]) * 60), 60)
            duree_fmt = f"{h}h{m:02d}"
        except (ValueError, TypeError):
            duree_fmt = r.get("duree_h", "—")
        table.add_row(
            r.get("date", ""),
            r.get("poste", ""),
            duree_fmt,
            r.get("debut", "") or "—",
            r.get("fin", "") or "—",
            r.get("notes", "") or "",
            r.get("source", ""),
        )
    console.print(table)


# ---------------------------------------------------------------------------
# Ingestion CSV → Parquet
# ---------------------------------------------------------------------------
def cmd_ingest() -> pl.DataFrame:
    """Convertit le CSV source en Parquet enrichi."""
    csv_path = _csv_path()
    _, proc_dir = _paths()

    rows = _read_rows(csv_path)
    if not rows:
        log.warning("Aucune donnée à ingérer.")
        return pl.DataFrame()

    df = pl.DataFrame(rows).with_columns([
        pl.col("date").str.to_date("%Y-%m-%d", strict=False),
        pl.col("duree_h").cast(pl.Float64, strict=False),
    ])

    # Enrichissement temporel
    df = df.with_columns([
        pl.col("date").dt.year().alias("annee"),
        pl.col("date").dt.month().alias("mois"),
        pl.col("date").dt.week().alias("semaine"),
        pl.col("date").dt.weekday().alias("jour_semaine"),  # 0=lundi
    ])

    out = proc_dir / "heures.parquet"
    df.write_parquet(out, compression="zstd")
    log.info(f"  ✓ {out.name}  ({len(df):,} entrées)")
    return df


def cmd_verify() -> None:
    """Résumé du Parquet existant."""
    import duckdb
    _, proc_dir = _paths()
    path = str(proc_dir / "heures.parquet")
    try:
        result = duckdb.sql(f"""
            SELECT
                COUNT(*)                        AS n_entrees,
                ROUND(SUM(duree_h), 1)          AS total_heures,
                MIN(date)::TEXT                 AS premiere_date,
                MAX(date)::TEXT                 AS derniere_date,
                COUNT(DISTINCT poste)           AS n_postes
            FROM read_parquet('{path}')
        """).fetchone()
        rows_poste = duckdb.sql(f"""
            SELECT poste, COUNT(*) AS n, ROUND(SUM(duree_h), 1) AS heures
            FROM read_parquet('{path}')
            GROUP BY poste ORDER BY heures DESC
        """).fetchall()

        print("\n── Vérification heures ──────────────────────────────────")
        print(f"  Entrées          : {result[0]:,}")
        print(f"  Total heures     : {result[1]:.1f} h")
        print(f"  Période          : {result[2]} → {result[3]}")
        print(f"  Postes distincts : {result[4]}")
        print("\n  Heures par poste :")
        for poste, n, heures in rows_poste:
            print(f"    {(poste or '?'):25s} {heures:6.1f} h  ({n} entrées)")
        print("─────────────────────────────────────────────────────────\n")
    except Exception as e:
        log.error(f"Erreur vérification : {e}")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Suivi des heures de travail — agriTools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
exemples :
  start désherbage
  stop "planche nord terminée"
  add semis 2h30
  add récolte 1.5h hier "tomates serre 2"
  add commercialisation 3h lundi
  list --n 20
  ingest
  verify
        """,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # start
    p_start = sub.add_parser("start", help="Démarre un timer")
    p_start.add_argument("poste", help="Poste de travail")
    p_start.add_argument("notes", nargs="?", default="", help="Notes (optionnel)")

    # stop
    p_stop = sub.add_parser("stop", help="Arrête le timer en cours")
    p_stop.add_argument("notes", nargs="?", default="", help="Notes (optionnel)")

    # add
    p_add = sub.add_parser("add", help="Saisie a posteriori")
    p_add.add_argument("poste",    help="Poste de travail")
    p_add.add_argument("duree",    help="Durée : 2h30  1.5h  90min")
    p_add.add_argument("date",     nargs="?", default="", help="Date : hier  lundi  2026-04-01 (défaut: aujourd'hui)")
    p_add.add_argument("notes",    nargs="?", default="", help="Notes libres")

    # list
    p_list = sub.add_parser("list", help="Affiche les dernières entrées")
    p_list.add_argument("--n", type=int, default=15, help="Nombre d'entrées (défaut: 15)")

    # ingest
    sub.add_parser("ingest", help="Convertit le CSV en Parquet")

    # verify
    sub.add_parser("verify", help="Résumé du Parquet existant")

    args = parser.parse_args()

    if args.cmd == "start":
        cmd_start(args.poste, args.notes)
    elif args.cmd == "stop":
        cmd_stop(args.notes)
    elif args.cmd == "add":
        cmd_add(args.poste, args.duree, args.date, args.notes)
    elif args.cmd == "list":
        cmd_list(args.n)
    elif args.cmd == "ingest":
        cmd_ingest()
    elif args.cmd == "verify":
        cmd_verify()


if __name__ == "__main__":
    main()
