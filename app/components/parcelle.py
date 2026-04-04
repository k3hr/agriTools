"""
Helpers for parcel form persistence and preview.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from implantation.models.parcelle import Parcelle
from ingestion._config import load_config


def _parcelles_dir() -> Path:
    cfg = load_config()
    raw_dir = Path(cfg["paths"]["raw"])
    path = raw_dir / "perso" / "parcelles"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)


def save_parcelle(parcelle: Parcelle) -> Path:
    """Save a parcel as a JSON file under datalake/raw/perso/parcelles."""
    target_dir = _parcelles_dir()
    filename = f"{parcelle.id}_{parcelle.date_creation.strftime('%Y%m%d_%H%M%S')}.json"
    filename = _sanitize_filename(filename)
    path = target_dir / filename
    path.write_text(parcelle.model_dump_json(ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def list_parcelles() -> list[dict[str, Any]]:
    """Return a list of saved parcel metadata."""
    path = _parcelles_dir()
    files = sorted(path.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    parcels: list[dict[str, Any]] = []
    for file in files:
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            parcels.append({"id": data.get("id"), "nom": data.get("nom"), "path": str(file), "created": data.get("date_creation"), "raw": data})
        except Exception:
            continue
    return parcels
