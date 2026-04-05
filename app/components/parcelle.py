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


def build_parcelle_preview(parcelle: Parcelle) -> dict[str, Any]:
    """Build a human-friendly preview payload for Streamlit rendering."""
    data = parcelle.model_dump(mode="json")
    return {
        "Identite": {
            "id": data["id"],
            "nom": data["nom"],
            "surface_ha": data["surface_ha"],
            "commune": data["commune"],
            "departement": data["departement"],
            "coords_centroid": data["coords_centroid"],
        },
        "Economique": {
            "prix_achat": data["prix_achat"],
            "prix_location_annuel": data["prix_location_annuel"],
            "prix_comparable_eur_ha": data["prix_comparable_eur_ha"],
        },
        "Eau_irrigation": {
            "acces_eau": data["acces_eau"],
            "debit_estime_m3h": data["debit_estime_m3h"],
            "distance_cours_eau_m": data["distance_cours_eau_m"],
            "forages_brgm_count": data["forages_brgm_count"],
        },
        "Topographie_logistique": {
            "pente_pct": data["pente_pct"],
            "exposition": data["exposition"],
            "altitude_m": data["altitude_m"],
            "risque_gel_tardif": data["risque_gel_tardif"],
            "distance_marche_km": data["distance_marche_km"],
            "distance_agglo_km": data["distance_agglo_km"],
            "acces_vehicule": data["acces_vehicule"],
        },
        "Enrichissements": {
            "meteo_precip_annuelle_mm": data["meteo_precip_annuelle_mm"],
            "meteo_jours_gel": data["meteo_jours_gel"],
            "meteo_etp_annuelle_mm": data["meteo_etp_annuelle_mm"],
        },
        "Suivi": {
            "statut": data["statut"],
            "notes": data["notes"],
            "date_creation": data["date_creation"],
            "date_modification": data["date_modification"],
        },
    }


def render_parcelle_preview(
    parcelle: Parcelle,
    st_module: Any,
    score: Any = None,
) -> dict[str, Any]:
    """Render a parcel preview and return the displayed payload."""
    payload = build_parcelle_preview(parcelle)
    st_module.subheader(f"{parcelle.nom} ({parcelle.id})")
    st_module.caption(f"{parcelle.commune} ({parcelle.departement}) • {parcelle.surface_ha} ha")
    if score is None:
        st_module.info("Score non calcule pour le moment.")
    else:
        st_module.metric("Score global", f"{score}/100")
    st_module.json(payload)
    return payload


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


def load_parcelles() -> list[Parcelle]:
    """Load saved parcels from JSON files and return them as Parcelle instances."""
    path = _parcelles_dir()
    files = sorted(path.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    parcels: list[Parcelle] = []
    for file in files:
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            parcels.append(Parcelle.model_validate(data))
        except Exception:
            continue
    return parcels
