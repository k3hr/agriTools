"""Tests for parcel persistence utilities."""

import json
from pathlib import Path

import pytest

import app.components.parcelle as parcelle_mod
from implantation.models.parcelle import Parcelle


def test_save_and_list_parcelles(tmp_path, monkeypatch):
    def fake_dir() -> Path:
        target = tmp_path / "perso" / "parcelles"
        target.mkdir(parents=True, exist_ok=True)
        return target

    monkeypatch.setattr(parcelle_mod, "_parcelles_dir", fake_dir)

    parcelle = Parcelle(
        id="test_001",
        nom="Test Parcelle",
        surface_ha=1.5,
        commune="Sablé",
        departement="72",
        coords_centroid=(47.85, -0.34),
    )

    path = parcelle_mod.save_parcelle(parcelle)
    assert path.exists()

    entries = parcelle_mod.list_parcelles()
    assert len(entries) == 1
    assert entries[0]["id"] == "test_001"
    assert entries[0]["nom"] == "Test Parcelle"
    assert entries[0]["path"] == str(path)


def test_load_parcelles_ignores_corrupt_json(tmp_path, monkeypatch):
    def fake_dir() -> Path:
        target = tmp_path / "perso" / "parcelles"
        target.mkdir(parents=True, exist_ok=True)
        return target

    monkeypatch.setattr(parcelle_mod, "_parcelles_dir", fake_dir)

    valid_parcelle = Parcelle(
        id="test_002",
        nom="Valid Parcelle",
        surface_ha=2.0,
        commune="Sablé",
        departement="72",
        coords_centroid=(47.85, -0.34),
    )
    valid_path = parcelle_mod.save_parcelle(valid_parcelle)

    corrupt_path = fake_dir() / "corrupt.json"
    corrupt_path.write_text("{ invalid json", encoding="utf-8")

    loaded = parcelle_mod.load_parcelles()
    assert len(loaded) == 1
    assert loaded[0].id == "test_002"
    assert loaded[0].nom == "Valid Parcelle"
