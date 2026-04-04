#!/usr/bin/env python3
"""Test script for scoring integration in parcel form."""

from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import ScoringEngine

# Create a test parcel
parcelle = Parcelle(
    id="test_001",
    nom="Test Parcel",
    surface_ha=2.0,
    commune="Test City",
    departement="72",
    coords_centroid=(47.8, -0.9),
    prix_achat=10000,
    prix_comparable_eur_ha=12000,
    acces_eau="forage",
    debit_estime_m3h=3.0,
    distance_marche_km=15.0,
    pente_pct=5.0,
    exposition="S",
    altitude_m=100,
    acces_vehicule="facile",
    statut="prospect"
)

# Test scoring
engine = ScoringEngine()
score = engine.score_parcelle(parcelle)

print("Scoring test successful!")
print(f"Global score: {score.global_score}/100")
print(score.summary())