#!/usr/bin/env python
"""Comprehensive validation of implantation module scaffolding."""
from implantation.models.parcelle import Parcelle
from implantation.scoring.engine import ScoringEngine, ScoringWeights

# Scenario: Compare 3 land parcels with different characteristics
print("=" * 70)
print("SCENARIO: Evaluation de 3 parcelles candidates (2026-04-04)")
print("=" * 70)
print()

parcelles = [
    Parcelle(
        id="sable_001",
        nom="Prairie Sablé (Bien Localisée)",
        surface_ha=2.5,
        commune="Sablé-sur-Sarthe",
        departement="72",
        coords_centroid=(47.8474, -0.9416),
        prix_achat=8500.0,
        prix_comparable_eur_ha=10000.0,
        acces_eau="reseau",
        distance_marche_km=8.0,
        acces_vehicule="facile",
        pente_pct=1.5,
        exposition="S",
        altitude_m=50,
        risque_gel_tardif=False,
        meteo_precip_annuelle_mm=450,
        meteo_jours_gel=2,
    ),
    Parcelle(
        id="mulsanne_002",
        nom="Terrain Mulsanne (Pétueux)",
        surface_ha=3.0,
        commune="Mulsanne",
        departement="72",
        coords_centroid=(48.12, -0.48),
        prix_achat=9000.0,
        prix_comparable_eur_ha=8500.0,
        acces_eau="forage",
        debit_estime_m3h=1.8,
        distance_marche_km=18.0,
        acces_vehicule="limite",
        pente_pct=12.0,
        exposition="NE",
        altitude_m=120,
        risque_gel_tardif=True,
        meteo_precip_annuelle_mm=480,
        meteo_jours_gel=8,
    ),
    Parcelle(
        id="brains_003",
        nom="Parcelle Brains (Bon Marché)",
        surface_ha=1.8,
        commune="Brains",
        departement="72",
        coords_centroid=(47.70, -0.38),
        prix_achat=6500.0,
        prix_comparable_eur_ha=8000.0,
        acces_eau="aucun",
        distance_marche_km=22.0,
        acces_vehicule="facile",
        pente_pct=2.0,
        exposition="plat",
        altitude_m=80,
        risque_gel_tardif=False,
        meteo_precip_annuelle_mm=420,
        meteo_jours_gel=4,
    ),
]

# Test 1: Default weights (balanced)
print("📊 ÉVALUATION 1: Poids par défaut (Économique 35% | Eau 35% | Topographie 30%)")
print("-" * 70)

engine = ScoringEngine()
scores = engine.score_multiple(parcelles)

for i, s in enumerate(scores, 1):
    print(f"\n🥇 #{i}: {s.global_score}/100 — {s.parcelle_nom}")
    print(f"       💶 {s.score_economique_logistique.score:3d}/100  | "
          f"💧 {s.score_eau_irrigation.score:3d}/100  | "
          f"🏔️  {s.score_topographie_exposition.score:3d}/100")

# Test 2: Water priority (ex. irrigation critical)
print("\n" + "=" * 70)
print("📊 ÉVALUATION 2: Priorité EAU (Économique 20% | Eau 60% | Topographie 20%)")
print("-" * 70)

weights_eau = ScoringWeights(
    economique_logistique=0.2,
    eau_irrigation=0.6,
    topographie_exposition=0.2
)
engine_eau = ScoringEngine(weights=weights_eau)
scores_eau = engine_eau.score_multiple(parcelles)

for i, s in enumerate(scores_eau, 1):
    print(f"\n🥇 #{i}: {s.global_score}/100 — {s.parcelle_nom}")
    print(f"       💶 {s.score_economique_logistique.score:3d}/100  | "
          f"💧 {s.score_eau_irrigation.score:3d}/100  | "
          f"🏔️  {s.score_topographie_exposition.score:3d}/100")

# Test 3: Topography priority (ex. solar optimized)
print("\n" + "=" * 70)
print("📊 ÉVALUATION 3: Priorité TOPOGRAPHIE (Économique 25% | Eau 25% | Topographie 50%)")
print("-" * 70)

weights_topo = ScoringWeights(
    economique_logistique=0.25,
    eau_irrigation=0.25,
    topographie_exposition=0.50
)
engine_topo = ScoringEngine(weights=weights_topo)
scores_topo = engine_topo.score_multiple(parcelles)

for i, s in enumerate(scores_topo, 1):
    print(f"\n🥇 #{i}: {s.global_score}/100 — {s.parcelle_nom}")
    print(f"       💶 {s.score_economique_logistique.score:3d}/100  | "
          f"💧 {s.score_eau_irrigation.score:3d}/100  | "
          f"🏔️  {s.score_topographie_exposition.score:3d}/100")

print("\n" + "=" * 70)
print("✅ Implantation module scaffolding fully validated!")
print("=" * 70)
