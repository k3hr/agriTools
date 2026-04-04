"""
Guide d'utilisation du module implantation (Phase 3 - Scaffolding).

Ce document explique comment utiliser le moteur de scoring pour évaluer
des parcelles agricoles candidates.
"""

# Module Implantation — Guide Technique

## Vue d'ensemble

Le module `implantation/` préfigure la Phase 3 de l'outil d'aide à la décision foncière.
Pour l'instant, il fournit :

- **Modèle Parcelle** : représentation structurée d'une parcelle agricole candidate
- **Moteur de scoring** : notation sur 3 axes (Économique, Eau, Topographie)
- **Scoring pondéré** : poids customisables selon priorités de l'utilisateur

### Structure des fichiers

```
implantation/
├── models/
│   ├── __init__.py
│   └── parcelle.py              # Modèle Pydantic Parcelle
├── scoring/
│   ├── __init__.py
│   ├── criteria.py              # 13 critères individuels (0–100)
│   └── engine.py                # Agrégation pondérée + ParcelleScore
└── __init__.py
```

---

## Création d'une parcelle

Une parcelle se crée à partir du modèle Pydantic `Parcelle`.

### Champs obligatoires

```python
from implantation.models import Parcelle

parcelle = Parcelle(
    id="72181_2024_0123",           # Identifiant unique
    nom="Prairie Sablé Ouest",      # Libellé
    surface_ha=2.5,                 # Hectares
    commune="Sablé-sur-Sarthe",     # Commune
    departement="72",               # Dept
    coords_centroid=(47.8474, -0.9416)  # GPS (lat, lon) WGS84
)
```

### Champs optionnels (enrichis après)

```python
parcelle = Parcelle(
    id="...",
    # [...] obligatoires
    
    # Économie
    prix_achat=15000.0,             # € prix demandé
    prix_comparable_eur_ha=10000.0, # € /ha (comparables DVF)
    
    # Eau
    acces_eau="forage",             # forage|riviere|reseau|aucun|inconnu
    debit_estime_m3h=2.5,           # m³/h
    
    # Topographie
    pente_pct=3.0,                  # %
    exposition="S",                 # N|NE|E|SE|S|SO|O|NO|plat
    altitude_m=45,                  # m
    risque_gel_tardif=False,        # Gels mai/juin ?
    
    # Logistique
    distance_marche_km=12.5,        # km au marché
    acces_vehicule="facile",        # facile|limite|difficile
    
    # Enrichissements datalake
    meteo_precip_annuelle_mm=450,   # mm (depuis Open-Meteo)
    meteo_jours_gel=3,              # nb jours < 0°C
    forages_brgm_count=2,           # Forages BRGM < 5 km
    
    # Métadonnées
    notes="Accès facile, forage fonctionnel",
    statut="visite",                # prospect|visite|evalue|archive
)
```

---

## Scoring d'une parcelle

### Scoring simple avec poids par défaut

```python
from implantation.scoring import ScoringEngine

engine = ScoringEngine()
score = engine.score_parcelle(parcelle)

print(score.global_score)  # 0–100
print(f"Économique: {score.score_economique_logistique.score}")
print(f"Eau:        {score.score_eau_irrigation.score}")
print(f"Topographie: {score.score_topographie_exposition.score}")

# Résumé formaté
print(score.summary())
```

**Output :**
```
🌱 Parcelle: Prairie Sablé Ouest
📊 Score global: 68/100

  💶 Économique & logistique: 72/100 (poids 35%)
  💧 Eau & irrigation:        65/100 (poids 35%)
  🏔️  Topographie & exposition: 68/100 (poids 30%)
```

### Scoring avec poids customisés

```python
from implantation.scoring import ScoringEngine, ScoringWeights

# Priorité eau (ex. irrigation critique)
weights = ScoringWeights(
    economique_logistique=0.2,
    eau_irrigation=0.6,
    topographie_exposition=0.2
)

engine = ScoringEngine(weights=weights)
score = engine.score_parcelle(parcelle)
print(score.global_score)  # Score recalculé avec les nouveaux poids
```

### Comparaison multi-parcelle

La comparaison multi-parcelle permet de trier et visualiser plusieurs parcelles enregistrées côte à côte.

- `engine.score_multiple()` trie et renvoie les scores des parcelles sélectionnées.
- La page Streamlit dédiée `4_Comparaison_Parcelles.py` permet de comparer jusqu'à 4 parcelles enregistrées.

```python
parcelles = [parcelle_1, parcelle_2, parcelle_3]

scores = engine.score_multiple(
    parcelles,
    sort_by_score=True  # Tri décroissant
)

for s in scores:
    print(f"{s.global_score:3d}/100  {s.parcelle_nom:20s}")
```

### Page Streamlit de comparaison

Dans `app/pages/4_Comparaison_Parcelles.py`, les parcelles sauvegardées sont chargées depuis `datalake/raw/perso/parcelles/` et comparées avec :

```python
from app.components.parcelle import load_parcelles
from implantation.scoring.engine import ScoringEngine

parcelles = load_parcelles()
engine = ScoringEngine()
scores = engine.score_multiple(parcelles, sort_by_score=True)
```

La page affiche :
- un tableau résumé des scores globaux et des axes,
- une visualisation bar chart par axe,
- des détails de critères pour chaque parcelle.

---

## Architecture du scoring

### 3 axes d'évaluation

#### 1. Économique & Logistique (35% par défaut)

**Critères :**
- **Prix achat** : Comparaison prix_demande vs prix_comparable (DVF)
- **Distance marché** : Proximité au marché MIN de référence
- **Accès routier** : Qualité accès véhicule (facile/limite/difficile)

**Score** : Moyenne pondérée (40% prix + 35% marché + 25% accès)

#### 2. Eau & Irrigation (35% par défaut)

**Critères :**
- **Accès eau** : Réseau > Forage > Rivière > Rien
- **Ajustement débit** : Pénalité si forage < 1 m³/h
- **Précipitation annuelle** : Adéquation 300–600 mm (optimal ~500)

**Score** : Moyenne pondérée (60% accès + 40% pluie)

#### 3. Topographie & Exposition (30% par défaut)

**Critères :**
- **Pente** : Optimal 0–3%, problématique > 15%
- **Exposition** : Sud > Est/Ouest > Nord
- **Risque gel tardif** : Maj/Jun = critique pour tomate/courge
- **Altitude** : Optimal 0–150m, saison raccourcie > 250m

**Score** : Moyenne pondérée (30% pente + 25% expo + 25% gel + 20% altitude)

### Critères individuels (0–100)

Chaque critère est une fonction qui retourne 0–100, indépendante des autres.
Les logiques sont **maraîchage-centric** (légumes, fruits).

Voir `implantation/scoring/criteria.py` pour détails par critère.

---

## Intégration avec le datalake

Les parcelles peuvent être enrichies automatiquement depuis le datalake :

### Météo (Open-Meteo)

```python
from implantation.enrichment import ParcelleEnricher

enricher = ParcelleEnricher()
parcelle, diagnostics = enricher.enrich(parcelle)
```

### Prix comparables (DVF)

```python
# Requête DuckDB sur prix_dvf_*.parquet quand les colonnes nécessaires existent
# Remplit parcelle.prix_comparable_eur_ha sinon ajoute un warning diagnostic
```

### Forages (BRGM)

```python
# Spatial query rayon 5 km sur bss_stations.parquet
# Remplit forages_brgm_count et expose la station la plus proche dans diagnostics
```

---

## Tests

### Exécution

```bash
# Avec pytest (si installé)
cd agriTools
pytest tests/test_implantation.py -v

# Ou validation directe en Python
python -c "
from implantation.models import Parcelle
from implantation.scoring import ScoringEngine

p = Parcelle(id='test', nom='Test', ...)
engine = ScoringEngine()
print(engine.score_parcelle(p).global_score)
"
```

### Couverture

- ✅ Modèle Parcelle (création, validation, schéma)
- ✅ Critères individuels (12 fonctions, cas limites)
- ✅ Moteur scoring (3 axes, agrégation, pondération)
- ✅ Multi-parcelle (batch, tri)
- ✅ Enrichissement datalake (météo, DVF si exploitable, BSS)

---

## Prochaines étapes

### UI Streamlit (Phase 3 - Suite)

Pages à construire :
1. **Scores Parcelle** — Radar chart, scores détaillés par axe
2. **Comparaison** — Tableau side-by-side 3–5 parcelles
3. **Export** — PDF rapport, Markdown

### Enrichissements datalake

- Pull météo locale via Open-Meteo + coords
- Pull prix comparables DVF, SAFER
- Pull eau (BRGM, piézométrie ADES)
- Pull sol (INRAE GéoSol) → topographie dynamique

### Data persistence

- Stockage parcelles en JSON/CSV/DuckDB
- Historique modifications (audit trail)
- Scénarios multi-hypothèses
