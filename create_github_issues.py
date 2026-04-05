#!/usr/bin/env python3
"""
Script de création des tickets GitHub pour agriTools.
Usage : GITHUB_TOKEN=ghp_xxx python3 create_github_issues.py
"""

import os
import sys
import json
import time
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
REPO  = "k3hr/agriTools"
TOKEN = os.environ.get("GITHUB_TOKEN", "")
API   = f"https://api.github.com/repos/{REPO}"

if not TOKEN:
    sys.exit("❌  Définis la variable GITHUB_TOKEN=ghp_... avant de lancer le script.")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept":        "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "Content-Type":  "application/json",
}


def api_post(path: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(f"{API}{path}", data=data, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def api_get(path: str) -> dict:
    req = urllib.request.Request(f"{API}{path}", headers=HEADERS)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


# ── Labels ────────────────────────────────────────────────────────────────────
LABELS = [
    {"name": "bug",        "color": "d73a4a", "description": "Quelque chose ne fonctionne pas"},
    {"name": "ingestion",  "color": "0075ca", "description": "Modules d'ingestion de données"},
    {"name": "geo",        "color": "006b75", "description": "Données géographiques"},
    {"name": "tests",      "color": "e4e669", "description": "Tests unitaires et couverture"},
    {"name": "app",        "color": "7057ff", "description": "Interface Streamlit"},
    {"name": "phase4",     "color": "008672", "description": "Phase 4 — Consolidation & profondeur"},
    {"name": "phase5",     "color": "0e8a16", "description": "Phase 5 — Usage réel & industrialisation"},
    {"name": "docs",       "color": "cfd3d7", "description": "Documentation technique"},
    {"name": "implantation","color": "b60205","description": "Moteur de scoring foncier"},
    {"name": "enrichment", "color": "f9d0c4", "description": "Enrichissement automatique des parcelles"},
    {"name": "ux",         "color": "e99695", "description": "Expérience utilisateur Streamlit"},
    {"name": "pydantic",   "color": "1d76db", "description": "Validation avec Pydantic v2"},
    {"name": "refactor",   "color": "fef2c0", "description": "Refactoring sans changement de comportement"},
    {"name": "qualité",    "color": "d4c5f9", "description": "Qualité du code et robustesse"},
    {"name": "feature",    "color": "a2eeef", "description": "Nouvelle fonctionnalité"},
    {"name": "planning",   "color": "84b6eb", "description": "Planification culturale"},
    {"name": "compta",     "color": "bfd4f2", "description": "Comptabilité et finances"},
    {"name": "carry-over", "color": "e0e0e0", "description": "Tâche reportée d'une phase précédente"},
    {"name": "blocked",    "color": "b4a0e5", "description": "Bloqué en attente d'un prérequis externe"},
]

def ensure_labels():
    print("🏷️  Création des labels...")
    existing = {l["name"] for l in api_get("/labels")}
    for label in LABELS:
        if label["name"] in existing:
            print(f"   ↩  '{label['name']}' existe déjà")
        else:
            try:
                api_post("/labels", label)
                print(f"   ✅  '{label['name']}' créé")
            except urllib.error.HTTPError as e:
                print(f"   ⚠️  '{label['name']}' erreur : {e}")
        time.sleep(0.3)


# ── Issues ────────────────────────────────────────────────────────────────────
ISSUES = [
    # ── Ticket 2 ──────────────────────────────────────────────────────────────
    {
        "title": "Tests unitaires app/components/parcelle.py et 4_Comparaison_Parcelles.py",
        "labels": ["tests", "app", "phase4"],
        "body": """\
## Contexte

La page de comparaison multi-parcelles (`4_Comparaison_Parcelles.py`) et le composant `app/components/parcelle.py` ont été ajoutés en Phase 4 sans couverture de tests dédiée. La ROADMAP Phase 4 identifie cette extension comme *in progress*.

Le projet est à 127 tests passants — ces deux modules sont les principaux angles morts.

## Objectif

Atteindre ≥ 90 % de couverture sur `app/components/` et `app/pages/4_Comparaison_Parcelles.py`.

## Actions détaillées

### `tests/test_app_parcelle.py` (nouveau fichier)
- [ ] Tester le rendu du composant avec une `Parcelle` complète (tous les champs renseignés)
- [ ] Tester le rendu avec une `Parcelle` minimale (champs optionnels absents)
- [ ] Tester l'affichage quand `score` est `None` (pas encore calculé)
- [ ] Tester l'affichage quand `score == 0` (cas limite bas)
- [ ] Tester l'affichage quand `score == 100` (cas limite haut)
- [ ] Tester que les trois axes (économique, eau, topographie) sont bien affichés quand présents
- [ ] Vérifier qu'aucune exception n'est levée sur les types inattendus (ex: score sous forme de string)

### `tests/test_app_comparaison.py` (nouveau fichier)
- [ ] Tester la sélection de 2 parcelles → tableau side-by-side cohérent
- [ ] Tester la sélection de 0 parcelle → message vide approprié, pas de crash
- [ ] Tester la sélection de 1 seule parcelle → comportement défini (message ou affichage solo)
- [ ] Tester le tri par score global (croissant / décroissant)
- [ ] Tester la mise en évidence de la meilleure valeur par critère (couleur)
- [ ] Tester la cohérence du bar chart avec les données brutes
- [ ] Tester avec des pondérations custom (ex: axe eau = 70 %)

### Infra
- [ ] Vérifier que `pytest --cov=app --cov-report=term-missing` tourne sans erreur
- [ ] Ajouter les nouveaux fichiers de test dans la CI si applicable
- [ ] Documenter dans `docs/tests.md` la stratégie de test des composants Streamlit (mock vs rendering)

## Critère d'acceptation

```bash
pytest tests/test_app_parcelle.py tests/test_app_comparaison.py \\
  --cov=app/components --cov=app/pages \\
  --cov-report=term-missing
```
→ **Coverage ≥ 90 %** sur ces deux modules, tous les tests ✅.
""",
    },

    # ── Ticket 3 ──────────────────────────────────────────────────────────────
    {
        "title": "Mise à jour implantation/README.md + feuille de route Phase 5",
        "labels": ["docs", "implantation", "phase4"],
        "body": """\
## Contexte

La ROADMAP Phase 4 liste la mise à jour de `implantation/README.md` comme tâche *in progress*. Depuis la rédaction initiale, plusieurs éléments ont évolué :
- Ajout de la comparaison multi-parcelles
- Enrichissement automatique depuis le datalake
- Format d'identifiant parcelle stabilisé : `{dept}{commune}_{année}_{seq}`
- Génération de rapports PDF via ReportLab

Le README doit servir de point d'entrée unique pour comprendre et utiliser le module `implantation/`.

## Actions détaillées

### Contenu à ajouter / mettre à jour

- [ ] **Vue d'ensemble** : schéma du workflow complet
  `création parcelle → enrichissement datalake → scoring → rapport PDF`
- [ ] **Format identifiant** : documenter `{dept}{commune}_{année}_{seq}` avec exemples
- [ ] **Modèle Pydantic `Parcelle`** : lister tous les champs, obligatoires vs optionnels, types
- [ ] **Moteur de scoring** : décrire les 3 axes, les 13 critères, les pondérations par défaut et leur modification
- [ ] **Page Streamlit `3_Parcelle.py`** : captures d'écran ou description des interactions
- [ ] **Page Streamlit `4_Comparaison_Parcelles.py`** : documenter les paramètres, le tableau side-by-side, l'export
- [ ] **`implantation/enrichment/service.py`** : documenter l'API, les sources enrichies (météo, DVF, BSS/ADES), les cas d'absence de données
- [ ] **Génération de rapport PDF** : documenter `implantation/reports/`, les champs présents dans le rapport
- [ ] **Exemples de code** : vérifier que tous les snippets du README s'exécutent tels quels (pas de chemins hardcodés cassés)

### Section Phase 5
- [ ] Ajouter une section "Ce qui reste à faire (Phase 5)" dans le README :
  - Intégration pédologie INRAE GéoSol
  - Enrichissement auto depuis coordonnées GPS
  - Filtrage multi-parcelles
  - Validation terrain

### Qualité
- [ ] Relire à froid après 24h (ou faire relire)
- [ ] Vérifier tous les liens internes (ancres, chemins relatifs)
- [ ] S'assurer que le README est lisible sans avoir le code sous les yeux

## Critère d'acceptation

Un développeur qui n'a jamais vu le projet peut scorer une parcelle et générer un PDF en suivant uniquement le README, sans lire le code source.
""",
    },

    # ── Ticket 4 ──────────────────────────────────────────────────────────────
    {
        "title": "Ingestion données pédologiques INRAE GéoSol",
        "labels": ["ingestion", "geo", "implantation", "phase4", "feature"],
        "body": """\
## Contexte

Phase 4 prévoit l'intégration des données de sol INRAE GéoSol. Actuellement, le scoring implantation ne contient aucun critère pédologique — ce paramètre avait été écarté en Phase 3 faute de source de données disponible.

L'axe sol est pourtant déterminant pour le maraîchage (texture, profondeur, pH, drainage).

## Objectif

Créer un module d'ingestion `ingestion/geo/geosol.py` et intégrer un critère pédologique dans le moteur de scoring.

## Actions détaillées

### Exploration & qualification de la source
- [ ] Explorer le portail INRAE GéoSol : https://geosol.inrae.fr
- [ ] Identifier le format d'accès disponible (WFS, WCS, API REST, téléchargement CSV/shapefile)
- [ ] Vérifier la licence d'utilisation (Etalab / usage personnel OK ?)
- [ ] Documenter les variables disponibles : type de sol, texture (argile/limon/sable %), pH eau, profondeur utile, cailloux, drainage
- [ ] Identifier la résolution spatiale et la couverture nationale

### `ingestion/geo/geosol.py`
- [ ] Fonction `fetch_geosol(lat: float, lon: float, buffer_m: int = 500) -> dict` : interroge l'API par coordonnées ou bbox
- [ ] Normaliser la réponse vers un dict standard : `{"texture": str, "ph": float, "profondeur_cm": int, "drainage": str, ...}`
- [ ] Gérer les zones sans données (retour `None` ou valeurs `NaN`, pas d'exception)
- [ ] Sauvegarder en Parquet dans `datalake/processed/geo/geosol/` (partitionné par dept ou bbox)
- [ ] Logger les requêtes avec `rich` (cohérent avec les autres modules)

### Intégration dans le scoring
- [ ] Définir le critère pédologique : score 0-100 à partir de la texture, pH et profondeur
  - Exemple : argile > 40 % → malus drainage ; pH 6-7 → optimal ; profondeur < 30 cm → éliminatoire
- [ ] Décider de l'axe d'intégration : nouvel axe "Pédologie" (4ème axe) ou ajout dans l'axe eau/irrigation
- [ ] Mettre à jour `implantation/scoring/` en conséquence
- [ ] Adapter les pondérations par défaut (si 4ème axe : rebalancer les 3 autres à 100 %)
- [ ] Mettre à jour le modèle `Parcelle` (champs sol optionnels)

### Tests
- [ ] Créer `tests/test_geosol.py` :
  - Test avec coordonnées de référence (parcelle connue en Île-de-France)
  - Test zone sans données (mer, montagne)
  - Test parsing de la réponse GéoSol mockée
  - Test calcul du score pédologique avec valeurs extrêmes

### Documentation
- [ ] Mettre à jour `implantation/README.md` avec le nouveau critère
- [ ] Documenter la source dans `docs/sources.md`

## Critère d'acceptation

```python
from ingestion.geo.geosol import fetch_geosol
result = fetch_geosol(lat=48.85, lon=2.35)
assert result is not None
assert "texture" in result
assert 0 <= result["ph"] <= 14
```

Et : une parcelle test reçoit un score pédologique calculé depuis des données GéoSol réelles.
""",
    },

    # ── Ticket 5 ──────────────────────────────────────────────────────────────
    {
        "title": "Enrichissement automatique d'une parcelle depuis ses coordonnées GPS",
        "labels": ["implantation", "enrichment", "ux", "phase4"],
        "body": """\
## Contexte

`implantation/enrichment/service.py` existe mais l'UX de déclenchement n'est pas intégrée dans Streamlit. L'utilisateur doit actuellement renseigner manuellement les données météo, DVF, BSS/ADES sur une parcelle candidate.

L'objectif est qu'une simple saisie de coordonnées GPS déclenche l'enrichissement automatique depuis le datalake.

## Actions détaillées

### `implantation/enrichment/service.py`
- [ ] Vérifier que l'API accepte `(lat: float, lon: float)` comme point d'entrée (refactor si nécessaire)
- [ ] Documenter les sources enrichies et leur mapping vers les champs `Parcelle` :
  - Open-Meteo → `precipitations_mm_an`, `ensoleillement_h_an`, `gel_jours_an`
  - DVF → `prix_moyen_ha`, `surface_median_ha_commune`
  - BSS/ADES → `acces_eau_type`, `debit_m3h`, `profondeur_nappe_m`
  - RPG → `cultures_dominantes_voisinage`
  - GéoSol (à venir, ticket #4) → champs sol
- [ ] Gérer proprement les cas d'absence de données par source (zone non couverte, données trop anciennes) : log warning + champ laissé à `None`, pas d'exception
- [ ] Ajouter un paramètre `sources: list[str] = None` pour enrichissement sélectif
- [ ] Retourner un rapport d'enrichissement : `{"source": str, "status": "ok"|"missing"|"error", "fields_set": list}`

### `app/pages/3_Parcelle.py`
- [ ] Ajouter section "Enrichissement automatique" avec champs `latitude` + `longitude` (st.number_input)
- [ ] Ajouter un bouton "Enrichir depuis coordonnées"
- [ ] Afficher un `st.spinner("Enrichissement en cours...")` pendant l'appel
- [ ] Après enrichissement : pré-remplir les champs du formulaire avec les valeurs trouvées
- [ ] Afficher un tableau récapitulatif : source | statut | champs renseignés
- [ ] Les champs pré-remplis doivent rester éditables (pas de lock)
- [ ] Gérer le cas où aucune source ne renvoie de données (message informatif, pas de crash)

### Tests
- [ ] Étendre `tests/test_implantation_enrichment.py` :
  - Test enrichissement complet avec mocks des 4 sources
  - Test source DVF absente (zone rurale sans transactions)
  - Test source BSS hors périmètre (zone non couverte BRGM)
  - Test enrichissement sélectif (sources=['meteo', 'dvf'])
  - Test que les champs `None` ne provoquent pas d'erreur dans le scoring

## Critère d'acceptation

1. Saisie de `lat=48.xx, lon=2.xx` dans la page Parcelle
2. Clic "Enrichir depuis coordonnées"
3. → Spinner affiché pendant le traitement
4. → Formulaire pré-rempli : météo + DVF + BSS sans intervention manuelle
5. → Tableau récapitulatif des sources (statut ok/manquant)
6. `pytest tests/test_implantation_enrichment.py` ✅
""",
    },

    # ── Ticket 6 ──────────────────────────────────────────────────────────────
    {
        "title": "Contrats Pydantic sur les parseurs d'ingestion (fail fast)",
        "labels": ["refactor", "ingestion", "pydantic", "qualité", "carry-over"],
        "body": """\
## Contexte

Carry-over de Phase 1. Les parseurs dans `ingestion/meteo/`, `ingestion/prix/`, `ingestion/geo/` retournent des DataFrames Polars sans validation de schéma explicite. Pydantic v2 est déjà en dépendance du projet.

Sans validation à la sortie des parseurs, une API qui change silencieusement (nouveau champ, type changé, clé renommée) passe inaperçue et corrompt le datalake. Le bug ne se manifeste qu'au moment de la requête DuckDB, loin de l'ingestion.

## Objectif

Ajouter des contrats de sortie sur chaque parseur pour échouer tôt, avec un message d'erreur explicite.

## Actions détaillées

### Définition de l'approche
- [ ] Choisir entre deux approches et documenter le choix dans `docs/architecture.md` :
  - **Option A (Pydantic)** : définir des `BaseModel` de sortie, valider un échantillon avant écriture Parquet
  - **Option B (Polars schema)** : définir des `pl.Schema` explicites avec `enforce_schema=True` à la lecture
  - Recommandation : Option A pour les parseurs métier (riche en sémantique), Option B pour les parseurs bulk simples

### Modèles Pydantic à créer (si Option A)

Créer `ingestion/_schemas.py` :
- [ ] `MeteoRecord` : `date`, `temperature_max`, `temperature_min`, `precipitation_mm`, `ensoleillement_h` (types stricts, validateurs de plage)
- [ ] `PrixMINRecord` : `date`, `produit`, `unite`, `prix_min`, `prix_max`, `prix_moyen`, `marche`
- [ ] `DVFRecord` : `date_mutation`, `valeur_fonciere`, `surface_reelle_bati`, `surface_terrain`, `commune`, `code_postal`
- [ ] `RPGRecord` : `code_culture`, `libelle_culture`, `geometry` (WKT ou bbox), `annee`
- [ ] `BSSOuvrage` : `bss_id`, `commune`, `profondeur_m`, `debit_m3h`, `nappe`
- [ ] `ADESRecord` : `bss_id`, `date`, `niveau_nappe_m`

### Intégration dans les parseurs
- [ ] `ingestion/meteo/open_meteo.py` : valider chaque ligne avec `MeteoRecord.model_validate(row)` avant concat Polars
- [ ] `ingestion/prix/rnm.py` : idem avec `PrixMINRecord`
- [ ] `ingestion/geo/dvf.py` : valider un échantillon (1 %) + schema global
- [ ] `ingestion/geo/rpg.py` : valider le schema Polars (Option B recommandée vu le volume)
- [ ] `ingestion/geo/bss.py` + `ades.py` : valider avec modèles BSS/ADES

### Comportement attendu
- [ ] En cas d'échec de validation : logger l'erreur avec `rich`, lever `IngestionValidationError` (exception custom)
- [ ] Ajouter `IngestionValidationError` dans `ingestion/__init__.py`
- [ ] Ne pas écrire en Parquet si la validation échoue (atomic write : valider → écrire, pas écrire → valider)

### Tests
- [ ] Pour chaque parseur : ajouter un test avec réponse API valide → validation OK
- [ ] Pour chaque parseur : ajouter un test avec champ manquant → `IngestionValidationError` levée
- [ ] Pour chaque parseur : ajouter un test avec type incorrect (ex: prix = "N/A") → exception explicite

## Critère d'acceptation

```python
# Simuler une API qui renvoie un type cassé
response_broken = {"date": "2024-01-01", "precipitation_mm": "N/A", ...}
# → doit lever IngestionValidationError avec message clair, pas KeyError ou ValueError silencieux
```

Aucune écriture Parquet ne se fait si la validation échoue.
""",
    },

    # ── Ticket 7 ──────────────────────────────────────────────────────────────
    {
        "title": "Filtrage et tri avancés dans la comparaison multi-parcelles",
        "labels": ["app", "ux", "implantation", "phase5"],
        "body": """\
## Contexte

La page `4_Comparaison_Parcelles.py` affiche un tableau side-by-side et des bar charts. Avec plusieurs parcelles candidates (5-10+), l'interface devient difficile à lire sans capacité de filtrage et de tri.

Phase 5 prévoit des capacités de filtrage multi-parcelles. C'est l'extension naturelle du travail déjà fait en Phase 4.

## Actions détaillées

### Filtrage (sidebar Streamlit)
- [ ] Slider "Score minimum global" → masquer les parcelles sous le seuil
- [ ] Sliders par axe : "Score éco minimum", "Score eau minimum", "Score topo minimum"
- [ ] Filtre par département (multiselect si parcelles dans plusieurs depts)
- [ ] Filtre par surface : range slider (min ha / max ha)
- [ ] Filtre par prix : range slider (€/ha min / max)
- [ ] Checkbox "Masquer les parcelles sans données complètes"
- [ ] Bouton "Réinitialiser les filtres"
- [ ] Afficher en temps réel : "X parcelles affichées sur Y"

### Tri du tableau
- [ ] Tri par score global (▲ / ▼)
- [ ] Tri par axe (clic sur l'en-tête de colonne de chaque axe)
- [ ] Tri par critère individuel (prix/ha, précipitations, pente, etc.)
- [ ] Conserver l'ordre de tri en mémoire session (`st.session_state`)

### Mise en évidence
- [ ] Surligner la meilleure valeur de chaque ligne en vert (déjà partiel → vérifier cohérence)
- [ ] Surligner la moins bonne valeur en rouge
- [ ] Option "Mode daltonisme" : utiliser des icônes (✅/⚠️/❌) en plus des couleurs

### Export
- [ ] Bouton "Exporter la comparaison (CSV)" → télécharger le tableau filtré + trié
- [ ] Bouton "Exporter le rapport multi-parcelles (PDF)" → un PDF avec le tableau comparatif et les bar charts
- [ ] Nommer le fichier exporté avec la date et la liste des identifiants de parcelles

### Tests
- [ ] Test : 10 parcelles, filtre score > 60 → seules les parcelles qualifiées restent
- [ ] Test : 0 parcelle après filtre → message "Aucune parcelle ne correspond aux filtres"
- [ ] Test : 1 seule parcelle après filtre → affichage solo cohérent
- [ ] Test : export CSV contient exactement les parcelles filtrées
- [ ] Test : le tri par score global est stable (stable sort)

## Critère d'acceptation

Un utilisateur peut isoler les 3 meilleures parcelles sur l'axe eau depuis une liste de 10, sans modifier le code, en moins de 30 secondes.

Export CSV fonctionnel avec les données filtrées.
""",
    },

    # ── Ticket 8 ──────────────────────────────────────────────────────────────
    {
        "title": "Module planification culturale — modèle de données + vue Streamlit calendrier",
        "labels": ["feature", "planning", "phase4", "app"],
        "body": """\
## Contexte

Phase 4 liste un "outil de planification culturale" parmi les items prévus. Il n'existe actuellement aucune structure pour gérer les rotations, les fenêtres de semis/plantation/récolte ou les planches de culture.

Phase 4 : **ébauche** (pas de moteur d'optimisation — affichage seul). L'objectif est de poser les fondations propres.

## Actions détaillées

### Modèle de données (`ingestion/perso/cultures.py`)
- [ ] Définir les modèles Pydantic dans `ingestion/perso/_schemas.py` :
  - `Culture` : `nom`, `famille`, `j_semis_avant_plantation`, `j_croissance`, `j_recolte_echelonnee`, `espacement_cm`, `besoins_eau` (faible/moyen/fort), `sensibilite_gel`
  - `Planche` : `id`, `surface_m2`, `exposition`, `sol_type` (optionnel)
  - `Lot` : `id`, `culture: Culture`, `planche: Planche`, `date_semis`, `date_plantation`, `date_recolte_debut`, `date_recolte_fin` (calculées ou saisies)
  - `Saison` : `annee`, `lots: list[Lot]`
- [ ] Créer `ingestion/perso/cultures.py` :
  - Lire depuis `datalake/raw/perso/planning/cultures.csv` (ou `.toml`) les cultures paramétrées
  - Lire depuis `datalake/raw/perso/planning/planning_{annee}.csv` les lots planifiés
  - Valider avec les modèles Pydantic
  - Sauvegarder en Parquet : `datalake/processed/perso/planning/`

### Format des fichiers d'entrée
- [ ] Créer `datalake/raw/perso/planning/cultures_template.csv` avec les colonnes et quelques exemples (tomates, carottes, salades)
- [ ] Créer `datalake/raw/perso/planning/planning_template.csv` avec exemples de lots sur 1 saison
- [ ] Documenter le format dans `docs/planning.md`
- [ ] Ajouter les fichiers perso dans `.gitignore`, garder uniquement les templates versionnés

### Page Streamlit `app/pages/5_Planning.py`
- [ ] Vue "Calendrier saison" : Gantt horizontal sur 12 mois
  - Axe X : mois (janv → déc)
  - Axe Y : planches / lots
  - Barres colorées par culture (palette distincte)
  - Bibliothèque : Plotly `timeline` (recommandé) ou fallback tableau Polars stylisé
- [ ] Vue "Liste des lots" : tableau éditable (st.data_editor si Streamlit >= 1.23)
- [ ] Sélecteur d'année (si plusieurs saisons disponibles)
- [ ] Indicateur d'occupation : % de surface occupée par mois
- [ ] Message si aucun planning trouvé : lien vers le fichier template

### Tests
- [ ] `tests/test_planning.py` :
  - Test parsing `cultures_template.csv` → liste de `Culture` valides
  - Test parsing `planning_template.csv` → liste de `Lot` valides
  - Test calcul date de récolte depuis date semis + durées
  - Test chevauchement de lots sur la même planche (doit lever un warning, pas une exception)
  - Test saison vide → liste vide, pas d'exception

## Critère d'acceptation

1. Remplir `planning_2026.csv` avec 5 cultures sur 3 planches
2. Lancer Streamlit → page 5_Planning affiche le Gantt
3. `pytest tests/test_planning.py` ✅
""",
    },

    # ── Ticket 9 ──────────────────────────────────────────────────────────────
    {
        "title": "Module documentation ferme — index de documents administratifs",
        "labels": ["feature", "docs", "phase4", "app"],
        "body": """\
## Contexte

Phase 4 prévoit un "module documentation ferme" pour centraliser les documents administratifs et techniques liés à l'exploitation (baux, cerfa, plans d'irrigation, certifications, factures de matériel, etc.).

Contrainte forte : aucun document sensible ne doit être versionné dans Git. Seul l'index des métadonnées est versionnable.

## Actions détaillées

### Structure datalake
- [ ] Créer convention de répertoires dans `datalake/raw/perso/docs/` :
  ```
  docs/
  ├── administratif/    # baux, cerfa, enregistrements
  ├── technique/        # plans irrigation, fiches matériel, certifications
  ├── financier/        # devis, factures (hors comptabilité hledger)
  └── divers/
  ```
- [ ] Ajouter `datalake/raw/perso/docs/**` dans `.gitignore`
- [ ] Versionner uniquement `datalake/processed/perso/docs/index.parquet`

### `ingestion/perso/docs.py`
- [ ] Fonction `index_documents(root_dir: Path) -> pl.DataFrame` :
  - Scanner récursivement `datalake/raw/perso/docs/`
  - Extraire les métadonnées : `nom_fichier`, `chemin_relatif`, `type_doc` (PDF/image/autre), `categorie` (déduite du sous-dossier), `taille_ko`, `date_modification`, `tags` (lus depuis un fichier `.tags` optionnel côté fichier)
  - Retourner un DataFrame Polars avec ces colonnes
- [ ] Sauvegarder en `datalake/processed/perso/docs/index.parquet`
- [ ] Fonction `add_tags(fichier: str, tags: list[str])` : écrire un `.tags` à côté du fichier

### Système de tags (`.tags`)
- [ ] Format : fichier texte `{nom_doc}.tags` dans le même répertoire, une ligne = un tag
- [ ] Exemple : `bail_ferme.pdf.tags` contient `bail\nfoncier\n2025`
- [ ] Documenter dans `docs/documentation_ferme.md`

### Page Streamlit `app/pages/6_Documentation.py`
- [ ] Afficher la liste des documents depuis `index.parquet`
- [ ] Filtres : par catégorie (multiselect), par tag (multiselect), par type de fichier
- [ ] Champ de recherche texte (filtre sur `nom_fichier`)
- [ ] Tri par date de modification (le plus récent en haut par défaut)
- [ ] Bouton "Ouvrir" → `st.download_button` avec le contenu du fichier
- [ ] Bouton "Réindexer" → relance `index_documents()` et recharge
- [ ] Si `index.parquet` absent : message clair + bouton pour lancer l'indexation initiale

### Tests
- [ ] `tests/test_docs.py` :
  - Test indexation d'un dossier de test avec 5 fichiers factices
  - Test que les fichiers `.tags` sont bien lus et associés
  - Test que l'index Parquet contient les bonnes colonnes et types
  - Test qu'un dossier vide retourne un DataFrame vide (pas d'exception)
  - Test filtre par catégorie

## Critère d'acceptation

1. Déposer 3 PDFs dans `datalake/raw/perso/docs/administratif/`
2. Lancer `python -m ingestion.perso.docs` → `index.parquet` créé
3. Page Streamlit 6_Documentation → liste les 3 fichiers avec filtres
4. Aucun fichier de `docs/` n'apparaît dans `git status`
5. `pytest tests/test_docs.py` ✅
""",
    },

    # ── Ticket 10 ─────────────────────────────────────────────────────────────
    {
        "title": "Ingestion comptabilité hledger → datalake → tableau de bord Streamlit",
        "labels": ["ingestion", "compta", "carry-over", "blocked", "feature"],
        "body": """\
## Contexte

Carry-over de Phase 2. Ce ticket est **bloqué** jusqu'à la mise en place de hledger sur la machine de production. À débloquer dès que `ferme.journal` est initialisé et que hledger est installé.

hledger est un outil de comptabilité en texte brut (plain-text accounting). Il est idéal pour une exploitation maraîchère : versioning Git des écritures, pas de dépendance cloud, format ouvert.

## Prérequis (hors scope du ticket, à faire avant)
- [ ] Installer hledger (`winget install hledger` ou `brew install hledger`)
- [ ] Créer `ferme.journal` avec le plan comptable de base
- [ ] Définir les comptes : `revenus:ventes`, `charges:semences`, `charges:materiel`, `charges:cotisations`, `actif:banque`, `actif:caisse`

## Actions détaillées

### `ingestion/perso/comptabilite.py`
- [ ] Fonction `parse_journal(journal_path: Path) -> pl.DataFrame` :
  - Appel à `hledger bal --output-format=csv` ou `hledger register --output-format=csv`
  - Parser la sortie CSV en DataFrame Polars
  - Colonnes attendues : `date`, `compte`, `montant`, `devise`, `libelle`, `tags`
- [ ] Fonction `export_to_parquet(df: pl.DataFrame)` → `datalake/processed/perso/compta/`
- [ ] Modèle Pydantic `EcritureComptable` pour valider chaque ligne
- [ ] Ajouter `ferme.journal` dans `.gitignore` (données financières privées)
- [ ] Versionner uniquement `chart_of_accounts.journal` (plan comptable sans transactions)

### Agrégations (à calculer au moment de l'ingestion)
- [ ] Résumé mensuel : `produit_brut`, `charges_totales`, `resultat_net` par mois
- [ ] Résumé par catégorie de charges (semences, matériel, cotisations, etc.)
- [ ] Trésorerie : solde banque + caisse par semaine

### Page Streamlit `app/pages/7_Comptabilité.py`
- [ ] Vue "P&L mensuel" : graphe barres (produit brut vs charges) par mois sur l'année en cours
- [ ] Vue "Charges par catégorie" : camembert ou treemap
- [ ] Vue "Trésorerie" : courbe du solde dans le temps
- [ ] Sélecteur d'année fiscale
- [ ] KPIs en haut de page : chiffre d'affaires YTD, charges YTD, résultat YTD, solde trésorerie actuel
- [ ] Message "Données non disponibles" si `compta/*.parquet` absent

### Tests
- [ ] Créer `tests/data/test.journal` avec une vingtaine de transactions fictives (données non sensibles)
- [ ] `tests/test_comptabilite.py` :
  - Test parsing du journal de test → DataFrame correct
  - Test calcul P&L mensuel sur données fictives
  - Test que les valeurs négatives (charges) sont correctement signées
  - Test journal vide → DataFrame vide, pas d'exception
  - Test modèle Pydantic sur ligne malformée → exception explicite

## Critère d'acceptation

```bash
# Avec hledger installé et ferme.journal initialisé :
python -m ingestion.perso.comptabilite
# → datalake/processed/perso/compta/*.parquet créé

# Streamlit → page 7_Comptabilité affiche P&L et trésorerie
pytest tests/test_comptabilite.py  # ✅ avec journal de test fictif
```

**Note :** Les tests doivent passer même sans hledger installé (journal de test pré-parsé mockable).
""",
    },
]


def create_issues():
    print(f"\n📋  Création de {len(ISSUES)} tickets GitHub sur {REPO}...\n")
    created = []
    for i, issue in enumerate(ISSUES, start=2):
        print(f"   [{i}/10] {issue['title'][:70]}...")
        try:
            result = api_post("/issues", issue)
            url = result.get("html_url", "?")
            num = result.get("number", "?")
            print(f"          ✅  #{num} → {url}")
            created.append((num, issue["title"], url))
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"          ❌  Erreur {e.code} : {body[:200]}")
        time.sleep(1)  # respecter le rate limit GitHub
    return created


def main():
    print(f"\n🚀  agriTools — Création des tickets GitHub")
    print(f"    Repo : https://github.com/{REPO}\n")

    ensure_labels()
    print()

    created = create_issues()

    print(f"\n{'─'*60}")
    print(f"✅  {len(created)}/9 tickets créés\n")
    for num, title, url in created:
        print(f"  #{num} — {title[:55]}")
        print(f"       {url}")
    print()


if __name__ == "__main__":
    main()
