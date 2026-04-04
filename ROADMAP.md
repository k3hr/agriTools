# agriTools — Roadmap

**Dernière mise à jour :** 2026-04-04
**État global :** Phases 0–3 terminées · Phase 4 en cours de constitution

---

## État des phases

| Phase | Intitulé | Statut |
|---|---|---|
| Phase 0 | Bootstrap | ✅ Terminée |
| Phase 1 | Datalake fondations | ✅ Terminée |
| Phase 2 | Données personnelles + enrichissement geo | ✅ Terminée |
| Phase 3 | Outil d'aide à l'implantation v1 | ✅ Terminée |
| Phase 4 | Consolidation & profondeur | 🔵 En cours (comparaison multi-parcelles ajouté) |

---

## 1. Vision & Périmètre

**agriTools** est une suite d'outils personnels à destination d'un maraîcher souhaitant piloter son activité par la donnée. Le projet n'a pas de vocation commerciale. Il doit pouvoir vivre et évoluer sur l'ensemble d'une carrière.

| Composant | Objectif |
|---|---|
| **Datalake** | Centraliser, versionner et interroger l'ensemble des données utiles à la ferme (open data + données personnelles + documentation) |
| **Outil d'aide à l'implantation** | Comparer des opportunités foncières en modélisant des scénarios selon les paramètres du terrain |

---

## 2. Principes directeurs

- **Souveraineté totale des données** — 100 % local, aucune dépendance à un service cloud tiers pour le stockage ou le traitement.
- **Simplicité d'abord** — Pas d'infrastructure Kafka, Spark ou Airflow. La complexité s'ajoute quand le besoin est avéré, pas par anticipation.
- **Durabilité** — Les formats de fichiers choisis (Parquet, CSV, GeoJSON) sont lisibles sans outillage propriétaire dans 20 ans.
- **Itérabilité** — Chaque phase doit produire quelque chose d'utilisable. Aucune phase "invisible" de plus de 4 semaines.
- **Git comme colonne vertébrale** — Le code est versionné. Les données brutes ne le sont pas (trop lourdes), mais leur catalogue l'est.

---

## 3. Choix techniques

### 3.1 Stack Python-first

| Couche | Outil | Justification |
|---|---|---|
| Runtime | Python 3.12+ | Écosystème data mature, confort |
| Gestion packages | **uv** | Remplacement moderne de pip/venv, ultra-rapide |
| ETL / transformation | **Polars ≥ 0.20** | Plus rapide que pandas, API lazy evaluation, gestion native Parquet |
| Moteur analytique | **DuckDB ≥ 1.0** (fichier `.duckdb`) | SQL in-process sur Parquet, zéro serveur |
| Validation schémas | **Pydantic v2** | Contrats de données explicites sur les modèles métier |
| Interface utilisateur | **Streamlit ≥ 1.35** | Prototypage rapide d'apps data, pas de JS requis |
| Cartographie | **GeoPandas + Folium** | Analyse spatiale + rendu carte dans Streamlit |
| Scheduler | **Windows Scheduled Tasks** (scripts PowerShell) | Solution native Windows, zéro dépendance Python supplémentaire |
| Génération PDF | **reportlab ≥ 4.0** | Rapports A4 structurés pour l'outil d'implantation |
| Tests | **pytest** | Couverture des parsers et du moteur de scoring |

### 3.2 Stockage

```
Format source de vérité : Parquet (colonnaire, compressé ZSTD, requêtable directement par DuckDB)
Format staging/raw     : CSV, JSON, GeoJSON — conservés tels quels
Format catalogue       : YAML (lisible humain, un fichier par dataset)
Format config          : TOML (pyproject.toml, config.toml + config.local.toml)
Format rapports        : PDF (reportlab)
```

DuckDB joue le rôle de moteur de requête unifié : il peut interroger des fichiers Parquet, CSV, JSON
et des tables persistées dans un seul fichier `.duckdb`. Pas besoin de Postgres pour un usage solo.

### 3.3 Structure du dépôt

```
agriTools/
├── datalake/
│   ├── raw/                        # données brutes, jamais modifiées
│   │   ├── meteo/
│   │   ├── prix/
│   │   ├── geo/
│   │   └── perso/                  # données personnelles (gitignore)
│   ├── processed/                  # Parquet nettoyés, partitionnés
│   │   ├── meteo/                  # partitionné par année
│   │   ├── prix/
│   │   ├── geo/                    # bss_stations.parquet, ades_chroniques.parquet, rpg_parcelles.parquet
│   │   └── perso/                  # heures_travail.parquet
│   ├── catalog/                    # YAML de métadonnées par dataset (7 fichiers)
│   ├── reports/
│   │   └── implantation/           # PDF générés (gitignore)
│   ├── logs/                       # logs scheduler (gitignore)
│   └── agritools.duckdb            # base DuckDB locale (gitignore)
│
├── ingestion/                      # scripts ETL, un module par source
│   ├── _config.py                  # chargement config.toml + config.local.toml
│   ├── meteo/
│   │   └── open_meteo.py
│   ├── prix/
│   │   ├── rnm.py
│   │   └── dvf.py
│   ├── geo/
│   │   ├── rpg.py
│   │   ├── bss.py
│   │   └── ades.py
│   └── perso/
│       └── heures.py
│
├── implantation/                   # outil d'aide à la décision foncière
│   ├── models/
│   │   └── parcelle.py             # Pydantic Parcelle (26 champs)
│   ├── scoring/
│   │   ├── engine.py               # ScoringEngine, ScoringWeights, ParcelleScore
│   │   └── criteria.py             # 13 critères individuels (0–100)
│   ├── enrichment/                 # enrichissement depuis le datalake (Phase 4)
│   └── reports/
│       └── pdf_report.py           # génération rapport A4 via reportlab
│
├── app/                            # application Streamlit
│   ├── main.py                     # page d'accueil avec métriques globales
│   ├── pages/
│   │   ├── 0_Tableau_de_Bord.py    # état sources, alertes fraîcheur
│   │   ├── 1_Météo.py              # températures, pluie, ETP, bilan hydrique
│   │   ├── 2_Prix.py               # évolution, distribution, saisonnalité RNM
│   │   └── 3_Parcelle.py           # saisie, scoring, export PDF
│   └── components/
│       ├── data.py                 # couche d'accès aux données (cache Streamlit)
│       └── parcelle.py             # persistance JSON parcelles
│
├── scripts/
│   ├── schedule_meteo.ps1          # tâche planifiée Windows météo (daily 6h)
│   └── schedule_rnm.ps1            # tâche planifiée Windows RNM (vendredi 7h)
│
├── tests/                          # 127 tests, tous verts
├── config.toml                     # paramètres globaux (chemins, API, rayon ferme)
├── config.local.toml               # surcharge locale — coords réelles (gitignore)
├── pyproject.toml
└── ROADMAP.md
```

---

## 4. Catalogue des sources de données

### 4.1 Open Data

| Domaine | Source | Statut | Fréquence | Format |
|---|---|---|---|---|
| Météo historique + prévision | **Open-Meteo** | ✅ Ingéré | Quotidien (scheduled) | JSON/API |
| Prix marchés MIN | **FranceAgriMer / RNM** | ✅ Ingéré | Hebdomadaire (scheduled) | CSV/ZIP |
| Registre Parcellaire Graphique | **IGN / RPG 2023** | ✅ Ingéré | Annuel | GeoJSON |
| Prix fonciers agricoles | **DVF (data.gouv.fr)** | ✅ Ingéré | Semestriel | CSV |
| Hydrogéologie / forages | **BRGM / BSS (Hub'eau)** | ✅ Ingéré | Statique | JSON/API |
| Eaux souterraines piézométrie | **ADES (Hub'eau)** | ✅ Ingéré | Mensuel | JSON/API |
| Météo officielle FR | **Météo-France API** | ⬜ Phase 4 | Quotidien | JSON/API |
| Stats agricoles | **Agreste (MASA)** | ⬜ Phase 4 | Annuel | CSV |
| Prix fonciers SAFER | **SAFER** | ⬜ Phase 4 | Annuel | PDF/CSV |
| Données sol (pédologie) | **INRAE GéoSol** | ⬜ Phase 4 | Statique | WMS/WFS |
| Indices économiques | **INSEE** | ⬜ Phase 4 | Annuel | CSV |

### 4.2 Données personnelles

| Domaine | Statut | Format d'entrée |
|---|---|---|
| Relevés heures travail | ✅ Ingéré | CLI (`heures.py`) + CSV |
| Comptabilité | 🔵 Phase 4 | Export hledger CSV + relevé banque CSV |
| Relevés sondes terrain | 🔵 Phase 4 | CSV Raspberry/Arduino (matériel en attente) |
| Inventaire semences | ⬜ Phase 5+ | CSV / formulaire Streamlit |
| Journal cultural | ⬜ Phase 5+ | Markdown structuré |

---

## 5. Phase 4 — Consolidation & profondeur (backlog)

### Reportés depuis phases précédentes

| Item | Origine | Notes |
|---|---|---|
| Pydantic contracts sur parsers | Phase 1 | Parsers robustes pour usage solo — priorité basse |
| Module ingestion comptabilité | Phase 2 | Attente mise en place hledger + relevés bancaires |
| Page dashboard comptabilité | Phase 2 | Dépend du module ingestion compta |
| Module ingestion sondes terrain | Phase 2 | Attente réception matériel |

### Nouvelles entrées Phase 4

| Item | Notes |
|---|---|
| Enrichissement pédologique | INRAE GéoSol — type sol, texture, pH moyen zone |
| Enrichissement automatique parcelle | Pull météo + DVF + BSS depuis coords au moment de la saisie |
| Comparaison multi-parcelles | ✅ Tableau side-by-side + bar chart Streamlit |
| Outil planification culturale | Rotations basiques, calendrier semis/récolte |
| Module documentation ferme | Markdown searchable via DuckDB FTS |
| Couverture de tests étendue | 🔵 Tests `app/components/parcelle.py` et comparaison par charge |
| Documentation technique | 🔵 Mise à jour `implantation/README.md` + feuille de route Phase 5 |
| Rétrospective & roadmap Phase 5+ | ✅ Phase 5 planifié dans roadmap |

---

## 5. Phase 5 — Usage réel & industrialisation (planifié)

| Item | Notes |
|---|---|
| Feedback terrain | Collecter les retours sur les premières parcelles comparées pour affiner les critères et les poids |
| Documentation utilisateur | Guide d'usage Streamlit + process d'enrichissement des parcelles |
| Enrichissement sol | Intégrer données pédologiques INRAE GéoSol et texture/pH dans le scoring |
| Automatisation locale | Valider les tâches planifiées Windows + rapports PDF automatiques |
| Gouvernance des données | Définir lifecycle raw/processed/perso et audit trails |
| Pilotage multi-parcelles | Filtrage, tri et comparaison de lots plus larges | 

---

## 6. Points d'attention & décisions prises

### Données personnelles & confidentialité

Tout ce qui est sous `datalake/raw/perso/`, `datalake/processed/perso/` et `datalake/reports/` est dans `.gitignore`. `config.local.toml` (coords réelles de la ferme) également.

### Identifiant parcelle

Format retenu : `{dept}{commune}_{annee}_{seq}` (ex : `72181_2026_0042`). Permet le croisement avec RPG et DVF. Les JSONs de parcelles sont persistés sous `datalake/raw/perso/parcelles/`.

### Scheduler Windows

Windows Scheduled Tasks via PowerShell (`Register-ScheduledTask`) retenu au lieu d'APScheduler — plus fiable en production Windows, pas de processus Python toujours actif requis. APScheduler reste disponible dans `ingestion/meteo/open_meteo.py` pour usage ad-hoc.

### Hub'eau API — particularités connues

- Coordonnées exposées sous `x`/`y` (pas `longitude`/`latitude`)
- `codes_bdlisa` est une liste — prendre `[0]`
- `altitude_station` est une string — caster en float
- Offset max : `page × size ≤ 20 000` — paginer en conséquence
- Paramètre `distance` non supporté sur `/stations` → utiliser `bbox` + filtre haversine local

### Comptabilité

hledger retenu comme outil de saisie (plain-text accounting). Module d'ingestion à développer une fois hledger configuré et premiers exports disponibles.

---

## 7. Métriques de succès

| Métrique | Cible initiale | Réalisé (Phase 3) |
|---|---|---|
| Sources open data ingérées automatiquement | 3 (Phase 1) | **6** (météo, RNM, RPG, DVF, BSS, ADES) |
| Tests pytest | 60 % couverture | **127 tests** (parsers + scoring) |
| Temps de chargement dashboard météo | < 2 s | < 2 s ✅ |
| Parcelles modélisables dans l'outil | ≥ 10 | Illimité (JSON + Parquet) ✅ |
| Temps pour comparer 2 parcelles | < 5 min (saisie → score) | ~2 min (formulaire → score → PDF) ✅ |
| Rapport PDF implantation | Phase 4 initialement | **Livré en Phase 3** ✅ |

---

## 8. Archives — Phases 0–3 (livré)

### Phase 0 — Bootstrap ✅

| Item | Notes |
|---|---|
| `pyproject.toml` avec uv | Python 3.10+, dépendances core + reportlab |
| Structure dossiers | Conforme section 3.3 |
| `.gitignore` | Données brutes, duckdb, perso/, config.local.toml |
| `config.toml` + `config.local.toml` | Coords ferme réelles dans local (Sarthe, 47.8474 / -0.9416) |
| Ingestion Open-Meteo | Parquet partitionné par an, DuckDB validé |
| README | Installation, premier run |

### Phase 1 — Datalake fondations ✅

| Item | Notes |
|---|---|
| Ingestion météo Open-Meteo | 2021–2026, 1 917 jours, Parquet partitionné par an |
| Ingestion prix RNM | A24+A25+A26 via ZIP locaux, stade Expédition départ bassin |
| Ingestion RPG 2023 | 67 081 parcelles, 185 833 ha, 98 cultures, rayon 25 km |
| Catalogues YAML | `meteo_open_meteo.yaml`, `prix_rnm.yaml`, `geo_rpg.yaml` |
| Dashboard météo | `1_Météo.py` — températures, pluie, ETP, bilan hydrique |
| Dashboard prix | `2_Prix.py` — évolution, distribution, saisonnalité |
| Dashboard tableau de bord | `0_Tableau_de_Bord.py` — état sources, alertes fraîcheur |
| Scheduler Windows | Météo daily 6h, RNM vendredi 7h · logs `datalake/logs/` |
| Tests | 103 tests — open_meteo, rnm, rpg |

### Phase 2 — Données personnelles + enrichissement geo ✅

| Item | Notes |
|---|---|
| Ingestion DVF | `ingestion/prix/dvf.py` — filtre zone géographique, Parquet annuel |
| Catalogue YAML DVF | `datalake/catalog/prix_dvf.yaml` |
| Ingestion BRGM BSS | `ingestion/geo/bss.py` — Hub'eau API, bbox+haversine, 5 stations locales |
| Catalogue YAML BSS | `datalake/catalog/geo_bss.yaml` |
| Ingestion ADES | `ingestion/geo/ades.py` — Hub'eau /chroniques, batching, 24 tests |
| Catalogue YAML ADES | `datalake/catalog/geo_ades.yaml` |
| Module heures de travail | `ingestion/perso/heures.py` — CLI start/stop/add/list/ingest/verify, NLP durée+date |
| Tests ajoutés | dvf (9), bss (21), ades (24), heures (35) → total **127 tests** |

### Phase 3 — Outil d'aide à l'implantation v1 ✅

| Item | Notes |
|---|---|
| Modèle Pydantic Parcelle | `implantation/models/parcelle.py` — 26 champs validés |
| Moteur scoring | `implantation/scoring/` — 13 critères, 3 axes pondérables (éco/eau/topo) |
| Tests implantation | `tests/test_implantation.py` — 47 tests |
| UI formulaire parcelle | `app/pages/3_Parcelle.py` — saisie, validation, scoring temps réel |
| Persistance parcelle | `app/components/parcelle.py` — JSON sous `datalake/raw/perso/parcelles/` |
| Rapport implantation PDF | `implantation/reports/pdf_report.py` — reportlab, 2 pages A4, barres de score colorées, bouton download Streamlit |
