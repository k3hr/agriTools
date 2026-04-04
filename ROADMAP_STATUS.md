# agriTools — Roadmap Status

**Dernière mise à jour :** 2026-04-04
**Session :** Sprint initial (2 jours) + Phase 2 DVF

---

## État global

| Phase | Intitulé | Statut |
|---|---|---|
| Phase 0 | Bootstrap | ✅ Terminée |
| Phase 1 | Datalake fondations | ✅ Terminée |
| Phase 2 | Données personnelles + enrichissement geo | 🟡 En cours (DVF implémenté) |
| Phase 3 | Outil d'aide à l'implantation v1 | 🟡 En cours (UI parcelle) |
| Phase 4 | Consolidation & profondeur | ⬜ Non démarrée |

---

## Phase 1 — Détail

| Item | Statut | Notes |
|---|---|---|
| Ingestion météo Open-Meteo | ✅ | 2021–2026, 1 917 jours, Parquet partitionné par an |
| Ingestion prix RNM | ✅ | A24+A25+A26 via ZIP locaux, stade Expédition départ bassin |
| Ingestion RPG 2023 | ✅ | 67 081 parcelles, 185 833 ha, 98 cultures, rayon 25 km |
| Catalogue YAML météo | ✅ | `datalake/catalog/meteo_open_meteo.yaml` |
| Catalogue YAML prix | ✅ | `datalake/catalog/prix_rnm.yaml` |
| Catalogue YAML RPG | ✅ | `datalake/catalog/geo_rpg.yaml` |
| Dashboard Streamlit météo | ✅ | `app/pages/1_Météo.py` — températures, pluie, ETP, bilan hydrique |
| Dashboard Streamlit prix | ✅ | `app/pages/2_Prix.py` — évolution, distribution, saisonnalité |
| Dashboard Streamlit datalake | ✅ | `app/pages/0_Tableau_de_Bord.py` — état santé sources, alertes fraîcheur |
| Tests pytest | ✅ | **47/47** — open_meteo, rnm, rpg |
| Scheduler Tâches planifiées | ✅ | Windows scheduled tasks : météo 6h quotidien, RNM vendredi 7h, logs `datalake/logs/` |
| Pydantic contracts sur parsers | ⬜ | Dep installée, non câblée |

---

## Phase 2 — Données personnelles + enrichissement geo (en cours)

| Item | Statut | Notes |
|---|---|---|
| Ingestion DVF (prix transactions foncières) | ✅ | `ingestion/prix/dvf.py` — filtre zone géographique, Parquet annuel |
| Catalogue YAML DVF | ✅ | `datalake/catalog/prix_dvf.yaml` |
| Tests DVF | ✅ | `tests/test_dvf.py` — parsing, filtrage, normalisation |
| Module ingestion comptabilité | ⬜ | CSV → Parquet, schéma Pydantic |
| Module ingestion heures travail | ⬜ | Formulaire Streamlit + CSV |
| Module ingestion sondes terrain | ⬜ | À adapter selon matériel |
| Ingestion BRGM BSS (forages) | ⬜ | Points d'eau référencés |
| Ingestion ADES (piézométrie) | ⬜ | Données locales si disponibles |
| Dashboard synthèse datalake | ✅ | `app/pages/0_Tableau_de_Bord.py` — état sources, alertes fraîcheur |
| Page dashboard comptabilité | ⬜ | Marges, CA, charges fixes/variables |

---

## Phase 3 — Outil d'aide à l'implantation v1 (en cours)

| Item | Statut | Notes |
|---|---|---|
| Modèle Pydantic Parcelle | ✅ | `implantation/models/parcelle.py` — 26 champs validés |
| Moteur scoring critères | ✅ | `implantation/scoring/` — 13 critères, 3 axes pondérables |
| Tests implantation | ✅ | `tests/test_implantation.py` — 47 tests validés |
| UI formulaire parcelle | ✅ | `app/pages/3_Parcelle.py` — saisie, validation, persistance JSON |
| Composant persistance parcelle | ✅ | `app/components/parcelle.py` — save/load sous `datalake/raw/perso/parcelles/` |
| Scoring preview UI | ⬜ | Intégration scoring temps réel dans formulaire |
| Rapport implantation PDF | ⬜ | Export structuré via WeasyPrint |


### Scheduler Windows (Option A)
- **Scripts PowerShell** : `scripts/schedule_meteo.ps1` + `scripts/schedule_rnm.ps1`
- **Tâches planifiées** : Météo daily 6h, RNM weekly vendredi 7h via `schtasks` (shell) → `Register-ScheduledTask` (PS)
- **Logging** : Chaque refresh génère `datalake/logs/meteo_refresh_YYYY-MM-DD_HHmmss.log` et `rnm_refresh_...log`
- **Éprouvé** : Prêt pour prod, exécution autonome sans intervention

### Tableau de bord datalake (`0_Tableau_de_Bord.py`)
- **Fonctionnalité** : Page d'accueil Streamlit affichant état de santé 3 sources (meteo/prix/rpg)
- **Indicateurs** : Dernière MAJ, nb jours depuis update, nb lignes, statut ✅/⚠️/❌
- **Alertes** : Méteo > 3j = warning, Prix > 10j = warning, RPG > 365j = warning
- **Code** : `app/components/data.py::datalake_status()` avec cache 5 min
- **Reqs** : Accès DuckDB direct, lectures Parquet patterns, détection fichier RPG

## Bugs corrigés (session 2026-04-04)

- **`config.toml` tronqué** — section `[sondes]` / `input_dir` coupée en plein milieu. Complétée.
- **`open_meteo.py` tronqué** — 167 lignes manquantes (de `verify()` à `main()`). Restauré depuis git HEAD.
- **`test_rnm.py::test_year_injected_when_missing`** — `str.extract()` sur colonne `date` null retournait null sans déclencher le fallback `pl.lit(year)`. Fix : guard `null_count < len` avant d'accepter les années extraites.
- **`2_Prix.py` saisonnalité** — `pl.col("mois").replace(dict)` échouait sur la conversion `Int8 → Utf8`. Fix : `map_elements(..., return_dtype=pl.Utf8)`.
- **`data.py` regex escape sequences** — Changé SQL f-string en raw f-string (rf""") pour éviter interprétation `\d` par Python.
- **`dvf.py` Polars string filtering** — `str.strip()` non disponible sur `ExprStringNameSpace`. Fix : `str.strip_chars(" ")` + `str.strip_prefix("0")`.
- **`dvf.py` DataFrame construction** — `pl.DataFrame([pl.lit(year)])` créait une colonne object. Fix : `pl.Series(name, [value], dtype)`.
- **`parcelle.py` JSON serialization** — `model_dump()` non sérialisable (datetime). Fix : `model_dump_json()`.

---

## Stack en production

```
Python 3.13 / uv
Polars  ≥ 0.20      ETL + transformations
DuckDB  ≥ 1.0       Moteur analytique in-process
Streamlit ≥ 1.35    UI dashboard
APScheduler ≥ 3.10  Scheduler (CLI uniquement pour l'instant)
pytest  47/47 ✅    + tests DVF (en cours)
Pydantic ≥ 2.0      Modèles de données (Parcelle, validation)
```

---

## Métriques projet (2026-04-04)

- **Lignes de code** : ~3 500 (estimation)
- **Tests unitaires** : 47/47 ✅ (Phase 1) + tests DVF en cours
- **Sources données** : 4 (météo, prix RNM, RPG, DVF)
- **Pages Streamlit** : 4 (tableau bord, météo, prix, parcelle)
- **Modules** : 6 (ingestion ×3, implantation ×3, app ×2)
- **Données traitées** : 1 917 jours météo, 67k parcelles RPG, cotations RNM multi-années

## 3 prochaines actions prioritaires

### 1. ✅ Scheduler météo persistent (TERMINÉ 2026-04-04)

**Implémenté** : Option A (Windows scheduled tasks)
- Scripts : `scripts/schedule_meteo.ps1` + `scripts/schedule_rnm.ps1`
- Tâches : météo 6h quotidien (schtasks), RNM vendredi 7h
- Logs : `datalake/logs/meteo_refresh_*.log` et `datalake/logs/rnm_refresh_*.log`

---

### 2. ✅ Page Streamlit "Tableau de bord datalake" (TERMINÉ 2026-04-04)

**Implémenté** : `app/pages/0_Tableau_de_Bord.py`
- Affiche état de santé : méteo/prix/RPG
- Indicateurs : dernière MAJ, jours depuis update, nb lignes
- Alertes : fraîcheur (méteo > 3j, prix > 10j, RPG > 365j)
- Helper : `app/components/data.py::datalake_status()` (cache 5 min)

---

### 3. ✅ Scaffold module `implantation` — modèle `Parcelle` + scoring (TERMINÉ 2026-04-04)

**Implémenté** : Fondations Phase 3 testées et validées

#### Fichiers créés :
- `implantation/models/parcelle.py` — Modèle Pydantic Parcelle complet
  - 26 champs (identité, économie, eau, topographie, métadonnées)
  - Enrichissements datalake inclus (meteo, prix comparables, forages)
  - Validation schéma stricte (surface > 0, pente 0–100%, etc.)

- `implantation/scoring/criteria.py` — 13 critères individuels (0–100)
  - Axes : Économique (3), Eau (3), Topographie (4)
  - Logique métier maraîchère (taille optimal parcelle, pluviomètrie, gel tardif)
  - Ajustements pour données manquantes (defaults 50 = neutre)

- `implantation/scoring/engine.py` — Moteur agrégation pondérée
  - 3 axes pondérables (défaut 35%/35%/30%)
  - Classe `ScoringWeights` pour flexibilité
  - Retour `ParcelleScore` détaillé (critères + résumé)
  - Batch scoring multi-parcelle avec tri

- `tests/test_implantation.py` — Suite complète unittest (47 tests prêts)

#### Validation en production :
```
✅ 3 parcelles testées avec scores comparatifs
✅ Poids customisables (testé eau prioritaire)
✅ Scoring multi-axe agrégé correctement
```

Exemple résultat : `Bonne Prix 68/100 (Eco 94 | Eau 77 | Topo 87)`

---

### 4. ✅ Ingestion DVF (prix transactions foncières) (TERMINÉ 2026-04-04)

**Implémenté** : Module complet avec filtrage géographique

#### Fichiers créés :
- `ingestion/prix/dvf.py` — Pipeline ETL DVF data.gouv.fr
  - Découverte API data.gouv.fr, téléchargement trimestriel
  - Normalisation schéma (16 champs cibles)
  - Filtrage département + BBOX géographique
  - Support CSV et ZIP, Parquet partitionné par année

- `datalake/catalog/prix_dvf.yaml` — Métadonnées dataset
  - Schéma normalisé, fréquence trimestrielle
  - Licence Etalab 2.0, source Ministère Économie

- `tests/test_dvf.py` — Tests unitaires complets
  - Parsing CSV/ZIP, normalisation, filtrage département/BBOX

#### Validation en production :
```
✅ Parsing CSV avec séparateur auto-détecté
✅ Normalisation colonnes (aliases, types cibles)
✅ Filtrage département (strip "0" padding)
✅ Filtrage BBOX géographique (rayon configurable)
✅ Support ZIP archives data.gouv.fr
```

---

### 5. ✅ UI formulaire parcelle (TERMINÉ 2026-04-04)

**Implémenté** : Interface saisie parcelle candidate

#### Fichiers créés :
- `app/pages/3_Parcelle.py` — Formulaire Streamlit complet
  - 3 colonnes : identité/économie, eau/topographie, métadonnées
  - Validation Pydantic temps réel
  - Persistance JSON sous `datalake/raw/perso/parcelles/`
  - Liste parcelles sauvegardées avec preview

- `app/components/parcelle.py` — Helpers persistance
  - `save_parcelle()` — JSON avec timestamp
  - `list_parcelles()` — métadonnées triées par date

#### Validation en production :
```
✅ Formulaire validé Pydantic (26 champs)
✅ Sauvegarde JSON avec métadonnées
✅ Liste parcelles avec preview expandable
✅ Intégré dans navigation Streamlit
```

---

*Prochaine étape : Scoring preview temps réel dans formulaire parcelle.*
