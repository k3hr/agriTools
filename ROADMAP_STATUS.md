# agriTools — Roadmap Status

**Dernière mise à jour :** 2026-04-04
**Session :** Sprint initial (2 jours)

---

## État global

| Phase | Intitulé | Statut |
|---|---|---|
| Phase 0 | Bootstrap | ✅ Terminée |
| Phase 1 | Datalake fondations | 🟡 Quasi-terminée |
| Phase 2 | Données personnelles + enrichissement geo | ⬜ Non démarrée |
| Phase 3 | Outil d'aide à l'implantation v1 | ⬜ Non démarrée |
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

## Implémentations complétées (2026-04-04)

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

---

## Stack en production

```
Python 3.12 / uv
Polars  ≥ 0.20      ETL + transformations
DuckDB  ≥ 1.0       Moteur analytique in-process
Streamlit ≥ 1.35    UI dashboard
APScheduler ≥ 3.10  Scheduler (CLI uniquement pour l'instant)
pytest  47/47 ✅
```

---

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

### 3. 🔴 Scaffold module `implantation` — modèle `Parcelle` + scoring (À FAIRE)

Démarrer la Phase 3 avec les fondations du scoring parcellaire :
- `implantation/models/parcelle.py` — modèle Pydantic `Parcelle` (tel que défini dans le ROADMAP)
- `implantation/scoring/engine.py` — moteur de scoring pondéré (axes Économique, Eau, Topographie)
- `implantation/scoring/criteria.py` — critères individuels 0–100

Pas d'UI encore, juste le cœur métier testable en Python pur. Tests pytest inclus.

Estimé : 1–2 h pour les 3 fichiers + tests.

---

*Reprise prévue : Phase 3 — Implantation.*
