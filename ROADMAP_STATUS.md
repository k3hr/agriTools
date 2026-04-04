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
| Tests pytest | ✅ | **47/47** — open_meteo, rnm, rpg |
| Scheduler APScheduler | ⬜ | `--schedule` dispo en CLI, pas de démon configuré |
| Pydantic contracts sur parsers | ⬜ | Dep installée, non câblée |

---

## Bugs corrigés (session 2026-04-04)

- **`config.toml` tronqué** — section `[sondes]` / `input_dir` coupée en plein milieu. Complétée.
- **`open_meteo.py` tronqué** — 167 lignes manquantes (de `verify()` à `main()`). Restauré depuis git HEAD.
- **`test_rnm.py::test_year_injected_when_missing`** — `str.extract()` sur colonne `date` null retournait null sans déclencher le fallback `pl.lit(year)`. Fix : guard `null_count < len` avant d'accepter les années extraites.
- **`2_Prix.py` saisonnalité** — `pl.col("mois").replace(dict)` échouait sur la conversion `Int8 → Utf8`. Fix : `map_elements(..., return_dtype=pl.Utf8)`.

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

### 1. Scheduler météo persistent (cron Windows ou APScheduler service)

Le refresh quotidien Open-Meteo existe en CLI (`--schedule`) mais tourne en process bloquant. Il faut le rendre autonome :

- **Option A (simple) :** Tâche planifiée Windows (`schtasks`) qui lance `uv run python -m ingestion.meteo.open_meteo` chaque matin à 6h.
- **Option B (propre) :** Script PowerShell wrapper + entrée dans le Planificateur de tâches avec logging dans `datalake/logs/`.

Même chose pour les prix RNM (hebdomadaire, vendredi matin).

---

### 2. Page Streamlit "Tableau de bord datalake" (synthèse sources)

Page d'accueil enrichie qui affiche pour chaque source :
- date de dernière mise à jour (lire les Parquet)
- nombre de lignes
- alertes si la donnée est trop ancienne (météo > 3 jours, prix > 10 jours)

Donne une vision de l'état de santé du datalake sans ouvrir DuckDB. Utile au quotidien.

---

### 3. Scaffold module `implantation` — modèle `Parcelle` + scoring

Démarrer la Phase 3 avec les fondations :
- `implantation/models/parcelle.py` — modèle Pydantic `Parcelle` (tel que défini dans le ROADMAP)
- `implantation/scoring/engine.py` — moteur de scoring pondéré (axes Économique, Eau, Topographie)
- `implantation/scoring/criteria.py` — critères individuels 0–100

Pas d'UI encore, juste le cœur métier testable en Python pur.

---

*Reprise prévue : prochaine session.*
