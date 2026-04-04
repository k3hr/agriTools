# agriTools — Roadmap Status

**Dernière mise à jour :** 2026-04-04
**Session :** Sprint initial (2 jours) + Phase 2 DVF + BSS + heures + ADES + Phase 3 scoring preview

---

## État global

| Phase | Intitulé | Statut |
|---|---|---|
| Phase 0 | Bootstrap | ✅ Terminée |
| Phase 1 | Datalake fondations | ✅ Terminée |
| Phase 2 | Données personnelles + enrichissement geo | 🟡 En cours (compta + sondes) |
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
| Tests pytest | ✅ | **127/127** — open_meteo, rnm, rpg, implantation, dvf, bss, heures, ades |
| Scheduler Tâches planifiées | ✅ | Windows scheduled tasks : météo 6h quotidien, RNM vendredi 7h, logs `datalake/logs/` |
| Pydantic contracts sur parsers | 🔵 | Déplacé en Phase 4 — parsers déjà robustes, priorité basse pour usage solo |

---

## Phase 2 — Données personnelles + enrichissement geo (en cours)

| Item | Statut | Notes |
|---|---|---|
| Ingestion DVF (prix transactions foncières) | ✅ | `ingestion/prix/dvf.py` — filtre zone géographique, Parquet annuel |
| Catalogue YAML DVF | ✅ | `datalake/catalog/prix_dvf.yaml` |
| Tests DVF | ✅ | `tests/test_dvf.py` — parsing, filtrage, normalisation |
| Module ingestion comptabilité | ⬜ | CSV → Parquet, schéma Pydantic |
| Module ingestion heures travail | ✅ | `ingestion/perso/heures.py` — dupliqué dans le bloc ci-dessous |
| Module ingestion sondes terrain | ⬜ | À adapter selon matériel |
| Ingestion BRGM BSS (forages) | ✅ | `ingestion/geo/bss.py` — Hub'eau API, bbox+haversine, 5 stations, 21 tests |
| Catalogue YAML BSS | ✅ | `datalake/catalog/geo_bss.yaml` |
| Module heures de travail | ✅ | `ingestion/perso/heures.py` — CLI start/stop/add/list/ingest/verify, parser NLP durée+date, 35 tests |
| Ingestion ADES (piézométrie) | ✅ | `ingestion/geo/ades.py` — Hub'eau /chroniques, batching par station, 24 tests |
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
| Scoring preview UI | ✅ | Intégration scoring temps réel dans formulaire après sauvegarde |
| Rapport implantation PDF | ⬜ | Export structuré via WeasyPrint |


### Scheduler Windows (Option A)
- **Scripts PowerShell** : `scripts/schedule_meteo.ps1` + `scripts/schedule_rnm.ps1`
- **Tâches planifiées** : Météo daily 6h, RNM weekly vendredi 7h via `schtasks` (shell) → `Register-ScheduledTask` (PS)
- **Logging** : Chaque refresh génère `datalake/logs/meteo_refresh_YYYY-MM-DD_HHmmss.log` et `rnm_refresh_...log`
- **Éprouvé** : Prêt pour prod, exécution autonome sans intervention

### Tableau d