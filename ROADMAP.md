# agriTools — Roadmap v0.1
*Dernière mise à jour : 2026-04-03*

---

## 1. Vision & Périmètre

**agriTools** est une suite d'outils personnels à destination d'un maraîcher souhaitant piloter son activité par la donnée. Le projet n'a pas de vocation commerciale. Il doit pouvoir vivre et évoluer sur l'ensemble d'une carrière.

Deux composants sont au cœur de cette première itération :

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
| ETL / transformation | **Polars** | Plus rapide que pandas, API lazy evaluation, gestion native Parquet |
| Moteur analytique | **DuckDB** (fichier `.duckdb`) | SQL in-process sur Parquet, zéro serveur, performances impressionnantes |
| Validation schémas | **Pydantic v2** | Contrats de données explicites dès l'ingestion |
| Interface utilisateur | **Streamlit** | Prototypage rapide d'apps data, pas de JS requis |
| Cartographie | **GeoPandas + Folium** | Analyse spatiale + rendu carte dans Streamlit |
| Scheduler | **APScheduler** ou cron système | Ingestion automatique périodique, sans Airflow |
| Tests | **pytest** | Couverture des parsers et du moteur de scoring |

### 3.2 Stockage

```
Format source de vérité : Parquet (colonnaire, compressé, requêtable directement par DuckDB)
Format staging/raw     : CSV, JSON, GeoJSON — conservés tels quels
Format catalogue       : YAML (lisible humain)
Format config          : TOML (pyproject.toml, config.toml)
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
│   │   ├── stats/
│   │   ├── geo/
│   │   └── perso/                  # données personnelles (gitignore)
│   ├── processed/                  # Parquet nettoyés, partitionnés
│   │   ├── meteo/
│   │   ├── prix/
│   │   ├── stats/
│   │   ├── geo/
│   │   └── perso/
│   ├── catalog/                    # YAML de métadonnées par dataset
│   └── agritools.duckdb            # base DuckDB locale (gitignore)
│
├── ingestion/                      # scripts ETL, un module par source
│   ├── meteo/
│   ├── prix/
│   ├── geo/
│   └── perso/
│
├── implantation/                   # outil d'aide à la décision foncière
│   ├── models/                     # Pydantic models (Parcelle, Scenario…)
│   ├── scoring/                    # moteur de scoring pondéré
│   ├── enrichment/                 # enrichissement depuis le datalake
│   └── reports/                    # templates export
│
├── app/                            # applications Streamlit
│   ├── pages/
│   └── components/
│
├── notebooks/                      # exploration ad-hoc (jamais en prod)
├── tests/
├── docs/
├── config.toml                     # paramètres globaux (chemins, clés API)
├── pyproject.toml
└── ROADMAP.md
```

---

## 4. Catalogue des sources de données

### 4.1 Open Data

| Domaine | Source | URL | Fréquence | Format |
|---|---|---|---|---|
| Météo historique + prévision | **Open-Meteo** | https://open-meteo.com | Quotidien | JSON/API (gratuit, sans clé) |
| Météo officielle FR | **Météo-France API** | https://portail-api.meteofrance.fr | Quotidien | JSON/API (clé gratuite) |
| Prix marchés MIN | **FranceAgriMer / RNM** | https://rnm.franceagrimer.fr | Hebdomadaire | CSV/Excel |
| Stats agricoles | **Agreste (MASA)** | https://agreste.agriculture.gouv.fr | Annuel | CSV/Excel |
| Registre Parcellaire Graphique | **IGN / RPG** | https://geoservices.ign.fr/rpg | Annuel | GeoJSON/Shapefile |
| Prix fonciers agricoles | **DVF (data.gouv.fr)** | https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres | Semestriel | CSV |
| Prix fonciers SAFER | **SAFER** | https://www.safer.fr/publications | Annuel | PDF/CSV (selon région) |
| Hydrogéologie / forages | **BRGM / BSS** | https://infoterre.brgm.fr | Statique | WFS/JSON |
| Eaux souterraines | **ADES** | https://ades.eaufrance.fr | Variable | CSV/API |
| Données sol (pédologie) | **INRAE GéoSol** | https://geosol.inrae.fr | Statique | WMS/WFS |
| Indices économiques | **INSEE** | https://www.insee.fr/fr/statistiques | Annuel | CSV |

### 4.2 Données personnelles

| Domaine | Description | Format d'entrée envisagé |
|---|---|---|
| Comptabilité | Recettes, charges, immobilisations | CSV export logiciel compta / saisie manuelle |
| Relevés sondes terrain | Température sol, humidité, pH | CSV ou API selon matériel (ex. Metos, Sencrop…) |
| Relevés heures travail | Journaux d'activité par poste | CSV / formulaire Streamlit |
| Inventaire semences | Stocks, achats, taux germination | CSV / formulaire Streamlit |
| Journal cultural | Notes parcelle, interventions | Markdown structuré |

---

## 5. Roadmap

### Phase 0 — Bootstrap (S1–S2)

**Objectif** : Poser les fondations techniques, rien de plus.

- [X] Initialiser `pyproject.toml` avec `uv` (Python 3.12, dépendances core)
- [X] Créer la structure de dossiers du dépôt
- [X] Configurer `.gitignore` (données brutes, duckdb, perso/, secrets)
- [X] Écrire `config.toml` : chemins absolus configurables, pas de hardcode
- [X] Premier script d'ingestion : **Open-Meteo** (point GPS de référence → Parquet)
- [ ] Vérifier que DuckDB interroge le Parquet produit
- [ ] README opérationnel (installation, premier run)

**Livrable** : `python -m ingestion.meteo.open_meteo` produit un fichier Parquet requêtable.

---

### Phase 1 — Datalake fondations (M1–M2)

**Objectif** : Avoir un datalake vivant avec les données open data essentielles et un premier dashboard.

- [ ] Ingestion **météo** complète (historical backfill 5 ans + cron quotidien)
- [X] Ingestion **prix MIN** (FranceAgriMer RNM — marchés de référence région, local ZIP support ajouté)
- [ ] Ingestion **RPG** (Registre Parcellaire Graphique — couverture département)
- [ ] Catalogue YAML pour chaque dataset (source, schéma, fréquence MAJ, licence)
- [ ] Scheduler APScheduler : météo quotidien, prix hebdomadaire
- [ ] Premier dashboard **Streamlit** : météo locale (température, pluie, ETP) sur fenêtre glissante
- [ ] Tests pytest : parsers des 3 sources, contrats Pydantic

**Livrable** : Dashboard météo fonctionnel, 3 sources ingérées automatiquement.

---

### Phase 2 — Données personnelles + enrichissement open data (M3–M4)

**Objectif** : Intégrer les données de la ferme et enrichir le datalake géographique et foncier.

- [ ] Module ingestion **comptabilité** (CSV → Parquet, schéma Pydantic)
- [ ] Module ingestion **heures travail** (formulaire Streamlit + CSV)
- [ ] Module ingestion **sondes terrain** (à adapter selon matériel)
- [ ] Ingestion **DVF** (prix transactions foncières, filtré zone géographique)
- [ ] Ingestion **BRGM BSS** (forages et points d'eau référencés)
- [ ] Ingestion **ADES** (piézométrie locale si données disponibles)
- [ ] Dashboard **synthèse datalake** : état de chaque source (dernière MAJ, nb lignes, alertes)
- [ ] Page dashboard **comptabilité** : marges, CA, charges fixes/variables

**Livrable** : Vision consolidée des données ferme + open data foncier/eau.

---

### Phase 3 — Outil d'aide à l'implantation v1 (M5–M7)

**Objectif** : Permettre de saisir des parcelles candidates, les enrichir automatiquement, et comparer des scénarios.

#### Modèle de données `Parcelle`

```python
class Parcelle(BaseModel):
    id: str
    nom: str
    surface_ha: float
    commune: str
    departement: str
    coords_centroid: tuple[float, float]           # lat/lon WGS84
    prix_achat: float | None
    prix_location_annuel: float | None

    # Eau
    acces_eau: Literal["forage", "riviere", "reseau", "aucun", "inconnu"]
    debit_estime_m3h: float | None
    distance_cours_eau_m: float | None

    # Topographie
    pente_pct: float | None
    exposition: Literal["N","NE","E","SE","S","SO","O","NO","plat"] | None
    altitude_m: float | None
    risque_gel_tardif: bool | None

    # Logistique
    distance_marche_km: float | None
    distance_agglo_km: float | None
    acces_vehicule: Literal["facile","limite","difficile"]

    notes: str = ""
    statut: Literal["prospect","visite","evalue","archive"] = "prospect"
```

#### Moteur de scoring

- Critères pondérables par l'utilisateur (curseurs dans l'UI)
- Score 0–100 par critère, agrégation pondérée
- Trois axes principaux (selon tes priorités) :
  1. **Économique & logistique** (prix/ha vs zone, accessibilité, bassins de conso proches)
  2. **Eau & irrigation** (accès, débit, pluviométrie locale issue datalake)
  3. **Topographie & exposition** (pente, orientation, risque gel, altitude)

#### Enrichissement automatique depuis le datalake

- Pull météo locale (pluviométrie moyenne, jours de gel, ETP) depuis Open-Meteo via coords
- Pull prix foncier médian de la zone (DVF, SAFER si dispo) pour contextualiser le prix demandé
- Pull forages BRGM dans un rayon configurable

#### Interface Streamlit — pages

1. **Mes parcelles** — liste, statuts, carte Folium globale
2. **Saisie / édition parcelle** — formulaire + enrichissement auto
3. **Analyse parcelle** — scores par axe, radar chart, données enrichies
4. **Comparaison scénarios** — tableau side-by-side, export Markdown/PDF

**Livrable** : Comparer 3 parcelles candidates avec scores et carte.

---

### Phase 4 — Consolidation & profondeur (M8–M12)

**Objectif** : Affiner sur la base de l'usage réel, ajouter de la profondeur analytique.

- [ ] Historisation des données personnelles (accumulation sur saisons)
- [ ] Module **documentation** intégré (Markdown searchable via DuckDB FTS)
- [ ] Enrichissement pédologique (INRAE GéoSol — type sol, texture, pH moyen zone)
- [ ] Outil **planification culturale** basique (rotations, calendrier semis/récolte)
- [ ] Export rapport implantation (PDF structuré via WeasyPrint ou reportlab)
- [ ] Couverture de tests étendue
- [ ] Documentation technique complète (`docs/`)
- [ ] Rétrospective : ajustements roadmap Phase 5+

---

## 6. Points d'attention & décisions à prendre

### Données personnelles & confidentialité

Les données personnelles (comptabilité, heures) ne doivent **jamais** être committées dans Git.
Règle : tout ce qui est sous `datalake/raw/perso/` et `datalake/processed/perso/` est dans `.gitignore`.
Envisager un `.env` local pour les chemins sensibles.

### Identifiant parcelle et géoréférencement

Utiliser le **numéro de parcelle cadastrale** (format : `{dept}{commune}{section}{numero}`) comme identifiant stable, complété par les coordonnées GPS du centroïde. Cela permet le croisement avec RPG et DVF.

### Scheduler : cron vs APScheduler

Pour démarrer : `cron` système suffit (fiable, sans dépendance Python).
APScheduler devient pertinent quand on veut gérer les échecs, les retries et un log centralisé.

### Versioning des données

Les fichiers Parquet sont partitionnés par `source/annee/mois` — cela donne une forme naturelle de versioning sans DVC. DVC peut être envisagé à partir de la Phase 4 si le volume de données personnelles le justifie.

### Accès mobile / tablette

Streamlit est responsive par défaut. Si un accès terrain (tablette) est nécessaire, envisager un accès réseau local (Streamlit sur le serveur/NAS, accès WiFi ferme).

---

## 7. Métriques de succès

| Métrique | Cible Phase 1 | Cible Phase 3 |
|---|---|---|
| Sources ingérées automatiquement | 3 | 8+ |
| Couverture tests (parsers + scoring) | 60 % | 80 % |
| Temps de chargement dashboard météo | < 2 s | < 2 s |
| Parcelles modélisables dans l'outil | — | ≥ 10 |
| Temps pour comparer 2 parcelles | — | < 5 min (saisie → score) |

---

*Ce document est vivant. Il sera mis à jour à chaque fin de phase.*
