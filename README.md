# agriTools

Suite d'outils personnels à destination d'un maraîcher souhaitant piloter son activité par la donnée. Le projet n'a pas de vocation commerciale et doit pouvoir vivre et évoluer sur l'ensemble d'une carrière.

## Vue d'ensemble

agriTools centralise, versionne et interroge l'ensemble des données utiles à la ferme :
- **Données open data** : météo, prix MIN, registres fonciers
- **Données personnelles** : comptabilité, sondes terrain, heures travail
- **Documentation** : plans de culture, rotations, analyses

Deux composants principaux :
- **Datalake** : stockage et interrogation des données (DuckDB + Parquet)
- **Outil d'implantation** : simulation d'opportunités foncières

## Installation

### Prérequis

- **Python 3.10+** (recommandé 3.12+)
- **uv** pour la gestion des dépendances (optionnel mais recommandé)

### Installation rapide

```bash
# Cloner le dépôt
git clone <repository-url>
cd agriTools

# Installer avec uv (recommandé)
uv sync

# Ou avec pip
pip install -e .
```

### Installation avec dépendances optionnelles

```bash
# Avec support cartographique
uv sync --extra geo

# Avec outils de développement
uv sync --extra dev
```

## Configuration

### Fichiers de configuration

1. **`config.toml`** : configuration par défaut (versionnée)
2. **`config.local.toml`** : surcharge locale (ignoré par git)

### Configuration obligatoire

Copiez et personnalisez `config.local.toml` :

```toml
[farm]
nom = "Ma Ferme Maraîchère"
latitude = 47.1234   # Coordonnées GPS réelles
longitude = -0.5678
timezone = "Europe/Paris"

[prix]
# Marchés MIN à surveiller
marches = ["NANTES SAINT LUCE", "RUNGIS"]
stades = ["GROS"]
```

### Structure des données

```
agriTools/
├── datalake/
│   ├── raw/           # Données brutes (non versionnées)
│   ├── processed/     # Parquet nettoyés (versionnés)
│   ├── catalog/       # Métadonnées YAML
│   └── agritools.duckdb  # Base analytique
├── ingestion/         # Scripts ETL par source
├── implantation/      # Outil d'aide à l'implantation
├── app/              # Interface Streamlit
└── tests/            # Tests unitaires
```

## Premier lancement

### 1. Vérifier l'installation

```bash
# Activer l'environnement virtuel
uv run python --version

# Tester l'import
uv run python -c "import polars as pl; print('Polars OK')"
```

### 2. Configuration des données

```bash
# Copier le template de configuration
cp config.toml config.local.toml

# Éditer config.local.toml avec vos coordonnées GPS
# et préférences de marchés
```

### 3. Premier run - météo

```bash
# Premier backfill météo (5 ans par défaut, configurable)
uv run python -m ingestion.meteo.open_meteo

# Forcer une reconstruction complète de l'historique
uv run python -m ingestion.meteo.open_meteo --full-refresh

# Lancer le scheduler local pour un refresh quotidien à 06:00
uv run python -m ingestion.meteo.open_meteo --schedule --hour 6 --minute 0

# Vérifier que les fichiers Parquet ont été créés
ls -la datalake/processed/meteo/
```

### 4. Premier run - prix MIN

```bash
# Télécharger les cotations RNM pour l'année en cours
uv run python -m ingestion.prix.rnm

# Ou pour une année spécifique
uv run python -m ingestion.prix.rnm --year 2024

# Vérifier les données
ls -la datalake/processed/prix/
```

### 5. Explorer les données

```bash
# Ouvrir une session DuckDB
uv run python -c "
import duckdb
con = duckdb.connect('datalake/agritools.duckdb')

# Lister les tables disponibles
print(con.sql('SHOW TABLES'))

# Exemple de requête
result = con.sql('''
    SELECT COUNT(*) as n_prix
    FROM read_parquet('datalake/processed/prix/*.parquet')
''')
print(result)
"
```

## Commandes disponibles

### Ingestion de données

```bash
# Météo Open-Meteo (point GPS de référence)
uv run python -m ingestion.meteo.open_meteo
uv run python -m ingestion.meteo.open_meteo --full-refresh
uv run python -m ingestion.meteo.open_meteo --lookback-days 14
uv run python -m ingestion.meteo.open_meteo --schedule --hour 6 --minute 0

# Prix MIN FranceAgriMer (marchés de gros)
uv run python -m ingestion.prix.rnm --year 2024
uv run python -m ingestion.prix.rnm --years 2022 2024

# Options RNM
uv run python -m ingestion.prix.rnm --list-marches    # Marchés disponibles
uv run python -m ingestion.prix.rnm --list-produits  # Produits cotés
uv run python -m ingestion.prix.rnm --all-marches    # Sans filtre marché
uv run python -m ingestion.prix.rnm --verify         # Résumé des données
```

### Analyse et exploration

```bash
# Interface Streamlit (à venir)
uv run streamlit run app/main.py

# Sessions DuckDB interactives
uv run python -c "
import duckdb
con = duckdb.connect('datalake/agritools.duckdb')
# Vos requêtes SQL ici
"
```

Le mode par défaut fait un **backfill initial** si aucun Parquet météo n'existe encore, puis passe en **refresh incrémental** sur les exécutions suivantes avec une fenêtre de recouvrement pour consolider les derniers jours disponibles via l'archive Open-Meteo.
Le mode `--schedule` lance un scheduler APScheduler local et bloque le processus tant qu'il tourne.

## Sources de données

| Source | Fréquence | Format | Description |
|--------|-----------|--------|-------------|
| **Open-Meteo** | Quotidienne | JSON → Parquet | Température, précipitations, ETP |
| **RNM FranceAgriMer** | Hebdomadaire | CSV/ZIP → Parquet | Cotations MIN marchés gros |
| **RPG IGN** | Annuelle | GeoPackage | Registre Parcellaire Graphique |
| **DVF** | Trimestrielle | CSV | Transactions foncières |
| **BRGM** | Statique | GeoJSON | Points d'eau et forages |

## Développement

### Tests

```bash
# Lancer tous les tests
uv run pytest

# Avec couverture
uv run pytest --cov=ingestion --cov-report=html
```

### Structure du code

- **`ingestion/`** : ETL par source de données
  - `_config.py` : gestion de la configuration
  - `meteo/` : données météorologiques
  - `prix/` : cotations de prix
  - `geo/` : données géographiques

- **`implantation/`** : moteur de scoring foncier
- **`app/`** : interface utilisateur Streamlit
- **`tests/`** : tests unitaires

### Contribution

1. Créer une branche feature
2. Écrire des tests pour les nouvelles fonctionnalités
3. Respecter les principes du ROADMAP
4. Documentation dans `docs/`

## Support et dépannage

### Problèmes courants

**Erreur "Module not found"**
```bash
# Réinstaller les dépendances
uv sync --reinstall
```

**Données manquantes**
```bash
# Vérifier la configuration
uv run python -c "from ingestion._config import load_config; print(load_config()['farm'])"
```

**Erreurs réseau**
- Vérifier la connexion internet
- Les APIs open data peuvent être temporairement indisponibles

### Logs et debug

Les commandes utilisent le logging Rich pour une sortie colorée. Pour plus de détails :

```bash
# Debug mode
export PYTHONPATH=. && python -c "import logging; logging.basicConfig(level=logging.DEBUG)" -m ingestion.prix.rnm
```

## Roadmap

Voir [ROADMAP.md](ROADMAP.md) pour les fonctionnalités planifiées.

## Licence

Ce projet est personnel et non commercial. Les données open data respectent leurs licences respectives (Open Data FranceAgriMer, IGN, etc.).
