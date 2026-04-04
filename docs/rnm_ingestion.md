# RNM Price Data Ingestion

## Overview

The RNM (Réseau des Nouvelles des Marchés) ingestion module handles weekly price quotations for fruits and vegetables from French wholesale markets (MIN - Marchés d'Intérêt National).

## Data Source

**Original Source**: FranceAgriMer via data.gouv.fr
**Format**: Weekly CSV/Excel files containing price quotations by product, market, and commercial stage
**Frequency**: Weekly updates
**Coverage**: All major French wholesale markets

## Ingestion Pipeline

### 1. Data Discovery

The module supports two modes of data discovery:

#### Remote Mode (Default)
- Queries data.gouv.fr API for available RNM datasets
- Downloads CSV/XLSX files directly from the platform
- Requires internet connectivity

#### Local Mode (Preferred)
- Automatically detects local ZIP archives in the project root
- Supports manual placement of RNM data archives
- Works offline once archives are downloaded

### 2. Data Processing

#### ZIP Archive Handling
- Extracts CSV and Excel files from ZIP archives
- Supports multiple files per archive (concatenated into single dataset)
- Handles both `.csv` and `.xls`/`.xlsx` formats

#### Schema Normalization
- Maps various column names to standardized schema
- Handles French decimal separators (comma → dot)
- Injects year information when missing
- Converts data types appropriately

#### Filtering
- Filters by market names (configurable in `config.toml`)
- Filters by commercial stages (GROS, DETAIL, etc.)
- Case-insensitive matching with whitespace trimming

### 3. Data Storage

- **Format**: Parquet with ZSTD compression
- **Partitioning**: By year (`prix_rnm_2024.parquet`, etc.)
- **Location**: `datalake/processed/prix/`
- **Query**: Accessible via DuckDB for analytics

## Configuration

### config.toml Settings

```toml
[prix]
marches = ["NANTES SAINT LUCE", "RUNGIS"]  # Markets to include
stades = ["GROS"]                          # Commercial stages to include
historical_years = 5                       # Years of history for backfill
```

### Local Archive Placement

Place RNM ZIP files directly in the `agriTools/` root directory:

```
agriTools/
├── COT-MUL-prd_RNM-A24.zip    # 2024 data
├── COT-MUL-prd_RNM-A25.zip    # 2025 data
├── COT-MUL-prd_RNM-A26.zip    # 2026 data
└── ...
```

**Naming Convention**: `COT-MUL-prd_RNM-A{year}.zip`

## Usage

### Command Line

```bash
# Ingest specific years
python -m ingestion.prix.rnm --year 2024
python -m ingestion.prix.rnm --years 2022 2024

# Include all markets (ignore config filters)
python -m ingestion.prix.rnm --all-marches

# Verify existing data
python -m ingestion.prix.rnm --verify

# List available markets/products
python -m ingestion.prix.rnm --list-marches
python -m ingestion.prix.rnm --list-produits
```

### Programmatic Usage

```python
import ingestion.prix.rnm as rnm

# Ingest 2024 data with default filters
dfs = rnm.run(target_years=[2024])

# Ingest all markets for 2023-2025
dfs = rnm.run(target_years=[2023, 2024, 2025], all_marches=True)
```

## Data Schema

### Normalized Columns

| Column | Type | Description |
|--------|------|-------------|
| annee | Int32 | Year |
| semaine | Int32 | Week number |
| date | Utf8 | Date string (various formats) |
| produit | Utf8 | Product name |
| marche | Utf8 | Market name |
| stade | Utf8 | Commercial stage |
| categorie | Utf8 | Product category |
| calibre | Utf8 | Size/grade |
| variete | Utf8 | Variety |
| origine | Utf8 | Origin |
| unite | Utf8 | Unit (KG, PIECE, etc.) |
| prix_min | Float64 | Minimum price |
| prix_max | Float64 | Maximum price |
| prix_moyen | Float64 | Average price |

### Column Aliases

The module handles multiple column name variations from different years:

```python
COL_ALIASES = {
    "produit": ["produit_libelle", "produit", "Produit", "libelle_produit"],
    "marche": ["marche_libelle", "marche", "Marche", "libelle_marche"],
    "prix_min": ["prix_min", "Prix Min", "PrixMin", "min"],
    # ... etc
}
```

## Recent Changes (2026-04-04)

### Local ZIP Archive Support

**Problem**: The data.gouv.fr API changed its dataset structure, no longer providing direct CSV downloads but instead linking to a ZIP archive on FranceAgriMer's website.

**Solution**: Added automatic detection and processing of local ZIP archives placed in the project root.

**Implementation**:
- Added `find_local_zip_resources()` to scan for `COT-MUL-prd_RNM-*.zip` files
- Added `parse_zip()` to extract and process CSV/Excel files from archives
- Modified `run()` to prefer local archives over remote API calls
- Maintained backward compatibility with existing remote ingestion

**Benefits**:
- Works offline once archives are downloaded
- Handles the new ZIP-based data distribution
- No dependency on external API availability
- Faster processing (no network downloads during ingestion)

## Troubleshooting

### No Data After Filtering

If ingestion reports "0 rows after filter":

1. Check market names in `config.toml` match actual data
2. Run `python -m ingestion.prix.rnm --list-marches` to see available markets
3. Use `--all-marches` flag to bypass market filtering

### Archive Not Found

If local ZIP archives aren't detected:

1. Ensure files are named `COT-MUL-prd_RNM-A{year}.zip`
2. Place files directly in `agriTools/` directory (not subdirectories)
3. Check file permissions and ZIP integrity

### Encoding Issues

If CSV parsing fails:

1. The module auto-detects UTF-8 vs Latin-1 encoding
2. Handles French decimal separators (comma → dot)
3. Supports various CSV delimiters (semicolon preferred)

## Testing

Run the test suite:

```bash
python -m pytest tests/test_rnm.py -v
```

Tests cover:
- CSV parsing and normalization
- Column alias resolution
- Market/stage filtering
- Schema validation
- Error handling