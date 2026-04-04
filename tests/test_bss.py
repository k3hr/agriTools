"""
Tests unitaires — ingestion BSS (BRGM Hub'eau).

Les tests couvrent la transformation et le calcul géométrique,
sans appel réseau (pas de mock HTTP nécessaire).
"""
import pytest
import polars as pl

from ingestion.geo.bss import (
    _haversine_km,
    _bbox,
    stations_to_dataframe,
    TARGET_SCHEMA,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
FARM_LAT = 47.8474
FARM_LON = -0.9416

SAMPLE_STATIONS = [
    {
        "code_bss": "07548X0009/S",
        "bss_id": "BSS000AAAA",
        "libelle_pe": "SABLÉ NORD",
        "nom_commune": "Sablé-sur-Sarthe",
        "code_commune_insee": "72264",
        "code_departement": "72",
        "nom_departement": "Sarthe",
        "x": -0.9200,          # longitude dans l'API Hub'eau
        "y": 47.8350,          # latitude  dans l'API Hub'eau
        "altitude_station": "35.0",   # string dans l'API
        "profondeur_investigation": 42.0,
        "date_debut_mesure": "1985-01-01",
        "date_fin_mesure": "2026-03-01",
        "nb_mesures_piezo": 15243,
        "codes_bdlisa": ["GF002"],
        "noms_masse_eau_edl": ["Alluvions de la Sarthe"],
    },
    {
        "code_bss": "07548X0042/F",
        "bss_id": "BSS000BBBB",
        "libelle_pe": "AVOISE FORAGE",
        "nom_commune": "Avoise",
        "code_commune_insee": "72020",
        "code_departement": "72",
        "nom_departement": "Sarthe",
        "x": -0.8800,
        "y": 47.8700,
        "altitude_station": "55.5",
        "profondeur_investigation": 120.0,
        "date_debut_mesure": "2001-06-15",
        "date_fin_mesure": "2026-01-10",
        "nb_mesures_piezo": 3201,
        "codes_bdlisa": ["GF002"],
        "noms_masse_eau_edl": ["Alluvions de la Sarthe"],
    },
    {
        "code_bss": "07548X0101/P",
        "bss_id": "BSS000CCCC",
        "libelle_pe": "PARCÉ PUITS",
        "nom_commune": "Parcé-sur-Sarthe",
        "code_commune_insee": "72230",
        "code_departement": "72",
        "nom_departement": "Sarthe",
        "x": -0.9800,
        "y": 47.8600,
        "altitude_station": None,
        "profondeur_investigation": None,   # profondeur inconnue
        "date_debut_mesure": None,
        "date_fin_mesure": None,
        "nb_mesures_piezo": 0,
        "codes_bdlisa": [],
        "noms_masse_eau_edl": [],
    },
]


# ---------------------------------------------------------------------------
# Tests bbox
# ---------------------------------------------------------------------------
class TestBbox:
    def test_returns_four_values(self):
        result = _bbox(47.85, -0.94, 25)
        assert len(result) == 4

    def test_order_lon_min_lat_min_lon_max_lat_max(self):
        lon_min, lat_min, lon_max, lat_max = _bbox(47.85, -0.94, 25)
        assert lon_min < -0.94 < lon_max
        assert lat_min < 47.85 < lat_max

    def test_center_inside_bbox(self):
        lon_min, lat_min, lon_max, lat_max = _bbox(47.85, -0.94, 25)
        assert lat_min < 47.85 < lat_max
        assert lon_min < -0.94 < lon_max

    def test_radius_scaling(self):
        _, lat_min_10, _, lat_max_10 = _bbox(47.85, -0.94, 10)
        _, lat_min_20, _, lat_max_20 = _bbox(47.85, -0.94, 20)
        assert (lat_max_20 - lat_min_20) > (lat_max_10 - lat_min_10)


# ---------------------------------------------------------------------------
# Tests haversine
# ---------------------------------------------------------------------------
class TestHaversine:
    def test_zero_distance(self):
        assert _haversine_km(48.0, 2.0, 48.0, 2.0) == pytest.approx(0.0, abs=1e-6)

    def test_known_distance(self):
        # Paris ↔ Lyon ≈ 392 km
        d = _haversine_km(48.8566, 2.3522, 45.7640, 4.8357)
        assert 385 <= d <= 400

    def test_symmetry(self):
        d1 = _haversine_km(47.85, -0.94, 47.90, -0.88)
        d2 = _haversine_km(47.90, -0.88, 47.85, -0.94)
        assert d1 == pytest.approx(d2, rel=1e-6)

    def test_short_distance(self):
        # ~1 km nord
        d = _haversine_km(47.8474, -0.9416, 47.8564, -0.9416)
        assert 0.9 <= d <= 1.1


# ---------------------------------------------------------------------------
# Tests stations_to_dataframe
# ---------------------------------------------------------------------------
class TestStationsToDataframe:
    def test_schema_columns(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        assert set(TARGET_SCHEMA.keys()) == set(df.columns)

    def test_row_count(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        assert len(df) == 3

    def test_empty_input_returns_empty_df(self):
        df = stations_to_dataframe([], FARM_LAT, FARM_LON)
        assert len(df) == 0
        assert set(df.columns) == set(TARGET_SCHEMA.keys())

    def test_sorted_by_distance(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        dists = df["distance_km"].to_list()
        assert dists == sorted(d for d in dists if d is not None)

    def test_distance_computed(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        assert df["distance_km"].null_count() == 0
        assert all(d >= 0 for d in df["distance_km"].to_list())

    def test_distance_reasonable(self):
        # Les 3 stations du fixture sont dans un rayon de ~5 km
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        assert df["distance_km"].max() < 10.0

    def test_null_profondeur_preserved(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        # Le 3e fixture (Parcé) a profondeur_investigation=None
        parcé = df.filter(pl.col("code_bss") == "07548X0101/P")
        assert parcé["profondeur_investigation"].null_count() == 1

    def test_null_bdlisa_preserved(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        # La 3e station a codes_bdlisa=[] → code_bdlisa null
        assert df["code_bdlisa"].null_count() == 1

    def test_code_bss_values(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        codes = df["code_bss"].to_list()
        assert "07548X0009/S" in codes
        assert "07548X0042/F" in codes

    def test_types_respected(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        assert df["distance_km"].dtype == pl.Float64
        assert df["profondeur_investigation"].dtype == pl.Float64
        assert df["altitude_station"].dtype == pl.Float64
        assert df["nb_mesures_piezo"].dtype == pl.Int64
        assert df["code_bss"].dtype == pl.Utf8

    def test_altitude_cast_from_string(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        # "35.0" et "55.5" doivent être castées en float, None reste null
        non_null = df["altitude_station"].drop_nulls()
        assert len(non_null) == 2
        assert 35.0 in non_null.to_list()

    def test_bdlisa_first_element(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        non_null = df["code_bdlisa"].drop_nulls().to_list()
        assert all(v == "GF002" for v in non_null)

    def test_nom_masse_eau(self):
        df = stations_to_dataframe(SAMPLE_STATIONS, FARM_LAT, FARM_LON)
        non_null = df["nom_masse_eau"].drop_nulls().to_list()
        assert all("Sarthe" in v for v in non_null)
