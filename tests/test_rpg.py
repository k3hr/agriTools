"""
Tests unitaires — ingestion RPG.
"""
import pytest
import polars as pl
import math

from ingestion.geo.rpg import (
    _bbox,
    _geojson_geom_to_wkt,
    _features_to_dataframe,
    CODES_CULTURE,
)


class TestBbox:
    def test_center_is_inside(self):
        min_lon, min_lat, max_lon, max_lat = _bbox(47.85, -0.94, 25)
        assert min_lon < -0.94 < max_lon
        assert min_lat < 47.85 < max_lat

    def test_symmetry(self):
        min_lon, min_lat, max_lon, max_lat = _bbox(47.85, -0.94, 25)
        assert pytest.approx(max_lat - 47.85, abs=1e-6) == pytest.approx(47.85 - min_lat, abs=1e-6)

    def test_radius_scaling(self):
        _, min_lat_10, _, max_lat_10 = _bbox(47.85, -0.94, 10)
        _, min_lat_20, _, max_lat_20 = _bbox(47.85, -0.94, 20)
        assert pytest.approx((max_lat_20 - min_lat_20) / (max_lat_10 - min_lat_10), rel=1e-3) == 2.0

    def test_km_accuracy(self):
        _, min_lat, _, max_lat = _bbox(47.85, -0.94, 25)
        assert pytest.approx((max_lat - 47.85) * 111.32, rel=0.01) == 25.0


class TestGeoJsonToWkt:
    def test_polygon(self):
        geom = {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]
        }
        wkt = _geojson_geom_to_wkt(geom)
        assert wkt.startswith("POLYGON")
        assert "0.0 0.0" in wkt

    def test_multipolygon(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]]
        }
        wkt = _geojson_geom_to_wkt(geom)
        assert wkt.startswith("MULTIPOLYGON")

    def test_unknown_type_returns_empty(self):
        assert _geojson_geom_to_wkt({"type": "Point", "coordinates": [0, 0]}) == ""

    def test_empty_geom_returns_empty(self):
        assert _geojson_geom_to_wkt({}) == ""


class TestFeaturesToDataframe:
    def _make_feature(self, id_parcel, code, surface, coords=None):
        coords = coords or [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]
        return {
            "properties": {
                "id_parcel": id_parcel,
                "code_cultu": code,
                "surf_parc": surface,
            },
            "geometry": {"type": "Polygon", "coordinates": coords},
        }

    def test_basic_dataframe(self):
        features = [
            self._make_feature("P001", "15", 0.5),
            self._make_feature("P002", "11", 2.3),
        ]
        df = _features_to_dataframe(features)
        assert len(df) == 2
        assert set(df.columns) == {"id_parcel", "code_culture", "surface_ha", "libelle_culture", "geometry_wkt"}

    def test_libelle_resolved(self):
        features = [self._make_feature("P001", "15", 0.5)]
        df = _features_to_dataframe(features)
        assert df["libelle_culture"][0] == "Légumes ou fleurs"

    def test_unknown_code(self):
        features = [self._make_feature("P001", "99", 1.0)]
        df = _features_to_dataframe(features)
        assert df["libelle_culture"][0] == "Inconnu"

    def test_geometry_wkt_present(self):
        features = [self._make_feature("P001", "15", 0.5)]
        df = _features_to_dataframe(features)
        assert df["geometry_wkt"][0].startswith("POLYGON")

    def test_empty_returns_empty_df(self):
        df = _features_to_dataframe([])
        assert len(df) == 0


class TestCodesCulture:
    def test_maraichage(self):
        assert CODES_CULTURE["15"] == "Légumes ou fleurs"

    def test_prairies(self):
        assert CODES_CULTURE["11"] == "Prairies permanentes"
        assert CODES_CULTURE["12"] == "Prairies temporaires"
