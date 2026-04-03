"""
Tests unitaires — ingestion RPG.
"""
import pytest
import polars as pl
import math

from ingestion.geo.rpg import _bbox_from_center, CODES_CULTURE


class TestBboxFromCenter:
    def test_basic_shape(self):
        min_lon, min_lat, max_lon, max_lat = _bbox_from_center(47.85, -0.94, 25)
        assert min_lon < -0.94 < max_lon
        assert min_lat < 47.85 < max_lat

    def test_symmetry(self):
        min_lon, min_lat, max_lon, max_lat = _bbox_from_center(47.85, -0.94, 25)
        assert pytest.approx(max_lat - 47.85, abs=1e-6) == pytest.approx(47.85 - min_lat, abs=1e-6)

    def test_radius_scaling(self):
        # Un rayon 2× plus grand → delta_lat 2× plus grand
        _, min_lat_10, _, max_lat_10 = _bbox_from_center(47.85, -0.94, 10)
        _, min_lat_20, _, max_lat_20 = _bbox_from_center(47.85, -0.94, 20)
        delta_10 = max_lat_10 - min_lat_10
        delta_20 = max_lat_20 - min_lat_20
        assert pytest.approx(delta_20 / delta_10, rel=1e-3) == 2.0

    def test_approx_distance(self):
        # 25 km → delta_lat ≈ 0.225°
        min_lon, min_lat, max_lon, max_lat = _bbox_from_center(47.85, -0.94, 25)
        delta_lat_deg = max_lat - 47.85
        delta_lat_km = delta_lat_deg * 111.32
        assert pytest.approx(delta_lat_km, rel=0.01) == 25.0

    def test_zero_radius(self):
        min_lon, min_lat, max_lon, max_lat = _bbox_from_center(47.85, -0.94, 0)
        assert min_lon == max_lon == pytest.approx(-0.94)
        assert min_lat == max_lat == pytest.approx(47.85)


class TestCodesCulture:
    def test_known_codes(self):
        assert CODES_CULTURE["11"] == "Prairies permanentes"
        assert CODES_CULTURE["15"] == "Légumes ou fleurs"
        assert CODES_CULTURE["1"] == "Blé tendre"

    def test_coverage(self):
        # Doit contenir au moins les grandes catégories maraîchage/prairies
        assert "15" in CODES_CULTURE  # Légumes ou fleurs
        assert "11" in CODES_CULTURE  # Prairies permanentes
        assert "12" in CODES_CULTURE  # Prairies temporaires
