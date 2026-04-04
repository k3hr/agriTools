"""
Tests unitaires — ingestion Open-Meteo.

Couvre la transformation de la réponse API en DataFrame Polars.
Pas de mock HTTP : les tests API réels sont dans tests/integration/.
"""
import pytest
import polars as pl
from datetime import date

from ingestion.meteo.open_meteo import (
    compute_backfill_start,
    determine_fetch_start,
    merge_weather_data,
    response_to_dataframe,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def valid_response() -> dict:
    return {
        "latitude": 46.0,
        "longitude": 2.0,
        "daily": {
            "time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "temperature_2m_max": [10.0, 12.0, 8.5],
            "temperature_2m_min": [4.0, 5.5, 2.0],
            "temperature_2m_mean": [7.0, 8.75, 5.25],
            "precipitation_sum": [0.0, 2.5, 0.0],
            "et0_fao_evapotranspiration": [0.8, 1.1, 0.7],
            "wind_speed_10m_max": [15.0, 22.0, 10.0],
            "shortwave_radiation_sum": [3.2, 4.1, 2.9],
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestResponseToDataframe:
    def test_basic_shape(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert len(df) == 3
        assert df.shape[1] >= 3  # au moins date, lat, lon

    def test_date_column_type(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert df["date"].dtype == pl.Date

    def test_date_values(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert df["date"][0] == date(2024, 1, 1)
        assert df["date"][-1] == date(2024, 1, 3)

    def test_coords_propagated(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert df["latitude"][0] == pytest.approx(46.0)
        assert df["longitude"][0] == pytest.approx(2.0)

    def test_column_order(self, valid_response):
        df = response_to_dataframe(valid_response)
        # date, latitude, longitude doivent être les 3 premières colonnes
        assert df.columns[:3] == ["date", "latitude", "longitude"]

    def test_numeric_values(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert df["temperature_2m_max"][1] == pytest.approx(12.0)
        assert df["precipitation_sum"][1] == pytest.approx(2.5)

    def test_invalid_response_raises(self):
        with pytest.raises(ValueError, match="daily.time"):
            response_to_dataframe({"latitude": 46.0, "longitude": 2.0, "daily": {}})

    def test_empty_daily_raises(self):
        with pytest.raises(ValueError):
            response_to_dataframe({"latitude": 46.0, "longitude": 2.0})

    def test_no_nulls_in_date(self, valid_response):
        df = response_to_dataframe(valid_response)
        assert df["date"].null_count() == 0


class TestRefreshWindow:
    def test_compute_backfill_start_uses_inclusive_calendar_years(self):
        assert compute_backfill_start(date(2026, 4, 4), 5) == date(2022, 1, 1)

    def test_compute_backfill_start_rejects_zero_years(self):
        with pytest.raises(ValueError):
            compute_backfill_start(date(2026, 4, 4), 0)

    def test_determine_fetch_start_without_existing_data_is_full(self):
        start, mode = determine_fetch_start(
            existing_df=None,
            end=date(2026, 4, 4),
            backfill_years=5,
            refresh_lookback_days=30,
            force_full_refresh=False,
        )
        assert start == date(2022, 1, 1)
        assert mode == "full"

    def test_determine_fetch_start_with_existing_data_is_incremental(self):
        existing_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 20), date(2026, 3, 21), date(2026, 3, 22)],
                "temperature_2m_max": [15.0, 16.0, 17.0],
            }
        )

        start, mode = determine_fetch_start(
            existing_df=existing_df,
            end=date(2026, 4, 4),
            backfill_years=5,
            refresh_lookback_days=10,
            force_full_refresh=False,
        )
        assert start == date(2026, 3, 12)
        assert mode == "incremental"

    def test_force_full_refresh_ignores_existing_data(self):
        existing_df = pl.DataFrame(
            {"date": [date(2026, 3, 22)], "temperature_2m_max": [17.0]}
        )

        start, mode = determine_fetch_start(
            existing_df=existing_df,
            end=date(2026, 4, 4),
            backfill_years=5,
            refresh_lookback_days=10,
            force_full_refresh=True,
        )
        assert start == date(2022, 1, 1)
        assert mode == "full"


class TestMergeWeatherData:
    def test_merge_weather_data_prefers_newest_row_for_same_date(self):
        existing_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 20), date(2026, 3, 21)],
                "temperature_2m_max": [15.0, 16.0],
                "precipitation_sum": [0.0, 1.0],
            }
        )
        new_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 21), date(2026, 3, 22)],
                "temperature_2m_max": [18.5, 19.0],
                "precipitation_sum": [2.5, 0.0],
            }
        )

        merged = merge_weather_data(existing_df, new_df)

        assert merged["date"].to_list() == [
            date(2026, 3, 20),
            date(2026, 3, 21),
            date(2026, 3, 22),
        ]
        assert merged.filter(pl.col("date") == date(2026, 3, 21))["temperature_2m_max"][0] == pytest.approx(18.5)

    def test_merge_weather_data_accepts_empty_existing_dataset(self):
        new_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 22)],
                "temperature_2m_max": [19.0],
            }
        )

        merged = merge_weather_data(None, new_df)

        assert merged.equals(new_df.sort("date"))
