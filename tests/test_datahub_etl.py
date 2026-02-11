import pytest
import json
import tempfile
import os
from unittest.mock import Mock, MagicMock, patch
from datahub_etl.weather_etl import (
    extract_observations_data,
    transform_observations_data,
    transform_observation,
    load_geohash_cache,
    save_geohash_cache
)
from tests.fixtures import (
    NEAREST_STATION_RESPONSE,
    OBSERVATIONS_RESPONSE,
    SAMPLE_SITE,
    EXPECTED_TRANSFORMED_ROW
)


class TestGeohashCaching:
    def test_load_cache_from_file(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump({"3005": "gfxnj5"}, f)
            cache_file = f.name

        try:
            cache = load_geohash_cache(cache_file=cache_file)
            assert cache == {"3005": "gfxnj5"}
        finally:
            os.unlink(cache_file)

    def test_load_cache_no_file(self):
        cache = load_geohash_cache(cache_file="nonexistent.json")
        assert cache == {}

    def test_save_cache_to_file(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            cache_file = f.name

        try:
            save_geohash_cache({"3005": "gfxnj5"}, cache_file=cache_file)

            with open(cache_file, 'r') as f:
                saved_cache = json.load(f)

            assert saved_cache == {"3005": "gfxnj5"}
        finally:
            os.unlink(cache_file)


class TestDataTransformation:
    def test_transform_observation_maps_fields_correctly(self):
        obs = {
            "datetime": "2026-02-11T12:00:00Z",
            "humidity": "85",
            "mslp": "1013",
            "pressure_tendency": "F",
            "temperature": "8.5",
            "visibility": "15000",
            "weather_code": "7",
            "wind_direction": "SW",
            "wind_gust": "25",
            "wind_speed": "18",
            "_site_metadata": SAMPLE_SITE,
            "_geohash": "gfxnj5"
        }

        result = transform_observation(obs)

        assert result == EXPECTED_TRANSFORMED_ROW

    def test_transform_observation_adds_europe_continent(self):
        obs = {
            "datetime": "2026-02-11T12:00:00Z",
            "_site_metadata": SAMPLE_SITE
        }

        result = transform_observation(obs)

        assert result["site_continent"] == "EUROPE"

    def test_transform_observation_converts_datetime_format(self):
        obs = {
            "datetime": "2026-02-11T12:00:00Z",
            "_site_metadata": SAMPLE_SITE
        }

        result = transform_observation(obs)

        assert result["observation_ts"] == "2026-02-11 12:00:00"

    def test_transform_observation_handles_missing_fields(self):
        obs = {
            "datetime": "2026-02-11T12:00:00Z",
            "_site_metadata": SAMPLE_SITE
        }

        result = transform_observation(obs)

        assert result["wind_direction"] == ""
        assert result["temperature"] == ""
        assert result["pressure"] == ""


class TestExtractObservations:
    @patch('datahub_etl.weather_etl.SITES', [SAMPLE_SITE])
    def test_extract_with_cache_hit(self):
        mock_client = Mock()
        mock_client.get_observations.return_value = OBSERVATIONS_RESPONSE

        output_file = tempfile.mktemp(suffix='.json')

        try:
            with patch('datahub_etl.weather_etl.load_geohash_cache') as mock_load:
                with patch('datahub_etl.weather_etl.save_geohash_cache'):
                    mock_load.return_value = {"3005": "gfxnj5"}
                    extract_observations_data(output_file, mock_client)

            mock_client.get_nearest_station.assert_not_called()
            mock_client.get_observations.assert_called_once_with("gfxnj5")

            with open(output_file, 'r') as f:
                data = json.load(f)

            assert len(data) == 2
            assert data[0]["_site_metadata"]["site_id"] == "3005"
            assert data[0]["_geohash"] == "gfxnj5"
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    @patch('datahub_etl.weather_etl.SITES', [SAMPLE_SITE])
    def test_extract_with_cache_miss(self):
        mock_client = Mock()
        mock_client.get_nearest_station.return_value = NEAREST_STATION_RESPONSE
        mock_client.get_observations.return_value = OBSERVATIONS_RESPONSE

        output_file = tempfile.mktemp(suffix='.json')
        saved_cache = {}

        def save_cache_side_effect(cache, **kwargs):
            saved_cache.update(cache)

        try:
            with patch('datahub_etl.weather_etl.load_geohash_cache') as mock_load:
                with patch('datahub_etl.weather_etl.save_geohash_cache') as mock_save:
                    mock_load.return_value = {}
                    mock_save.side_effect = save_cache_side_effect
                    extract_observations_data(output_file, mock_client)

            mock_client.get_nearest_station.assert_called_once_with(60.139, -1.183)
            mock_client.get_observations.assert_called_once_with("gfxnj5")

            assert saved_cache == {"3005": "gfxnj5"}
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    @patch('datahub_etl.weather_etl.SITES', [SAMPLE_SITE])
    def test_extract_handles_api_errors(self):
        mock_client = Mock()
        mock_client.get_nearest_station.side_effect = Exception("API Error")

        output_file = tempfile.mktemp(suffix='.json')

        try:
            with patch('datahub_etl.weather_etl.load_geohash_cache') as mock_load:
                with patch('datahub_etl.weather_etl.save_geohash_cache'):
                    mock_load.return_value = {}
                    result = extract_observations_data(output_file, mock_client)

            with open(output_file, 'r') as f:
                data = json.load(f)

            assert data == []
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)


class TestTransformObservationsData:
    def test_transform_creates_newline_delimited_json(self):
        input_file = tempfile.mktemp(suffix='.json')
        output_file = tempfile.mktemp(suffix='.json')

        test_data = [
            {
                "datetime": "2026-02-11T12:00:00Z",
                "temperature": "8.5",
                "humidity": "85",
                "_site_metadata": SAMPLE_SITE,
                "_geohash": "gfxnj5"
            }
        ]

        with open(input_file, 'w') as f:
            json.dump(test_data, f)

        try:
            transform_observations_data(input_file, output_file)

            with open(output_file, 'r') as f:
                lines = f.readlines()

            assert len(lines) == 1
            row = json.loads(lines[0])
            assert row["site_id"] == "3005"
            assert row["temperature"] == "8.5"
            assert row["screen_relative_humidity"] == "85"
        finally:
            if os.path.exists(input_file):
                os.unlink(input_file)
            if os.path.exists(output_file):
                os.unlink(output_file)
