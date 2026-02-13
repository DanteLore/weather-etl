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
    save_geohash_cache,
    _build_site_priority_queue
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

        assert result["wind_direction"] is None
        assert result["temperature"] is None
        assert result["pressure"] is None


class TestExtractObservations:
    @patch('datahub_etl.weather_etl.get_sites', return_value=[SAMPLE_SITE])
    def test_extract_with_cache_hit(self, mock_get_sites):
        mock_client = Mock()
        mock_client.get_observations.return_value = OBSERVATIONS_RESPONSE

        output_file = tempfile.mktemp(suffix='.json')

        try:
            with patch('datahub_etl.weather_etl.load_geohash_cache') as mock_load:
                with patch('datahub_etl.weather_etl.save_geohash_cache'):
                    mock_load.return_value = {"3005": {"geohash": "gfxnj5", "last_fetched": "2026-01-01T00:00:00Z"}}
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

    @patch('datahub_etl.weather_etl.get_sites', return_value=[SAMPLE_SITE])
    def test_extract_with_cache_miss(self, mock_get_sites):
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

            assert "3005" in saved_cache
            assert saved_cache["3005"]["geohash"] == "gfxnj5"
            assert "last_fetched" in saved_cache["3005"]
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    @patch('datahub_etl.weather_etl.get_sites', return_value=[SAMPLE_SITE])
    def test_extract_handles_api_errors(self, mock_get_sites):
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


class TestBatchPrioritization:
    def test_build_priority_queue_sorts_by_timestamp(self):
        """Sites should be sorted by last_fetched timestamp, oldest first"""
        sites = [
            {"site_id": "3005", "site_name": "Site A"},
            {"site_id": "3017", "site_name": "Site B"},
            {"site_id": "3026", "site_name": "Site C"}
        ]

        cache = {
            "3005": {"geohash": "abc", "last_fetched": "2026-02-13T10:00:00Z"},
            "3017": {"geohash": "def", "last_fetched": "2026-02-12T10:00:00Z"},  # Oldest
            "3026": {"geohash": "ghi", "last_fetched": "2026-02-13T15:00:00Z"}
        }

        queue = _build_site_priority_queue(sites, cache)

        assert len(queue) == 3
        assert queue[0][0]["site_id"] == "3017"  # Oldest timestamp
        assert queue[1][0]["site_id"] == "3005"
        assert queue[2][0]["site_id"] == "3026"  # Newest timestamp

    def test_build_priority_queue_prioritizes_never_fetched(self):
        """Sites never fetched should get 1970 timestamp and be processed first"""
        sites = [
            {"site_id": "3005", "site_name": "Site A"},
            {"site_id": "3017", "site_name": "Site B"},
            {"site_id": "3026", "site_name": "Site C"}
        ]

        cache = {
            "3005": {"geohash": "abc", "last_fetched": "2026-02-13T10:00:00Z"},
            # 3017 not in cache - should be first
            "3026": {"geohash": "ghi", "last_fetched": "2026-02-12T10:00:00Z"}
        }

        queue = _build_site_priority_queue(sites, cache)

        assert queue[0][0]["site_id"] == "3017"  # Never fetched
        assert queue[0][1] == "1970-01-01T00:00:00Z"  # Default timestamp

    def test_build_priority_queue_handles_none_last_fetched(self):
        """Sites with None last_fetched should get 1970 timestamp"""
        sites = [
            {"site_id": "3005", "site_name": "Site A"},
            {"site_id": "3017", "site_name": "Site B"}
        ]

        cache = {
            "3005": {"geohash": "abc", "last_fetched": None},
            "3017": {"geohash": "def", "last_fetched": "2026-02-13T10:00:00Z"}
        }

        queue = _build_site_priority_queue(sites, cache)

        assert queue[0][0]["site_id"] == "3005"
        assert queue[0][1] == "1970-01-01T00:00:00Z"
        assert queue[1][0]["site_id"] == "3017"

    @patch('datahub_etl.weather_etl.get_sites')
    def test_batching_respects_priority_order(self, mock_get_sites):
        """When batch_size < total sites, oldest sites should be processed first"""
        sites = [
            {"site_id": "3005", "site_name": "Site A", "lat": 60.0, "lon": -1.0},
            {"site_id": "3017", "site_name": "Site B", "lat": 61.0, "lon": -2.0},
            {"site_id": "3026", "site_name": "Site C", "lat": 62.0, "lon": -3.0}
        ]
        mock_get_sites.return_value = sites

        cache = {
            "3005": {"geohash": "abc", "last_fetched": "2026-02-13T10:00:00Z"},
            "3017": {"geohash": "def", "last_fetched": "2026-02-12T10:00:00Z"},  # Oldest
            "3026": {"geohash": "ghi", "last_fetched": "2026-02-13T15:00:00Z"}
        }

        mock_client = Mock()
        mock_client.get_observations.return_value = [
            {"datetime": "2026-02-13T12:00:00Z", "temperature": "10"}
        ]

        output_file = tempfile.mktemp(suffix='.json')

        try:
            with patch('datahub_etl.weather_etl.load_geohash_cache') as mock_load:
                with patch('datahub_etl.weather_etl.save_geohash_cache'):
                    mock_load.return_value = cache
                    extract_observations_data(output_file, mock_client, batch_size=2)

            # Should process 3017 first (oldest), then 3005
            # 3026 should not be processed (batch_size=2)
            calls = mock_client.get_observations.call_args_list
            assert len(calls) == 2
            assert calls[0][0][0] == "def"  # 3017's geohash
            assert calls[1][0][0] == "abc"  # 3005's geohash
        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)
