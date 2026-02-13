import pytest
import os
from unittest.mock import Mock, patch
from datahub_etl.site_loader import get_sites, load_sites_from_athena, clear_cache
from tests.fixtures import SAMPLE_SITE


class TestGetSites:
    def setup_method(self):
        """Clear cache before each test"""
        clear_cache()

    @patch('datahub_etl.site_loader.load_sites_from_athena')
    def test_get_sites_loads_from_athena(self, mock_load):
        """get_sites should load from Athena"""
        mock_load.return_value = [SAMPLE_SITE]

        sites = get_sites()

        mock_load.assert_called_once()
        assert sites == [SAMPLE_SITE]
        assert all('site_name' in s for s in sites)

    @patch.dict(os.environ, {'USE_ATHENA_SITES': 'false'})
    @patch('datahub_etl.site_loader.load_sites_from_athena')
    def test_get_sites_respects_env_var_false(self, mock_load):
        """Env var no longer used, always loads from Athena"""
        mock_load.return_value = [SAMPLE_SITE]

        sites = get_sites()

        mock_load.assert_called_once()
        assert len(sites) == 1


class TestLoadSitesFromAthena:
    def setup_method(self):
        """Clear cache before each test"""
        clear_cache()

    @patch('helpers.aws.execute_athena_query')
    @patch('helpers.aws.load_text_from_s3')
    def test_load_from_athena_parses_csv_correctly(self, mock_load_text, mock_execute):
        """Should parse Athena CSV results into site dictionaries"""
        # Mock Athena query result location
        mock_execute.return_value = "s3://dantelore.queryresults/results/abc123.csv"

        # Mock S3 CSV content
        csv_content = """site_id,site_name,site_country,site_elevation,lat,lon
3005,LERWICK (S. SCREEN),SCOTLAND,82.0,60.139,-1.183
3017,KIRKWALL AIRPORT,SCOTLAND,26.0,58.954,-2.9"""

        mock_load_text.return_value = csv_content

        sites = load_sites_from_athena()

        assert len(sites) == 2
        assert sites[0] == {
            'site_id': '3005',
            'site_name': 'LERWICK (S. SCREEN)',
            'site_country': 'SCOTLAND',
            'site_elevation': 82.0,
            'lat': 60.139,
            'lon': -1.183
        }

        mock_execute.assert_called_once()
        mock_load_text.assert_called_once_with('dantelore.queryresults', 'results/abc123.csv')

    @patch('helpers.aws.execute_athena_query')
    @patch('helpers.aws.load_text_from_s3')
    def test_load_from_athena_caches_results(self, mock_load_text, mock_execute):
        """Should cache results and not re-query Athena"""
        mock_execute.return_value = "s3://dantelore.queryresults/results/abc123.csv"

        csv_content = """site_id,site_name,site_country,site_elevation,lat,lon
3005,LERWICK (S. SCREEN),SCOTLAND,82.0,60.139,-1.183"""

        mock_load_text.return_value = csv_content

        # First call
        sites1 = load_sites_from_athena()
        # Second call
        sites2 = load_sites_from_athena()

        assert sites1 == sites2
        mock_execute.assert_called_once()  # Only called once due to caching

    @patch('helpers.aws.execute_athena_query')
    def test_load_from_athena_handles_query_failure(self, mock_execute):
        """Should fail fast when Athena query fails"""
        mock_execute.return_value = None

        with pytest.raises(AttributeError):
            load_sites_from_athena()


class TestCacheClearing:
    def test_clear_cache_resets_module_cache(self):
        """Should reset cache to allow fresh queries"""
        with patch('helpers.aws.execute_athena_query') as mock_execute:
            with patch('helpers.aws.load_text_from_s3') as mock_load_text:
                mock_execute.return_value = "s3://bucket/key.csv"

                csv_content = """site_id,site_name,site_country,site_elevation,lat,lon
3005,LERWICK (S. SCREEN),SCOTLAND,82.0,60.139,-1.183"""

                mock_load_text.return_value = csv_content

                # Load with caching
                load_sites_from_athena()
                assert mock_execute.call_count == 1

                # Load again (should use cache)
                load_sites_from_athena()
                assert mock_execute.call_count == 1

                # Clear cache
                clear_cache()

                # Load again (should query Athena)
                load_sites_from_athena()
                assert mock_execute.call_count == 2
