"""
Integration tests for ICOS module.

These tests make actual HTTP requests to the ICOS Carbon Portal API
and should be run separately from unit tests. They may be slower and
require network connectivity.

Run with: pytest tests/integration/test_icos_integration.py -v
Skip with: pytest -m "not integration"
"""

import logging
import os
import tempfile
from unittest.mock import patch

import pytest
import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.sources.icos import (
    ICOS_API_URL,
    ICOS_QUERY,
    download_icos_data,
    get_icos_data,
    parse_icos_response,
)

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestICOSAPIIntegration:
    """Integration tests for ICOS Carbon Portal API functionality."""

    def test_get_icos_data_real_api(self):
        """Test getting ICOS data from real Carbon Portal API (if accessible)."""
        try:
            # This tests the real SPARQL endpoint
            data = get_icos_data()

            # Verify we got a dictionary
            assert isinstance(data, dict)

            # If we got data, verify the structure
            if data:
                logging.info(f"Retrieved {len(data)} ICOS data objects")

                # Check that we have the expected structure
                for obj_id, obj_data in data.items():
                    assert isinstance(obj_data, dict)
                    # Should have basic fields from SPARQL query
                    expected_fields = ["station", "fileName", "size"]
                    for field in expected_fields:
                        if field in obj_data:  # Some fields might be optional
                            assert obj_data[field] is not None

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS Carbon Portal API not accessible: {e}")

    def test_icos_sparql_query_structure(self):
        """Test that the SPARQL query is well-formed."""
        # Verify the query contains expected SPARQL elements
        assert "prefix cpmeta:" in ICOS_QUERY
        assert "select" in ICOS_QUERY.lower()
        assert "where" in ICOS_QUERY.lower()
        assert "?dobj" in ICOS_QUERY
        assert "miscFluxnetArchiveProduct" in ICOS_QUERY

    def test_icos_api_endpoint_structure(self):
        """Test that the ICOS API endpoint is correctly structured."""
        assert ICOS_API_URL.startswith("https://")
        assert "icos-cp.eu" in ICOS_API_URL

        # The endpoint should be accessible for SPARQL queries
        try:
            response = requests.get(ICOS_API_URL, timeout=10)
            # Should get some response (might be 400 without proper SPARQL query)
            assert response.status_code in [200, 400, 405]

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS endpoint not accessible: {e}")

    def test_parse_icos_response_with_real_structure(self):
        """Test parsing ICOS response with realistic data structure."""
        # Create mock response object that has json() method
        from unittest.mock import MagicMock

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test1"},
                        "station": {"value": "https://meta.icos-cp.eu/resources/stations/test_station"},
                        "fileName": {"value": "FLX_TEST_FLUXNET_ARCHIVE_2020-2021_1.zip"},
                        "size": {"value": "1048576"},
                        "submTime": {"value": "2023-01-01T00:00:00Z"},
                        "timeStart": {"value": "2020-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2020-12-31T23:59:59Z"},
                    }
                ]
            }
        }

        parsed_data = parse_icos_response(mock_response)

        assert isinstance(parsed_data, dict)
        assert len(parsed_data) == 1

        # Check the parsed structure - get the site_id key (extracted from station URL)
        site_id = list(parsed_data.keys())[0]
        obj_data = parsed_data[site_id]

        # Check for fields that should be present based on parsing logic
        assert "site_id" in obj_data
        assert "filename" in obj_data
        assert "download_link" in obj_data
        assert "first_year" in obj_data
        assert "last_year" in obj_data
        assert "version" in obj_data

        # Verify parsed values
        assert obj_data["filename"] == "FLX_TEST_FLUXNET_ARCHIVE_2020-2021_1.zip"
        assert obj_data["first_year"] == "2020"
        assert obj_data["last_year"] == "2021"
        assert obj_data["version"] == "1"

    @pytest.mark.slow
    def test_full_icos_workflow_limited(self):
        """Test the complete ICOS workflow with limited data."""
        try:
            # Step 1: Get ICOS data
            data = get_icos_data()

            if not data:
                pytest.skip("No ICOS data available")

            # Limit to first few objects to keep test reasonable
            limited_data = dict(list(data.items())[:3])
            logging.info(f"Testing workflow with {len(limited_data)} ICOS objects")

            # Step 2: Verify data structure
            for obj_id, obj_data in limited_data.items():
                assert isinstance(obj_data, dict)
                assert "filename" in obj_data  # Field is "filename" not "fileName"

            logging.info(f"Successfully processed {len(limited_data)} ICOS objects")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS API not accessible: {e}")


class TestICOSDownloadIntegration:
    """Integration tests for ICOS download functionality."""

    def test_download_with_mock_file_creation(self):
        """Test download functionality with mocked file operations but real URL structure."""
        # Create test data for individual download
        site_id = "UK-AMo"
        filename = "FLX_TEST_FLUXNET_ARCHIVE_2020.zip"
        download_link = "https://meta.icos-cp.eu/objects/test1"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock both license check and file download
            with patch("fluxnet_shuttle_lib.sources.icos.requests.get") as mock_get:

                # Mock file download with successful response
                mock_response = mock_get.return_value
                mock_response.status_code = 200  # Set successful status
                mock_response.iter_content.return_value = [b"test icos data chunk"]
                mock_response.headers = {"content-length": "1048576"}

                # Change to temp directory for download
                original_dir = os.getcwd()
                os.chdir(temp_dir)
                try:
                    # Since the function doesn't return anything on success, just call it
                    download_icos_data(site_id, filename, download_link)

                    # Check if file was "created" in temp directory
                    expected_path = os.path.join(temp_dir, filename)
                    assert os.path.exists(expected_path)
                    assert os.path.getsize(expected_path) > 0  # File should have content
                finally:
                    os.chdir(original_dir)

    @pytest.mark.slow
    def test_icos_license_acceptance_flow(self):
        """Test the ICOS license acceptance workflow."""
        # Create a test object URL
        test_object_url = "https://meta.icos-cp.eu/objects/test_object"

        with patch("requests.head") as mock_head, patch("requests.get") as mock_get:

            # Test scenario 1: License already accepted
            mock_head.return_value.status_code = 200

            # The download function should proceed with download
            mock_response = mock_get.return_value
            mock_response.status_code = 200
            mock_response.iter_content.return_value = [b"test data"]
            mock_response.headers = {"content-length": "100"}

            site_id = "UK-AMo"
            filename = "test_file.zip"
            download_link = test_object_url

            with tempfile.TemporaryDirectory() as temp_dir:
                # Change to temp directory for download
                original_dir = os.getcwd()
                os.chdir(temp_dir)
                try:
                    download_icos_data(site_id, filename, download_link)
                    # Check if file was created
                    assert os.path.exists(os.path.join(temp_dir, filename))
                finally:
                    os.chdir(original_dir)

    def test_icos_error_handling_scenarios(self):
        """Test various error scenarios in ICOS download."""
        site_id = "UK-AMo"
        filename = "invalid_file.zip"
        download_link = "https://meta.icos-cp.eu/objects/invalid"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test scenario: Download failure (404)
            with patch("fluxnet_shuttle_lib.sources.icos.requests.get") as mock_get:
                mock_response = mock_get.return_value
                mock_response.status_code = 404  # Not found

                # Change to temp directory for download
                original_dir = os.getcwd()
                os.chdir(temp_dir)
                try:
                    # Should raise FLUXNETShuttleError on failure
                    with pytest.raises(FLUXNETShuttleError):
                        download_icos_data(site_id, filename, download_link)
                finally:
                    os.chdir(original_dir)


class TestICOSErrorHandling:
    """Integration tests for ICOS error handling."""

    def test_invalid_sparql_endpoint_handling(self):
        """Test handling of invalid SPARQL endpoints."""
        # Mock requests.post to simulate connection error
        with patch("fluxnet_shuttle_lib.sources.icos.requests.post") as mock_post:
            mock_post.side_effect = requests.ConnectionError("Connection failed")

            # ICOS functions should handle connection errors gracefully
            result = get_icos_data()
            assert result is None

    def test_malformed_sparql_response_handling(self):
        """Test handling of malformed SPARQL responses."""
        with patch("fluxnet_shuttle_lib.sources.icos.requests.post") as mock_post:
            # Simulate a request that raises an exception
            mock_post.side_effect = requests.exceptions.RequestException("Malformed response")

            # Should handle request exceptions gracefully
            result = get_icos_data()
            # Function should return None when request fails
            assert result is None

    def test_network_timeout_handling(self):
        """Test handling of network timeouts for ICOS."""
        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.exceptions.Timeout("SPARQL request timed out")

            # ICOS functions return None on timeout instead of raising
            result = get_icos_data()
            assert result is None

    def test_empty_sparql_response_handling(self):
        """Test handling of empty SPARQL responses."""
        with patch("requests.post") as mock_post:
            mock_response = mock_post.return_value
            mock_response.json.return_value = {"results": {"bindings": []}}
            mock_response.raise_for_status.return_value = None

            data = get_icos_data()
            # Empty results should return None according to current implementation
            assert data is None


@pytest.mark.performance
class TestICOSPerformance:
    """Performance tests for ICOS operations."""

    def test_sparql_query_performance(self):
        """Test that SPARQL query completes in reasonable time."""
        import time

        try:
            start_time = time.time()
            data = get_icos_data()
            end_time = time.time()

            duration = end_time - start_time

            # SPARQL queries should complete within 60 seconds
            assert duration < 60, f"SPARQL query took {duration:.2f} seconds, too slow"
            logging.info(f"Retrieved {len(data)} ICOS objects in {duration:.2f} seconds")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS Carbon Portal not accessible: {e}")

    def test_data_parsing_performance(self):
        """Test performance of parsing large ICOS responses."""
        # Create a large mock response
        large_bindings = []
        for i in range(1000):  # 1000 objects
            binding = {
                "dobj": {"value": f"https://meta.icos-cp.eu/objects/test{i}"},
                "station": {"value": f"https://meta.icos-cp.eu/resources/stations/station{i}"},
                "fileName": {"value": f"FLX_TEST{i}_FLUXNET_ARCHIVE_2020-2021_1.zip"},
                "size": {"value": str(1048576 + i)},
            }
            large_bindings.append(binding)

        large_response = {"results": {"bindings": large_bindings}}

        # Create a mock response object
        from unittest.mock import Mock

        mock_response = Mock()
        mock_response.json.return_value = large_response

        import time

        start_time = time.time()
        parsed_data = parse_icos_response(mock_response)
        end_time = time.time()

        duration = end_time - start_time

        # Parsing should be fast
        assert duration < 5, f"Parsing took {duration:.2f} seconds for 1000 objects, too slow"
        assert len(parsed_data) == 1000
        logging.info(f"Parsed 1000 ICOS objects in {duration:.4f} seconds")


class TestICOSDataQuality:
    """Integration tests for ICOS data quality and validation."""

    def test_icos_data_completeness(self):
        """Test that ICOS data contains expected fields."""
        try:
            data = get_icos_data()

            if not data:
                pytest.skip("No ICOS data available for quality test")

            # Sample a few objects for testing
            sample_data = dict(list(data.items())[:5])

            for obj_id, obj_data in sample_data.items():
                # Check that object ID is a string (could be site ID or URL)
                assert isinstance(obj_id, str), f"Invalid object ID type: {type(obj_id)}"

                # Check that essential fields are present
                assert isinstance(obj_data, dict), f"Object data should be dict for {obj_id}"

                # Check for common fields that should be present
                if "fileName" in obj_data:
                    assert isinstance(obj_data["fileName"], str), f"fileName should be string for {obj_id}"

                # If size is present, check it's reasonable
                if "size" in obj_data:
                    size_value = obj_data["size"]
                    if isinstance(size_value, str) and size_value.isdigit():
                        assert int(size_value) > 0, f"Size should be positive for {obj_id}"

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS API not accessible: {e}")

    def test_icos_station_url_format(self):
        """Test that ICOS station URLs have the expected format."""
        try:
            data = get_icos_data()

            if not data:
                pytest.skip("No ICOS data available for station URL test")

            sample_data = dict(list(data.items())[:3])

            for obj_id, obj_data in sample_data.items():
                if "station" in obj_data:
                    station_url = obj_data["station"]
                    assert station_url.startswith(
                        "https://meta.icos-cp.eu"
                    ), f"Unexpected station URL format: {station_url}"

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS API not accessible: {e}")


@pytest.mark.integration
@pytest.mark.slow
class TestCombinedIntegration:
    """Integration tests that combine AmeriFlux and ICOS functionality."""

    def test_compare_data_sources_structure(self):
        """Compare the data structures returned by AmeriFlux and ICOS."""
        try:
            # Get data from both sources
            from fluxnet_shuttle_lib.sources.ameriflux import AMERIFLUX_BASE_URL, get_ameriflux_fluxnet_sites

            ameriflux_sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            icos_data = get_icos_data()

            if not ameriflux_sites:
                pytest.skip("No AmeriFlux sites available")
            if not icos_data:
                pytest.skip("No ICOS data available")

            logging.info(f"AmeriFlux: {len(ameriflux_sites)} sites")
            logging.info(f"ICOS: {len(icos_data)} objects")

            # Both should return data
            assert len(ameriflux_sites) > 0
            assert len(icos_data) > 0

            # Data types should be correct
            assert isinstance(ameriflux_sites, list)
            assert isinstance(icos_data, dict)

        except requests.exceptions.RequestException as e:
            pytest.skip(f"One or both APIs not accessible: {e}")
