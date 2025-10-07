"""
Integration tests for AmeriFlux module.

These tests make actual HTTP requests to the AmeriFlux API endpoints
and should be run separately from unit tests. They may be slower and
require network connectivity.

Run with: pytest tests/integration/test_ameriflux_integration.py -v
Skip with: pytest -m "not integration"
"""

import logging
import os
import tempfile
from unittest.mock import patch

import pytest
import requests

from fluxnet_shuttle_lib.sources.ameriflux import (
    AMERIFLUX_BASE_URL,
    download_ameriflux_data,
    get_ameriflux_download_links,
    get_ameriflux_fluxnet_sites,
    parse_ameriflux_response,
)

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestAmeriFluxAPIIntegration:
    """Integration tests for AmeriFlux API functionality."""

    def test_get_ameriflux_sites_real_api(self):
        """Test getting AmeriFlux sites from real API (if accessible)."""
        try:
            # Use the correct API URL
            api_url = f"{AMERIFLUX_BASE_URL}api/v1/"
            sites = get_ameriflux_fluxnet_sites(api_url)

            # Verify we got a list of sites
            assert isinstance(sites, list)

            # If we got sites, verify they're strings
            if sites:
                assert all(isinstance(site, str) for site in sites)
                logging.info(f"Retrieved {len(sites)} AmeriFlux sites")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")

    def test_get_ameriflux_sites_with_custom_url(self):
        """Test getting AmeriFlux sites with custom API URL."""
        # Test with the base URL construction
        api_url = f"{AMERIFLUX_BASE_URL}api/v1/"

        try:
            sites = get_ameriflux_fluxnet_sites(api_url=api_url)
            assert isinstance(sites, list)

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")

    def test_download_links_integration_small_subset(self):
        """Test getting download links for a small subset of sites."""
        try:
            # First get available sites
            api_url = f"{AMERIFLUX_BASE_URL}api/v1/"
            sites = get_ameriflux_fluxnet_sites(api_url)
            if not sites:
                pytest.skip("No AmeriFlux sites available")

            # Test with just the first site if available
            test_sites = sites[:1] if sites else []

            if test_sites:
                links = get_ameriflux_download_links(api_url=f"{AMERIFLUX_BASE_URL}api/v1/", site_ids=test_sites)

                assert isinstance(links, dict)
                assert "data_urls" in links

                logging.info(f"Retrieved download links for {len(test_sites)} sites")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")

    @pytest.mark.slow
    def test_full_ameriflux_workflow_limited(self):
        """Test the complete AmeriFlux workflow with a limited number of sites."""
        try:
            # Step 1: Get available sites
            sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            if not sites:
                pytest.skip("No AmeriFlux sites available")

            # Limit to first 2 sites to keep test reasonable
            test_sites = sites[:2]
            logging.info(f"Testing workflow with sites: {test_sites}")

            # Step 2: Get download links
            api_url = f"{AMERIFLUX_BASE_URL}api/v1/"
            links_response = get_ameriflux_download_links(api_url=api_url, site_ids=test_sites)

            assert isinstance(links_response, dict)
            assert "data_urls" in links_response

            # Step 3: Parse the response
            parsed_data = parse_ameriflux_response(links_response)

            assert isinstance(parsed_data, dict)

            # Verify structure of parsed data
            for site_id, data in parsed_data.items():
                assert isinstance(data, dict)
                required_fields = ["publisher", "site_id", "download_link", "filename"]
                for field in required_fields:
                    assert field in data, f"Missing field {field} in parsed data for {site_id}"

            logging.info(f"Successfully processed {len(parsed_data)} sites")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")


class TestAmeriFluxDownloadIntegration:
    """Integration tests for AmeriFlux download functionality."""

    def test_download_with_mock_file_creation(self):
        """Test download functionality with mocked file operations but real URL parsing."""
        # Create test data for individual download
        site_id = "US-TEST"
        filename = "FLX_US-TEST_FLUXNET2015_FULLSET_2020-2023_1.zip"
        download_link = "https://example.com/FLX_US-TEST_FLUXNET2015_FULLSET_2020-2023_1.zip"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the actual download but test the file path logic
            with patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get") as mock_get:
                mock_response = mock_get.return_value
                mock_response.status_code = 200  # Set successful status
                mock_response.iter_content.return_value = [b"test data chunk"]
                mock_response.headers = {"content-length": "100"}

                # Change to temp directory for download
                original_dir = os.getcwd()
                os.chdir(temp_dir)
                try:
                    # Since the function doesn't return anything on success, just call it
                    download_ameriflux_data(site_id, filename, download_link)

                    # Check if file was "created" in temp directory
                    expected_path = os.path.join(temp_dir, filename)
                    assert os.path.exists(expected_path)
                    assert os.path.getsize(expected_path) > 0  # File should have content
                finally:
                    os.chdir(original_dir)

    @pytest.mark.slow
    def test_small_file_download_real(self):
        """Test downloading a small real file if available."""
        try:
            # Try to get a real small file from AmeriFlux
            sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            if not sites:
                pytest.skip("No AmeriFlux sites available for download test")

            # Get download links for the first site only
            test_site = sites[0]
            api_url = f"{AMERIFLUX_BASE_URL}api/v1/"

            links_response = get_ameriflux_download_links(api_url=api_url, site_ids=[test_site])
            parsed_data = parse_ameriflux_response(links_response)

            if not parsed_data:
                pytest.skip("No download links available for test")

            with tempfile.TemporaryDirectory() as temp_dir:
                # Download only the first file and limit size for testing
                site_id, site_data = list(parsed_data.items())[0]
                filename = site_data["filename"]
                download_link = site_data["download_link"]

                # Mock to prevent large downloads in tests
                with patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get") as mock_get:
                    mock_response = mock_get.return_value
                    mock_response.status_code = 200
                    mock_response.iter_content.return_value = [b"test data"]
                    mock_response.headers = {"content-length": "100"}

                    # Change to temp directory for download
                    original_dir = os.getcwd()
                    os.chdir(temp_dir)
                    try:
                        download_ameriflux_data(site_id, filename, download_link)
                        # Check if file was created
                        assert os.path.exists(os.path.join(temp_dir, filename))
                    finally:
                        os.chdir(original_dir)

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux download test failed: {e}")


class TestAmeriFluxErrorHandling:
    """Integration tests for AmeriFlux error handling."""

    def test_invalid_api_url_handling(self):
        """Test handling of invalid API URLs."""
        invalid_url = "https://invalid-domain-that-does-not-exist.com/api/"

        # AmeriFlux functions return None on error instead of raising
        result = get_ameriflux_fluxnet_sites(api_url=invalid_url)
        assert result is None

    def test_malformed_api_response_handling(self):
        """Test handling of malformed API responses."""
        # This would require mocking the requests to return malformed data
        with patch("requests.get") as mock_get:
            mock_response = mock_get.return_value
            mock_response.json.return_value = {"unexpected": "format"}
            mock_response.raise_for_status.return_value = None

            # Should handle unexpected response format gracefully
            sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            assert isinstance(sites, list)

    def test_network_timeout_handling(self):
        """Test handling of network timeouts."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

            # AmeriFlux functions return None on timeout instead of raising
            result = get_ameriflux_fluxnet_sites(api_url=f"{AMERIFLUX_BASE_URL}api/v1/")
            assert result is None


@pytest.mark.performance
class TestAmeriFluxPerformance:
    """Performance tests for AmeriFlux operations."""

    def test_site_retrieval_performance(self):
        """Test that site retrieval completes in reasonable time."""
        import time

        try:
            start_time = time.time()
            sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            end_time = time.time()

            duration = end_time - start_time

            # Should complete within 30 seconds
            assert duration < 30, f"Site retrieval took {duration:.2f} seconds, too slow"
            logging.info(f"Retrieved {len(sites)} sites in {duration:.2f} seconds")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")

    def test_download_links_performance_limited(self):
        """Test download links retrieval performance with limited sites."""
        try:
            sites = get_ameriflux_fluxnet_sites(f"{AMERIFLUX_BASE_URL}api/v1/")
            if not sites:
                pytest.skip("No sites available for performance test")

            # Test with maximum 3 sites for performance test
            test_sites = sites[:3]

            import time

            start_time = time.time()

            api_url = f"{AMERIFLUX_BASE_URL}api/v1/"
            links = get_ameriflux_download_links(api_url=api_url, site_ids=test_sites)

            # assert links is a dict with expected structure
            assert isinstance(links, dict)
            assert "data_urls" in links
            assert isinstance(links["data_urls"], list)
            for url in links["data_urls"]:
                assert "site_id" in url
                assert "url" in url

            end_time = time.time()
            duration = end_time - start_time

            # Should complete within 60 seconds for 3 sites
            assert duration < 60, f"Download links retrieval took {duration:.2f} seconds, too slow"
            logging.info(f"Retrieved download links for {len(test_sites)} sites in {duration:.2f} seconds")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")
