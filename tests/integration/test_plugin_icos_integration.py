"""
Integration tests for ICOS module.

These tests make actual HTTP requests to the ICOS API endpoints
and should be run separately from unit tests. They may be slower and
require network connectivity.

Run with: pytest tests/integration/test_plugin_icos_integration.py -v
Skip with: pytest -m "not integration"
"""

import logging

import pytest
import requests

from fluxnet_shuttle.models import FluxnetDatasetMetadata
from fluxnet_shuttle.plugins.icos import ICOSPlugin

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestICOSAPIIntegration:
    """Integration tests for ICOS API functionality."""

    def test_get_icos_sites_real_api(self):
        """Test getting ICOS sites from real API (if accessible)."""
        try:
            # Use the correct API URL
            sites = list(ICOSPlugin().get_sites())

            # Verify we got a list of sites
            assert isinstance(sites, list)

            # If we got sites, verify they're FluxnetDatasetMetadata objects
            if sites:
                assert all(isinstance(site, FluxnetDatasetMetadata) for site in sites)
                logging.info(f"Retrieved {len(sites)} ICOS sites")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"ICOS API not accessible: {e}")
