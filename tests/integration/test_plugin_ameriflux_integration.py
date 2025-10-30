"""
Integration tests for AmeriFlux module.

These tests make actual HTTP requests to the AmeriFlux API endpoints
and should be run separately from unit tests. They may be slower and
require network connectivity.

Run with: pytest tests/integration/test_plugin_ameriflux_integration.py -v
Skip with: pytest -m "not integration"
"""

import logging

import pytest
import requests

from fluxnet_shuttle.models import FluxnetDatasetMetadata
from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestAmeriFluxAPIIntegration:
    """Integration tests for AmeriFlux API functionality."""

    def test_get_ameriflux_sites_real_api(self):
        """Test getting AmeriFlux sites from real API (if accessible)."""
        try:
            # Use the correct API URL
            sites = list(AmeriFluxPlugin().get_sites())

            # Verify we got a list of sites
            assert isinstance(sites, list)

            # If we got sites, verify they're FluxnetDatasetMetadata objects
            if sites and len(sites) > 0:
                assert all(isinstance(site, FluxnetDatasetMetadata) for site in sites)
                logging.info(f"Retrieved {len(sites)} AmeriFlux sites")
            else:
                pytest.fail("No sites were returned.")

        except requests.exceptions.RequestException as e:
            pytest.skip(f"AmeriFlux API not accessible: {e}")
