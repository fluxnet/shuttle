"""Test suite for fluxnet_shuttle_lib.sources.icos module."""

from unittest.mock import AsyncMock, patch

import pytest

from fluxnet_shuttle_lib.models import FluxnetDatasetMetadata
from fluxnet_shuttle_lib.plugins.icos import ICOSPlugin


class TestICOSPlugin:
    """Test cases for ICOSPlugin."""

    def test_plugin_properties(self):
        """Test plugin basic properties."""
        plugin = ICOSPlugin()

        assert plugin.name == "icos"
        assert plugin.display_name == "ICOS"
        assert plugin.config == {}

    def test_plugin_with_config(self):
        """Test plugin initialization with config."""
        config = {"api_url": "https://test.icos-cp.eu", "timeout": 60}
        plugin = ICOSPlugin(config=config)

        assert plugin.config == config
        assert plugin.config["api_url"] == "https://test.icos-cp.eu"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.plugins.icos.NetworkPlugin._session_request")
    async def test_async_get_sites(self, mock_request):
        """Test async get_sites method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-ABC"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-ABC"},
                        "lat": {"value": "12.34"},
                        "lon": {"value": "56.78"},
                        "timeStart": {"value": "2021-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2021-12-31T23:59:59Z"},
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()

        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        assert len(sites) == 1
        assert all(isinstance(site, FluxnetDatasetMetadata) for site in sites)
        assert sites[0].site_info.site_id == "US-ABC"
        assert sites[0].site_info.network == "ICOS"
        assert sites[0].site_info.location_lat == 12.34
        assert sites[0].site_info.location_long == 56.78
        assert sites[0].product_data.first_year == 2021
        assert sites[0].product_data.last_year == 2021
        assert str(sites[0].product_data.download_link) == "https://meta.icos-cp.eu/objects/US-ABC"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.plugins.icos.NetworkPlugin._session_request")
    async def test_async_get_sites_with_time_errors(self, mock_request):
        """Test async get_sites method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-ABC"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-ABC"},
                        "lat": {"value": "12.34"},
                        "lon": {"value": "56.78"},
                        "timeStart": {"value": "BAAR-01-01T00:00:00Z"},
                        "timeEnd": {"value": "FOOO-12-31T23:59:59Z"},
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()

        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        assert len(sites) == 1
        assert all(isinstance(site, FluxnetDatasetMetadata) for site in sites)
        assert sites[0].site_info.site_id == "US-ABC"
        assert sites[0].site_info.network == "ICOS"
        assert sites[0].site_info.location_lat == 12.34
        assert sites[0].site_info.location_long == 56.78
        assert sites[0].product_data.first_year == 2000
        assert sites[0].product_data.last_year == 2020
        assert str(sites[0].product_data.download_link) == "https://meta.icos-cp.eu/objects/US-ABC"

        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.plugins.icos.NetworkPlugin._session_request")
    async def test_async_get_sites_with_errors(self, mock_request, caplog):
        """Test async get_sites method."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-ABC"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-ABC"},
                        "lat": {"value": "12.34ff"},
                        "lon": {"value": "56.78"},
                        "timeStart": {"value": "2023-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2023-12-31T23:59:59Z"},
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()

        with caplog.at_level("WARNING"):
            sites = []
            async for site in plugin.get_sites():
                sites.append(site)

            assert len(sites) == 0

            assert "Error parsing ICOS site data: could not convert string to float: '12.34ff'" in caplog.text

        assert mock_request.call_count == 1
