"""Test suite for fluxnet_shuttle.sources.icos module."""

from unittest.mock import AsyncMock, patch

import pytest

from fluxnet_shuttle.models import FluxnetDatasetMetadata
from fluxnet_shuttle.plugins.icos import ICOSPlugin


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
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
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
                        "ecosystemType": {"value": "http://meta.icos-cp.eu/ontologies/cpmeta/igbp_ENF"},
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
        assert sites[0].site_info.data_hub == "ICOS"
        assert sites[0].site_info.location_lat == 12.34
        assert sites[0].site_info.location_long == 56.78
        assert sites[0].site_info.igbp == "ENF"
        assert sites[0].product_data.first_year == 2021
        assert sites[0].product_data.last_year == 2021
        # Pre-encoded URL format: %5B=[, %5D=], %22="
        assert (
            str(sites[0].product_data.download_link) == "https://data.icos-cp.eu/licence_accept?ids=%5B%22US-ABC%22%5D"
        )

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
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
        assert sites[0].site_info.data_hub == "ICOS"
        assert sites[0].site_info.location_lat == 12.34
        assert sites[0].site_info.location_long == 56.78
        assert sites[0].site_info.igbp == "UNK"
        assert sites[0].product_data.first_year == 2000
        assert sites[0].product_data.last_year == 2020
        # Pre-encoded URL format: %5B=[, %5D=], %22="
        assert (
            str(sites[0].product_data.download_link) == "https://data.icos-cp.eu/licence_accept?ids=%5B%22US-ABC%22%5D"
        )

        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_errors(self, mock_request, caplog):
        """Test async get_sites method with invalid latitude."""
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

            # Site should still be yielded with fallback values
            assert len(sites) == 1
            assert sites[0].site_info.location_lat == 0.0  # Fallback value
            assert sites[0].site_info.location_long == 56.78
            assert sites[0].site_info.igbp == "UNK"

            # Check that warning was logged
            assert "Invalid latitude for station US-ABC" in caplog.text

        assert mock_request.call_count == 1

    def test_ecosystem_to_igbp_mapping(self):
        """Test ecosystem type to IGBP mapping from ICOS URIs."""
        plugin = ICOSPlugin()

        # Test IGBP codes from ICOS URIs (the actual format ICOS uses)
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_ENF") == "ENF"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_GRA") == "GRA"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_CRO") == "CRO"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_WET") == "WET"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_DBF") == "DBF"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_MF") == "MF"
        assert plugin._map_ecosystem_to_igbp("http://meta.icos-cp.eu/ontologies/cpmeta/igbp_SAV") == "SAV"

        # Test without URI prefix (just the igbp_XXX part)
        assert plugin._map_ecosystem_to_igbp("igbp_ENF") == "ENF"
        assert plugin._map_ecosystem_to_igbp("igbp_gra") == "GRA"  # Case insensitive

        # Test empty and unknown types
        assert plugin._map_ecosystem_to_igbp("") == "UNK"
        assert plugin._map_ecosystem_to_igbp("unknown_type") == "UNK"
        assert plugin._map_ecosystem_to_igbp("some_random_text") == "UNK"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_invalid_longitude(self, mock_request, caplog):
        """Test async get_sites method with invalid longitude."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-XYZ"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-XYZ"},
                        "lat": {"value": "45.5"},
                        "lon": {"value": "invalid_lon"},
                        "timeStart": {"value": "2020-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2021-12-31T23:59:59Z"},
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

            # Site should still be yielded with fallback values
            assert len(sites) == 1
            assert sites[0].site_info.location_lat == 45.5
            assert sites[0].site_info.location_long == 0.0  # Fallback value for invalid lon
            assert sites[0].site_info.igbp == "UNK"

            # Check that warning was logged
            assert "Invalid longitude for station US-XYZ" in caplog.text

        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_general_exception(self, mock_request, caplog):
        """Test async get_sites handles general exceptions during parsing."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        # Missing required 'dobj' field to trigger exception
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-BAD"},
                        "lat": {"value": "40.0"},
                        "lon": {"value": "-100.0"},
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

            # Should skip the malformed entry
            assert len(sites) == 0

            # Check that warning was logged
            assert "Error parsing ICOS site data" in caplog.text

        assert mock_request.call_count == 1
