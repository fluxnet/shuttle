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
                        "stationName": {"value": "Test ICOS Station"},
                        "fileName": {"value": "FLX_US-ABC_FLUXNET_2021-2021_v1_r0.zip"},
                        "lat": {"value": "12.34"},
                        "lon": {"value": "56.78"},
                        "ecosystemType": {"value": "http://meta.icos-cp.eu/ontologies/cpmeta/igbp_ENF"},
                        "timeStart": {"value": "2021-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2021-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation for US-ABC"},
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
        # Product ID should be the hashsum (last part of dobj URI)
        assert sites[0].product_data.product_id == "US-ABC"
        # Code version should be extracted from filename
        assert sites[0].product_data.code_version == "v1"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_time_errors(self, mock_request):
        """Test async get_sites method."""
        # Mock SPARQL response with citation included
        mock_sparql_response = AsyncMock()
        mock_sparql_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-ABC"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-ABC"},
                        "stationName": {"value": "Test Station"},
                        "fileName": {"value": "FLX_US-ABC_FLUXNET_2000-2020_v1_r0.zip"},
                        "lat": {"value": "12.34"},
                        "lon": {"value": "56.78"},
                        "timeStart": {"value": "BAAR-01-01T00:00:00Z"},
                        "timeEnd": {"value": "FOOO-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation for US-ABC"},
                    }
                ]
            }
        }

        mock_request.return_value.__aenter__.return_value = mock_sparql_response

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
        assert sites[0].product_data.product_citation == "Test citation for US-ABC"

        # Now we expect only 1 call for SPARQL (includes team members)
        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_errors(self, mock_request, caplog):
        """Test async get_sites method with invalid latitude."""
        # Mock SPARQL response with citation included
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-ABC"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-ABC"},
                        "stationName": {"value": "Test Station"},
                        "fileName": {"value": "FLX_US-ABC_FLUXNET_2023-2023_v1_r0.zip"},
                        "lat": {"value": "12.34ff"},
                        "lon": {"value": "56.78"},
                        "timeStart": {"value": "2023-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2023-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation"},
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

        # Now we expect only 1 call for SPARQL (includes team members)
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
        # Mock SPARQL response with citation included
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/US-XYZ"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/US-XYZ"},
                        "stationName": {"value": "XYZ Station"},
                        "fileName": {"value": "FLX_US-XYZ_FLUXNET_2020-2021_v1_r0.zip"},
                        "lat": {"value": "45.5"},
                        "lon": {"value": "invalid_lon"},
                        "timeStart": {"value": "2020-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2021-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation"},
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

        # Now we expect only 1 call for SPARQL (includes team members)
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
            assert "Error grouping ICOS site data" in caplog.text

        # Now we expect only 1 call for SPARQL (includes team members)
        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_team_members(self, mock_request):
        """Test async get_sites with team member information."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Hte"},
                        "stationName": {"value": "Huetelmoor"},
                        "fileName": {"value": "FLX_DE-Hte_FLUXNET_2009-2018_v1_r0.zip"},
                        "lat": {"value": "54.21"},
                        "lon": {"value": "12.18"},
                        "ecosystemType": {"value": "http://meta.icos-cp.eu/ontologies/cpmeta/igbp_WET"},
                        "timeStart": {"value": "2009-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2018-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation"},
                        "firstName": {"value": "Gerald"},
                        "lastName": {"value": "Jurasinski"},
                        "email": {"value": "gerald.jurasinski@uni-rostock.de"},
                        "roleName": {"value": "Principal Investigator"},
                        "orgName": {"value": "University of Rostock"},
                    },
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Hte"},
                        "stationName": {"value": "Huetelmoor"},
                        "fileName": {"value": "FLX_DE-Hte_FLUXNET_2009-2018_v1_r0.zip"},
                        "lat": {"value": "54.21"},
                        "lon": {"value": "12.18"},
                        "ecosystemType": {"value": "http://meta.icos-cp.eu/ontologies/cpmeta/igbp_WET"},
                        "timeStart": {"value": "2009-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2018-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation"},
                        "firstName": {"value": "Ute"},
                        "lastName": {"value": "Karstens"},
                        "email": {"value": "ute.karstens@nateko.lu.se"},
                        "roleName": {"value": "Researcher"},
                    },
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()
        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        assert len(sites) == 1
        site = sites[0]
        assert site.site_info.site_id == "DE-Hte"
        assert site.site_info.site_name == "Huetelmoor"
        assert site.product_data.code_version == "v1"

        # Verify team members
        team_members = site.site_info.group_team_member
        assert len(team_members) == 2

        assert team_members[0].team_member_name == "Gerald Jurasinski"
        assert team_members[0].team_member_role == "Principal Investigator"
        assert team_members[0].team_member_email == "gerald.jurasinski@uni-rostock.de"

        assert team_members[1].team_member_name == "Ute Karstens"
        assert team_members[1].team_member_role == "Researcher"
        assert team_members[1].team_member_email == "ute.karstens@nateko.lu.se"

        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_with_empty_name_team_member(self, mock_request):
        """Test that team members with only whitespace names are skipped."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Hte"},
                        "stationName": {"value": "Huetelmoor"},
                        "fileName": {"value": "FLX_DE-Hte_FLUXNET_2009-2018_v1_r0.zip"},
                        "lat": {"value": "54.21"},
                        "lon": {"value": "12.18"},
                        "timeStart": {"value": "2009-01-01T00:00:00Z"},
                        "timeEnd": {"value": "2018-12-31T23:59:59Z"},
                        "citationString": {"value": "Test citation for DE-Hte"},
                        "firstName": {"value": "   "},
                        "lastName": {"value": "   "},
                        "roleName": {"value": "PI"},
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()
        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        # Site should be created but with no team members (empty names filtered out)
        assert len(sites) == 1
        assert len(sites[0].site_info.group_team_member) == 0

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_error_parsing_site_data(self, mock_request, caplog):
        """Test error handling when parsing site data fails."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Hte"},
                        "stationName": {"value": "Huetelmoor"},
                        "fileName": {"value": "FLX_DE-Hte_FLUXNET_2009-2018_v1_r0.zip"},
                        "lat": {"value": "invalid"},  # This will cause error in float conversion
                        "lon": {"value": "12.18"},
                        "citationString": {"value": "Test citation for DE-Hte"},
                        # Missing required timeStart and timeEnd will use defaults
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

        # Site should still be created with fallback values
        assert len(sites) == 1
        # Should have warning about invalid latitude
        assert "Invalid latitude for station DE-Hte" in caplog.text

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_exception_in_metadata_creation(self, mock_request, caplog):
        """Test error handling when filename validation fails due to missing fileName field."""
        mock_response = AsyncMock()
        # Provide incomplete data without fileName field - will be skipped by validation
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/"},  # Invalid - too short
                        # Missing fileName field - will be skipped by validation
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()
        with caplog.at_level("INFO"):
            sites = []
            async for site in plugin.get_sites():
                sites.append(site)

        # Should skip site due to missing fileName field
        assert len(sites) == 0
        # Should log info about filename validation failure
        assert (
            "filename does not follow standard format" in caplog.text or "Error grouping ICOS site data" in caplog.text
        )

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    async def test_async_get_sites_missing_citation(self, mock_request, caplog):
        """Test that sites without citations are skipped with a helpful warning."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Tst"},
                        "stationName": {"value": "Test Site"},
                        "fileName": {"value": "FLX_DE-Tst_FLUXNET_2020-2021_v1_r0.zip"},
                        "lat": {"value": "50.5"},
                        "lon": {"value": "12.18"},
                        "timeStart": {"value": "2020-01-01"},
                        "timeEnd": {"value": "2021-12-31"},
                        # Missing citationString - should trigger skip
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        plugin = ICOSPlugin()
        # Capture logs from the ICOS plugin logger
        with caplog.at_level("WARNING", logger="fluxnet_shuttle.plugins.icos"):
            sites = []
            async for site in plugin.get_sites():
                sites.append(site)

        # Should skip the site
        assert len(sites) == 0
        # Should log warning about missing citation (lines 271-276)
        assert "Skipping site DE-Tst - no citation available" in caplog.text
        assert "support@fluxnet.org" in caplog.text

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.icos.DataHubPlugin._session_request")
    @patch("fluxnet_shuttle.plugins.icos.FluxnetDatasetMetadata")
    async def test_async_get_sites_generic_exception_handling(self, mock_metadata_class, mock_request, caplog):
        """Test generic exception handling in _parse_sparql_response when FluxnetDatasetMetadata creation fails."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "dobj": {"value": "https://meta.icos-cp.eu/objects/test123"},
                        "station": {"value": "https://meta.icos-cp.eu/stations/DE-Tst"},
                        "stationName": {"value": "Test Site"},
                        "fileName": {"value": "FLX_DE-Tst_FLUXNET_2020-2021_v1_r0.zip"},
                        "lat": {"value": "50.5"},
                        "lon": {"value": "12.18"},
                        "timeStart": {"value": "2020-01-01"},
                        "timeEnd": {"value": "2021-12-31"},
                        "citationString": {"value": "Test citation for DE-Tst"},
                    }
                ]
            }
        }
        mock_request.return_value.__aenter__.return_value = mock_response

        # Make FluxnetDatasetMetadata raise an unexpected exception
        mock_metadata_class.side_effect = RuntimeError("Unexpected error during metadata creation")

        plugin = ICOSPlugin()
        with caplog.at_level("WARNING"):
            sites = []
            async for site in plugin.get_sites():
                sites.append(site)

        # Should catch the exception and continue
        assert len(sites) == 0
        # Should log warning about parsing error
        assert "Error parsing ICOS site data" in caplog.text
        assert "Unexpected error during metadata creation" in caplog.text
