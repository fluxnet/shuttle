"""Test suite for fluxnet_shuttle.sources.ameriflux module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fluxnet_shuttle.core.exceptions import PluginError
from fluxnet_shuttle.plugins import ameriflux


class TestAmeriFluxPlugin:
    """Test cases for AmeriFluxPlugin."""

    def test_plugin_properties(self):
        """Test AmeriFluxPlugin properties."""
        plugin = ameriflux.AmeriFluxPlugin()

        assert plugin.name == "ameriflux"
        assert plugin.display_name == "AmeriFlux"

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_success(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test successful retrieval of sites."""
        mock_get_metadata.return_value = {
            "AR-Bal": {
                "site_name": "Balcarce BA",
                "grp_location": {"location_lat": "-37.7596", "location_long": "-58.3024"},
                "grp_igbp": {"igbp": "CRO"},
                "grp_publish_fluxnet": [2012, 2013],
                "doi": {"AmeriFlux": "10.17190/AMF/2315764", "FLUXNET": "10.17190/AMF/2571144"},
            },
            "AR-CCa": {
                "site_name": "Carmen del Aza",
                "grp_location": {"location_lat": "-31.4821", "location_long": "-63.6458"},
                "grp_igbp": {"igbp": "GRA"},
                "grp_publish_fluxnet": [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020],
                "doi": {"AmeriFlux": "10.17190/AMF/1880910", "FLUXNET": "10.17190/AMF/2571134"},
            },
        }
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "AR-Bal",
                    "url": "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/"
                    "AMF_AR-Bal_FLUXNET_2012-2013_v3_r7.zip",
                },
                {
                    "site_id": "AR-CCa",
                    "url": "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/"
                    "AMF_AR-CCa_FLUXNET_2012-2020_v3_r7.zip",
                },
            ]
        }
        mock_get_citations.return_value = {
            "AR-Bal": "Citation for AR-Bal site",
            "AR-CCa": "Citation for AR-CCa site",
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 2
        assert sites[0].site_info.site_id == "AR-Bal"
        assert sites[0].site_info.site_name == "Balcarce BA"
        assert sites[0].product_data.first_year == 2012
        assert sites[0].product_data.last_year == 2013
        assert (
            str(sites[0].product_data.download_link)
            == "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/AMF_AR-Bal_FLUXNET_2012-2013_v3_r7.zip"
        )
        assert sites[0].site_info.data_hub == "AmeriFlux"
        assert sites[0].site_info.location_lat == -37.7596  # Real value from metadata
        assert sites[0].site_info.location_long == -58.3024  # Real value from metadata
        assert sites[0].site_info.igbp == "CRO"  # Real value from metadata
        assert sites[0].product_data.product_id == "10.17190/AMF/2571144"  # FLUXNET DOI

        assert sites[1].site_info.site_id == "AR-CCa"
        assert sites[1].site_info.site_name == "Carmen del Aza"
        assert sites[1].product_data.first_year == 2012
        assert sites[1].product_data.last_year == 2020
        assert (
            str(sites[1].product_data.download_link)
            == "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/AMF_AR-CCa_FLUXNET_2012-2020_v3_r7.zip"
        )
        assert sites[1].site_info.data_hub == "AmeriFlux"
        assert sites[1].site_info.location_lat == -31.4821  # Real value from metadata
        assert sites[1].site_info.location_long == -63.6458  # Real value from metadata
        assert sites[1].site_info.igbp == "GRA"  # Real value from metadata
        assert sites[1].product_data.product_id == "10.17190/AMF/2571134"  # FLUXNET DOI

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_unexpected_filename_format(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites skips files with invalid filename format."""
        mock_get_metadata.return_value = {
            "US-XYZ": {
                "site_name": "Test Site XYZ",
                "grp_location": {"location_lat": "45.0", "location_long": "-90.0"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2005, 2006, 2007],
            }
        }
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "US-XYZ",
                    "url": "http://example.com/US-XYZ_invalidformat.zip",
                }
            ]
        }
        mock_get_citations.return_value = {"US-XYZ": "Citation for US-XYZ"}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        # Sites with invalid filename format should be skipped
        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    def test_get_sites_no_sites_found(self, mock_get_metadata):
        """Test get_sites returns empty when no sites found."""
        mock_get_metadata.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No sites should be returned

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_partial_failure(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites handles partial failures in download link retrieval."""
        mock_get_metadata.return_value = {
            "US-ABC": {
                "site_name": "Test Site ABC",
                "grp_location": {"location_lat": "40.0", "location_long": "-100.0"},
                "grp_igbp": {"igbp": "GRA"},
                "grp_publish_fluxnet": [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012],
            },
            "US-DEF": {
                "site_name": "Test Site DEF",
                "grp_location": {"location_lat": "41.0", "location_long": "-101.0"},
                "grp_igbp": {"igbp": "CRO"},
                "grp_publish_fluxnet": [2010, 2011],
            },
        }
        mock_get_links.return_value = {
            "data_urls": [
                {"site_id": "US-ABC", "url": "http://example.com/AMF_US-ABC_FLUXNET_2005-2012_v3_r7.zip"},
                # Missing entry for US-DEF to simulate failure
            ]
        }
        mock_get_citations.return_value = {"US-ABC": "Citation for US-ABC"}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-ABC"
        assert str(sites[0].product_data.download_link) == "http://example.com/AMF_US-ABC_FLUXNET_2005-2012_v3_r7.zip"
        assert sites[0].site_info.data_hub == "AmeriFlux"
        assert sites[0].site_info.location_lat == 40.0  # From metadata
        assert sites[0].site_info.location_long == -100.0  # From metadata
        assert sites[0].site_info.igbp == "GRA"  # From metadata
        assert sites[0].product_data.first_year == 2005  # From publish_years
        assert sites[0].product_data.last_year == 2012  # From publish_years

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_api_failure(self, mock_get_links, mock_get_metadata):
        """Test get_sites handles API failure gracefully."""
        mock_get_metadata.side_effect = Exception("API failure")

        plugin = ameriflux.AmeriFluxPlugin()
        with pytest.raises(Exception) as excinfo:
            list(plugin.get_sites())

        assert "API failure" in str(excinfo.value)

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_download_links_failure(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites handles download links API failure gracefully."""
        mock_get_metadata.return_value = {"US-ABC": {"grp_publish_fluxnet": [2005]}}
        mock_get_links.side_effect = Exception("Download links API failure")

        plugin = ameriflux.AmeriFluxPlugin()
        with pytest.raises(Exception) as excinfo:
            list(plugin.get_sites())

        assert "Download links API failure" in str(excinfo.value)

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_no_download_links(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites handles missing download links gracefully."""
        mock_get_metadata.return_value = {"US-XYZ": {"grp_publish_fluxnet": [2005]}}
        # No download links available
        mock_get_links.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No valid sites should be returned due to malformed data

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request")
    async def test__get_site_metadata(self, mock_request):
        """Test _get_site_metadata handles _session_request correctly."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "values": [
                {
                    "site_id": "US-Tst",
                    "site_name": "Test Site",
                    "grp_location": {"location_lat": "40.0", "location_long": "-105.0"},
                    "grp_igbp": {"igbp": "ENF"},
                    "grp_publish_fluxnet": [2005, 2006, 2007],
                },
                {
                    "site_id": "US-EXM",
                    "site_name": "Example Site",
                    "grp_location": {"location_lat": "41.0", "location_long": "-106.0"},
                    "grp_igbp": {"igbp": "DBF"},
                    "grp_publish_fluxnet": [2010, 2011, 2012],
                },
            ]
        }
        mock_response.raise_for_status.side_effect = None
        mock_request.return_value.__aenter__.return_value = mock_response

        metadata = await ameriflux.AmeriFluxPlugin()._get_site_metadata(api_url="http://example.com")
        assert "US-Tst" in metadata
        assert "US-EXM" in metadata
        assert metadata["US-Tst"]["grp_location"]["location_lat"] == "40.0"

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_site_metadata_failure(self, mock_request):
        """Test _get_site_metadata raises PluginError on failure."""
        with pytest.raises(PluginError) as exc_info:
            await ameriflux.AmeriFluxPlugin()._get_site_metadata(api_url="http://example.com")

        assert "ameriflux" in str(exc_info.value).lower()
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_download_links_with_failure(self, mock_request):
        """Test _get_download_links raises PluginError on failure."""
        with pytest.raises(PluginError) as exc_info:
            await ameriflux.AmeriFluxPlugin()._get_download_links(base_url="http://example.com", site_ids=["US-Tst"])

        assert "ameriflux" in str(exc_info.value).lower()
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request")
    async def test__get_download_links_success(self, mock_request):
        """Test _get_download_links returns expected data on success."""
        mock_response = MagicMock()
        mock_request.return_value.__aenter__.return_value = mock_response
        mock_response.raise_for_status.side_effect = None

        async def mock_json():
            return {
                "data_urls": [{"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2005-2012_v3_r7.zip"}]
            }

        mock_response.json = mock_json

        links = await ameriflux.AmeriFluxPlugin()._get_download_links(
            base_url="http://example.com", site_ids=["US-Tst"]
        )
        assert links == {
            "data_urls": [{"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2005-2012_v3_r7.zip"}]
        }

    def test_parse_response_with_invalid_format(self):
        """Test _parse_response method skips invalid entries and continues processing."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2005-2012_v3_r7.zip"},
                {
                    "site_id": "",
                },  # Invalid format - missing 'url' key (should be skipped)
            ]
        }
        site_metadata = {
            "US-Tst": {
                "site_name": "Test Site",
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012],
            }
        }
        citations = {"US-Tst": "Citation for US-Tst"}

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, citations))

        # Should only return valid entries, skipping the invalid one
        assert len(results) == 1
        assert results[0].site_info.site_id == "US-Tst"
        assert results[0].product_data.first_year == 2005
        assert results[0].product_data.last_year == 2012
        assert str(results[0].product_data.download_link) == "http://example.com/AMF_US-Tst_FLUXNET_2005-2012_v3_r7.zip"
        assert results[0].site_info.data_hub == "AmeriFlux"

    def test_parse_response_filters_sites_without_publish_years(self):
        """Test _parse_response filters out sites with no publish years and invalid filenames."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2005-2007_v1_r0.zip"},
                {"site_id": "US-NODATA", "url": "http://example.com/AMF_US-NODATA_FLUXNET_2010-2015_v1_r0.zip"},
            ]
        }
        site_metadata = {
            "US-Tst": {
                "site_name": "Test Site",
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2005, 2006, 2007],
            },
            "US-NODATA": {
                "site_name": "No Data Site",
                "grp_location": {"location_lat": "41.0", "location_long": "-106.0"},
                "grp_igbp": {"igbp": "GRA"},
                # Missing grp_publish_fluxnet - should be filtered out
            },
        }
        citations = {"US-Tst": "Citation for US-Tst"}

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, citations))

        assert len(results) == 1  # Only US-Tst should be included
        assert results[0].site_info.site_id == "US-Tst"

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_no_data_availability(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites returns empty when site has no grp_publish_fluxnet."""
        mock_get_metadata.return_value = {"US-Tst": {}}
        mock_get_links.return_value = {"data_urls": []}
        mock_get_citations.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_all_sites_have_empty_publish_years(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites returns empty when all sites have empty grp_publish_fluxnet."""
        mock_get_metadata.return_value = {
            "US-Tst": {"grp_publish_fluxnet": []},
            "US-EXM": {"grp_publish_fluxnet": []},
        }
        mock_get_links.return_value = {"data_urls": []}
        mock_get_citations.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0

    def test_parse_response_with_invalid_lat_lon(self):
        """Test _parse_response handles invalid lat/lon values gracefully."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2005-2006_v1_r0.zip"},
            ]
        }
        site_metadata = {
            "US-Tst": {
                "site_name": "Test Site",
                "grp_location": {"location_lat": "invalid", "location_long": "also_invalid"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2005, 2006],
            }
        }
        citations = {"US-Tst": "Citation for US-Tst"}

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, citations))

        assert len(results) == 1
        assert results[0].site_info.site_id == "US-Tst"
        assert results[0].site_info.location_lat == 0.0  # Fallback value
        assert results[0].site_info.location_long == 0.0  # Fallback value
        assert results[0].site_info.igbp == "ENF"

    def test_build_product_data_with_empty_publish_years(self):
        """Test _build_product_data raises ValueError when publish_years is empty."""
        plugin = ameriflux.AmeriFluxPlugin()

        with pytest.raises(ValueError) as exc_info:
            plugin._build_product_data(
                [],
                "http://example.com/test.zip",
                product_id="test-id",
                citation="test citation",
                oneflux_code_version="v1",
                product_source_network="AMF",
                fluxnet_product_name="test.zip",
            )

        assert "publish_years cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    async def test_get_sites_reraises_plugin_error(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test get_sites re-raises PluginError from processing."""
        mock_get_metadata.return_value = {
            "US-Tst": {
                "site_name": "Test Site",
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2020, 2021],
            }
        }
        mock_get_links.return_value = {"data_urls": [{"site_id": "US-Tst", "url": "http://example.com/test.zip"}]}
        mock_get_citations.return_value = {"US-Tst": "Citation for US-Tst"}

        plugin = ameriflux.AmeriFluxPlugin()

        # Mock _parse_response to raise a PluginError
        with patch.object(plugin, "_parse_response", side_effect=PluginError("ameriflux", "Test error")):
            with pytest.raises(PluginError) as exc_info:
                async for _ in plugin.get_sites():
                    pass

            assert "ameriflux" in str(exc_info.value).lower()

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_doi(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test that DOI is properly extracted from site metadata."""
        mock_get_metadata.return_value = {
            "US-Ts1": {
                "site_name": "Test Site 1",
                "grp_location": {"location_lat": "45.0", "location_long": "-90.0"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2020, 2021],
                "doi": {"AmeriFlux": "10.17190/AMF/1234567", "FLUXNET": "10.17190/AMF/7654321"},
            },
            "US-Ts2": {
                "site_name": "Test Site 2",
                "grp_location": {"location_lat": "40.0", "location_long": "-95.0"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2020, 2021],
                "doi": {},  # Empty DOI
            },
            "US-Ts3": {
                "site_name": "Test Site 3",
                "grp_location": {"location_lat": "35.0", "location_long": "-85.0"},
                "grp_igbp": {"igbp": "GRA"},
                "grp_publish_fluxnet": [2020, 2021],
                # No DOI field at all
            },
        }
        mock_get_links.return_value = {
            "data_urls": [
                {"site_id": "US-Ts1", "url": "http://example.com/AMF_US-Ts1_FLUXNET_2010-2012_v1_r0.zip"},
                {"site_id": "US-Ts2", "url": "http://example.com/AMF_US-Ts2_FLUXNET_2020-2021_v1_r0.zip"},
                {"site_id": "US-Ts3", "url": "http://example.com/AMF_US-Ts3_FLUXNET_2020-2021_v1_r0.zip"},
            ]
        }
        mock_get_citations.return_value = {
            "US-Ts1": "Citation for US-Ts1",
            "US-Ts2": "Citation for US-Ts2",
            "US-Ts3": "Citation for US-Ts3",
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 3
        # Test site with FLUXNET DOI
        assert sites[0].site_info.site_id == "US-Ts1"
        assert sites[0].product_data.product_id == "10.17190/AMF/7654321"

        # Test site with empty DOI dict
        assert sites[1].site_info.site_id == "US-Ts2"
        assert sites[1].product_data.product_id == ""

        # Test site with no DOI field
        assert sites[2].site_info.site_id == "US-Ts3"
        assert sites[2].product_data.product_id == ""

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_team_members(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test successful retrieval of sites with team member information."""
        mock_get_metadata.return_value = {
            "US-Ha1": {
                "site_name": "Harvard Forest",
                "grp_location": {"location_lat": "42.5378", "location_long": "-72.1715"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2018, 2019, 2020],
                "doi": {"FLUXNET": "10.17190/AMF/1234567"},
                "grp_team_member": [
                    {
                        "team_member_name": "John Doe",
                        "team_member_role": "PI",
                        "team_member_email": "john.doe@harvard.edu",
                    },
                    {
                        "team_member_name": "Jane Smith",
                        "team_member_role": "Researcher",
                        "team_member_email": "jane.smith@harvard.edu",
                    },
                ],
            }
        }
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "US-Ha1",
                    "url": "https://ftp.fluxdata.org/AMF_US-Ha1_FLUXNET_2018-2020_v1_r0.zip",
                }
            ]
        }
        mock_get_citations.return_value = {"US-Ha1": "Citation for US-Ha1"}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-Ha1"

        # Verify team members
        team_members = sites[0].site_info.group_team_member
        assert len(team_members) == 2

        assert team_members[0].team_member_name == "John Doe"
        assert team_members[0].team_member_role == "PI"
        assert team_members[0].team_member_email == "john.doe@harvard.edu"

        assert team_members[1].team_member_name == "Jane Smith"
        assert team_members[1].team_member_role == "Researcher"
        assert team_members[1].team_member_email == "jane.smith@harvard.edu"

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_citations(self, mock_get_citations, mock_get_links, mock_get_metadata):
        """Test successful retrieval of sites with citation information."""
        mock_get_metadata.return_value = {
            "US-Ha1": {
                "site_name": "Harvard Forest",
                "grp_location": {"location_lat": "42.5378", "location_long": "-72.1715"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2018, 2019, 2020],
                "doi": {"FLUXNET": "10.17190/AMF/1234567"},
            }
        }
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "US-Ha1",
                    "url": "https://ftp.fluxdata.org/AMF_US-Ha1_FLUXNET_2018-2020_v1_r0.zip",
                }
            ]
        }
        mock_get_citations.return_value = {
            "US-Ha1": "Munger, J.W., Wofsy, S.C. (2020). AmeriFlux US-Ha1 Harvard Forest..."
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-Ha1"
        assert (
            sites[0].product_data.product_citation
            == "Munger, J.W., Wofsy, S.C. (2020). AmeriFlux US-Ha1 Harvard Forest..."
        )

    def test_get_sites_skips_sites_without_citations(self, caplog):
        """Test that sites without citations are skipped and warning is logged."""
        import logging

        sample_response = {
            "data_urls": [
                {
                    "site_id": "US-Ha1",
                    "url": "https://ftp.fluxdata.org/AMF_US-Ha1_FLUXNET_2018-2020_v1_r0.zip",
                },
                {
                    "site_id": "US-Nc1",
                    "url": "https://ftp.fluxdata.org/AMF_US-Nc1_FLUXNET_2020-2021_v1_r0.zip",
                },
            ]
        }
        site_metadata = {
            "US-Ha1": {
                "site_name": "Harvard Forest",
                "grp_location": {"location_lat": "42.5378", "location_long": "-72.1715"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2018, 2019, 2020],
                "doi": {"FLUXNET": "10.17190/AMF/1234567"},
            },
            "US-Nc1": {
                "site_name": "Site Without Citation",
                "grp_location": {"location_lat": "45.0", "location_long": "-90.0"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2020, 2021],
            },
        }
        # Only provide citation for one site
        citations = {"US-Ha1": "Munger, J.W., Wofsy, S.C. (2020). AmeriFlux US-Ha1 Harvard Forest..."}

        plugin = ameriflux.AmeriFluxPlugin()

        # Capture logs from the ameriflux plugin logger
        with caplog.at_level(logging.WARNING, logger="fluxnet_shuttle.plugins.ameriflux"):
            # This will exercise the warning log lines 328-332
            sites = list(plugin._parse_response(sample_response, site_metadata, citations))

        # Only one site should be returned (the one with citation)
        # Site without citation should be skipped
        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-Ha1"

        # Verify the warning was logged
        assert "Skipping site US-Nc1 - no citation available" in caplog.text
        assert "ameriflux-support@lbl.gov" in caplog.text

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request")
    async def test__get_citations_success(self, mock_request):
        """Test _get_citations returns expected data on success."""
        mock_response = MagicMock()
        mock_request.return_value.__aenter__.return_value = mock_response
        mock_response.raise_for_status.side_effect = None

        async def mock_json():
            return {
                "values": [
                    {"site_id": "US-Ha1", "citation": "Citation for US-Ha1"},
                    {"site_id": "US-MMS", "citation": "Citation for US-MMS"},
                ]
            }

        mock_response.json = mock_json

        citations = await ameriflux.AmeriFluxPlugin()._get_citations(
            base_url="http://example.com/", site_ids=["US-Ha1", "US-MMS"]
        )
        assert citations == {"US-Ha1": "Citation for US-Ha1", "US-MMS": "Citation for US-MMS"}

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_citations_failure(self, mock_request):
        """Test _get_citations handles failure gracefully and returns empty dict."""
        citations = await ameriflux.AmeriFluxPlugin()._get_citations(
            base_url="http://example.com/", site_ids=["US-Ha1"]
        )

        assert citations == {}  # Should return empty dict on failure
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.DataHubPlugin._session_request",
        side_effect=Exception("Generic error"),
    )
    async def test__get_citations_generic_exception(self, mock_request):
        """Test _get_citations handles generic exceptions gracefully."""
        citations = await ameriflux.AmeriFluxPlugin()._get_citations(
            base_url="http://example.com/", site_ids=["US-Ha1"]
        )

        assert citations == {}  # Should return empty dict on generic exception
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_citations")
    def test_get_sites_with_invalid_team_member(self, mock_get_citations, mock_get_links, mock_get_metadata, caplog):
        """Test handling of invalid team member data."""
        mock_get_metadata.return_value = {
            "US-Tst": {
                "site_name": "Test Site",
                "grp_location": {"location_lat": "45.0", "location_long": "-90.0"},
                "grp_igbp": {"igbp": "DBF"},
                "grp_publish_fluxnet": [2020],
                "grp_team_member": [
                    {
                        "team_member_name": "Valid Member",
                        "team_member_role": "PI",
                    },
                    {
                        # Missing team_member_name - will cause validation error
                        "team_member_role": "Researcher",
                    },
                ],
            }
        }
        mock_get_links.return_value = {
            "data_urls": [{"site_id": "US-Tst", "url": "http://example.com/AMF_US-Tst_FLUXNET_2020-2020_v1_r0.zip"}]
        }
        mock_get_citations.return_value = {"US-Tst": "Citation for US-Tst"}

        plugin = ameriflux.AmeriFluxPlugin()
        with caplog.at_level("WARNING"):
            sites = list(plugin.get_sites())

        # Should still create site with valid team member only
        assert len(sites) == 1
        assert len(sites[0].site_info.group_team_member) == 1
        assert sites[0].site_info.group_team_member[0].team_member_name == "Valid Member"

        # Check warning was logged for invalid member
        assert "Error parsing team member for site US-Tst" in caplog.text

    def test_parse_response_logs_debug_for_no_publish_years(self, caplog):
        """Test that debug logging is triggered when publish years are missing."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-NoY", "url": "http://example.com/AMF_US-NoY_FLUXNET_2020-2021_v1_r0.zip"},
            ]
        }
        site_metadata = {
            "US-NoY": {
                "site_name": "No Years Site",
                "grp_location": {"location_lat": "40.0", "location_long": "-105.0"},
                "grp_igbp": {"igbp": "ENF"},
                # grp_publish_fluxnet is missing
            }
        }

        plugin = ameriflux.AmeriFluxPlugin()
        with caplog.at_level("DEBUG", logger="fluxnet_shuttle.plugins.ameriflux"):
            results = list(plugin._parse_response(sample_response, site_metadata, {}))

        # Should filter out the site
        assert len(results) == 0

        # Check debug message was logged
        assert "Skipping site US-NoY - no publish years available" in caplog.text

    @pytest.mark.asyncio
    async def test_log_download_request_empty_filenames(self):
        """Test _log_download_request method with empty filenames."""
        plugin = ameriflux.AmeriFluxPlugin()
        result = await plugin._log_download_request(zip_filenames=[])
        assert result is False

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.get_session")
    async def test_log_download_request_success(self, mock_get_session):
        """Test successful _log_download_request call."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="Success")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        # Mock session
        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_response)

        # Mock get_session context manager
        mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_get_session.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await plugin._log_download_request(
            zip_filenames=["file1.zip", "file2.zip"],
            user_name="Test User",
            user_email="test@example.com",
            intended_use=1,
            description="Test download",
        )

        assert result is True
        mock_session.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.get_session")
    async def test_log_download_request_http_error(self, mock_get_session):
        """Test _log_download_request with HTTP error response."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock failed response
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.text = AsyncMock(return_value="Bad request")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        # Mock session
        mock_session = MagicMock()
        mock_session.request = MagicMock(return_value=mock_response)

        # Mock get_session context manager
        mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_get_session.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await plugin._log_download_request(
            zip_filenames=["file1.zip"], user_name="Test User", user_email="test@example.com"
        )

        assert result is False

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.get_session")
    async def test_log_download_request_exception(self, mock_get_session):
        """Test _log_download_request with exception."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock exception
        mock_get_session.side_effect = Exception("Network error")

        result = await plugin._log_download_request(
            zip_filenames=["file1.zip"], user_name="Test User", user_email="test@example.com"
        )

        assert result is False

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    @patch.object(ameriflux.AmeriFluxPlugin, "_log_download_request")
    async def test_download_stream_with_user_tracking(self, mock_log_download, mock_session_request):
        """Test download_stream with user tracking parameters."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock successful logging
        mock_log_download.return_value = True

        # Mock the download response
        mock_response = AsyncMock()
        mock_response.content = b"test content"
        mock_session_request.return_value.__aenter__.return_value = mock_response
        mock_session_request.return_value.__aexit__.return_value = None

        # Call download_file with user_info structure
        async with plugin.download_file(
            site_id="US-Ha1",
            download_link="https://example.com/file.zip",
            filename="test.zip",
            user_info={
                "ameriflux": {
                    "user_name": "Test User",
                    "user_email": "test@example.com",
                    "intended_use": 1,
                    "description": "Test download",
                }
            },
        ) as content:
            assert content == b"test content"

        # Verify logging was called with correct parameters
        mock_log_download.assert_called_once_with(
            zip_filenames=["test.zip"],
            user_name="Test User",
            user_email="test@example.com",
            intended_use=1,
            description="Test download",
        )

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    @patch.object(ameriflux.AmeriFluxPlugin, "_log_download_request")
    async def test_download_stream_logging_failure(self, mock_log_download, mock_session_request, caplog):
        """Test download_stream continues even when logging fails."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock failed logging (raises exception)
        mock_log_download.side_effect = Exception("Logging failed")

        # Mock the download response
        mock_response = AsyncMock()
        mock_response.content = b"test content"
        mock_session_request.return_value.__aenter__.return_value = mock_response
        mock_session_request.return_value.__aexit__.return_value = None

        # Download should still work even if logging fails
        with caplog.at_level("WARNING", logger="fluxnet_shuttle.plugins.ameriflux"):
            async with plugin.download_file(
                site_id="US-Ha1",
                download_link="https://example.com/file.zip",
                filename="test.zip",
                user_info={
                    "ameriflux": {
                        "user_name": "Test User",
                        "user_email": "test@example.com",
                    }
                },
            ) as content:
                assert content == b"test content"

        # Should log warning about failed tracking
        assert "Failed to log download request for US-Ha1" in caplog.text

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    async def test_download_stream_without_user_tracking(self, mock_session_request):
        """Test download_stream without user tracking parameters."""
        plugin = ameriflux.AmeriFluxPlugin()

        # Mock the download response
        mock_response = AsyncMock()
        mock_response.content = b"test content"
        mock_session_request.return_value.__aenter__.return_value = mock_response
        mock_session_request.return_value.__aexit__.return_value = None

        # Call without user tracking - should not attempt logging
        async with plugin.download_file(
            site_id="US-Ha1", download_link="https://example.com/file.zip", filename="test.zip"
        ) as content:
            assert content == b"test content"


class TestIntendedUse:
    """Test cases for IntendedUse enum."""

    def test_from_code_valid_codes(self):
        """Test from_code with all valid codes."""
        from fluxnet_shuttle.plugins.ameriflux import IntendedUse

        assert IntendedUse.from_code(1) == IntendedUse.SYNTHESIS
        assert IntendedUse.from_code(2) == IntendedUse.MODEL
        assert IntendedUse.from_code(3) == IntendedUse.REMOTE_SENSING
        assert IntendedUse.from_code(4) == IntendedUse.OTHER_RESEARCH
        assert IntendedUse.from_code(5) == IntendedUse.EDUCATION
        assert IntendedUse.from_code(6) == IntendedUse.OTHER

    def test_from_code_invalid_code_returns_default(self):
        """Test from_code returns SYNTHESIS as default for invalid codes."""
        from fluxnet_shuttle.plugins.ameriflux import IntendedUse

        # Test with various invalid codes
        assert IntendedUse.from_code(999) == IntendedUse.SYNTHESIS
        assert IntendedUse.from_code(0) == IntendedUse.SYNTHESIS
        assert IntendedUse.from_code(-1) == IntendedUse.SYNTHESIS
        assert IntendedUse.from_code(100) == IntendedUse.SYNTHESIS

    def test_get_value_str(self):
        """Test get_value_str returns correct string values."""
        from fluxnet_shuttle.plugins.ameriflux import IntendedUse

        assert IntendedUse.get_value_str(1) == "synthesis"
        assert IntendedUse.get_value_str(2) == "model"
        assert IntendedUse.get_value_str(3) == "remote_sensing"
        assert IntendedUse.get_value_str(4) == "other_research"
        assert IntendedUse.get_value_str(5) == "education"
        assert IntendedUse.get_value_str(6) == "other"

    def test_get_value_str_invalid_code(self):
        """Test get_value_str returns default value for invalid codes."""
        from fluxnet_shuttle.plugins.ameriflux import IntendedUse

        # Invalid codes should default to SYNTHESIS
        assert IntendedUse.get_value_str(999) == "synthesis"
        assert IntendedUse.get_value_str(0) == "synthesis"
