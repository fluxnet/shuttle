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
    def test_get_sites_success(self, mock_get_links, mock_get_metadata):
        """Test successful retrieval of sites."""
        mock_get_metadata.return_value = {
            "AR-Bal": {
                "grp_location": {"location_lat": "-37.7596", "location_long": "-58.3024"},
                "grp_igbp": {"igbp": "CRO"},
                "grp_publish_fluxnet": [2012, 2013],
            },
            "AR-CCa": {
                "grp_location": {"location_lat": "-31.4821", "location_long": "-63.6458"},
                "grp_igbp": {"igbp": "GRA"},
                "grp_publish_fluxnet": [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020],
            },
        }
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "AR-Bal",
                    "url": "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/"
                    "AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip",
                },
                {
                    "site_id": "AR-CCa",
                    "url": "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/"
                    "AMF_AR-CCa_FLUXNET_FULLSET_2012-2020_3-7.zip",
                },
            ]
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 2
        assert sites[0].site_info.site_id == "AR-Bal"
        assert sites[0].product_data.first_year == 2012
        assert sites[0].product_data.last_year == 2013
        assert (
            str(sites[0].product_data.download_link)
            == "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip"
        )
        assert sites[0].site_info.network == "AmeriFlux"
        assert sites[0].site_info.location_lat == -37.7596  # Real value from metadata
        assert sites[0].site_info.location_long == -58.3024  # Real value from metadata
        assert sites[0].site_info.igbp == "CRO"  # Real value from metadata

        assert sites[1].site_info.site_id == "AR-CCa"
        assert sites[1].product_data.first_year == 2012
        assert sites[1].product_data.last_year == 2020
        assert (
            str(sites[1].product_data.download_link)
            == "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/AMF_AR-CCa_FLUXNET_FULLSET_2012-2020_3-7.zip"
        )
        assert sites[1].site_info.network == "AmeriFlux"
        assert sites[1].site_info.location_lat == -31.4821  # Real value from metadata
        assert sites[1].site_info.location_long == -63.6458  # Real value from metadata
        assert sites[1].site_info.igbp == "GRA"  # Real value from metadata

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_with_unexpected_filename_format(self, mock_get_links, mock_get_metadata):
        """Test get_sites uses publish_years from grp_publish_fluxnet instead of parsing filename."""
        mock_get_metadata.return_value = {
            "US-XYZ": {
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

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-XYZ"
        assert sites[0].product_data.first_year == 2005  # From publish_years
        assert sites[0].product_data.last_year == 2007  # From publish_years
        assert str(sites[0].product_data.download_link) == "http://example.com/US-XYZ_invalidformat.zip"
        assert sites[0].site_info.network == "AmeriFlux"
        assert sites[0].site_info.location_lat == 45.0  # From metadata
        assert sites[0].site_info.location_long == -90.0  # From metadata
        assert sites[0].site_info.igbp == "DBF"  # From metadata

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    def test_get_sites_no_sites_found(self, mock_get_metadata):
        """Test get_sites returns empty when no sites found."""
        mock_get_metadata.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No sites should be returned

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_partial_failure(self, mock_get_links, mock_get_metadata):
        """Test get_sites handles partial failures in download link retrieval."""
        mock_get_metadata.return_value = {
            "US-ABC": {
                "grp_location": {"location_lat": "40.0", "location_long": "-100.0"},
                "grp_igbp": {"igbp": "GRA"},
                "grp_publish_fluxnet": [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012],
            },
            "US-DEF": {
                "grp_location": {"location_lat": "41.0", "location_long": "-101.0"},
                "grp_igbp": {"igbp": "CRO"},
                "grp_publish_fluxnet": [2010, 2011],
            },
        }
        mock_get_links.return_value = {
            "data_urls": [
                {"site_id": "US-ABC", "url": "http://example.com/US-ABC__FLUXNET_FULLSET_2005-2012_3-7.zip"},
                # Missing entry for US-DEF to simulate failure
            ]
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-ABC"
        assert (
            str(sites[0].product_data.download_link) == "http://example.com/US-ABC__FLUXNET_FULLSET_2005-2012_3-7.zip"
        )
        assert sites[0].site_info.network == "AmeriFlux"
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
    def test_get_sites_download_links_failure(self, mock_get_links, mock_get_metadata):
        """Test get_sites handles download links API failure gracefully."""
        mock_get_metadata.return_value = {"US-ABC": {"grp_publish_fluxnet": [2005]}}
        mock_get_links.side_effect = Exception("Download links API failure")

        plugin = ameriflux.AmeriFluxPlugin()
        with pytest.raises(Exception) as excinfo:
            list(plugin.get_sites())

        assert "Download links API failure" in str(excinfo.value)

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_with_no_download_links(self, mock_get_links, mock_get_metadata):
        """Test get_sites handles missing download links gracefully."""
        mock_get_metadata.return_value = {"US-XYZ": {"grp_publish_fluxnet": [2005]}}
        # No download links available
        mock_get_links.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No valid sites should be returned due to malformed data

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.NetworkPlugin._session_request")
    async def test__get_site_metadata(self, mock_request):
        """Test _get_site_metadata handles _session_request correctly."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "values": [
                {
                    "site_id": "US-TEST",
                    "grp_location": {"location_lat": "40.0", "location_long": "-105.0"},
                    "grp_igbp": {"igbp": "ENF"},
                },
                {
                    "site_id": "US-EXM",
                    "grp_location": {"location_lat": "41.0", "location_long": "-106.0"},
                    "grp_igbp": {"igbp": "DBF"},
                },
            ]
        }
        mock_response.raise_for_status.side_effect = None
        mock_request.return_value.__aenter__.return_value = mock_response

        metadata = await ameriflux.AmeriFluxPlugin()._get_site_metadata(api_url="http://example.com", timeout=10)
        assert "US-TEST" in metadata
        assert "US-EXM" in metadata
        assert metadata["US-TEST"]["grp_location"]["location_lat"] == "40.0"

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.NetworkPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_site_metadata_failure(self, mock_request):
        """Test _get_site_metadata raises PluginError on failure."""
        with pytest.raises(PluginError) as exc_info:
            await ameriflux.AmeriFluxPlugin()._get_site_metadata(api_url="http://example.com", timeout=10)

        assert "ameriflux" in str(exc_info.value).lower()
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle.plugins.ameriflux.NetworkPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_download_links_with_failure(self, mock_request):
        """Test _get_download_links raises PluginError on failure."""
        with pytest.raises(PluginError) as exc_info:
            await ameriflux.AmeriFluxPlugin()._get_download_links(
                base_url="http://example.com", site_ids=["US-TEST"], timeout=10
            )

        assert "ameriflux" in str(exc_info.value).lower()
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.NetworkPlugin._session_request")
    async def test__get_download_links_success(self, mock_request):
        """Test _get_download_links returns expected data on success."""
        mock_response = MagicMock()
        mock_request.return_value.__aenter__.return_value = mock_response
        mock_response.raise_for_status.side_effect = None

        async def mock_json():
            return {
                "data_urls": [
                    {"site_id": "US-TEST", "url": "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"}
                ]
            }

        mock_response.json = mock_json

        links = await ameriflux.AmeriFluxPlugin()._get_download_links(
            base_url="http://example.com", site_ids=["US-TEST"], timeout=10
        )
        assert links == {
            "data_urls": [
                {"site_id": "US-TEST", "url": "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"}
            ]
        }

    def test_parse_response_with_invalid_format(self):
        """Test _parse_response method skips invalid entries and continues processing."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-TEST", "url": "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"},
                {
                    "site_id": "",
                },  # Invalid format - missing 'url' key (should be skipped)
            ]
        }
        site_metadata = {
            "US-TEST": {
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
            }
        }
        sites_with_data = {"US-TEST": [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012]}

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, sites_with_data))

        # Should only return valid entries, skipping the invalid one
        assert len(results) == 1
        assert results[0].site_info.site_id == "US-TEST"
        assert results[0].product_data.first_year == 2005
        assert results[0].product_data.last_year == 2012
        assert (
            str(results[0].product_data.download_link)
            == "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"
        )
        assert results[0].site_info.network == "AmeriFlux"

    def test_parse_response_filters_sites_without_publish_years(self):
        """Test _parse_response filters out sites with no publish years."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-TEST", "url": "http://example.com/US-TEST.zip"},
                {"site_id": "US-NODATA", "url": "http://example.com/US-NODATA.zip"},
            ]
        }
        site_metadata = {
            "US-TEST": {
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
            },
            "US-NODATA": {
                "grp_location": {"location_lat": "41.0", "location_long": "-106.0"},
                "grp_igbp": {"igbp": "GRA"},
            },
        }
        sites_with_data = {
            "US-TEST": [2005, 2006, 2007],
            # US-NODATA has no entry (filtered out during availability check)
        }

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, sites_with_data))

        assert len(results) == 1  # Only US-TEST should be included
        assert results[0].site_info.site_id == "US-TEST"

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    def test_get_sites_no_data_availability(self, mock_get_metadata):
        """Test get_sites returns empty when site has no grp_publish_fluxnet."""
        mock_get_metadata.return_value = {"US-TEST": {}}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    def test_get_sites_all_sites_have_empty_publish_years(self, mock_get_metadata):
        """Test get_sites returns empty when all sites have empty grp_publish_fluxnet."""
        mock_get_metadata.return_value = {
            "US-TEST": {"grp_publish_fluxnet": []},
            "US-EXM": {"grp_publish_fluxnet": []},
        }

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0

    def test_parse_response_with_invalid_lat_lon(self):
        """Test _parse_response handles invalid lat/lon values gracefully."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-TEST", "url": "http://example.com/US-TEST.zip"},
            ]
        }
        site_metadata = {
            "US-TEST": {
                "grp_location": {"location_lat": "invalid", "location_long": "also_invalid"},
                "grp_igbp": {"igbp": "ENF"},
            }
        }
        sites_with_data = {"US-TEST": [2005, 2006]}

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response, site_metadata, sites_with_data))

        assert len(results) == 1
        assert results[0].site_info.site_id == "US-TEST"
        assert results[0].site_info.location_lat == 0.0  # Fallback value
        assert results[0].site_info.location_long == 0.0  # Fallback value
        assert results[0].site_info.igbp == "ENF"

    def test_build_product_data_with_empty_publish_years(self):
        """Test _build_product_data raises ValueError when publish_years is empty."""
        plugin = ameriflux.AmeriFluxPlugin()

        with pytest.raises(ValueError) as exc_info:
            plugin._build_product_data([], "http://example.com/test.zip")

        assert "publish_years cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_site_metadata")
    @patch("fluxnet_shuttle.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    async def test_get_sites_reraises_plugin_error(self, mock_get_links, mock_get_metadata):
        """Test get_sites re-raises PluginError from processing."""
        mock_get_metadata.return_value = {
            "US-TEST": {
                "grp_location": {"location_lat": "40.5", "location_long": "-105.5"},
                "grp_igbp": {"igbp": "ENF"},
                "grp_publish_fluxnet": [2020, 2021],
            }
        }
        mock_get_links.return_value = {"data_urls": [{"site_id": "US-TEST", "url": "http://example.com/test.zip"}]}

        plugin = ameriflux.AmeriFluxPlugin()

        # Mock _parse_response to raise a PluginError
        with patch.object(plugin, "_parse_response", side_effect=PluginError("ameriflux", "Test error")):
            with pytest.raises(PluginError) as exc_info:
                async for _ in plugin.get_sites():
                    pass

            assert "ameriflux" in str(exc_info.value).lower()
