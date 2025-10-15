"""Test suite for fluxnet_shuttle_lib.sources.ameriflux module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fluxnet_shuttle_lib.core.exceptions import PluginError
from fluxnet_shuttle_lib.plugins import ameriflux


class TestAmeriFluxPlugin:
    """Test cases for AmeriFluxPlugin."""

    def test_plugin_properties(self):
        """Test AmeriFluxPlugin properties."""
        plugin = ameriflux.AmeriFluxPlugin()

        assert plugin.name == "ameriflux"
        assert plugin.display_name == "AmeriFlux"

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_success(self, mock_get_links, mock_get_sites):
        """Test successful retrieval of sites."""
        mock_get_sites.return_value = ["AR-Bal", "AR-CCa"]
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
        assert sites[0].site_info.location_lat == 0.0  # Placeholder value
        assert sites[0].site_info.location_long == 0.0  # Placeholder value
        assert sites[0].site_info.igbp == "UNK"  # Placeholder value

        assert sites[1].site_info.site_id == "AR-CCa"
        assert sites[1].product_data.first_year == 2012
        assert sites[1].product_data.last_year == 2020
        assert (
            str(sites[1].product_data.download_link)
            == "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/AMF_AR-CCa_FLUXNET_FULLSET_2012-2020_3-7.zip"
        )
        assert sites[1].site_info.network == "AmeriFlux"
        assert sites[1].site_info.location_lat == 0.0  # Placeholder value
        assert sites[1].site_info.location_long == 0.0  # Placeholder value
        assert sites[1].site_info.igbp == "UNK"  # Placeholder value

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_with_unexpected_filename_format(self, mock_get_links, mock_get_sites):
        """Test get_sites handles unexpected filename format gracefully."""
        mock_get_sites.return_value = ["US-XYZ"]
        mock_get_links.return_value = {
            "data_urls": [
                {
                    "site_id": "US-XYZ",
                    "url": "http://example.com/US-XYZ_invalidformat.zip",
                }
            ]
        }  # Filename does not follow expected pattern

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-XYZ"
        assert sites[0].product_data.first_year == 2000  # Default fallback year
        assert sites[0].product_data.last_year == 2020  # Default fallback year
        assert str(sites[0].product_data.download_link) == "http://example.com/US-XYZ_invalidformat.zip"
        assert sites[0].site_info.network == "AmeriFlux"
        assert sites[0].site_info.location_lat == 0.0  # Placeholder value
        assert sites[0].site_info.location_long == 0.0  # Placeholder value
        assert sites[0].site_info.igbp == "UNK"  # Placeholder value

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    def test_get_sites_no_sites_found(self, mock_get_sites):
        """Test get_sites returns empty when no sites found."""
        mock_get_sites.return_value = []

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No sites should be returned

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_partial_failure(self, mock_get_links, mock_get_sites):
        """Test get_sites handles partial failures in download link retrieval."""
        mock_get_sites.return_value = ["US-ABC", "US-DEF"]
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
        assert sites[0].site_info.location_lat == 0.0  # Placeholder value
        assert sites[0].site_info.location_long == 0.0  # Placeholder value
        assert sites[0].site_info.igbp == "UNK"  # Placeholder value
        assert sites[0].product_data.first_year == 2005  # Extracted from filename
        assert sites[0].product_data.last_year == 2012  # Extracted from filename

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_api_failure(self, mock_get_links, mock_get_sites):
        """Test get_sites handles API failure gracefully."""
        mock_get_sites.side_effect = Exception("API failure")

        plugin = ameriflux.AmeriFluxPlugin()
        with pytest.raises(Exception) as excinfo:
            list(plugin.get_sites())

        assert "API failure" in str(excinfo.value)

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_download_links_failure(self, mock_get_links):
        """Test get_sites handles download links API failure gracefully."""
        mock_get_links.side_effect = Exception("Download links API failure")

        plugin = ameriflux.AmeriFluxPlugin()
        with patch.object(plugin, "_get_fluxnet_sites", return_value=["US-ABC"]):
            with pytest.raises(Exception) as excinfo:
                list(plugin.get_sites())

        assert "Download links API failure" in str(excinfo.value)

    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.AmeriFluxPlugin._get_download_links")
    def test_get_sites_with_no_download_links(self, mock_get_links, mock_get_sites):
        """Test get_sites handles missing download links gracefully."""
        mock_get_sites.return_value = ["US-XYZ"]
        # No download links available
        mock_get_links.return_value = {}

        plugin = ameriflux.AmeriFluxPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0  # No valid sites should be returned due to malformed data

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.NetworkPlugin._session_request")
    async def test__get_fluxnet_sites(self, mock_request):
        """Test get_sites handles _session_request correctly."""
        mock_response = AsyncMock()
        mock_response.json.return_value = [["US-TEST", "Test Site"], ["US-EXM", "Example Site"]]
        mock_response.raise_for_status.side_effect = None
        mock_request.return_value.__aenter__.return_value = mock_response

        sites = await ameriflux.AmeriFluxPlugin()._get_fluxnet_sites(api_url="http://example.com", timeout=10)
        assert sites == ["US-TEST", "US-EXM"]

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle_lib.plugins.ameriflux.NetworkPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_fluxnet_sites_failure(self, mock_request):
        """Test _get_fluxnet_sites handles _session_request failure gracefully."""

        sites = await ameriflux.AmeriFluxPlugin()._get_fluxnet_sites(api_url="http://example.com", timeout=10)
        assert sites is None  # Should return None on failure

        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch(
        "fluxnet_shuttle_lib.plugins.ameriflux.NetworkPlugin._session_request",
        side_effect=PluginError("ameriflux", "Test error"),
    )
    async def test__get_download_links_with_failure(self, mock_request):
        """Test _get_download_links handles aiohttp.ClientSession failure gracefully."""

        links = await ameriflux.AmeriFluxPlugin()._get_download_links(
            base_url="http://example.com", site_ids=["US-TEST"], timeout=10
        )
        assert links is None  # Should return None on failure
        assert mock_request.call_count == 1  # Ensure the request was attempted

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.plugins.ameriflux.NetworkPlugin._session_request")
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
        """Test _parse_response method."""
        sample_response = {
            "data_urls": [
                {"site_id": "US-TEST", "url": "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"},
                {
                    "site_id": "",
                },  # Invalid format
            ]
        }

        plugin = ameriflux.AmeriFluxPlugin()
        results = list(plugin._parse_response(sample_response))

        assert len(results) == 1  # Only one valid entry
        assert results[0].site_info.site_id == "US-TEST"
        assert results[0].product_data.first_year == 2005
        assert results[0].product_data.last_year == 2012
        assert (
            str(results[0].product_data.download_link)
            == "http://example.com/US-TEST__FLUXNET_FULLSET_2005-2012_3-7.zip"
        )
        assert results[0].site_info.network == "AmeriFlux"
        assert results[0].site_info.location_lat == 0.0  # Placeholder value
        assert results[0].site_info.location_long == 0.0  # Placeholder value
        assert results[0].site_info.igbp == "UNK"  # Placeholder value
