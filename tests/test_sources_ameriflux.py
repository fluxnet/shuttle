"""Test suite for fluxnet_shuttle_lib.sources.ameriflux module."""

from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.sources.ameriflux import (
    AMERIFLUX_AVAILABILITY_PATH,
    AMERIFLUX_BASE_PATH,
    AMERIFLUX_BASE_URL,
    AMERIFLUX_DOWNLOAD_PATH,
    AMERIFLUX_HEADERS,
    download_ameriflux_data,
    get_ameriflux_data,
    get_ameriflux_download_links,
    get_ameriflux_fluxnet_sites,
    parse_ameriflux_response,
)


class TestAmerifluxConstants:
    """Test AmeriFlux constants."""

    def test_constants_values(self):
        """Test that constants have expected values."""
        assert AMERIFLUX_BASE_URL == "https://amfcdn.lbl.gov/"
        assert AMERIFLUX_BASE_PATH == "api/v1/"
        assert AMERIFLUX_AVAILABILITY_PATH == ("site_availability/AmeriFlux/FLUXNET/CCBY4.0")
        assert AMERIFLUX_DOWNLOAD_PATH == "data_download"

    def test_headers_structure(self):
        """Test that headers have correct structure."""
        assert isinstance(AMERIFLUX_HEADERS, dict)
        assert "accept" in AMERIFLUX_HEADERS
        assert "content-type" in AMERIFLUX_HEADERS
        assert AMERIFLUX_HEADERS["accept"] == "application/json"
        assert AMERIFLUX_HEADERS["content-type"] == "application/json"


class TestGetAmerifluxFluxnetSites:
    """Test get_ameriflux_fluxnet_sites function."""

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_successful_site_retrieval(self, mock_get):
        """Test successful retrieval of site list."""
        mock_response = MagicMock()
        mock_response.json.return_value = [["US-TEST1"], ["US-TEST2"], ["US-TEST3"]]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_ameriflux_fluxnet_sites("http://api.test/")

        assert result == ["US-TEST1", "US-TEST2", "US-TEST3"]
        mock_get.assert_called_once_with(
            url=f"http://api.test/{AMERIFLUX_AVAILABILITY_PATH}",
            headers=AMERIFLUX_HEADERS,
        )

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_request_exception(self, mock_get):
        """Test handling of request exceptions."""
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        result = get_ameriflux_fluxnet_sites("http://api.test/")

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        result = get_ameriflux_fluxnet_sites("http://api.test/")

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_custom_endpoint_and_headers(self, mock_get):
        """Test with custom endpoint and headers."""
        mock_response = MagicMock()
        mock_response.json.return_value = [["US-CUSTOM"]]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        custom_headers = {"accept": "application/xml"}
        result = get_ameriflux_fluxnet_sites("http://api.test/", "custom/endpoint", custom_headers)

        assert result == ["US-CUSTOM"]
        mock_get.assert_called_once_with(url="http://api.test/custom/endpoint", headers=custom_headers)

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_empty_response(self, mock_get):
        """Test handling of empty response."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = get_ameriflux_fluxnet_sites("http://api.test/")

        assert result == []


class TestGetAmerifluxDownloadLinks:
    """Test get_ameriflux_download_links function."""

    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    def test_successful_download_links(self, mock_post, mock_get_sites):
        """Test successful retrieval of download links."""
        mock_response = MagicMock()
        test_data = {"data_urls": [{"site_id": "US-TEST", "url": "http://download.test/file.zip"}]}
        mock_response.json.return_value = test_data
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_ameriflux_download_links("http://api.test/", ["US-TEST"])

        assert result == test_data
        mock_post.assert_called_once()
        # Verify the POST data structure
        call_args = mock_post.call_args
        assert call_args[1]["json"]["site_ids"] == ["US-TEST"]
        assert call_args[1]["json"]["user_id"] == "fluxnetshuttle"

    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    def test_no_site_ids_calls_get_sites(self, mock_post, mock_get_sites):
        """Test that None site_ids triggers get_sites call."""
        mock_get_sites.return_value = ["US-AUTO1", "US-AUTO2"]
        mock_response = MagicMock()
        mock_response.json.return_value = {"data_urls": []}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        get_ameriflux_download_links("http://api.test/", None)

        mock_get_sites.assert_called_once_with(api_url="http://api.test/")
        # Verify auto-retrieved site_ids were used
        call_args = mock_post.call_args
        assert call_args[1]["json"]["site_ids"] == ["US-AUTO1", "US-AUTO2"]

    @patch("builtins.open", mock_open())
    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    def test_output_file_saving(self, mock_post):
        """Test saving response to output file."""
        test_data = {"data_urls": []}
        mock_response = MagicMock()
        mock_response.json.return_value = test_data
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_ameriflux_download_links("http://api.test/", ["US-TEST"], output_file="test.json")

        assert result == test_data

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    def test_request_exception(self, mock_post):
        """Test handling of request exceptions."""
        mock_post.side_effect = requests.exceptions.RequestException("Connection error")

        result = get_ameriflux_download_links("http://api.test/", ["US-TEST"])

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    def test_custom_parameters(self, mock_post):
        """Test with custom endpoint and headers."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data_urls": []}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        custom_endpoint = "custom/download"
        custom_headers = {"accept": "application/xml"}

        result = get_ameriflux_download_links(
            "http://api.test/",
            ["US-TEST"],
            url_post_query_endpoint=custom_endpoint,
            headers=custom_headers,
        )

        assert result == {"data_urls": []}
        mock_post.assert_called_once()
        # Verify URL was correctly constructed
        call_kwargs = mock_post.call_args.kwargs
        assert "url" in call_kwargs or mock_post.call_args.args
        assert call_kwargs.get("headers") == custom_headers


class TestParseAmerifluxResponse:
    """Test parse_ameriflux_response function."""

    def test_single_site_parsing(self):
        """Test parsing response with single site."""
        test_data = {
            "data_urls": [
                {
                    "site_id": "US-TEST",
                    "url": "http://example.com/FLX_US-TEST_FLUXNET2015_FULLSET_" "2020-2023_1.zip",
                }
            ]
        }

        result = parse_ameriflux_response(test_data)

        expected = {
            "US-TEST": {
                "network": "AmeriFlux",
                "publisher": "AMP",
                "site_id": "US-TEST",
                "first_year": "2020",
                "last_year": "2023",
                "download_link": "http://example.com/FLX_US-TEST_FLUXNET2015_FULLSET_" "2020-2023_1.zip",
                "filename": "FLX_US-TEST_FLUXNET2015_FULLSET_2020-2023_1.zip",
                "version": "1",
            }
        }
        assert result == expected

    def test_multiple_sites_parsing(self):
        """Test parsing response with multiple sites."""
        test_data = {
            "data_urls": [
                {
                    "site_id": "US-TEST1",
                    "url": "http://example.com/FLX_US-TEST1_FLUXNET2015_FULLSET_" "2020-2023_1.zip",
                },
                {
                    "site_id": "US-TEST2",
                    "url": "http://example.com/FLX_US-TEST2_FLUXNET2015_FULLSET_" "2019-2022_2.zip",
                },
            ]
        }

        result = parse_ameriflux_response(test_data)

        assert len(result) == 2
        assert "US-TEST1" in result
        assert "US-TEST2" in result
        assert result["US-TEST1"]["first_year"] == "2020"
        assert result["US-TEST2"]["first_year"] == "2019"
        assert result["US-TEST1"]["version"] == "1"
        assert result["US-TEST2"]["version"] == "2"

    def test_empty_data_parsing(self):
        """Test parsing empty data."""
        test_data = {"data_urls": []}
        result = parse_ameriflux_response(test_data)
        assert result == {}

    def test_url_parsing_edge_cases(self):
        """Test URL parsing with different filename formats."""
        test_data = {
            "data_urls": [
                {
                    "site_id": "US-EDGE",
                    "url": "http://example.com/path/FLX_US-EDGE_FLUXNET2015_" "SUBSET_2015-2015_3.zip?param=value",
                }
            ]
        }

        result = parse_ameriflux_response(test_data)

        assert result["US-EDGE"]["filename"] == "FLX_US-EDGE_FLUXNET2015_SUBSET_2015-2015_3.zip"
        assert result["US-EDGE"]["first_year"] == "2015"
        assert result["US-EDGE"]["last_year"] == "2015"
        assert result["US-EDGE"]["version"] == "3"


class TestGetAmerifluxData:
    """Test get_ameriflux_data function."""

    @patch("fluxnet_shuttle_lib.sources.ameriflux.parse_ameriflux_response")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_download_links")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    def test_successful_data_retrieval(self, mock_get_sites, mock_get_links, mock_parse):
        """Test successful complete data retrieval workflow."""
        mock_get_sites.return_value = ["US-TEST"]
        mock_get_links.return_value = {"data_urls": []}
        mock_parse.return_value = {"US-TEST": {"site_id": "US-TEST"}}

        result = get_ameriflux_data()

        assert result == {"US-TEST": {"site_id": "US-TEST"}}
        mock_get_sites.assert_called_once()
        mock_get_links.assert_called_once()
        mock_parse.assert_called_once()

    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    def test_no_sites_found(self, mock_get_sites):
        """Test handling when no sites are found."""
        mock_get_sites.return_value = None

        result = get_ameriflux_data()

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_download_links")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    def test_no_download_links(self, mock_get_sites, mock_get_links):
        """Test handling when no download links are found."""
        mock_get_sites.return_value = ["US-TEST"]
        mock_get_links.return_value = None

        result = get_ameriflux_data()

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.ameriflux.parse_ameriflux_response")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_download_links")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.get_ameriflux_fluxnet_sites")
    def test_custom_parameters(self, mock_get_sites, mock_get_links, mock_parse):
        """Test with custom base URL and endpoint."""
        mock_get_sites.return_value = ["US-TEST"]
        mock_get_links.return_value = {"data_urls": []}
        mock_parse.return_value = {"US-TEST": {"site_id": "US-TEST"}}

        result = get_ameriflux_data(
            base_url="http://custom.api/",
            endpoint="v2/",
            ameriflux_output_filename="custom.json",
        )

        assert result == {"US-TEST": {"site_id": "US-TEST"}}
        # Verify the API URL construction
        expected_api_url = "http://custom.api/v2/"
        mock_get_sites.assert_called_once_with(api_url=expected_api_url)


class TestDownloadAmerifluxData:
    """Test download_ameriflux_data function."""

    def test_successful_download(self):
        """Test successful file download."""
        with (
            patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            download_ameriflux_data("US-TEST", "test.zip", "http://example.com/test.zip")

            mock_get.assert_called_once_with("http://example.com/test.zip", stream=True)

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_download_failure_404(self, mock_get):
        """Test handling of 404 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download AmeriFlux file.*404"):
            download_ameriflux_data("US-TEST", "test.zip", "http://example.com/test.zip")

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_download_failure_500(self, mock_get):
        """Test handling of 500 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download AmeriFlux file.*500"):
            download_ameriflux_data("US-TEST", "test.zip", "http://example.com/test.zip")

    def test_file_writing(self):
        """Test that file is written correctly."""
        with (
            patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get") as mock_get,
            patch("builtins.open", mock_open()) as mock_file,
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            test_chunks = [b"data_chunk_1", b"data_chunk_2"]
            mock_response.iter_content.return_value = test_chunks
            mock_get.return_value = mock_response

            download_ameriflux_data("US-TEST", "output.zip", "http://example.com/file.zip")

            # Verify file was opened for writing
            mock_file.assert_called_once_with("output.zip", "wb")
            # Verify all chunks were written
            handle = mock_file.return_value.__enter__.return_value
            assert handle.write.call_count == len(test_chunks)


class TestAmerifluxIntegration:
    """Integration tests for AmeriFlux module."""

    def test_constants_integration(self):
        """Test that constants work together correctly."""
        api_url = f"{AMERIFLUX_BASE_URL}{AMERIFLUX_BASE_PATH}"
        availability_url = f"{api_url}{AMERIFLUX_AVAILABILITY_PATH}"
        download_url = f"{api_url}{AMERIFLUX_DOWNLOAD_PATH}"

        assert "https://amfcdn.lbl.gov/api/v1/" in api_url
        assert "site_availability" in availability_url
        assert "data_download" in download_url

    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.post")
    @patch("fluxnet_shuttle_lib.sources.ameriflux.requests.get")
    def test_complete_workflow(self, mock_get, mock_post):
        """Test complete workflow from site discovery to data parsing."""
        # Mock site discovery
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = [["US-TEST1"], ["US-TEST2"]]
        mock_get_response.raise_for_status.return_value = None
        mock_get.return_value = mock_get_response

        # Mock download links
        mock_post_response = MagicMock()
        mock_post_response.json.return_value = {
            "data_urls": [
                {
                    "site_id": "US-TEST1",
                    "url": "http://example.com/FLX_US-TEST1_FLUXNET2015_FULLSET_" "2020-2023_1.zip",
                }
            ]
        }
        mock_post_response.raise_for_status.return_value = None
        mock_post.return_value = mock_post_response

        result = get_ameriflux_data()

        assert isinstance(result, dict)
        assert "US-TEST1" in result
        assert result["US-TEST1"]["network"] == "AmeriFlux"
        assert result["US-TEST1"]["publisher"] == "AMP"
