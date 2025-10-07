"""Test suite for fluxnet_shuttle_lib.sources.icos module."""

from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.sources.icos import (
    ICOS_API_URL,
    download_icos_data,
    get_icos_data,
)


class TestIcosConstants:
    """Test ICOS constants."""

    def test_api_url_constant(self):
        """Test that ICOS API URL has expected value."""
        assert ICOS_API_URL == "https://meta.icos-cp.eu/sparql"


class TestGetIcosData:
    """Test get_icos_data function."""

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_successful_data_retrieval(self, mock_post):
        """Test successful ICOS data retrieval."""
        mock_response = MagicMock()
        # Simulate SPARQL JSON response with correct field names
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "station": {"value": "http://meta.icos-cp.eu/resources/SITES/ES_LJU"},
                        "dobj": {"value": "http://meta.icos-cp.eu/objects/ES-LJU-data"},
                        "fileName": {"value": "FLUXNET_ES-LJU_2020-2021_v1.zip"},
                    },
                    {
                        "station": {"value": "http://meta.icos-cp.eu/resources/SITES/FI_HYY"},
                        "dobj": {"value": "http://meta.icos-cp.eu/objects/FI-HYY-data"},
                        "fileName": {"value": "FLUXNET_FI-HYY_2019-2020_v2.zip"},
                    },
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_icos_data()

        assert isinstance(result, dict)
        assert len(result) == 2
        assert "ES_LJU" in result
        assert "FI_HYY" in result

        # Check ES_LJU entry
        assert result["ES_LJU"]["network"] == "ICOS"
        assert result["ES_LJU"]["publisher"] == "ICOS-ETC"
        assert result["ES_LJU"]["site_id"] == "ES_LJU"
        assert result["ES_LJU"]["filename"] == "FLUXNET_ES-LJU_2020-2021_v1.zip"
        assert result["ES_LJU"]["download_link"] == "http://meta.icos-cp.eu/objects/ES-LJU-data"
        assert result["ES_LJU"]["first_year"] == "2020"
        assert result["ES_LJU"]["last_year"] == "2021"
        assert result["ES_LJU"]["version"] == "v1"

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_request_exception(self, mock_post):
        """Test handling of request exceptions."""
        mock_post.side_effect = requests.exceptions.RequestException("Connection error")

        result = get_icos_data()

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_http_error(self, mock_post):
        """Test handling of HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        mock_post.return_value = mock_response

        result = get_icos_data()

        assert result is None

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_sparql_query_content(self, mock_post):
        """Test that SPARQL query is sent correctly."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": {"bindings": []}}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        get_icos_data()

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs

        # Verify the request was made to the correct URL
        assert call_kwargs.get("url") == ICOS_API_URL

        # Verify headers include proper content type
        assert "headers" in call_kwargs
        assert "Accept" in call_kwargs["headers"]
        assert call_kwargs["headers"]["Accept"] == "application/json"

        # Verify SPARQL query is in the data (as bytes)
        assert "data" in call_kwargs
        query_data = call_kwargs["data"]
        if isinstance(query_data, bytes):
            query_str = query_data.decode("utf-8")
        else:
            query_str = query_data
        assert "select" in query_str  # SPARQL uses lowercase 'select'
        assert "miscFluxnetArchiveProduct" in query_str

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_empty_response(self, mock_post):
        """Test handling of empty SPARQL response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": {"bindings": []}}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_icos_data()

        # Empty response returns None according to the implementation
        assert result is None

    @patch("builtins.open", mock_open())
    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_output_file_saving(self, mock_post):
        """Test saving response to output file."""
        test_data = {"results": {"bindings": []}}
        mock_response = MagicMock()
        mock_response.json.return_value = test_data
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_icos_data(icos_output_filename="test_icos.json")

        # Empty response returns None
        assert result is None

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_filename_parsing(self, mock_post):
        """Test various filename parsing scenarios."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "station": {"value": "http://meta.icos-cp.eu/resources/SITES/DE_THA"},
                        "dobj": {"value": "http://meta.icos-cp.eu/objects/DE-THA-data"},
                        "fileName": {"value": "FLUXNET_DE-THA_2015-2020_v1.zip"},
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_icos_data()

        assert "DE_THA" in result  # Site ID from station URL is DE_THA
        # Check that filename parsing extracts years correctly
        site_data = result["DE_THA"]
        assert site_data["filename"] == "FLUXNET_DE-THA_2015-2020_v1.zip"
        assert site_data["first_year"] == "2015"
        assert site_data["last_year"] == "2020"
        assert site_data["version"] == "v1"

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_malformed_sparql_response(self, mock_post):
        """Test handling of malformed SPARQL response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"malformed": "response"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # This should raise a KeyError due to missing 'results' key
        with pytest.raises(KeyError):
            get_icos_data()


class TestDownloadIcosData:
    """Test download_icos_data function."""

    def test_successful_download(self):
        """Test successful file download."""
        with (
            patch("fluxnet_shuttle_lib.sources.icos.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            download_icos_data("FI-HYY", "test.zip", "http://meta.icos-cp.eu/objects/test.zip")

            # The download function modifies the URL for license acceptance
            expected_url = 'https://data.icos-cp.eu/licence_accept?ids=["test.zip"]'
            mock_get.assert_called_once_with(expected_url, stream=True)

    @patch("fluxnet_shuttle_lib.sources.icos.requests.get")
    def test_download_failure_404(self, mock_get):
        """Test handling of 404 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download ICOS file.*404"):
            download_icos_data("FI-HYY", "test.zip", "http://example.com/test.zip")

    @patch("fluxnet_shuttle_lib.sources.icos.requests.get")
    def test_download_failure_500(self, mock_get):
        """Test handling of 500 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download ICOS file.*500"):
            download_icos_data("FI-HYY", "test.zip", "http://example.com/test.zip")

    def test_file_writing(self):
        """Test that file is written correctly."""
        with (
            patch("fluxnet_shuttle_lib.sources.icos.requests.get") as mock_get,
            patch("builtins.open", mock_open()) as mock_file,
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            test_chunks = [b"data_chunk_1", b"data_chunk_2"]
            mock_response.iter_content.return_value = test_chunks
            mock_get.return_value = mock_response

            download_icos_data("FI-HYY", "output.zip", "http://example.com/file.zip")

            # Verify file was opened for writing
            mock_file.assert_called_once_with("output.zip", "wb")
            # Verify all chunks were written
            handle = mock_file.return_value.__enter__.return_value
            assert handle.write.call_count == len(test_chunks)


class TestIcosIntegration:
    """Integration tests for ICOS module."""

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_complete_workflow(self, mock_post):
        """Test complete workflow from SPARQL query to data parsing."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "station": {"value": "http://meta.icos-cp.eu/resources/SITES/BE_VIE"},
                        "dobj": {"value": "http://meta.icos-cp.eu/objects/BE-VIE-data"},
                        "fileName": {"value": "FLUXNET_BE-VIE_2018-2021_v2.zip"},
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_icos_data()

        assert isinstance(result, dict)
        assert "BE_VIE" in result  # Site ID from station URL is BE_VIE
        assert result["BE_VIE"]["network"] == "ICOS"
        assert result["BE_VIE"]["publisher"] == "ICOS-ETC"
        assert result["BE_VIE"]["site_id"] == "BE_VIE"

    def test_api_url_integration(self):
        """Test that API URL constant is properly used."""
        assert ICOS_API_URL.startswith("https://")
        assert "icos-cp.eu" in ICOS_API_URL
        assert "sparql" in ICOS_API_URL

    @patch("fluxnet_shuttle_lib.sources.icos.requests.post")
    def test_sparql_query_structure(self, mock_post):
        """Test that SPARQL query has required structure."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": {"bindings": []}}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        get_icos_data()

        # Verify SPARQL query contains required elements
        call_data = mock_post.call_args.kwargs["data"]
        if isinstance(call_data, bytes):
            call_data = call_data.decode("utf-8")

        assert "prefix" in call_data
        assert "select" in call_data
        assert "where" in call_data
        assert "miscFluxnetArchiveProduct" in call_data
        assert "station" in call_data
        assert "fileName" in call_data
        assert "dobj" in call_data
