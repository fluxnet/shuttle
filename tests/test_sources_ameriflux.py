"""Test suite for fluxnet_shuttle_lib.sources.ameriflux module."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.sources.ameriflux import download_ameriflux_data


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
