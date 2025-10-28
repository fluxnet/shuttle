"""Test suite for fluxnet_shuttle.shuttle module."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

import pytest

from fluxnet_shuttle import FLUXNETShuttleError
from fluxnet_shuttle.shuttle import _download_dataset, download, listall


class TestDownloadDataset:
    """Test cases for the _download_dataset private function."""

    def test_successful_download_ameriflux(self):
        """Test successful file download for AmeriFlux."""
        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

            mock_get.assert_called_once_with("http://example.com/test.zip", stream=True)

    def test_successful_download_icos(self):
        """Test successful file download for ICOS."""
        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            # ICOS plugin provides ready-to-use URL with license acceptance
            _download_dataset(
                "FI-HYY", "ICOS", "test.zip", "https://data.icos-cp.eu/licence_accept?ids=%5B%22test%22%5D"
            )

            mock_get.assert_called_once_with("https://data.icos-cp.eu/licence_accept?ids=%5B%22test%22%5D", stream=True)

    @patch("fluxnet_shuttle.shuttle.requests.get")
    def test_download_failure_404(self, mock_get):
        """Test handling of 404 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download.*404"):
            _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

    @patch("fluxnet_shuttle.shuttle.requests.get")
    def test_download_failure_500(self, mock_get):
        """Test handling of 500 download failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(FLUXNETShuttleError, match="Failed to download.*500"):
            _download_dataset("FI-HYY", "ICOS", "test.zip", "http://example.com/test.zip")

    def test_file_writing(self):
        """Test that file is written correctly."""
        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            patch("builtins.open", mock_open()) as mock_file,
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            test_chunks = [b"data_chunk_1", b"data_chunk_2"]
            mock_response.iter_content.return_value = test_chunks
            mock_get.return_value = mock_response

            _download_dataset("US-TEST", "AmeriFlux", "output.zip", "http://example.com/file.zip")

            # Verify file was opened for writing
            mock_file.assert_called_once_with("output.zip", "wb")
            # Verify all chunks were written
            handle = mock_file.return_value.__enter__.return_value
            assert handle.write.call_count == len(test_chunks)

    @patch("fluxnet_shuttle.shuttle.requests.get")
    def test_request_exception_handling(self, mock_get):
        """Test handling of request exceptions."""
        import requests

        mock_get.side_effect = requests.RequestException("Network error")

        with pytest.raises(FLUXNETShuttleError, match="Failed to download.*Network error"):
            _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")


class TestDownload:
    """Test cases for the download function."""

    def test_download_no_site_ids_raises_error(self):
        """Test that download raises error when no site IDs provided."""
        with pytest.raises(FLUXNETShuttleError, match="No site IDs provided"):
            download([], "test.csv")

    def test_download_no_runfile_raises_error(self):
        """Test that download raises error when no run file provided."""
        with pytest.raises(FLUXNETShuttleError, match="No run file provided"):
            download(["US-Ha1"], "")

    def test_download_nonexistent_runfile_raises_error(self):
        """Test that download raises error when run file doesn't exist."""
        with pytest.raises(FLUXNETShuttleError, match="does not exist"):
            download(["US-Ha1"], "nonexistent.csv")

    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,network,download_link\n" "US-TEST,AmeriFlux,http://example.com/test.zip\n"),
    )
    def test_download_ameriflux_site_success(self, mock_exists, mock_download):
        """Test successful download of AmeriFlux site."""
        mock_exists.return_value = True
        mock_download.return_value = None

        result = download(["US-TEST"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="US-TEST", network="AmeriFlux", filename="test.zip", download_link="http://example.com/test.zip"
        )

    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,network,download_link\n" "FI-HYY,ICOS,http://example.com/test.zip\n"),
    )
    def test_download_icos_site_success(self, mock_exists, mock_download):
        """Test successful download of ICOS site."""
        mock_exists.return_value = True
        mock_download.return_value = None

        result = download(["FI-HYY"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="FI-HYY", network="ICOS", filename="test.zip", download_link="http://example.com/test.zip"
        )

    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,network,download_link\n" "US-TEST,AmeriFlux,http://example.com/test.zip\n"),
    )
    def test_download_site_not_in_runfile_raises_error(self, mock_exists):
        """Test that download raises error when site not in run file."""
        mock_exists.return_value = True

        with pytest.raises(FLUXNETShuttleError, match="not found in run file"):
            download(["NonExistent"], "test.csv")

    def test_download_with_real_csv_file(self):
        """Test download function with real CSV file but missing site."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write("site_id,network,download_link\n")
            tmp_file.write("US-Ha1,AmeriFlux,http://example.com/test.zip\n")
            temp_filename = tmp_file.name

        try:
            # Should raise error because site not found
            with pytest.raises(FLUXNETShuttleError, match="not found in run file"):
                download(["NonExistent"], temp_filename)
        finally:
            # Clean up
            os.unlink(temp_filename)


class TestListall:
    """Test cases for the listall function."""

    def test_listall_function_exists(self):
        """Test that listall function exists and is callable."""
        assert callable(listall)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle.aiofiles.open")
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writerow", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writeheader", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.datetime")
    async def test_listall_basic_functionality(self, mock_datetime, mock_write_header, mock_write_row, mock_open):
        """Test basic listall functionality without external calls."""

        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.strftime.return_value = "20251013T075248"
        result = await listall(ameriflux=False, icos=False)

        assert isinstance(result, str)
        assert result.endswith(".csv")

        assert mock_open.called
        assert mock_open.call_count == 1
        assert mock_write_header.called
        assert mock_write_header.call_count == 1
        assert mock_write_header.call_args == call()
        assert not mock_write_row.called  # No rows should be written when no networks are enabled

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle.aiofiles.open")
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writerow", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writeheader", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.datetime")
    @patch("fluxnet_shuttle.core.shuttle.FluxnetShuttle.get_all_sites", new_callable=MagicMock)
    async def test_listall_with_networks(
        self, mock_get_all_sites, mock_datetime, mock_write_header, mock_write_row, mock_open
    ):
        """Test listall with both networks enabled."""
        mock_get_all_sites.return_value = AsyncMock()
        mock_get_all_sites.return_value.__aiter__.return_value = {
            MagicMock(
                site_info=MagicMock(
                    site_id="US-TEST",
                    network="AmeriFlux",
                    location_lat=45.0,
                    location_long=-120.0,
                ),
                product_data=MagicMock(
                    first_year=2000,
                    last_year=2020,
                    download_link="http://example.com/test.zip",
                ),
            ),
            MagicMock(
                site_info=MagicMock(
                    site_id="FI-HYY",
                    network="ICOS",
                    location_lat=61.85,
                    location_long=24.29,
                ),
                product_data=MagicMock(
                    first_year=2005,
                    last_year=2018,
                    download_link="http://example.com/icos.zip",
                ),
            ),
        }

        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.strftime.return_value = "20251013T075248"

        result = await listall(ameriflux=True, icos=True)

        assert isinstance(result, str)
        assert result.endswith(".csv")
        assert mock_open.called
        assert mock_open.call_count == 1
        assert mock_open.call_args[0][0] == "data_availability_20251013T075248.csv"
        assert mock_write_header.called
        assert mock_write_header.call_count == 1
        assert mock_write_header.call_args == call()
        assert mock_get_all_sites.called
        assert mock_get_all_sites.call_count == 1
        assert mock_write_row.called
        assert mock_write_row.call_count == 2  # Two rows should be written for two sites


class TestModuleFunctions:
    """Test basic module functionality."""

    def test_download_function_exists(self):
        """Test that download function exists."""
        assert callable(download)

    def test_listall_function_exists(self):
        """Test that listall function exists."""
        assert callable(listall)

    def test_test_connectivity_function_exists(self):
        """Test that test connectivity function exists."""
        from fluxnet_shuttle.shuttle import test

        assert callable(test)

        with pytest.raises(NotImplementedError):
            test()

    def test_import_works(self):
        """Test that imports work correctly."""
        from fluxnet_shuttle.shuttle import download, listall

        assert download is not None
        assert listall is not None
