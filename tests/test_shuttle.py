"""Test suite for fluxnet_shuttle_lib.shuttle module."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

import pytest

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.shuttle import download, listall


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

    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,network,filename,download_link\n" "US-TEST,Unknown,test.zip,http://example.com\n"),
    )
    def test_download_unsupported_network_raises_error(self, mock_exists):
        """Test that download raises error for unsupported network."""
        mock_exists.return_value = True

        with pytest.raises(FLUXNETShuttleError, match="Network Unknown not supported for download"):
            download(["US-TEST"], "test.csv")

    @patch("fluxnet_shuttle_lib.shuttle.download_ameriflux_data")
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
            site_id="US-TEST", filename="test.zip", download_link="http://example.com/test.zip"
        )

    @patch("fluxnet_shuttle_lib.shuttle.download_icos_data")
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
            site_id="FI-HYY", filename="test.zip", download_link="http://example.com/test.zip"
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
    @patch("fluxnet_shuttle_lib.shuttle.aiofiles.open")
    @patch("fluxnet_shuttle_lib.shuttle.csv.DictWriter.writerow", new_callable=AsyncMock)
    @patch("fluxnet_shuttle_lib.shuttle.csv.DictWriter.writeheader", new_callable=AsyncMock)
    @patch("fluxnet_shuttle_lib.shuttle.datetime")
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
    @patch("fluxnet_shuttle_lib.shuttle.aiofiles.open")
    @patch("fluxnet_shuttle_lib.shuttle.csv.DictWriter.writerow", new_callable=AsyncMock)
    @patch("fluxnet_shuttle_lib.shuttle.csv.DictWriter.writeheader", new_callable=AsyncMock)
    @patch("fluxnet_shuttle_lib.shuttle.datetime")
    @patch("fluxnet_shuttle_lib.core.shuttle.FluxnetShuttle.get_all_sites", new_callable=MagicMock)
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
        from fluxnet_shuttle_lib.shuttle import test

        assert callable(test)

        with pytest.raises(NotImplementedError):
            test()

    def test_import_works(self):
        """Test that imports work correctly."""
        from fluxnet_shuttle_lib.shuttle import download, listall

        assert download is not None
        assert listall is not None
