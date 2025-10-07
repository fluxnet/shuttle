"""Test suite for fluxnet_shuttle_lib.shuttle module."""

import os
import tempfile
from unittest.mock import mock_open, patch

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
        mock_open(
            read_data="site_id,network,filename,download_link\n" "US-TEST,AmeriFlux,test.zip,http://example.com\n"
        ),
    )
    def test_download_ameriflux_site_success(self, mock_exists, mock_download):
        """Test successful download of AmeriFlux site."""
        mock_exists.return_value = True
        mock_download.return_value = None

        result = download(["US-TEST"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="US-TEST", filename="test.zip", download_link="http://example.com"
        )

    @patch("fluxnet_shuttle_lib.shuttle.download_icos_data")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,network,filename,download_link\n" "FI-HYY,ICOS,test.zip,http://example.com\n"),
    )
    def test_download_icos_site_success(self, mock_exists, mock_download):
        """Test successful download of ICOS site."""
        mock_exists.return_value = True
        mock_download.return_value = None

        result = download(["FI-HYY"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(site_id="FI-HYY", filename="test.zip", download_link="http://example.com")

    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,network,filename,download_link\n" "US-TEST,AmeriFlux,test.zip,http://example.com\n"
        ),
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
            tmp_file.write("site_id,network,filename,download_link\n")
            tmp_file.write("US-Ha1,AmeriFlux,test.zip,http://example.com\n")
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

    def test_listall_basic_functionality(self):
        """Test basic listall functionality without external calls."""
        # Call with no networks to avoid external API calls
        ameriflux_patch = "fluxnet_shuttle_lib.shuttle.get_ameriflux_data"
        icos_patch = "fluxnet_shuttle_lib.shuttle.get_icos_data"

        with (
            patch(ameriflux_patch) as mock_ameriflux,
            patch(icos_patch) as mock_icos,
            patch("builtins.open", mock_open()),
        ):

            mock_ameriflux.return_value = {}
            mock_icos.return_value = {}

            result = listall(ameriflux=False, icos=False)

            assert isinstance(result, str)
            assert result.endswith(".csv")
            mock_ameriflux.assert_not_called()
            mock_icos.assert_not_called()

    def test_listall_with_networks(self):
        """Test listall with both networks enabled."""
        ameriflux_patch = "fluxnet_shuttle_lib.shuttle.get_ameriflux_data"
        icos_patch = "fluxnet_shuttle_lib.shuttle.get_icos_data"

        with (
            patch(ameriflux_patch) as mock_ameriflux,
            patch(icos_patch) as mock_icos,
            patch("builtins.open", mock_open()),
        ):

            mock_ameriflux.return_value = {"US-TEST": {"site_id": "US-TEST"}}
            mock_icos.return_value = {"IT-TEST": {"site_id": "IT-TEST"}}

            result = listall(ameriflux=True, icos=True)

            assert isinstance(result, str)
            assert result.endswith(".csv")
            mock_ameriflux.assert_called_once()
            mock_icos.assert_called_once()


class TestModuleFunctions:
    """Test basic module functionality."""

    def test_download_function_exists(self):
        """Test that download function exists."""
        assert callable(download)

    def test_listall_function_exists(self):
        """Test that listall function exists."""
        assert callable(listall)

    def test_import_works(self):
        """Test that imports work correctly."""
        from fluxnet_shuttle_lib.shuttle import download, listall

        assert download is not None
        assert listall is not None
