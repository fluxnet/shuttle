"""Test suite for fluxnet_shuttle.shuttle module."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

import pytest

from fluxnet_shuttle import FLUXNETShuttleError
from fluxnet_shuttle.shuttle import (
    _download_dataset,
    _extract_filename_from_headers,
    _extract_filename_from_url,
    download,
    extract_fluxnet_filename_metadata,
    listall,
    validate_fluxnet_filename_format,
)


class TestExtractFilenameFromUrl:
    """Test cases for the _extract_filename_from_url helper function."""

    def test_simple_url_without_query_params(self):
        """Test URL without query parameters."""
        url = "https://example.com/path/to/file.zip"
        result = _extract_filename_from_url(url)
        assert result == "file.zip"

    def test_ameriflux_url_with_query_params(self):
        """Test AmeriFlux URL with query parameters."""
        url = (
            "https://ftp.fluxdata.org/.ameriflux_downloads/FLUXNET/"
            "AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip?=fluxnetshuttle"
        )
        result = _extract_filename_from_url(url)
        assert result == "AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip"

    def test_icos_license_url(self):
        """Test ICOS license acceptance URL."""
        url = "https://data.icos-cp.eu/licence_accept?ids=%5B%220ZIsO-A84jm8raOmFfQ1TSbY%22%5D"
        result = _extract_filename_from_url(url)
        assert result == "licence_accept"

    def test_url_with_encoded_characters(self):
        """Test URL with percent-encoded characters."""
        url = "https://example.com/path/file%20name.zip"
        result = _extract_filename_from_url(url)
        assert result == "file name.zip"

    def test_url_with_multiple_query_params(self):
        """Test URL with multiple query parameters."""
        url = "https://example.com/download/data.csv?param1=value1&param2=value2"
        result = _extract_filename_from_url(url)
        assert result == "data.csv"


class TestExtractFilenameFromHeaders:
    """Test cases for the _extract_filename_from_headers helper function."""

    def test_content_disposition_with_filename(self):
        """Test Content-Disposition header with filename."""
        headers = {"Content-Disposition": 'attachment; filename="test.zip"'}
        result = _extract_filename_from_headers(headers)
        assert result == "test.zip"

    def test_content_disposition_without_quotes(self):
        """Test Content-Disposition header without quotes around filename."""
        headers = {"Content-Disposition": "attachment; filename=test.zip"}
        result = _extract_filename_from_headers(headers)
        assert result == "test.zip"

    def test_content_disposition_with_encoded_filename(self):
        """Test Content-Disposition header with percent-encoded filename."""
        headers = {"Content-Disposition": "attachment; filename=file%20name.zip"}
        result = _extract_filename_from_headers(headers)
        assert result == "file name.zip"

    def test_no_content_disposition(self):
        """Test when Content-Disposition header is missing."""
        headers = {"Content-Type": "application/zip"}
        result = _extract_filename_from_headers(headers)
        assert result is None

    def test_empty_headers(self):
        """Test with empty headers dictionary."""
        headers = {}
        result = _extract_filename_from_headers(headers)
        assert result is None

    def test_content_disposition_without_filename(self):
        """Test Content-Disposition header without filename parameter."""
        headers = {"Content-Disposition": "attachment"}
        result = _extract_filename_from_headers(headers)
        assert result is None


class TestExtractFluxnetFilenameMetadata:
    """Test cases for the extract_fluxnet_filename_metadata function (combined extraction)."""

    def test_valid_amf_filename(self):
        """Test extracting both metadata from AmeriFlux filename."""
        filename = "AMF_US-Ha1_FLUXNET_2005-2012_v3_r7.zip"
        source_network, version = extract_fluxnet_filename_metadata(filename)
        assert source_network == "AMF"
        assert version == "v3"

    def test_valid_icosetc_filename(self):
        """Test extracting both metadata from ICOS filename."""
        filename = "ICOSETC_BE-Bra_FLUXNET_2020-2024_v1.4_r1.zip"
        source_network, version = extract_fluxnet_filename_metadata(filename)
        assert source_network == "ICOSETC"
        assert version == "v1.4"

    def test_valid_filename_with_url(self):
        """Test extracting both metadata from full URL."""
        url = "https://example.com/AMF_AR-Bal_FLUXNET_2012-2013_v3_r7.zip"
        source_network, version = extract_fluxnet_filename_metadata(url)
        assert source_network == "AMF"
        assert version == "v3"

    def test_invalid_filename_format(self):
        """Test with filename that doesn't match pattern."""
        filename = "invalid_filename.zip"
        source_network, version = extract_fluxnet_filename_metadata(filename)
        assert source_network == ""
        assert version == ""

    def test_empty_filename(self):
        """Test with empty filename."""
        source_network, version = extract_fluxnet_filename_metadata("")
        assert source_network == ""
        assert version == ""

    def test_none_filename(self):
        """Test with None filename."""
        source_network, version = extract_fluxnet_filename_metadata(None)
        assert source_network == ""
        assert version == ""


class TestValidateFluxnetFilenameFormat:
    """Test cases for the validate_fluxnet_filename_format function."""

    def test_valid_ameriflux_format(self):
        """Test valid AmeriFlux filename format."""
        filename = "AMF_US-Ha1_FLUXNET_2005-2012_v3_r7.zip"
        assert validate_fluxnet_filename_format(filename) is True

    def test_valid_icos_format(self):
        """Test valid ICOS filename format."""
        filename = "FLX_DE-Hte_FLUXNET_2009-2018_v1_r0.zip"
        assert validate_fluxnet_filename_format(filename) is True

    def test_valid_format_with_url(self):
        """Test valid filename within URL."""
        url = "https://example.com/AMF_AR-Bal_FLUXNET_2012-2013_v3_r7.zip"
        assert validate_fluxnet_filename_format(url) is True

    def test_invalid_format(self):
        """Test invalid filename format."""
        filename = "invalid_filename.zip"
        assert validate_fluxnet_filename_format(filename) is False

    def test_empty_filename(self):
        """Test empty filename."""
        assert validate_fluxnet_filename_format("") is False

    def test_partial_format(self):
        """Test filename with partial format."""
        filename = "US-Ha1_FLUXNET_2005-2012.zip"
        assert validate_fluxnet_filename_format(filename) is False


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
            mock_response.headers = {}
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            result = _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

            assert result == "./test.zip"
            mock_get.assert_called_once_with("http://example.com/test.zip", stream=True)

    def test_successful_download_icos(self):
        """Test successful file download for ICOS."""
        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {}
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            # ICOS plugin provides ready-to-use URL with license acceptance
            result = _download_dataset(
                "FI-HYY", "ICOS", "test.zip", "https://data.icos-cp.eu/licence_accept?ids=%5B%22test%22%5D"
            )

            assert result == "./test.zip"
            mock_get.assert_called_once_with("https://data.icos-cp.eu/licence_accept?ids=%5B%22test%22%5D", stream=True)

    def test_successful_download_with_content_disposition(self):
        """Test that filename from Content-Disposition header overrides provided filename."""
        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            patch("builtins.open", mock_open()),
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {"Content-Disposition": 'attachment; filename="actual_file.zip"'}
            mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
            mock_get.return_value = mock_response

            result = _download_dataset("FI-HYY", "ICOS", "placeholder.zip", "http://example.com/download")

            # Should use filename from Content-Disposition header
            assert result == "./actual_file.zip"
            mock_get.assert_called_once_with("http://example.com/download", stream=True)

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
            mock_file.assert_called_once_with("./output.zip", "wb")
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

    def test_file_overwrite_warning(self):
        """Test that a warning is logged when overwriting an existing file."""
        import tempfile

        with (
            patch("fluxnet_shuttle.shuttle.requests.get") as mock_get,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Create an existing file
            existing_file = os.path.join(tmpdir, "existing.zip")
            with open(existing_file, "w") as f:
                f.write("old content")

            # Mock successful download
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {}
            mock_response.iter_content.return_value = [b"new content"]
            mock_get.return_value = mock_response

            # Download to the same location
            with patch("fluxnet_shuttle.shuttle._log") as mock_log:
                result = _download_dataset(
                    "US-TEST", "AmeriFlux", "existing.zip", "http://example.com/test.zip", tmpdir
                )

                # Check that warning was logged
                mock_log.warning.assert_called_once()
                assert "file already exists and will be overwritten" in mock_log.warning.call_args[0][0]
                assert existing_file in mock_log.warning.call_args[0][0]

            assert result == existing_file


class TestDownload:
    """Test cases for the download function."""

    def test_download_no_site_ids_downloads_all(self, tmp_path):
        """Test that download downloads all sites when no site IDs provided."""
        # Create a mock snapshot file with multiple sites
        snapshot_file = tmp_path / "snapshot.csv"
        snapshot_file.write_text(
            "data_hub,site_id,first_year,last_year,download_link\n"
            "AmeriFlux,US-Ha1,2000,2020,https://example.com/US-Ha1.zip\n"
            "AmeriFlux,US-MMS,2005,2021,https://example.com/US-MMS.zip\n"
        )

        # Mock the _download_dataset function
        with patch("fluxnet_shuttle.shuttle._download_dataset") as mock_download:
            mock_download.side_effect = ["US-Ha1.zip", "US-MMS.zip"]

            # Call download with no site IDs (None or empty list)
            result = download(site_ids=None, snapshot_file=str(snapshot_file), output_dir=str(tmp_path))

            # Verify all sites were downloaded
            assert len(result) == 2
            assert result == ["US-Ha1.zip", "US-MMS.zip"]
            assert mock_download.call_count == 2

    def test_download_no_snapshot_file_raises_error(self):
        """Test that download raises error when no snapshot file provided."""
        with pytest.raises(FLUXNETShuttleError, match="No snapshot file provided"):
            download(["US-Ha1"], "")

    def test_download_nonexistent_snapshot_file_raises_error(self):
        """Test that download raises error when snapshot file doesn't exist."""
        with pytest.raises(FLUXNETShuttleError, match="does not exist"):
            download(["US-Ha1"], "nonexistent.csv")

    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,data_hub,download_link\n" "US-TEST,AmeriFlux,http://example.com/test.zip\n"),
    )
    def test_download_ameriflux_site_success(self, mock_exists, mock_download):
        """Test successful download of AmeriFlux site."""
        mock_exists.return_value = True
        mock_download.return_value = "test.zip"

        result = download(["US-TEST"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="US-TEST",
            data_hub="AmeriFlux",
            filename="test.zip",
            download_link="http://example.com/test.zip",
            output_dir=".",
        )

    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,data_hub,download_link\n" "FI-HYY,ICOS,http://example.com/test.zip\n"),
    )
    def test_download_icos_site_success(self, mock_exists, mock_download):
        """Test successful download of ICOS site."""
        mock_exists.return_value = True
        mock_download.return_value = "test.zip"

        result = download(["FI-HYY"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="FI-HYY",
            data_hub="ICOS",
            filename="test.zip",
            download_link="http://example.com/test.zip",
            output_dir=".",
        )

    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,data_hub,download_link\n"
            "US-TEST,AmeriFlux,http://example.com/file.zip?=fluxnetshuttle\n"
        ),
    )
    def test_download_with_query_params_in_url(self, mock_exists, mock_download):
        """Test that download correctly handles URLs with query parameters."""
        mock_exists.return_value = True
        mock_download.return_value = "file.zip"

        result = download(["US-TEST"], "test.csv")

        assert result == ["file.zip"]
        # Verify that filename passed to _download_dataset has no query params
        mock_download.assert_called_once_with(
            site_id="US-TEST",
            data_hub="AmeriFlux",
            filename="file.zip",
            download_link="http://example.com/file.zip?=fluxnetshuttle",
            output_dir=".",
        )

    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,data_hub,download_link\n" "US-TEST,AmeriFlux,http://example.com/test.zip\n"),
    )
    def test_download_site_not_in_snapshot_file_raises_error(self, mock_exists):
        """Test that download raises error when site not in snapshot file."""
        mock_exists.return_value = True

        with pytest.raises(FLUXNETShuttleError, match="not found in snapshot file"):
            download(["NonExistent"], "test.csv")

    def test_download_with_real_csv_file(self):
        """Test download function with real CSV file but missing site."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write("site_id,data_hub,download_link\n")
            tmp_file.write("US-Ha1,AmeriFlux,http://example.com/test.zip\n")
            temp_filename = tmp_file.name

        try:
            # Should raise error because site not found
            with pytest.raises(FLUXNETShuttleError, match="not found in snapshot file"):
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
        result = await listall(data_hubs=[])

        assert isinstance(result, str)
        assert result.endswith(".csv")

        assert mock_open.called
        assert mock_open.call_count == 1
        assert mock_write_header.called
        assert mock_write_header.call_count == 1
        assert mock_write_header.call_args == call()
        assert not mock_write_row.called  # No rows should be written when no data hubs are enabled

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle.aiofiles.open")
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writerow", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.csv.DictWriter.writeheader", new_callable=AsyncMock)
    @patch("fluxnet_shuttle.shuttle.datetime")
    @patch("fluxnet_shuttle.core.shuttle.FluxnetShuttle.get_all_sites", new_callable=MagicMock)
    async def test_listall_with_data_hubs(
        self, mock_get_all_sites, mock_datetime, mock_write_header, mock_write_row, mock_open
    ):
        """Test listall with both data hubs enabled."""
        mock_get_all_sites.return_value = AsyncMock()
        mock_get_all_sites.return_value.__aiter__.return_value = {
            MagicMock(
                site_info=MagicMock(
                    site_id="US-TEST",
                    site_name="Test Site",
                    data_hub="AmeriFlux",
                    location_lat=45.0,
                    location_long=-120.0,
                    igbp="DBF",
                    group_team_member=[],
                    network=[],
                    model_dump=lambda exclude=None: {
                        "site_id": "US-TEST",
                        "site_name": "Test Site",
                        "data_hub": "AmeriFlux",
                        "location_lat": 45.0,
                        "location_long": -120.0,
                        "igbp": "DBF",
                    },
                ),
                product_data=MagicMock(
                    first_year=2000,
                    last_year=2020,
                    download_link="http://example.com/test.zip",
                    product_citation="Test citation",
                    product_id="test-id",
                    oneflux_code_version="v1",
                    product_source_network="AMF",
                    model_dump=lambda: {
                        "first_year": 2000,
                        "last_year": 2020,
                        "download_link": "http://example.com/test.zip",
                        "product_citation": "Test citation",
                        "product_id": "test-id",
                        "oneflux_code_version": "v1",
                        "product_source_network": "AMF",
                    },
                ),
            ),
            MagicMock(
                site_info=MagicMock(
                    site_id="FI-HYY",
                    site_name="Hyytiälä",
                    data_hub="ICOS",
                    location_lat=61.85,
                    location_long=24.29,
                    igbp="ENF",
                    group_team_member=[],
                    network=[],
                    model_dump=lambda exclude=None: {
                        "site_id": "FI-HYY",
                        "site_name": "Hyytiälä",
                        "data_hub": "ICOS",
                        "location_lat": 61.85,
                        "location_long": 24.29,
                        "igbp": "ENF",
                    },
                ),
                product_data=MagicMock(
                    first_year=2005,
                    last_year=2018,
                    download_link="http://example.com/icos.zip",
                    product_citation="ICOS citation",
                    product_id="icos-id",
                    oneflux_code_version="v2",
                    product_source_network="ICOSETC",
                    model_dump=lambda: {
                        "first_year": 2005,
                        "last_year": 2018,
                        "download_link": "http://example.com/icos.zip",
                        "product_citation": "ICOS citation",
                        "product_id": "icos-id",
                        "oneflux_code_version": "v2",
                        "product_source_network": "ICOSETC",
                    },
                ),
            ),
        }

        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.strftime.return_value = "20251013T075248"

        result = await listall(data_hubs=["ameriflux", "icos"])

        assert isinstance(result, str)
        assert result.endswith(".csv")
        assert mock_open.called
        assert mock_open.call_count == 1
        assert mock_open.call_args[0][0] == "./fluxnet_shuttle_snapshot_20251013T075248.csv"
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

    def test_import_works(self):
        """Test that imports work correctly."""
        from fluxnet_shuttle.shuttle import download, listall

        assert download is not None
        assert listall is not None
