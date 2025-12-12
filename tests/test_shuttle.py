"""Test suite for fluxnet_shuttle.shuttle module."""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

import pytest

from fluxnet_shuttle import FLUXNETShuttleError
from fluxnet_shuttle.shuttle import (
    _download_dataset,
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


class TestExtractFluxnetFilenameMetadata:
    """Test cases for the extract_fluxnet_filename_metadata function (combined extraction)."""

    def test_valid_amf_filename(self):
        """Test extracting metadata from AmeriFlux filename."""
        filename = "AMF_US-Ha1_FLUXNET_2005-2012_v3_r7.zip"
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(filename)
        assert source_network == "AMF"
        assert version == "v3"
        assert first_year == 2005
        assert last_year == 2012
        assert run == "r7"

    def test_valid_icosetc_filename(self):
        """Test extracting metadata from ICOS filename."""
        filename = "ICOSETC_BE-Bra_FLUXNET_2020-2024_v1.4_r1.zip"
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(filename)
        assert source_network == "ICOSETC"
        assert version == "v1.4"
        assert first_year == 2020
        assert last_year == 2024
        assert run == "r1"

    def test_valid_filename_with_url(self):
        """Test extracting metadata from full URL."""
        url = "https://example.com/AMF_AR-Bal_FLUXNET_2012-2013_v3_r7.zip"
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(url)
        assert source_network == "AMF"
        assert version == "v3"
        assert first_year == 2012
        assert last_year == 2013
        assert run == "r7"

    def test_invalid_filename_format(self):
        """Test with filename that doesn't match pattern."""
        filename = "invalid_filename.zip"
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(filename)
        assert source_network == ""
        assert version == ""
        assert first_year == 0
        assert last_year == 0
        assert run == ""

    def test_empty_filename(self):
        """Test with empty filename."""
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata("")
        assert source_network == ""
        assert version == ""
        assert first_year == 0
        assert last_year == 0
        assert run == ""

    def test_none_filename(self):
        """Test with None filename."""
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(None)
        assert source_network == ""
        assert version == ""
        assert first_year == 0
        assert last_year == 0
        assert run == ""

    def test_filename_with_rbeta_run_number(self):
        """Test that filename with 'rbeta' as run number fails validation."""
        filename = "TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rbeta.zip"
        source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(filename)
        # Should return empty strings because 'rbeta' doesn't match the pattern (r\d+)
        assert source_network == ""
        assert version == ""
        assert first_year == 0
        assert last_year == 0
        assert run == ""


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

    def test_rbeta_run_number_invalid(self):
        """Test that filename with 'rbeta' as run number is invalid."""
        filename = "TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rbeta.zip"
        assert validate_fluxnet_filename_format(filename) is False


class TestDownloadDataset:
    """Test cases for the _download_dataset private function."""

    @pytest.mark.asyncio
    async def test_successful_download_ameriflux(self):
        """Test successful file download for AmeriFlux."""
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        async def mock_iter_chunked(size):
            for chunk in [b"chunk1", b"chunk2"]:
                yield chunk

        with (
            patch.object(AmeriFluxPlugin, "download_file") as mock_download_file,
            patch("builtins.open", mock_open()),
        ):
            # Create async mock stream
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked

            # Mock async context manager to return just the stream
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            result = await _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

            assert result == "./test.zip"

    @pytest.mark.asyncio
    async def test_successful_download_icos(self):
        """Test successful file download for ICOS."""
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.icos import ICOSPlugin

        async def mock_iter_chunked(size):
            for chunk in [b"chunk1", b"chunk2"]:
                yield chunk

        with (
            patch.object(ICOSPlugin, "download_file") as mock_download_file,
            patch("builtins.open", mock_open()),
        ):
            # Create async mock stream
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked

            # Mock async context manager to return (stream, filename) tuple
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            # ICOS plugin provides ready-to-use URL with license acceptance
            result = await _download_dataset(
                "FI-HYY", "ICOS", "test.zip", "https://data.icos-cp.eu/licence_accept?ids=%5B%22test%22%5D"
            )

            assert result == "./test.zip"

    @pytest.mark.asyncio
    async def test_successful_download_with_content_disposition(self):
        """Test that ICOS validates filename from Content-Disposition header but uses metadata filename."""
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.icos import ICOSPlugin

        async def mock_iter_chunked(size):
            for chunk in [b"chunk1", b"chunk2"]:
                yield chunk

        with (
            patch.object(ICOSPlugin, "download_file") as mock_download_file,
            patch("builtins.open", mock_open()),
        ):
            # Create async mock stream
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked

            # ICOS plugin validates filename from header but returns just the stream
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            result = await _download_dataset("FI-HYY", "ICOS", "metadata_file.zip", "http://example.com/download")

            # Should use filename from metadata (the one passed as argument)
            assert result == "./metadata_file.zip"

    @pytest.mark.asyncio
    async def test_download_failure_404(self):
        """Test handling of 404 download failure."""
        from fluxnet_shuttle.core.exceptions import PluginError
        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        with patch.object(AmeriFluxPlugin, "download_file") as mock_download_file:
            # Simulate 404 error by raising PluginError
            mock_download_file.return_value.__aenter__.side_effect = PluginError("ameriflux", "HTTP 404 Not Found")

            with pytest.raises(FLUXNETShuttleError, match="Failed to download"):
                await _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

    @pytest.mark.asyncio
    async def test_download_failure_500(self):
        """Test handling of 500 download failure."""
        from fluxnet_shuttle.core.exceptions import PluginError
        from fluxnet_shuttle.plugins.icos import ICOSPlugin

        with patch.object(ICOSPlugin, "download_file") as mock_download_file:
            # Simulate 500 error by raising PluginError
            mock_download_file.return_value.__aenter__.side_effect = PluginError(
                "icos", "HTTP 500 Internal Server Error"
            )

            with pytest.raises(FLUXNETShuttleError, match="Failed to download"):
                await _download_dataset("FI-HYY", "ICOS", "test.zip", "http://example.com/test.zip")

    @pytest.mark.asyncio
    async def test_file_writing(self):
        """Test that file is written correctly."""
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        test_chunks = [b"data_chunk_1", b"data_chunk_2"]

        async def mock_iter_chunked(size):
            for chunk in test_chunks:
                yield chunk

        with (
            patch.object(AmeriFluxPlugin, "download_file") as mock_download_file,
            patch("builtins.open", mock_open()) as mock_file,
        ):
            # Create async mock stream
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked

            # Mock async context manager to return (stream, filename) tuple
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            await _download_dataset("US-TEST", "AmeriFlux", "output.zip", "http://example.com/file.zip")

            # Verify file was opened for writing
            mock_file.assert_called_once_with("./output.zip", "wb")
            # Verify all chunks were written
            handle = mock_file.return_value.__enter__.return_value
            assert handle.write.call_count == len(test_chunks)

    @pytest.mark.asyncio
    async def test_request_exception_handling(self):
        """Test handling of request exceptions."""
        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        with patch.object(AmeriFluxPlugin, "download_file") as mock_download_file:
            # Simulate network error
            mock_download_file.return_value.__aenter__.side_effect = Exception("Network error")

            with pytest.raises(FLUXNETShuttleError, match="Failed to download"):
                await _download_dataset("US-TEST", "AmeriFlux", "test.zip", "http://example.com/test.zip")

    @pytest.mark.asyncio
    async def test_file_overwrite_warning(self):
        """Test that a warning is logged when overwriting an existing file."""
        import tempfile
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        async def mock_iter_chunked(size):
            yield b"new content"

        with (
            patch.object(AmeriFluxPlugin, "download_file") as mock_download_file,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            # Create an existing file
            existing_file = os.path.join(tmpdir, "existing.zip")
            with open(existing_file, "w") as f:
                f.write("old content")

            # Mock successful download
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked

            # Mock async context manager to return (stream, filename) tuple
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            # Download to the same location
            with patch("fluxnet_shuttle.shuttle._log") as mock_log:
                result = await _download_dataset(
                    "US-TEST", "AmeriFlux", "existing.zip", "http://example.com/test.zip", tmpdir
                )

                # Check that warning was logged
                mock_log.warning.assert_called_once()
                assert "file already exists and will be overwritten" in mock_log.warning.call_args[0][0]
                assert existing_file in mock_log.warning.call_args[0][0]

            assert result == existing_file

    @pytest.mark.asyncio
    async def test_plugin_not_found_error(self):
        """Test handling when plugin is not found for data hub."""
        from fluxnet_shuttle.core.registry import registry

        # Mock registry to return None for unknown plugin
        with patch.object(registry, "get_plugin", return_value=None):
            with pytest.raises(FLUXNETShuttleError, match="Data hub plugin UnknownHub not found for site US-TEST"):
                await _download_dataset("US-TEST", "UnknownHub", "test.zip", "http://example.com/test.zip")

    @pytest.mark.asyncio
    async def test_download_with_user_info_kwargs(self):
        """Test _download_dataset passes plugin-specific user_info to download_file."""
        from unittest.mock import AsyncMock

        from fluxnet_shuttle.plugins.ameriflux import AmeriFluxPlugin

        async def mock_iter_chunked(size):
            for chunk in [b"test_data"]:
                yield chunk

        with (
            patch.object(AmeriFluxPlugin, "download_file") as mock_download_file,
            patch("builtins.open", mock_open()),
        ):
            # Create async mock stream
            mock_stream = AsyncMock()
            mock_stream.iter_chunked = mock_iter_chunked
            mock_download_file.return_value.__aenter__.return_value = mock_stream

            # Pass user_info in kwargs
            user_info = {
                "ameriflux": {
                    "user_name": "Test User",
                    "user_email": "test@example.com",
                    "intended_use": 1,
                    "description": "Test",
                }
            }

            result = await _download_dataset(
                "US-TEST", "ameriflux", "test.zip", "http://example.com/test.zip", user_info=user_info
            )

            # Verify plugin's download_file was called with user_info in kwargs
            mock_download_file.assert_called_once()
            call_kwargs = mock_download_file.call_args[1]
            # Should receive the full user_info dict (not extracted)
            assert "user_info" in call_kwargs
            assert call_kwargs["user_info"] == user_info
            assert call_kwargs["filename"] == "test.zip"
            assert result == "./test.zip"


class TestDownload:
    """Test cases for the download function."""

    @pytest.mark.asyncio
    async def test_download_no_site_ids_downloads_all(self, tmp_path):
        """Test that download downloads all sites when no site IDs provided."""
        # Create a mock snapshot file with multiple sites
        snapshot_file = tmp_path / "snapshot.csv"
        snapshot_file.write_text(
            "data_hub,site_id,first_year,last_year,download_link,fluxnet_product_name\n"
            "AmeriFlux,US-Ha1,2000,2020,https://example.com/US-Ha1.zip,US-Ha1.zip\n"
            "AmeriFlux,US-MMS,2005,2021,https://example.com/US-MMS.zip,US-MMS.zip\n"
        )

        # Mock the _download_dataset function to return coroutines
        with patch("fluxnet_shuttle.shuttle._download_dataset") as mock_download:
            # Create async mock that returns coroutines
            async def mock_download_side_effect(*args, **kwargs):
                return ["US-Ha1.zip", "US-MMS.zip"][mock_download.call_count - 1]

            mock_download.side_effect = mock_download_side_effect

            # Call download with no site IDs (None or empty list)
            result = await download(site_ids=None, snapshot_file=str(snapshot_file), output_dir=str(tmp_path))

            # Verify all sites were downloaded
            assert len(result) == 2
            assert result == ["US-Ha1.zip", "US-MMS.zip"]
            assert mock_download.call_count == 2

    @pytest.mark.asyncio
    async def test_download_no_snapshot_file_raises_error(self):
        """Test that download raises error when no snapshot file provided."""
        with pytest.raises(FLUXNETShuttleError, match="No snapshot file provided"):
            await download(["US-Ha1"], "")

    @pytest.mark.asyncio
    async def test_download_nonexistent_snapshot_file_raises_error(self):
        """Test that download raises error when snapshot file doesn't exist."""
        with pytest.raises(FLUXNETShuttleError, match="does not exist"):
            await download(["US-Ha1"], "nonexistent.csv")

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,data_hub,download_link,fluxnet_product_name\n"
            "US-TEST,AmeriFlux,http://example.com/test.zip,test.zip\n"
        ),
    )
    async def test_download_ameriflux_site_success(self, mock_exists, mock_download):
        """Test successful download of AmeriFlux site."""
        mock_exists.return_value = True

        async def mock_return():
            return "test.zip"

        mock_download.return_value = mock_return()

        result = await download(["US-TEST"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="US-TEST",
            data_hub="AmeriFlux",
            filename="test.zip",
            download_link="http://example.com/test.zip",
            output_dir=".",
        )

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,data_hub,download_link,fluxnet_product_name\n"
            "FI-HYY,ICOS,http://example.com/test.zip,test.zip\n"
        ),
    )
    async def test_download_icos_site_success(self, mock_exists, mock_download):
        """Test successful download of ICOS site."""
        mock_exists.return_value = True

        async def mock_return():
            return "test.zip"

        mock_download.return_value = mock_return()

        result = await download(["FI-HYY"], "test.csv")

        assert result == ["test.zip"]
        mock_download.assert_called_once_with(
            site_id="FI-HYY",
            data_hub="ICOS",
            filename="test.zip",
            download_link="http://example.com/test.zip",
            output_dir=".",
        )

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,data_hub,download_link,fluxnet_product_name\n"
            "US-TEST,AmeriFlux,http://example.com/file.zip?=fluxnetshuttle,file.zip\n"
        ),
    )
    async def test_download_with_query_params_in_url(self, mock_exists, mock_download):
        """Test that download correctly handles URLs with query parameters."""
        mock_exists.return_value = True

        async def mock_return():
            return "file.zip"

        mock_download.return_value = mock_return()

        result = await download(["US-TEST"], "test.csv")

        assert result == ["file.zip"]
        # Verify that filename passed to _download_dataset has no query params
        mock_download.assert_called_once_with(
            site_id="US-TEST",
            data_hub="AmeriFlux",
            filename="file.zip",
            download_link="http://example.com/file.zip?=fluxnetshuttle",
            output_dir=".",
        )

    @pytest.mark.asyncio
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,data_hub,download_link\n" "US-TEST,AmeriFlux,http://example.com/test.zip\n"),
    )
    async def test_download_site_not_in_snapshot_file_raises_error(self, mock_exists):
        """Test that download raises error when site not in snapshot file."""
        mock_exists.return_value = True

        with pytest.raises(FLUXNETShuttleError, match="not found in snapshot file"):
            await download(["NonExistent"], "test.csv")

    @pytest.mark.asyncio
    async def test_download_with_real_csv_file(self):
        """Test download function with real CSV file but missing site."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            tmp_file.write("site_id,data_hub,download_link\n")
            tmp_file.write("US-Ha1,AmeriFlux,http://example.com/test.zip\n")
            temp_filename = tmp_file.name

        try:
            # Should raise error because site not found
            with pytest.raises(FLUXNETShuttleError, match="not found in snapshot file"):
                await download(["NonExistent"], temp_filename)
        finally:
            # Clean up
            os.unlink(temp_filename)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle._download_dataset")
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(
            read_data="site_id,data_hub,download_link,fluxnet_product_name\n"
            "US-TEST,ameriflux,http://example.com/test.zip,test.zip\n"
        ),
    )
    async def test_download_with_user_info(self, mock_exists, mock_download):
        """Test download with user_info parameter passes plugin-specific data."""
        mock_exists.return_value = True

        async def mock_return():
            return "test.zip"

        mock_download.return_value = mock_return()

        # Create user_info with ameriflux-specific data
        user_info = {
            "ameriflux": {
                "user_name": "Test User",
                "user_email": "test@example.com",
                "intended_use": 1,
                "description": "Test download",
            }
        }

        result = await download(["US-TEST"], "test.csv", user_info=user_info)

        assert result == ["test.zip"]
        # Verify _download_dataset was called with user_info
        mock_download.assert_called_once()
        call_kwargs = mock_download.call_args[1]
        assert "user_info" in call_kwargs
        assert call_kwargs["user_info"] == user_info

    @pytest.mark.asyncio
    @patch("os.path.exists")
    @patch(
        "builtins.open",
        mock_open(read_data="site_id,data_hub,download_link\n" "US-TEST,ameriflux,http://example.com/test.zip\n"),
    )
    async def test_download_missing_filename_skips_site(self, mock_exists, caplog):
        """Test download skips sites with missing fluxnet_product_name."""
        mock_exists.return_value = True

        # CSV without fluxnet_product_name column - site should be skipped
        with caplog.at_level("ERROR", logger="fluxnet_shuttle.shuttle"):
            result = await download(["US-TEST"], "test.csv")

        # Should return empty list since site was skipped
        assert result == []
        # Should log error about missing filename
        assert "No filename found for site US-TEST" in caplog.text
        assert "Skipping download" in caplog.text


class TestListall:
    """Test cases for the listall function."""

    def test_listall_function_exists(self):
        """Test that listall function exists and is callable."""
        assert callable(listall)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.shuttle.datetime")
    @patch("fluxnet_shuttle.shuttle._write_snapshot_file")
    async def test_listall_basic_functionality(self, mock_write_snapshot, mock_datetime):
        """Test basic listall functionality without external calls."""
        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.strftime.return_value = "20251013T075248"

        # Mock _write_snapshot_file to return empty counts
        async def mock_write():
            return {}

        mock_write_snapshot.return_value = mock_write()

        result = await listall(data_hubs=[])

        assert isinstance(result, str)
        assert result.endswith(".csv")
        assert result == "./fluxnet_shuttle_snapshot_20251013T075248.csv"

        # Verify _write_snapshot_file was called
        assert mock_write_snapshot.called
        assert mock_write_snapshot.call_count == 1

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
