"""
Tests for the command line interface (CLI) functionality.

This module tests the CLI entry points and basic functionality
to ensure the command-line tool works correctly.
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from unittest.mock import patch

import pytest

from fluxnet_shuttle.main import cmd_download, cmd_listall, cmd_sources, main, setup_logging


class TestCLIIntegration:
    """Integration tests for CLI commands (using subprocess to test actual CLI behavior)."""

    @pytest.mark.integration
    def test_cli_help(self):
        """Test that CLI help command works."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
sys.argv = ["fluxnet-shuttle", "--help"]
from fluxnet_shuttle.main import main
try:
    main()
except SystemExit as e:
    if e.code == 0:
        sys.exit(0)
    sys.exit(e.code)
""",
            ],
            capture_output=True,
            text=True,
        )

        # Help should exit with code 0 and contain usage information
        assert result.returncode == 0
        # The help text goes to stderr with argparse
        output = result.stdout + result.stderr
        assert "FLUXNET Shuttle Library" in output
        assert "positional arguments" in output or "command" in output.lower()

    @pytest.mark.integration
    def test_cli_import(self):
        """Test that CLI module can be imported."""
        result = subprocess.run(
            [sys.executable, "-c", 'from fluxnet_shuttle.main import main; print("CLI import successful")'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "CLI import successful" in result.stdout

    @pytest.mark.integration
    def test_cli_missing_command(self):
        """Test CLI behavior with missing command."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
sys.argv = ["fluxnet-shuttle"]
from fluxnet_shuttle.main import main
try:
    main()
except SystemExit as e:
    sys.exit(e.code)
""",
            ],
            capture_output=True,
            text=True,
        )

        # Should fail with missing command
        assert result.returncode == 2  # argparse error code
        output = result.stdout + result.stderr
        assert "required" in output or "COMMAND" in output

    @pytest.mark.integration
    def test_cli_invalid_command(self):
        """Test CLI behavior with invalid command."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
sys.argv = ["fluxnet-shuttle", "invalid"]
from fluxnet_shuttle.main import main
try:
    main()
except SystemExit as e:
    sys.exit(e.code)
""",
            ],
            capture_output=True,
            text=True,
        )

        # Should fail with invalid choice
        assert result.returncode == 2  # argparse error code
        output = result.stdout + result.stderr
        assert "invalid choice" in output or "unrecognized" in output

    @pytest.mark.integration
    def test_cli_listall_help(self):
        """Test that listall command help works."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
sys.argv = ["fluxnet-shuttle", "listall", "--help"]
from fluxnet_shuttle.main import main
try:
    main()
except SystemExit as e:
    if e.code == 0:
        sys.exit(0)
    sys.exit(e.code)
""",
            ],
            capture_output=True,
            text=True,
        )

        # Command help should work
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "listall" in output.lower() or "list" in output.lower()

    @pytest.mark.integration
    def test_cli_download_missing_args(self):
        """Test download command with missing required arguments."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import sys
sys.argv = ["fluxnet-shuttle", "download", "--no-logfile"]
from fluxnet_shuttle.main import main
try:
    main()
except SystemExit as e:
    sys.exit(e.code)
""",
            ],
            capture_output=True,
            text=True,
        )

        # Should fail due to missing required arguments
        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "No site IDs provided" in output or "No run file provided" in output


class TestCLIFunctions:
    """Unit tests for CLI functions (with proper mocking to avoid external calls)."""

    def test_setup_logging_default(self):
        """Test setup_logging with default parameters."""
        # Clear any existing handlers
        logger = logging.getLogger()
        logger.handlers.clear()

        setup_logging()

        # Should have one handler (stdout)
        assert len(logger.handlers) >= 1
        assert logger.level == logging.DEBUG

    def test_setup_logging_with_file(self):
        """Test setup_logging with file output."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            log_file = tmp.name

        try:
            # Clear any existing handlers
            logger = logging.getLogger()
            logger.handlers.clear()

            setup_logging(filename=log_file, level=logging.WARNING)

            # Should have handlers for both file and stdout
            assert len(logger.handlers) >= 1

            # Test that logging actually works
            test_logger = logging.getLogger("test")
            test_logger.warning("Test message")

            # Check file was created
            assert os.path.exists(log_file)

        finally:
            # Cleanup
            if os.path.exists(log_file):
                os.unlink(log_file)

    @patch("fluxnet_shuttle.main.listall")
    def test_cmd_listall_basic(self, mock_listall):
        """Test cmd_listall function."""
        mock_listall.return_value = [{"site_id": "US-Ha1", "network": "AmeriFlux", "data_url": "http://example.com"}]

        # Create mock args
        args = argparse.Namespace(output="test_output.csv", logfile="test.log", no_logfile=False, verbose=False)

        # Test the function
        cmd_listall(args)

        # Verify listall was called
        mock_listall.assert_called_once()

    @patch("fluxnet_shuttle.main.listall")
    def test_cmd_listall_no_logfile(self, mock_listall):
        """Test cmd_listall with no log file."""
        mock_listall.return_value = []

        args = argparse.Namespace(output="test_output.csv", logfile=None, no_logfile=True, verbose=True)

        cmd_listall(args)
        mock_listall.assert_called_once()

    @patch("fluxnet_shuttle.main.download")
    def test_cmd_download_with_sites_and_snapshot_file(self, mock_download):
        """Test cmd_download with both site IDs and snapshot file."""
        mock_download.return_value = []

        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp.write("site_id,network\nUS-Ha1,AmeriFlux\nUS-MMS,AmeriFlux\n")
            csv_file = tmp.name

        try:
            args = argparse.Namespace(
                sites=["US-Ha1", "US-MMS"],
                snapshot_file=csv_file,
                output_dir=".",
                logfile="test.log",
                no_logfile=False,
                verbose=False,
            )

            cmd_download(args)

            # Verify download was called with correct sites
            mock_download.assert_called_once()
            call_args = mock_download.call_args
            sites = call_args[1]["site_ids"]  # keyword argument
            assert "US-Ha1" in sites
            assert "US-MMS" in sites

        finally:
            os.unlink(csv_file)

    @patch("fluxnet_shuttle.main.download")
    def test_cmd_download_with_snapshot_file_only(self, mock_download):
        """Test cmd_download with snapshot file only (sites extracted from CSV)."""
        mock_download.return_value = []

        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp.write("site_id,network\nUS-Ha1,AmeriFlux\nUS-MMS,AmeriFlux\n")
            csv_file = tmp.name

        try:
            args = argparse.Namespace(
                sites=None,
                snapshot_file=csv_file,
                output_dir=".",
                quiet=True,
                logfile="test.log",
                no_logfile=False,
                verbose=False,
            )

            cmd_download(args)

            # Should have called download with sites from CSV
            mock_download.assert_called_once()
            call_args = mock_download.call_args
            sites = call_args[1]["site_ids"]  # keyword argument
            assert "US-Ha1" in sites
            assert "US-MMS" in sites

        finally:
            os.unlink(csv_file)

    def test_cmd_download_sites_without_snapshot_file(self):
        """Test cmd_download with sites but no snapshot file."""
        args = argparse.Namespace(
            sites=["US-Ha1", "US-MMS"],
            snapshot_file=None,
            output_dir=".",
            logfile="test.log",
            no_logfile=False,
            verbose=False,
        )

        # Should raise SystemExit due to missing snapshot file
        with pytest.raises(SystemExit) as exc_info:
            cmd_download(args)
        assert exc_info.value.code == 1

    def test_cmd_download_no_sites_or_snapshot_file(self):
        """Test cmd_download with no sites or snapshot file."""
        args = argparse.Namespace(
            sites=None,
            snapshot_file=None,
            output_dir=".",
            quiet=True,
            logfile="test.log",
            no_logfile=False,
            verbose=False,
        )

        # Should raise SystemExit due to no input
        with pytest.raises(SystemExit) as exc_info:
            cmd_download(args)
        assert exc_info.value.code == 1

    def test_cmd_download_csv_no_site_id_column(self):
        """Test cmd_download with CSV file missing site_id column."""
        # Create a temporary CSV file without site_id column
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp.write("name,network\nTest Site,AmeriFlux\n")
            csv_file = tmp.name

        try:
            args = argparse.Namespace(
                sites=None,
                snapshot_file=csv_file,
                output_dir=".",
                quiet=True,
                logfile="test.log",
                no_logfile=False,
                verbose=False,
            )

            with pytest.raises(SystemExit) as exc_info:
                cmd_download(args)
            assert exc_info.value.code == 1

        finally:
            os.unlink(csv_file)

    def test_cmd_download_invalid_snapshot_file(self):
        """Test cmd_download with invalid snapshot file."""
        args = argparse.Namespace(
            sites=None,
            snapshot_file="nonexistent.csv",
            output_dir=".",
            quiet=True,
            logfile="test.log",
            no_logfile=False,
            verbose=False,
        )

        # Should raise SystemExit due to missing file
        with pytest.raises(SystemExit) as exc_info:
            cmd_download(args)
        assert exc_info.value.code == 1

    def test_cmd_sources(self):
        """Test cmd_sources function."""
        args = argparse.Namespace(logfile="test.log", no_logfile=False, verbose=False)

        # Should not raise any exception
        cmd_sources(args)

    def test_main_with_version(self):
        """Test main function with --version argument."""
        test_args = ["fluxnet-shuttle", "--version"]

        with patch("sys.argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    @patch("fluxnet_shuttle.main.cmd_listall")
    def test_main_with_listall_command(self, mock_cmd):
        """Test main function with listall command."""
        test_args = ["fluxnet-shuttle", "--no-logfile", "listall"]

        with patch("sys.argv", test_args):
            main()
            mock_cmd.assert_called_once()

    @patch("fluxnet_shuttle.main.cmd_download")
    def test_main_with_download_command(self, mock_cmd):
        """Test main function with download command."""
        test_args = ["fluxnet-shuttle", "--no-logfile", "download", "-f", "test.csv", "-s", "US-Ha1"]

        with patch("sys.argv", test_args):
            main()
            mock_cmd.assert_called_once()

    @patch("fluxnet_shuttle.main.cmd_sources")
    def test_main_with_sources_command(self, mock_cmd):
        """Test main function with sources command."""
        test_args = ["fluxnet-shuttle", "--no-logfile", "sources"]

        with patch("sys.argv", test_args):
            main()
            mock_cmd.assert_called_once()

    def test_main_unknown_command_error(self):
        """Test main function with unknown command (should not happen due to argparse)."""
        # This tests the else clause in the command dispatch
        test_args = ["fluxnet-shuttle", "unknown", "--no-logfile"]

        with patch("sys.argv", test_args):
            # Mock argparse to allow unknown command to pass through
            with patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_args = argparse.Namespace(
                    command="unknown", logfile=None, no_logfile=True, verbose=False, version=False
                )
                mock_parse.return_value = mock_args

                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1

    @patch("fluxnet_shuttle.main.cmd_sources")
    def test_main_fluxnet_shuttle_error(self, mock_cmd):
        """Test main function error handling for FLUXNETShuttleError."""
        from fluxnet_shuttle import FLUXNETShuttleError

        mock_cmd.side_effect = FLUXNETShuttleError("Test error")

        test_args = ["fluxnet-shuttle", "--no-logfile", "sources"]

        with patch("sys.argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("fluxnet_shuttle.main.cmd_sources")
    def test_main_unexpected_error(self, mock_cmd):
        """Test main function error handling for unexpected exceptions."""
        mock_cmd.side_effect = RuntimeError("Unexpected error")

        test_args = ["fluxnet-shuttle", "--no-logfile", "sources"]

        with patch("sys.argv", test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_cmd_download_csv_read_error(self):
        """Test cmd_download with CSV file that causes read error."""
        # Create a temporary file that's not readable (permission error)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp.write("site_id,network\nUS-Ha1,AmeriFlux\n")
            csv_file = tmp.name

        try:
            # Make file unreadable
            os.chmod(csv_file, 0o000)

            args = argparse.Namespace(
                sites=None,
                snapshot_file=csv_file,
                output_dir=".",
                quiet=True,
                logfile="test.log",
                no_logfile=False,
                verbose=False,
            )

            with pytest.raises(SystemExit) as exc_info:
                cmd_download(args)
            assert exc_info.value.code == 1

        finally:
            # Restore permissions and delete
            os.chmod(csv_file, 0o644)
            os.unlink(csv_file)

    def test_validate_output_directory_not_writable(self):
        """Test _validate_output_directory with non-writable directory."""
        import tempfile

        from fluxnet_shuttle.main import _validate_output_directory

        # Create a temp directory and make it read-only
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "readonly")
            os.makedirs(test_dir)
            os.chmod(test_dir, 0o444)  # Read-only

            try:
                with pytest.raises(SystemExit) as exc_info:
                    _validate_output_directory(test_dir)
                assert exc_info.value.code == 1
            finally:
                # Restore permissions for cleanup
                os.chmod(test_dir, 0o755)

    def test_validate_output_directory_does_not_exist(self):
        """Test _validate_output_directory with non-existent directory."""
        from fluxnet_shuttle.main import _validate_output_directory

        with pytest.raises(SystemExit) as exc_info:
            _validate_output_directory("/nonexistent/path/that/does/not/exist")
        assert exc_info.value.code == 1

    def test_validate_output_directory_is_file(self):
        """Test _validate_output_directory with a file path instead of directory."""
        import tempfile

        from fluxnet_shuttle.main import _validate_output_directory

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_file = tmp.name

        try:
            with pytest.raises(SystemExit) as exc_info:
                _validate_output_directory(tmp_file)
            assert exc_info.value.code == 1
        finally:
            os.unlink(tmp_file)

    def test_cmd_download_input_confirmation_no(self):
        """Test cmd_download with user declining confirmation."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            tmp.write("site_id,network\nUS-Ha1,AmeriFlux\n")
            csv_file = tmp.name

        try:
            args = argparse.Namespace(
                sites=None,
                snapshot_file=csv_file,
                output_dir=".",
                quiet=False,  # Don't skip confirmation
                logfile="test.log",
                no_logfile=False,
                verbose=False,
            )

            # Mock input to return 'n' (no)
            with patch("builtins.input", return_value="n"):
                with pytest.raises(SystemExit) as exc_info:
                    cmd_download(args)
                assert exc_info.value.code == 0
        finally:
            os.unlink(csv_file)

    def test_cmd_listall_with_output_dir(self):
        """Test cmd_listall with custom output directory."""
        import tempfile

        from fluxnet_shuttle.main import cmd_listall

        with tempfile.TemporaryDirectory() as tmpdir:
            args = argparse.Namespace(output_dir=tmpdir, logfile="test.log", no_logfile=False, verbose=False)

            with patch("fluxnet_shuttle.main.listall") as mock_listall:
                mock_listall.return_value = os.path.join(tmpdir, "test.csv")
                result = cmd_listall(args)
                assert tmpdir in result
                mock_listall.assert_called_once()

    def test_cmd_sources_no_plugins(self):
        """Test cmd_sources when no plugins are registered."""
        from fluxnet_shuttle.main import cmd_sources

        args = argparse.Namespace(logfile="test.log", no_logfile=False, verbose=False)

        # Mock registry to return empty list
        with patch("fluxnet_shuttle.core.registry.registry.list_plugins", return_value=[]):
            # Should not raise exception, just log warning
            cmd_sources(args)

    def test_version_import_error(self):
        """Test version fallback when package is not found."""
        # We need to test the module-level import, which is tricky
        # We'll patch the version function to raise PackageNotFoundError
        import sys
        from importlib.metadata import PackageNotFoundError

        # Save original module
        original_module = sys.modules.get("fluxnet_shuttle.main")

        # Remove module from cache to force reimport
        if "fluxnet_shuttle.main" in sys.modules:
            del sys.modules["fluxnet_shuttle.main"]

        with patch("importlib.metadata.version", side_effect=PackageNotFoundError):
            # Import the module to trigger the exception handler
            import fluxnet_shuttle.main as main_module

            # After import, __version__ should be "unknown"
            assert main_module.__version__ == "unknown"

        # Restore original module
        if original_module:
            sys.modules["fluxnet_shuttle.main"] = original_module
