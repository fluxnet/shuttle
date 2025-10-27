"""Test suite for fluxnet_shuttle.__init__ module."""

import logging
import os
import tempfile
import warnings
from unittest.mock import MagicMock, patch

import pytest

from fluxnet_shuttle import (
    LOG_DATEFMT,
    LOG_FMT,
    LOG_LEVELS,
    VERSION,
    FLUXNETShuttleError,
    __version__,
    add_file_log,
    format_warning,
    log_config,
    log_trace,
)


class TestFLUXNETShuttleError:
    """Test cases for FLUXNETShuttleError exception."""

    def test_fluxnet_shuttle_error_creation(self):
        """Test that FLUXNETShuttleError can be created and raised."""
        with pytest.raises(FLUXNETShuttleError):
            raise FLUXNETShuttleError("Test error message")

    def test_fluxnet_shuttle_error_message(self):
        """Test that error message is preserved."""
        message = "Custom error message"
        try:
            raise FLUXNETShuttleError(message)
        except FLUXNETShuttleError as e:
            assert str(e) == message

    def test_fluxnet_shuttle_error_inheritance(self):
        """Test that FLUXNETShuttleError inherits from Exception."""
        error = FLUXNETShuttleError("test")
        assert isinstance(error, Exception)


class TestVersionConstants:
    """Test cases for version and metadata constants."""

    def test_version_exists(self):
        """Test that VERSION constant exists."""
        assert VERSION is not None
        assert isinstance(VERSION, str)

    def test_version_format(self):
        """Test that version follows expected format."""
        assert VERSION == "0.1.0"

    def test_dunder_version_exists(self):
        """Test that __version__ exists."""
        assert __version__ is not None
        assert __version__ == VERSION


class TestLogConfig:
    """Test logging configuration functionality."""

    def test_log_config_exists(self):
        """Test that log_config function exists."""
        assert callable(log_config)

    def test_log_config_runs_without_error(self):
        """Test that log_config runs without error."""
        try:
            log_config(std=False, filename=None)
        except Exception as e:
            pytest.fail(f"log_config raised an exception: {e}")

    def test_log_config_with_parameters(self):
        """Test log_config with different parameter combinations."""
        # Test with invalid level types
        log_config(level="invalid", filename_level="invalid", std_level="invalid")

    def test_log_config_with_file_logging(self):
        """Test log_config with file logging enabled."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            log_config(filename=tmp_path, filename_level=logging.INFO)
            assert os.path.exists(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_log_config_with_std_logging(self):
        """Test log_config with standard output logging."""
        log_config(std=True, std_level=logging.DEBUG)

    def test_log_constants(self):
        """Test logging constants."""
        assert LOG_DATEFMT == "%Y-%m-%d %H:%M:%S"
        assert "%(asctime)s" in LOG_FMT
        assert isinstance(LOG_LEVELS, dict)
        assert 50 in LOG_LEVELS  # CRITICAL
        assert 10 in LOG_LEVELS  # DEBUG


class TestAddFileLog:
    """Test add_file_log functionality."""

    def test_add_file_log_exists(self):
        """Test that add_file_log function exists."""
        assert callable(add_file_log)

    def test_add_file_log_with_existing_logger(self):
        """Test adding file log to existing logger."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Test with different level types
            add_file_log(tmp_path, level="invalid")
            assert os.path.exists(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestLogTrace:
    """Test log_trace functionality."""

    def test_log_trace_exists(self):
        """Test that log_trace function exists."""
        assert callable(log_trace)

    def test_log_trace_with_exception(self):
        """Test that log_trace can handle exceptions."""
        try:
            raise ValueError("Test exception")
        except Exception as e:
            result = log_trace(e)
            assert isinstance(result, str)
            assert "Test exception" in result

    def test_log_trace_with_none(self):
        """Test log_trace with None input."""
        result = log_trace(None)
        assert result is not None

    def test_log_trace_with_invalid_logger(self):
        """Test log_trace with invalid logger parameter."""
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            # Pass a non-Logger object to trigger error handling
            result = log_trace(e, log="not_a_logger")
            assert isinstance(result, str)

    def test_log_trace_with_alt_format(self):
        """Test log_trace with alternative output format."""
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            result = log_trace(e, output_fmt="alt")
            assert isinstance(result, str)
            assert "Trace for" in result

    def test_log_trace_with_std_format(self):
        """Test log_trace with standard output format."""
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            result = log_trace(e, output_fmt="std")
            assert isinstance(result, str)

    def test_log_trace_exception_during_formatting(self):
        """Test log_trace when an exception occurs during trace formatting."""
        # Create a mock exception that will cause an error during formatting
        with patch("sys.exc_info") as mock_exc_info:
            mock_exc_info.side_effect = RuntimeError("Mock formatting error")

            try:
                raise ValueError("Test exception")
            except ValueError as e:
                result = log_trace(e)
                assert isinstance(result, str)
                assert "Trace not generated" in result


class TestFormatWarning:
    """Test warning formatting functionality."""

    def test_format_warning_exists(self):
        """Test that format_warning function exists."""
        assert callable(format_warning)

    def test_format_warning_basic(self):
        """Test basic warning formatting."""
        with warnings.catch_warnings(record=True):
            warnings.warn("Test warning", UserWarning)

    def test_format_warning_with_special_chars(self):
        """Test warning formatting with special characters."""
        with patch("logging.getLogger") as mock_logger:
            mock_warning_logger = MagicMock()
            mock_logger.return_value = mock_warning_logger
            mock_warning_logger.handlers = []

            format_warning(
                "Test\nmessage\rwith\nchars",
                UserWarning,
                "test.py",
                123,
                None,
                "test line",
            )

            mock_warning_logger.addHandler.assert_called_once()


class TestModuleImports:
    """Test cases for module imports and structure."""

    def test_shuttle_functions_imported(self):
        """Test that shuttle functions are properly imported."""
        from fluxnet_shuttle import download, listall

        assert callable(download)
        assert callable(listall)

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        from fluxnet_shuttle import __all__

        expected_exports = [
            "download",
            "listall",
            "FLUXNETShuttleError",
            "log_config",
            "add_file_log",
            "log_trace",
        ]
        for export in expected_exports:
            assert export in __all__


class TestBasicFunctionality:
    """Test basic functionality without complex mocking."""

    def test_exception_creation(self):
        """Test creating custom exception."""
        error = FLUXNETShuttleError("test message")
        assert str(error) == "test message"

    def test_version_constants(self):
        """Test version constants."""
        assert VERSION
        assert __version__
        assert VERSION == __version__

    def test_function_existence(self):
        """Test that all expected functions exist."""
        functions = [log_config, add_file_log, log_trace, format_warning]
        for func in functions:
            assert callable(func)
