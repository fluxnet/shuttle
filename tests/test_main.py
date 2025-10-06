"""Test suite for fluxnet_shuttle_lib.main module."""

import pytest

from fluxnet_shuttle_lib.main import main


class TestMain:
    """Test cases for the main function."""

    def test_main_function_exists(self):
        """Test that the main function exists and is callable."""
        assert callable(main)

    def test_main_function_runs_without_error(self):
        """Test that the main function runs without raising any exceptions."""
        try:
            result = main()
            # The function should return None
            assert result is None
        except Exception as e:
            pytest.fail(f"main() raised an exception: {e}")

    def test_main_function_returns_none(self):
        """Test that the main function returns None."""
        result = main()
        assert result is None
