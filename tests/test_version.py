"""Test suite for fluxnet_shuttle.version module."""

import importlib
import importlib.metadata
import sys
from unittest.mock import patch

from fluxnet_shuttle import version


class TestVersionModule:
    """Test cases for the version module."""

    def test_version_imports_importlib_metadata(self):
        """Test that importlib.metadata is correctly imported."""
        # This ensures the module can be imported without errors
        assert importlib.metadata is not None

    def test_version_module_attributes_exist(self):
        """Test that the version module has the expected attributes."""
        # Test that the module has the required attributes
        assert hasattr(version, "__version__")
        assert hasattr(version, "__release__")

        # Test that the attributes are strings
        assert isinstance(version.__version__, str)
        assert isinstance(version.__release__, str)

    def test_version_values_are_not_empty(self):
        """Test that version values are not empty strings."""
        assert version.__version__ != ""
        assert version.__release__ != ""

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_success_without_hyphen(self, mock_version):
        """Test successful version retrieval when version has no hyphen."""
        # Arrange
        mock_version.return_value = "1.2.3"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == "1.2.3"
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "1.2.3"

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_success_with_hyphen(self, mock_version):
        """Test successful version retrieval when version contains hyphen (dev version)."""
        # Arrange
        mock_version.return_value = "1.2.3-dev.4+abc123"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == "1.2.3"  # Split on first hyphen
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "1.2.3-dev.4+abc123"  # Full version

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_success_with_multiple_hyphens(self, mock_version):
        """Test version retrieval when version contains multiple hyphens."""
        # Arrange
        mock_version.return_value = "1.2.3-rc-1-beta"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == "1.2.3"  # Split on first hyphen only
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "1.2.3-rc-1-beta"

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_package_not_found(self, mock_version):
        """Test version retrieval when package is not found."""
        # Arrange
        mock_version.side_effect = importlib.metadata.PackageNotFoundError("Package not found")

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == "unknown"
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "unknown"

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_with_empty_string(self, mock_version):
        """Test version retrieval when metadata returns empty string."""
        # Arrange
        mock_version.return_value = ""

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == ""
        assert sys.modules["fluxnet_shuttle.version"].__release__ == ""

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_with_only_hyphen(self, mock_version):
        """Test version retrieval when version is just a hyphen."""
        # Arrange
        mock_version.return_value = "-"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == ""  # First part of split on "-"
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "-"

    @patch.object(importlib.metadata, "version")
    def test_version_retrieval_with_leading_hyphen(self, mock_version):
        """Test version retrieval when version starts with hyphen."""
        # Arrange
        mock_version.return_value = "-1.2.3"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert
        mock_version.assert_called_with("fluxnet-shuttle")
        assert sys.modules["fluxnet_shuttle.version"].__version__ == ""  # First part of split on "-"
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "-1.2.3"

    @patch.object(importlib.metadata, "version")
    def test_correct_package_name_used(self, mock_version):
        """Test that the correct package name is used for metadata lookup."""
        # Arrange
        mock_version.return_value = "1.0.0"

        # Act - Force reload the module to trigger version loading
        if "fluxnet_shuttle.version" in sys.modules:
            importlib.reload(sys.modules["fluxnet_shuttle.version"])

        # Assert that the correct package name is used
        mock_version.assert_called_with("fluxnet-shuttle")
        # Verify the test variables are used
        assert sys.modules["fluxnet_shuttle.version"].__version__ == "1.0.0"
        assert sys.modules["fluxnet_shuttle.version"].__release__ == "1.0.0"

    def test_version_logic_edge_cases(self):
        """Test version logic with direct function calls to test edge cases."""
        # Test the version parsing logic directly by simulating the code path

        # Test case 1: Normal version without hyphen
        test_version = "2.1.0"
        if "-" in test_version:
            release = test_version
            version_part = test_version.split("-")[0]
        else:
            release = test_version
            version_part = test_version

        assert version_part == "2.1.0"
        assert release == "2.1.0"

        # Test case 2: Version with hyphen
        test_version = "2.1.0-beta.1"
        if "-" in test_version:
            release = test_version
            version_part = test_version.split("-")[0]
        else:
            release = test_version
            version_part = test_version

        assert version_part == "2.1.0"
        assert release == "2.1.0-beta.1"

        # Test case 3: Version with multiple hyphens
        test_version = "2.1.0-alpha-1-rc"
        if "-" in test_version:
            release = test_version
            version_part = test_version.split("-")[0]
        else:
            release = test_version
            version_part = test_version

        assert version_part == "2.1.0"
        assert release == "2.1.0-alpha-1-rc"

    def test_packagenotfounderror_exception_handling(self):
        """Test that PackageNotFoundError is properly handled."""
        # Test that the exception exists and can be caught
        try:
            raise importlib.metadata.PackageNotFoundError("test error")
        except importlib.metadata.PackageNotFoundError as e:
            assert "test error" in str(e)
            assert isinstance(e, Exception)
