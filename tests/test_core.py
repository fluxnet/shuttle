"""
Test Core Framework Components
=============================

Unit tests for the core framework components including decorators,
plugin base classes, and configuration.
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from fluxnet_shuttle.core.base import DataHubPlugin
from fluxnet_shuttle.core.config import DataHubConfig, ShuttleConfig
from fluxnet_shuttle.core.decorators import async_to_sync, async_to_sync_generator
from fluxnet_shuttle.core.exceptions import FLUXNETShuttleError, PluginError
from fluxnet_shuttle.models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata


class MockDataHubPlugin(DataHubPlugin):
    """Mock plugin for testing."""

    @property
    def name(self) -> str:
        return "mock"

    @property
    def display_name(self) -> str:
        return "Mock Data Hub"

    @async_to_sync_generator
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """Mock implementation that yields test data."""
        site_info = BadmSiteGeneralInfo(
            site_id="US-TEST",
            site_name="Test Site",
            data_hub="Mock",
            location_lat=40.0,
            location_long=-100.0,
            igbp="DBF",
        )

        product_data = DataFluxnetProduct(
            first_year=2020,
            last_year=2021,
            download_link="https://example.com/test.zip",
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="TEST",
            fluxnet_product_name="TEST_US-TEST_FLUXNET_2020-2021_v1_r0.zip",
        )

        yield FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)


class TestAsyncToSyncDecorator:
    """Test cases for async_to_sync decorator."""

    def test_async_to_sync_function(self):
        """Test that async_to_sync correctly converts async generator to sync."""

        @async_to_sync
        async def sample_async():
            return 0

        result = sample_async()
        assert result == 0

    def test_async_to_sync_invalid_usage(self):
        """Test that applying async_to_sync to a non-async function raises an error."""

        with pytest.raises(TypeError, match="The async_to_sync decorator can only be applied to async functions."):

            @async_to_sync
            def invalid_function():
                return 0

    @pytest.mark.asyncio
    async def test_async_to_sync_generator(self):
        """Test that async_to_sync_generator correctly converts async generator to sync generator."""

        class SampleClass:

            @async_to_sync_generator
            async def sample_async_gen(self):
                for i in range(3):
                    yield i

        result = [i async for i in SampleClass().sample_async_gen()]
        assert result == [0, 1, 2]

    def test_async_to_sync_generator_for_sync(self):
        """Test that async_to_sync_generator correctly converts async generator to sync generator."""

        class SampleClass:

            @async_to_sync_generator
            async def sample_async_gen(self):
                for i in range(3):
                    yield i

        result = [i for i in SampleClass().sample_async_gen()]
        assert result == [0, 1, 2]


class TestDataHubPlugin:
    """Test cases for DataHubPlugin base class."""

    def test_plugin_properties(self):
        """Test plugin basic properties."""
        plugin = MockDataHubPlugin()

        assert plugin.name == "mock"
        assert plugin.display_name == "Mock Data Hub"
        assert plugin.config == {}

    def test_plugin_with_config(self):
        """Test plugin initialization with config."""
        config = {"api_url": "https://test.com", "timeout": 60}
        plugin = MockDataHubPlugin(config=config)

        assert plugin.config == config
        assert plugin.config["api_url"] == "https://test.com"

    @pytest.mark.asyncio
    async def test_async_get_sites(self):
        """Test async get_sites method."""
        plugin = MockDataHubPlugin()

        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-TEST"

    def test_sync_get_sites(self):
        """Test sync version of get_sites (created by decorator)."""
        plugin = MockDataHubPlugin()

        # The sync version should be available due to the decorator
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-TEST"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    async def test_session_request_success(self, mock_session_request):
        """Test successful _session_request call."""
        plugin = MockDataHubPlugin()
        url = "https://api.example.com/data"

        # Mock the response
        mock_response = AsyncMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status.return_value = None

        # Mock the session_request context manager
        mock_session_request.return_value.__aenter__.return_value = mock_response
        mock_session_request.return_value.__aexit__.return_value = None

        async with plugin._session_request("GET", url) as response:
            data = await response.json()
            assert data["data"] == "test"

        mock_session_request.assert_called_once_with("GET", url)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    async def test_session_request_client_error(self, mock_session_request):
        """Test _session_request handling of aiohttp.ClientError."""
        plugin = MockDataHubPlugin()
        url = "https://api.example.com/data"

        # Make session_request raise a ClientError when entered
        mock_session_request.return_value.__aenter__.side_effect = aiohttp.ClientConnectionError("Connection failed")

        with pytest.raises(PluginError) as exc_info:
            async with plugin._session_request("GET", url) as response:  # noqa: F841
                pass

        error = exc_info.value
        assert error.plugin_name == "mock"
        assert "Failed to make HTTP request" in error.message
        assert isinstance(error.original_error, aiohttp.ClientConnectionError)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    async def test_session_request_unexpected_error(self, mock_session_request):
        """Test _session_request handling of unexpected errors."""
        plugin = MockDataHubPlugin()
        url = "https://api.example.com/data"

        # Make session_request raise a generic exception
        mock_session_request.side_effect = ValueError("Unexpected error")

        with pytest.raises(PluginError) as exc_info:
            async with plugin._session_request("GET", url) as response:  # noqa: F841
                pass

        error = exc_info.value
        assert error.plugin_name == "mock"
        assert "Unexpected error during HTTP request" in error.message
        assert isinstance(error.original_error, ValueError)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.base.session_request")
    async def test_default_download_file(self, mock_session_request):
        """Test default download_stream implementation in base class."""
        plugin = MockDataHubPlugin()
        download_link = "https://example.com/file.zip"

        # Mock the response with content
        mock_response = AsyncMock()
        mock_response.content = b"test file content"
        mock_session_request.return_value.__aenter__.return_value = mock_response
        mock_session_request.return_value.__aexit__.return_value = None

        # Test the default download_stream implementation
        async with plugin.download_file("US-TEST", download_link, filename="test.zip") as content:
            assert content == b"test file content"

        # Verify GET request was made to download link
        mock_session_request.assert_called_once_with("GET", download_link)


class TestShuttleConfig:
    """Test cases for ShuttleConfig."""

    def test_default_config_creation(self):
        """Test creating default configuration."""
        config = ShuttleConfig._create_default_config()

        assert config.parallel_requests == 2
        assert "ameriflux" in config.data_hubs
        assert "icos" in config.data_hubs

    def test_data_hub_config_creation(self):
        """Test creating DataHubConfig."""
        hub_config = DataHubConfig(enabled=True)

        assert hub_config.enabled is True

    def test_load_from_file_not_found(self, tmp_path):
        """Test loading config from a non-existent file falls back to defaults."""
        config_path = tmp_path / "nonexistent.yaml"
        config = ShuttleConfig.load_from_file(config_path)

        assert config.parallel_requests == 2
        assert "ameriflux" in config.data_hubs
        assert "icos" in config.data_hubs

    def test_load_from_file_invalid_yaml(self, tmp_path):
        """Test loading config from an invalid YAML file falls back to defaults."""
        config_path = tmp_path / "invalid.yaml"
        config_path.write_text("::: invalid yaml :::")

        config = ShuttleConfig.load_from_file(config_path)

        assert config.parallel_requests == 2
        assert "ameriflux" in config.data_hubs
        assert "icos" in config.data_hubs

    def test_load_from_file_valid_yaml(self, tmp_path):
        """Test loading config from a valid YAML file."""
        config_path = tmp_path / "valid.yaml"
        config_path.write_text(
            """
            parallel_requests: 3
            data_hubs:
              ameriflux:
                enabled: true
              icos:
                enabled: true
              fluxnet2015:
                enabled: false
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        assert config.parallel_requests == 3
        assert "ameriflux" in config.data_hubs
        assert "icos" in config.data_hubs
        assert "fluxnet2015" in config.data_hubs

    def test_data_hub_config_with_user_info(self):
        """Test DataHubConfig with user_info field."""
        user_info = {
            "user_name": "Test User",
            "user_email": "test@example.com",
            "intended_use": 1,
            "description": "Test description",
        }
        hub_config = DataHubConfig(enabled=True, user_info=user_info)

        assert hub_config.enabled is True
        assert hub_config.user_info == user_info
        assert hub_config.user_info["user_name"] == "Test User"

    def test_load_from_yaml_with_user_info(self, tmp_path):
        """Test loading YAML config with user_info."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
            data_hubs:
              ameriflux:
                enabled: true
                user_info:
                  user_name: Test User
                  user_email: test@example.com
                  intended_use: 1
              icos:
                enabled: true
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        assert "ameriflux" in config.data_hubs
        assert config.data_hubs["ameriflux"].user_info["user_name"] == "Test User"
        assert config.data_hubs["ameriflux"].user_info["intended_use"] == 1
        assert "icos" in config.data_hubs
        assert config.data_hubs["icos"].user_info == {}

    @patch.dict("os.environ", {"FLUXNET_SHUTTLE_CONFIG": ""})
    def test_get_user_config_path_not_found(self):
        """Test get_user_config_path when no config file exists."""
        config_path = ShuttleConfig.get_user_config_path()
        assert config_path is None

    def test_get_user_config_path_env_file_not_found(self):
        """Test get_user_config_path when env var points to non-existent file."""
        with patch.dict("os.environ", {"FLUXNET_SHUTTLE_CONFIG": "/nonexistent/config.yaml"}):
            config_path = ShuttleConfig.get_user_config_path()
            assert config_path is None

    @patch.dict("os.environ", {"FLUXNET_SHUTTLE_CONFIG": "/tmp/custom_config.yaml"})
    def test_get_user_config_path_from_env(self, tmp_path):
        """Test get_user_config_path using environment variable."""
        config_file = tmp_path / "custom_config.yaml"
        config_file.write_text("data_hubs: {}")

        with patch.dict("os.environ", {"FLUXNET_SHUTTLE_CONFIG": str(config_file)}):
            config_path = ShuttleConfig.get_user_config_path()
            assert config_path == config_file

    def test_load_user_config_no_file(self):
        """Test load_user_config when no user config file exists."""
        with patch.object(ShuttleConfig, "get_user_config_path", return_value=None):
            config = ShuttleConfig.load_user_config()
            # Should fall back to default config
            assert "ameriflux" in config.data_hubs

    def test_load_user_config_with_file(self, tmp_path):
        """Test load_user_config when user config file exists."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
            data_hubs:
              ameriflux:
                enabled: true
                user_info:
                  user_name: "Config User"
            """
        )

        with patch.object(ShuttleConfig, "get_user_config_path", return_value=config_file):
            config = ShuttleConfig.load_user_config()
            assert config.data_hubs["ameriflux"].user_info["user_name"] == "Config User"

    def test_load_from_yaml_invalid_user_info_type(self, tmp_path):
        """Test loading YAML config with invalid user_info type."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
            data_hubs:
              ameriflux:
                enabled: true
                user_info: "invalid_type"
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        # Should handle gracefully and set empty user_info
        assert "ameriflux" in config.data_hubs
        assert config.data_hubs["ameriflux"].user_info == {}

    def test_load_from_yaml_invalid_data_hub_type(self, tmp_path):
        """Test loading YAML config with invalid data hub type (not a dict)."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            """
            data_hubs:
              ameriflux: "invalid_string_value"
              icos:
                enabled: true
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        # Should skip invalid ameriflux config but load icos
        assert "icos" in config.data_hubs
        assert config.data_hubs["icos"].enabled is True

    def test_user_config_extends_shuttle_config(self, tmp_path):
        """Test that user config extends shuttle config without overwriting it.

        This test verifies that when a user provides only user_info in their config,
        the enabled status from the shuttle config is preserved.
        """
        config_path = tmp_path / "user_config.yaml"
        config_path.write_text(
            """
            data_hubs:
              ameriflux:
                user_info:
                  user_name: "Test User"
                  user_email: "test@example.com"
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        # Verify that ameriflux is enabled (from shuttle config)
        assert config.data_hubs["ameriflux"].enabled is True
        # Verify that user_info is populated (from user config)
        assert config.data_hubs["ameriflux"].user_info["user_name"] == "Test User"
        assert config.data_hubs["ameriflux"].user_info["user_email"] == "test@example.com"

    def test_user_config_can_override_enabled(self, tmp_path):
        """Test that user config can explicitly override enabled status."""
        config_path = tmp_path / "user_config.yaml"
        config_path.write_text(
            """
            data_hubs:
              icos:
                enabled: false
            """
        )

        config = ShuttleConfig.load_from_file(config_path)

        # Verify that icos is disabled (overridden by user config)
        assert config.data_hubs["icos"].enabled is False

    def test_user_config_merges_user_info(self, tmp_path):
        """Test that user_info fields are properly merged between configs."""
        # First create a shuttle config with some user_info
        shuttle_config_path = tmp_path / "shuttle_config.yaml"
        shuttle_config_path.write_text(
            """
            data_hubs:
              ameriflux:
                enabled: true
                user_info:
                  default_field: "default_value"
            """
        )

        # Load shuttle config as default
        with patch("importlib.resources.read_text", return_value=shuttle_config_path.read_text()):
            # Now create a user config that adds more user_info
            user_config_path = tmp_path / "user_config.yaml"
            user_config_path.write_text(
                """
                data_hubs:
                  ameriflux:
                    user_info:
                      user_name: "Test User"
                      user_email: "test@example.com"
                """
            )

            config = ShuttleConfig.load_from_file(user_config_path)

            # Verify both shuttle and user config user_info fields are present
            # Note: In the current implementation, shuttle config doesn't have user_info
            # so this test mainly verifies the user config user_info is properly loaded
            assert config.data_hubs["ameriflux"].user_info["user_name"] == "Test User"
            assert config.data_hubs["ameriflux"].user_info["user_email"] == "test@example.com"


class TestExceptions:
    """Test cases for custom exceptions."""

    def test_fluxnet_shuttle_error(self):
        """Test FLUXNETShuttleError creation."""
        error = FLUXNETShuttleError("Test error message")

        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.details == {}

    def test_fluxnet_shuttle_error_with_details(self):
        """Test FLUXNETShuttleError with details."""
        details = {"code": 500, "source": "test"}
        error = FLUXNETShuttleError("Test error", details=details)

        assert error.details == details
        assert error.details["code"] == 500

    def test_plugin_error(self):
        """Test PluginError creation."""
        original_error = ValueError("Original error")
        plugin_error = PluginError("test_plugin", "Plugin failed", original_error)

        assert "Plugin 'test_plugin': Plugin failed" in str(plugin_error)
        assert plugin_error.plugin_name == "test_plugin"
        assert plugin_error.original_error == original_error
