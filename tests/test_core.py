"""
Test Core Framework Components
=============================

Unit tests for the core framework components including decorators,
plugin base classes, and configuration.
"""

from collections.abc import AsyncGenerator

import pytest

from fluxnet_shuttle_lib.core.base import NetworkPlugin
from fluxnet_shuttle_lib.core.config import NetworkConfig, ShuttleConfig
from fluxnet_shuttle_lib.core.decorators import async_to_sync, async_to_sync_generator
from fluxnet_shuttle_lib.core.exceptions import FLUXNETShuttleError, PluginError
from fluxnet_shuttle_lib.models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata


class MockNetworkPlugin(NetworkPlugin):
    """Mock plugin for testing."""

    @property
    def name(self) -> str:
        return "mock"

    @property
    def display_name(self) -> str:
        return "Mock Network"

    @async_to_sync_generator
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """Mock implementation that yields test data."""
        site_info = BadmSiteGeneralInfo(
            site_id="US-TEST", network="Mock", location_lat=40.0, location_long=-100.0, igbp="DBF"
        )

        product_data = DataFluxnetProduct(first_year=2020, last_year=2021, download_link="https://example.com/test.zip")

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


class TestNetworkPlugin:
    """Test cases for NetworkPlugin base class."""

    def test_plugin_properties(self):
        """Test plugin basic properties."""
        plugin = MockNetworkPlugin()

        assert plugin.name == "mock"
        assert plugin.display_name == "Mock Network"
        assert plugin.config == {}

    def test_plugin_with_config(self):
        """Test plugin initialization with config."""
        config = {"api_url": "https://test.com", "timeout": 60}
        plugin = MockNetworkPlugin(config=config)

        assert plugin.config == config
        assert plugin.config["api_url"] == "https://test.com"

    @pytest.mark.asyncio
    async def test_async_get_sites(self):
        """Test async get_sites method."""
        plugin = MockNetworkPlugin()

        sites = []
        async for site in plugin.get_sites():
            sites.append(site)

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-TEST"

    def test_sync_get_sites(self):
        """Test sync version of get_sites (created by decorator)."""
        plugin = MockNetworkPlugin()

        # The sync version should be available due to the decorator
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "US-TEST"


class TestShuttleConfig:
    """Test cases for ShuttleConfig."""

    def test_default_config_creation(self):
        """Test creating default configuration."""
        config = ShuttleConfig._create_default_config()

        assert config.parallel_requests == 2
        assert "ameriflux" in config.networks
        assert "icos" in config.networks

    def test_network_config_creation(self):
        """Test creating NetworkConfig."""
        net_config = NetworkConfig(enabled=True)

        assert net_config.enabled is True

    def test_load_from_file_not_found(self, tmp_path):
        """Test loading config from a non-existent file falls back to defaults."""
        config_path = tmp_path / "nonexistent.yaml"
        config = ShuttleConfig.load_from_file(config_path)

        assert config.parallel_requests == 2
        assert "ameriflux" in config.networks
        assert "icos" in config.networks

    def test_load_from_file_invalid_yaml(self, tmp_path):
        """Test loading config from an invalid YAML file falls back to defaults."""
        config_path = tmp_path / "invalid.yaml"
        config_path.write_text("::: invalid yaml :::")

        config = ShuttleConfig.load_from_file(config_path)

        assert config.parallel_requests == 2
        assert "ameriflux" in config.networks
        assert "icos" in config.networks

    def test_load_from_file_valid_yaml(self, tmp_path):
        """Test loading config from a valid YAML file."""
        config_path = tmp_path / "valid.yaml"
        config_path.write_text(
            """
            parallel_requests: 3
            networks:
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
        assert "ameriflux" in config.networks
        assert "icos" in config.networks
        assert "fluxnet2015" in config.networks


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
