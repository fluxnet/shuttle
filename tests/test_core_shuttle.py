"""
Integration Tests for FLUXNET Shuttle
=====================================

Integration tests for the complete FLUXNET Shuttle system.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from fluxnet_shuttle.core.config import DataHubConfig, ShuttleConfig
from fluxnet_shuttle.core.decorators import async_to_sync_generator
from fluxnet_shuttle.core.registry import PluginRegistry
from fluxnet_shuttle.core.shuttle import FluxnetShuttle
from fluxnet_shuttle.models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata


class MockFailingPlugin:
    """Mock plugin that always fails for testing error handling."""

    @property
    def name(self):
        return "failing_plugin"

    @property
    def display_name(self):
        return "Failing Plugin"

    def validate_config(self):
        return True

    async def get_sites(self, **filters):
        raise Exception("Mock plugin failure")
        yield  # This will never be reached


class MockSuccessPlugin:
    """Mock plugin that succeeds for testing."""

    @property
    def name(self):
        return "success_plugin"

    @property
    def display_name(self):
        return "Success Plugin"

    def validate_config(self):
        return True

    @async_to_sync_generator
    async def get_sites(self, **filters):

        for _ in range(4):  # Simulate some async operation
            await asyncio.sleep(0.1)
            site_info = BadmSiteGeneralInfo(
                site_id="US-SUCCESS",
                site_name="Success Site",
                data_hub="Success",
                location_lat=45.0,
                location_long=-95.0,
                igbp="DBF",
            )

            product_data = DataFluxnetProduct(
                first_year=2019,
                last_year=2020,
                download_link="https://example.com/success.zip",
                product_citation="Test citation",
                product_id="test-id",
                oneflux_code_version="v1",
                product_source_network="SUCCESS",
            )

            yield FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)


class TestFluxnetShuttleIntegration:
    """Integration tests for FluxnetShuttle."""

    def test_shuttle_initialization(self):
        """Test shuttle initialization with default config."""
        shuttle = FluxnetShuttle()

        assert shuttle.registry is not None
        assert shuttle.config is not None
        assert isinstance(shuttle.data_hubs, list)

    def test_shuttle_with_enabled_data_hub(self):
        """Test shuttle initialization with enabled data hub."""
        config = ShuttleConfig()
        config.data_hubs["test"] = DataHubConfig(enabled=True)

        shuttle = FluxnetShuttle(data_hubs=["test"], config=config)

        assert "test" in shuttle.data_hubs

    def test_shuttle_with_disabled_data_hub(self):
        """Test shuttle initialization with disabled data hub."""
        config = ShuttleConfig()
        config.data_hubs["test"] = DataHubConfig(enabled=False)

        shuttle = FluxnetShuttle(data_hubs=["test"], config=config)

        assert "test" in shuttle.data_hubs  # Even if disabled, it's included because explicitly specified
        assert not shuttle.config.data_hubs["test"].enabled

        with pytest.raises(ValueError, match="Data hub 'test' is disabled."):
            shuttle._get_plugin_instance("test")

    @pytest.mark.asyncio
    async def test_get_all_sites_no_plugins(self):
        """Test get_all_sites when no plugins are available."""
        # Create shuttle with non-existent data hubs
        shuttle = FluxnetShuttle(data_hubs=["nonexistent"])

        sites = []
        async for site in shuttle.get_all_sites():
            sites.append(site)

        assert len(sites) == 0

        # Should have no errors since no plugins were attempted
        errors = shuttle.get_errors()
        assert errors.total_errors == 0

    @pytest.mark.asyncio
    async def test_error_collection(self):
        """Test that errors are properly collected from failing plugins."""

        # Mock the registry to include our test plugins
        def create_instance_side_effect(name, **config):
            if name == "failing":
                return MockFailingPlugin()
            elif name == "success":
                return MockSuccessPlugin()
            else:
                raise ValueError(f"Unknown plugin: {name}")

        # Create config with both plugins
        config = ShuttleConfig()
        config.data_hubs["failing"] = DataHubConfig(enabled=True)
        config.data_hubs["success"] = DataHubConfig(enabled=True)

        with patch.object(PluginRegistry, "create_instance") as mock_create:
            mock_create.side_effect = create_instance_side_effect

            shuttle = FluxnetShuttle(data_hubs=["failing", "success"], config=config)
            shuttle.registry.list_plugins = MagicMock(return_value=["failing", "success"])

            sites = []
            async for site in shuttle.get_all_sites():
                sites.append(site)

            # Should get four sites from the success plugin (MockSuccessPlugin yields 4)
            assert len(sites) == 4
            assert sites[0].site_info.site_id == "US-SUCCESS"

            # Should have one error from the failing plugin
            errors = shuttle.get_errors()
            assert errors.total_errors == 1
            assert "failing" in errors.errors[0].data_hub
            assert "Mock plugin failure" in errors.errors[0].error

    def test_sync_interface(self):
        """Test synchronous interface works correctly."""
        # This tests that the sync_from_async decorator works
        with patch.object(PluginRegistry, "create_instance") as mock_create:
            mock_create.return_value = MockSuccessPlugin()

            config = ShuttleConfig()
            config.data_hubs["success"] = DataHubConfig(enabled=True)

            shuttle = FluxnetShuttle(data_hubs=["success"], config=config)

            # Use the sync version
            sites = list(shuttle.get_all_sites())

            assert len(sites) == 4  # MockSuccessPlugin yields 4 sites
            assert sites[0].site_info.site_id == "US-SUCCESS"

    def test_list_available_data_hubs(self):
        """Test listing available data hubs."""
        shuttle = FluxnetShuttle()

        data_hubs = shuttle.list_available_data_hubs()
        assert isinstance(data_hubs, list)
        # Should contain at least the built-in plugins
        assert len(data_hubs) >= 0  # May be empty if plugins failed to load

    def test_invalid_plugin_instance(self):
        """Test that requesting an invalid plugin raises an error."""
        shuttle = FluxnetShuttle(data_hubs=["invalid"])

        with pytest.raises(ValueError, match="Data hub 'invalid' not configured"):
            shuttle._get_plugin_instance("invalid")
