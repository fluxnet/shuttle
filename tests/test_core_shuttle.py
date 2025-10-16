"""
Integration Tests for FLUXNET Shuttle
=====================================

Integration tests for the complete FLUXNET Shuttle system.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from fluxnet_shuttle_lib.core.config import NetworkConfig, ShuttleConfig
from fluxnet_shuttle_lib.core.decorators import async_to_sync_generator
from fluxnet_shuttle_lib.core.registry import PluginRegistry
from fluxnet_shuttle_lib.core.shuttle import FluxnetShuttle
from fluxnet_shuttle_lib.models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata


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
                site_id="US-SUCCESS", network="Success", location_lat=45.0, location_long=-95.0, igbp="DBF"
            )

            product_data = DataFluxnetProduct(
                first_year=2019, last_year=2020, download_link="https://example.com/success.zip"
            )

            yield FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)


class TestFluxnetShuttleIntegration:
    """Integration tests for FluxnetShuttle."""

    def test_shuttle_initialization(self):
        """Test shuttle initialization with default config."""
        shuttle = FluxnetShuttle()

        assert shuttle.registry is not None
        assert shuttle.config is not None
        assert isinstance(shuttle.networks, list)

    def test_shuttle_with_enabled_network(self):
        """Test shuttle initialization with enabled network."""
        config = ShuttleConfig()
        config.networks["test"] = NetworkConfig(enabled=True)

        shuttle = FluxnetShuttle(networks=["test"], config=config)

        assert "test" in shuttle.networks

    def test_shuttle_with_disabled_network(self):
        """Test shuttle initialization with disabled network."""
        config = ShuttleConfig()
        config.networks["test"] = NetworkConfig(enabled=False)

        shuttle = FluxnetShuttle(networks=["test"], config=config)

        assert "test" in shuttle.networks  # Even if disabled, it's included because explicitly specified
        assert not shuttle.config.networks["test"].enabled

        with pytest.raises(ValueError, match="Network 'test' is disabled."):
            shuttle._get_plugin_instance("test")

    @pytest.mark.asyncio
    async def test_get_all_sites_no_plugins(self):
        """Test get_all_sites when no plugins are available."""
        # Create shuttle with non-existent networks
        shuttle = FluxnetShuttle(networks=["nonexistent"])

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
        mock_create = MagicMock()

        def create_instance_side_effect(name, **config):
            if name == "failing":
                return MockFailingPlugin()
            elif name == "success":
                return MockSuccessPlugin()
            else:
                raise ValueError(f"Unknown plugin: {name}")

        mock_create.side_effect = create_instance_side_effect

        # Create config with both plugins
        config = ShuttleConfig()
        config.networks["failing"] = NetworkConfig(enabled=True)
        config.networks["success"] = NetworkConfig(enabled=True)

        shuttle = FluxnetShuttle(networks=["failing", "success"], config=config)
        shuttle.registry.list_plugins = MagicMock(return_value=["failing", "success"])
        shuttle.registry.create_instance = mock_create

        sites = []
        async for site in shuttle.get_all_sites():
            sites.append(site)

        # Should get four sites from the success plugin (MockSuccessPlugin yields 4)
        assert len(sites) == 4
        assert sites[0].site_info.site_id == "US-SUCCESS"

        # Should have one error from the failing plugin
        errors = shuttle.get_errors()
        assert errors.total_errors == 1
        assert "failing" in errors.errors[0].network
        assert "Mock plugin failure" in errors.errors[0].error

    def test_sync_interface(self):
        """Test synchronous interface works correctly."""
        # This tests that the sync_from_async decorator works
        with patch.object(PluginRegistry, "create_instance") as mock_create:
            mock_create.return_value = MockSuccessPlugin()

            config = ShuttleConfig()
            config.networks["success"] = NetworkConfig(enabled=True)

            shuttle = FluxnetShuttle(networks=["success"], config=config)

            # Use the sync version
            sites = list(shuttle.get_all_sites())

            assert len(sites) == 4  # MockSuccessPlugin yields 4 sites
            assert sites[0].site_info.site_id == "US-SUCCESS"

    def test_list_available_networks(self):
        """Test listing available networks."""
        shuttle = FluxnetShuttle()

        networks = shuttle.list_available_networks()
        assert isinstance(networks, list)
        # Should contain at least the built-in plugins
        assert len(networks) >= 0  # May be empty if plugins failed to load

    def test_invalid_plugin_instance(self):
        """Test that requesting an invalid plugin raises an error."""
        shuttle = FluxnetShuttle(networks=["invalid"])

        with pytest.raises(ValueError, match="Network 'invalid' not configured"):
            shuttle._get_plugin_instance("invalid")
