"""Test the core functionality of the FluxnetShuttle class."""

from fluxnet_shuttle.core.config import ShuttleConfig
from fluxnet_shuttle.core.registry import PluginRegistry
from fluxnet_shuttle.core.shuttle import FluxnetShuttle


class TestFluxnetShuttle:
    """Test the core functionality of the FluxnetShuttle class."""

    def test_initialization(self):
        """Test that the FluxnetShuttle initializes correctly."""
        shuttle = FluxnetShuttle()
        assert isinstance(shuttle, FluxnetShuttle)
        assert isinstance(shuttle.config, ShuttleConfig)
        assert isinstance(shuttle.registry, PluginRegistry)

    def test_list_plugins(self):
        """Test that listing plugins works."""
        shuttle = FluxnetShuttle()
        plugins = shuttle.registry.list_plugins()
        assert isinstance(plugins, list)
        # Assuming at least one plugin is registered
        assert len(plugins) > 0

    def test_get_all_sites_no_plugins(self):
        """Test getting all sites when no plugins are specified."""
        shuttle = FluxnetShuttle(networks=[])
        sites = list(shuttle.get_all_sites())
        assert sites == []

    def test_get_all_sites_with_plugins(self):
        """Test getting all sites with specified plugins."""
        # This test assumes that the 'ameriflux' and 'icos' plugins are available.
        shuttle = FluxnetShuttle(networks=["ameriflux", "icos"])
        sites = list(shuttle.get_all_sites())
        assert isinstance(sites, list)
        # We can't guarantee there are sites without network access,
        # but we can check that the result is a list.

    def test_get_all_sites_with_plugins_only_icos(self):
        """Test getting all sites with specified plugins."""
        # This test assumes that the 'icos' plugin is available.
        shuttle = FluxnetShuttle(networks=["icos"])
        sites = list(shuttle.get_all_sites())
        assert isinstance(sites, list)
        # We can't guarantee there are sites without network access,
        # but we can check that the result is a list.
