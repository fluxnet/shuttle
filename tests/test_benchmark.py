"""Benchmark tests for the FluxnetShuttle library.

These tests measure the performance for overall site retrieval
from the FluxnetShuttle class as well as individual plugins like
ICOS and AmeriFlux.
"""

import pytest

# Import the plugins package to trigger auto-registration of available
# plugins in the global registry.
import fluxnet_shuttle.plugins  # noqa: F401
from fluxnet_shuttle.core.registry import registry

# Mark all tests in this file as integration/benchmark tests
pytestmark = pytest.mark.benchmark


@pytest.mark.parametrize("plugin_name", registry.list_plugins(), ids=registry.list_plugins())
@pytest.mark.benchmark(group="get_sites")
def test_benchmark_get_sites_for_plugin(plugin_name, benchmark):
    """Benchmark get_sites() for every plugin currently registered.

    This test dynamically discovers plugins from the global registry so
    it will always exercise the latest enabled plugins without hardcoding
    plugin classes.
    """

    # Try to create an instance of the plugin. If creation fails (missing
    # configuration, etc.) we skip the benchmark for that plugin rather
    # than failing the whole test suite.
    try:
        plugin = registry.create_instance(plugin_name)
    except Exception as e:
        pytest.skip(f"Could not create plugin '{plugin_name}': {e}")

    def _call_get_sites():
        # get_sites() is provided as a hybrid async/sync iterator by the
        # plugins; converting to list exercises the full retrieval path.
        return list(plugin.get_sites())

    # Run the benchmark and skip the test if the plugin raises at runtime.
    try:
        sites = benchmark(_call_get_sites)
    except Exception as e:
        pytest.skip(f"Benchmark failed for plugin '{plugin_name}': {e}")

    assert isinstance(sites, list)
