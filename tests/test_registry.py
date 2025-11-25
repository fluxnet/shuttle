"""
Test Registry
"""

import pytest

from fluxnet_shuttle.core.base import DataHubPlugin
from fluxnet_shuttle.core.decorators import async_to_sync_generator
from fluxnet_shuttle.core.registry import ErrorCollectingIterator, PluginRegistry


class DummyPlugin(DataHubPlugin):
    @property
    def name(self):
        return "dummy"

    @property
    def display_name(self):
        return "Dummy Plugin"

    @async_to_sync_generator
    async def get_sites(self, **filters):
        for i in range(3):  # Dummy implementation
            yield {"id": i, "name": f"Site {i}"}


class TestPluginRegistry:
    """Test cases for PluginRegistry."""

    def test_register_and_get_plugin(self):
        """Test registering and retrieving a plugin."""
        registry = PluginRegistry()

        registry.register(DummyPlugin)
        plugin_cls = registry.get_plugin("dummy")
        assert plugin_cls is DummyPlugin
        assert plugin_cls().display_name == "Dummy Plugin"
        assert plugin_cls().name == "dummy"
        assert registry.list_plugins() == ["dummy"]

    def test_register_duplicate_plugin(self):
        """Test that registering a duplicate plugin raises an error."""
        registry = PluginRegistry()

        registry.register(DummyPlugin)
        with pytest.raises(ValueError, match="Plugin with name 'dummy' is already registered."):
            registry.register(DummyPlugin)

    def test_register_invalid_plugin(self):
        """Test that registering an invalid plugin raises an error."""
        registry = PluginRegistry()

        class InvalidPlugin:
            pass

        with pytest.raises(TypeError, match="Plugin class must inherit from DataHubPlugin"):
            registry.register(InvalidPlugin)

    def test_get_nonexistent_plugin(self):
        """Test that getting a non-existent plugin raises an error."""
        registry = PluginRegistry()
        with pytest.raises(ValueError, match="Plugin with name 'nonexistent' not found."):
            registry.get_plugin("nonexistent")

    def test_create_instance(self):
        """Test creating a plugin instance."""
        registry = PluginRegistry()

        registry.register(DummyPlugin)
        plugin_instance = registry.create_instance("dummy")
        assert plugin_instance.name == "dummy"
        assert isinstance(plugin_instance, DummyPlugin)
        with pytest.raises(ValueError, match="Plugin with name 'nonexistent' not found."):
            registry.create_instance("nonexistent")

    @pytest.mark.asyncio
    async def test_error_collecting_iterator_no_errors(self):
        """Test ErrorCollectingIterator functionality."""

        plugins = {
            "dummy": DummyPlugin(),
        }

        iterator = ErrorCollectingIterator(plugins, "get_sites")
        results = []
        async for item in iterator:
            results.append(item)

        assert results == [{"id": 0, "name": "Site 0"}, {"id": 1, "name": "Site 1"}, {"id": 2, "name": "Site 2"}]
        errors = iterator.get_error_summary()
        assert errors.total_errors == 0
        assert errors.total_results == 3
        assert errors.errors == []
        # Test that calling get_errors_summary again returns the same result
        errors_again = iterator.get_error_summary()
        assert errors == errors_again

    @pytest.mark.asyncio
    async def test_error_collecting_iterator_with_errors(self):
        """Test ErrorCollectingIterator with errors."""

        class SamplePlugin(DummyPlugin):

            @async_to_sync_generator
            async def error_generator(self):
                yield {"id": 0, "name": "Site 0"}
                raise ValueError("Test error")
                yield {"id": 1, "name": "Site 1"}
                yield {"id": 2, "name": "Site 2"}

        plugins = {
            "dummy": SamplePlugin(),
        }

        iterator = ErrorCollectingIterator(plugins, "error_generator")
        results = []
        async for item in iterator:
            results.append(item)

        assert results == [{"id": 0, "name": "Site 0"}]
        errors = iterator.get_error_summary()
        assert errors.total_errors == 1
        assert "Test error" in errors.errors[0].error
        # Test that calling get_errors again returns the same result
        errors_again = iterator.get_error_summary()
        assert errors == errors_again

    @pytest.mark.asyncio
    async def test_error_collecting_iterator_empty(self):
        """Test ErrorCollectingIterator with an empty generator."""

        class SamplePlugin(DummyPlugin):
            @async_to_sync_generator
            async def empty_generator(self):
                if False:
                    yield

        plugins = {
            "dummy": SamplePlugin(),
        }

        iterator = ErrorCollectingIterator(plugins, "empty_generator")
        results = []
        async for item in iterator:
            results.append(item)

        assert results == []
        errors = iterator.get_error_summary()
        assert errors.total_errors == 0
        assert errors.total_results == 0
        assert errors.errors == []
        # Test that calling get_errors again returns the same result
        errors_again = iterator.get_error_summary()
        assert errors == errors_again

    @pytest.mark.asyncio
    async def test_various_warnings(self, caplog):
        """Test ErrorCollectingIterator with various warning scenarios."""

        class NonCallablePlugin(DummyPlugin):
            some_method = "not a function"
            # Missing async generator method

        class ExceptionInInitPlugin(DummyPlugin):
            @async_to_sync_generator
            async def some_method(self):
                raise RuntimeError("Initialization error")

        class NonExistentMethodPlugin(DummyPlugin):
            @async_to_sync_generator
            async def get_sites(self):
                yield {"id": 0, "name": "Site 0"}

        class NotAnAsyncGeneratorPlugin(DummyPlugin):
            def some_method(self):
                return "I am not an async generator"

        plugins = {
            "non_callable": NonCallablePlugin(),
            "exception_in_init": ExceptionInInitPlugin(),
            "non_existent_method": NonExistentMethodPlugin(),
            "not_an_async_generator": NotAnAsyncGeneratorPlugin(),
        }
        iterator = ErrorCollectingIterator(plugins, "some_method")
        results = []

        with caplog.at_level("WARNING"):
            async for item in iterator:
                results.append(item)
            assert "Plugin 'non_callable' operation 'some_method' is not callable" in caplog.text
            assert "Plugin 'non_existent_method' does not have operation 'some_method'" in caplog.text
            assert (
                "Plugin 'exception_in_init' error in 'some_method': 'coroutine' object has no attribute '__anext__'"
                in caplog.text
            )
            assert "Plugin 'not_an_async_generator' operation 'some_method' is not an async generator" in caplog.text

        assert results == []
        errors = iterator.get_error_summary()
        assert iterator.has_errors()
        assert errors.total_errors == 4
        assert errors.total_results == 0
        assert len(errors.errors) == 4
