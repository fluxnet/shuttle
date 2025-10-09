"""
Plugin Registry and Error Collection
====================================

:module:: fluxnet_shuttle_lib.core.registry
:synopsis: Plugin registry and error collection for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.core.registry
.. autosummary::
    :toctree: generated/

    PluginErrorInfo
    PluginRegistry
    ErrorCollectingIterator



This module provides the plugin registry for managing network plugins
and error collection capabilities.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Type

from ..models import FluxnetDatasetMetadata
from .base import NetworkPlugin

logger = logging.getLogger(__name__)


@dataclass
class PluginErrorInfo:
    """Container for plugin execution errors."""

    plugin_name: str
    error: Exception
    timestamp: datetime = field(default_factory=datetime.now)
    operation: str = ""


class ErrorCollectingIterator:
    """
    Async iterator that collects errors while yielding results.

    This class implements the async iterator protocol and collects results
    from multiple plugins while isolating and collecting any errors that occur.
    """

    def __init__(self, plugins: Dict[str, NetworkPlugin], operation: str, **kwargs):
        """
        Initialize the error collecting iterator.

        Args:
            plugins: Dictionary of plugin instances to iterate over
            operation: The operation being performed (e.g., 'get_sites')
            **kwargs: Arguments to pass to the plugin operation
        """
        self.plugins = plugins
        self.operation = operation
        self.kwargs = kwargs
        self.errors: List[PluginErrorInfo] = []
        self._results_count = 0
        self._plugin_iterators: Dict[str, AsyncGenerator[FluxnetDatasetMetadata, None]] = {}
        self._completed_plugins: set[str] = set()

    def __aiter__(self):
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> FluxnetDatasetMetadata:
        """
        Get next result from any available plugin.

        Returns:
            FluxnetDatasetMetadata: Next available site metadata

        Raises:
            StopAsyncIteration: When no more results are available
        """
        # Initialize iterators for plugins that haven't been started
        for plugin_name, plugin in self.plugins.items():
            if plugin_name not in self._plugin_iterators and plugin_name not in self._completed_plugins:
                try:
                    # check if plugin has the requested operation
                    if not hasattr(plugin, self.operation):
                        logger.warning(f"Plugin '{plugin_name}' does not have operation '{self.operation}'")
                        self.add_error(
                            plugin_name, AttributeError(f"Operation '{self.operation}' not found"), self.operation
                        )
                        self._completed_plugins.add(plugin_name)
                        continue
                    # now check if it's callable and is an async generator as expected
                    if not callable(getattr(plugin, self.operation)):
                        logger.warning(f"Plugin '{plugin_name}' operation '{self.operation}' is not callable")
                        self.add_error(
                            plugin_name,
                            TypeError(f"Operation '{self.operation}' is not callable"),
                            self.operation,
                        )
                        self._completed_plugins.add(plugin_name)
                        continue
                    # Initialize the async generator
                    iterator = getattr(plugin, self.operation)(**self.kwargs)
                    if not hasattr(iterator, "__aiter__"):
                        logger.warning(f"Plugin '{plugin_name}' operation '{self.operation}' is not an async generator")
                        self.add_error(
                            plugin_name,
                            TypeError(f"Operation '{self.operation}' is not an async generator"),
                            self.operation,
                        )
                        self._completed_plugins.add(plugin_name)
                        continue
                    self._plugin_iterators[plugin_name] = getattr(plugin, self.operation)(**self.kwargs).__aiter__()
                except Exception as e:  # pragma: no cover
                    # should not happen, but just in case
                    logger.warning(f"Error initializing plugin '{plugin_name}': {e}")
                    self.add_error(plugin_name, e, self.operation)
                    self._completed_plugins.add(plugin_name)

        # Try to get next result from any plugin
        while self._plugin_iterators:
            # Try each plugin iterator
            for plugin_name in list(self._plugin_iterators.keys()):
                try:
                    result = await self._plugin_iterators[plugin_name].__anext__()
                    self._results_count += 1
                    return result
                except StopAsyncIteration:
                    # This plugin is done
                    del self._plugin_iterators[plugin_name]
                    self._completed_plugins.add(plugin_name)
                except Exception as e:
                    # Error in this plugin
                    self.add_error(plugin_name, e, self.operation)
                    del self._plugin_iterators[plugin_name]
                    self._completed_plugins.add(plugin_name)

        # No more results from any plugin
        raise StopAsyncIteration

    def add_error(self, plugin_name: str, error: Exception, operation: str = ""):
        """
        Add an error to the collection.

        Args:
            plugin_name: Name of the plugin that encountered the error
            error: The exception that occurred
            operation: The operation being performed when the error occurred
        """
        self.errors.append(PluginErrorInfo(plugin_name=plugin_name, error=error, operation=operation))
        logger.warning(f"Plugin '{plugin_name}' error in '{operation}': {error}")

    def has_errors(self) -> bool:
        """
        Check if any errors were collected.

        Returns:
            bool: True if any errors occurred
        """
        return len(self.errors) > 0

    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of all errors.

        Returns:
            Dict containing error summary information
        """
        return {
            "total_errors": len(self.errors),
            "total_results": self._results_count,
            "errors": [
                {
                    "network": error.plugin_name,
                    "operation": error.operation,
                    "error": str(error.error),
                    "timestamp": error.timestamp.isoformat(),
                }
                for error in self.errors
            ],
        }


class PluginRegistry:
    """
    Registry for managing network plugins with automatic discovery.

    This class manages the registration and instantiation of network plugins,
    including automatic discovery through entry points.
    """

    def __init__(self):
        """Initialize the plugin registry."""
        self._plugins: Dict[str, Type[NetworkPlugin]] = {}
        self._instances: Dict[str, NetworkPlugin] = {}

    def register(self, plugin_class: Type[NetworkPlugin]) -> None:
        """
        Register a network plugin.

        Args:
            plugin_class: The plugin class to register
        """
        if not issubclass(plugin_class, NetworkPlugin):
            raise TypeError("Plugin class must inherit from NetworkPlugin")

        # Check for duplicate names
        temp_instance = plugin_class()
        plugin_name = temp_instance.name.lower()
        if plugin_name in self._plugins:
            raise ValueError(f"Plugin with name '{plugin_name}' is already registered.")

        # Create a temporary instance to get the plugin name
        temp_instance = plugin_class()
        plugin_name = temp_instance.name.lower()
        self._plugins[plugin_name] = plugin_class
        logger.debug(f"Registered plugin: {plugin_name}")

    def get_plugin(self, name: str) -> Type[NetworkPlugin]:
        """
        Get a plugin by name.

        Args:
            name: Plugin name

        Returns:
            Plugin class or None if not found

        Raises:
            ValueError: If plugin is not found
        """
        # Check plugin, raise error if not found
        plugin = self._plugins.get(name.lower(), None)
        if not plugin:
            raise ValueError(f"Plugin with name '{name}' not found. Available plugins: {self.list_plugins()}")
        return plugin

    def list_plugins(self) -> List[str]:
        """
        List all registered plugin names.

        Returns:
            List of plugin names
        """
        return list(self._plugins.keys())

    def create_instance(self, name: str, **config) -> NetworkPlugin:
        """
        Create an instance of a plugin.

        Args:
            name: Plugin name
            **config: Configuration parameters

        Returns:
            Plugin instance

        Raises:
            ValueError: If plugin is not found
        """
        try:
            plugin_class = self.get_plugin(name)
            return plugin_class(config=config)
        except Exception as e:
            logger.error(f"Error creating plugin instance '{name}': {e}")
            raise e


# Global registry instance
registry = PluginRegistry()
