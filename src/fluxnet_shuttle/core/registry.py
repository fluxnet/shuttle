"""
Plugin Registry and Error Collection
====================================

:module:: fluxnet_shuttle.core.registry
:synopsis: Plugin registry and error collection for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle.core.registry


This module provides the plugin registry for managing data hub plugins
and error collection capabilities.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Type

from ..models import ErrorSummary, FluxnetDatasetMetadata, PluginErrorDetail
from .base import DataHubPlugin
from .config import ShuttleConfig

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

    def __init__(
        self,
        plugins: Dict[str, DataHubPlugin],
        operation: str,
        global_timeout: float = ShuttleConfig.global_timeout,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the error collecting iterator.

        Args:
            plugins: Dictionary of plugin instances to iterate over
            operation: The operation being performed (e.g., 'get_sites')
            global_timeout: Hard deadline in seconds for the entire iteration.
                When expired, all remaining plugins are killed and their errors logged.
            **kwargs: Arguments to pass to the plugin operation
        """
        self.plugins = plugins
        self.operation = operation
        self.kwargs = kwargs
        self.errors: List[PluginErrorInfo] = []
        self._results_count = 0
        self._plugin_iterators: Dict[str, AsyncGenerator[FluxnetDatasetMetadata, None]] = {}
        self._completed_plugins: set[str] = set()
        self._global_timeout: float = global_timeout
        self._global_start_time: Optional[float] = None

    def __aiter__(self) -> "ErrorCollectingIterator":
        """Return self as the async iterator."""
        return self

    async def _kill_plugin(self, plugin_name: str, error: Exception) -> None:
        """Kill a plugin iterator, close it, and record the error."""
        plugin_iter = self._plugin_iterators.pop(plugin_name, None)
        self._completed_plugins.add(plugin_name)
        self.add_error(plugin_name, error, self.operation)
        if plugin_iter is not None:
            try:
                await plugin_iter.aclose()
            except Exception:
                pass  # best-effort cleanup

    async def _kill_all_plugins(self, reason: str) -> None:
        """Kill all remaining plugin iterators with the given reason."""
        for name in list(self._plugin_iterators):
            await self._kill_plugin(name, TimeoutError(reason))

    def _global_remaining(self) -> float:
        """Return seconds left on the global deadline."""
        assert self._global_start_time is not None
        return self._global_timeout - (time.monotonic() - self._global_start_time)

    def _init_plugin(self, plugin_name: str, plugin: DataHubPlugin) -> None:
        """Validate and initialise a single plugin iterator. Errors are recorded internally."""
        if not hasattr(plugin, self.operation):
            logger.warning(f"Plugin '{plugin_name}' does not have operation '{self.operation}'")
            self.add_error(plugin_name, AttributeError(f"Operation '{self.operation}' not found"), self.operation)
            self._completed_plugins.add(plugin_name)
            return
        if not callable(getattr(plugin, self.operation)):
            logger.warning(f"Plugin '{plugin_name}' operation '{self.operation}' is not callable")
            self.add_error(
                plugin_name,
                TypeError(f"Operation '{self.operation}' is not callable"),
                self.operation,
            )
            self._completed_plugins.add(plugin_name)
            return
        iterator = getattr(plugin, self.operation)(**self.kwargs)
        if not hasattr(iterator, "__aiter__"):
            logger.warning(f"Plugin '{plugin_name}' operation '{self.operation}' is not an async generator")
            self.add_error(
                plugin_name,
                TypeError(f"Operation '{self.operation}' is not an async generator"),
                self.operation,
            )
            self._completed_plugins.add(plugin_name)
            return
        self._plugin_iterators[plugin_name] = getattr(plugin, self.operation)(**self.kwargs).__aiter__()

    async def __anext__(self) -> FluxnetDatasetMetadata:
        """
        Get next result from any available plugin.

        Returns:
            FluxnetDatasetMetadata: Next available site metadata

        Raises:
            StopAsyncIteration: When no more results are available
        """
        # Start global clock on first call
        if self._global_start_time is None:
            self._global_start_time = time.monotonic()

        # Check global deadline (may fire if deadline expired between __anext__ calls)
        remaining = self._global_remaining()
        if remaining <= 0:  # pragma: no cover
            await self._kill_all_plugins(f"Global deadline exceeded ({self._global_timeout}s)")
            raise StopAsyncIteration

        # Initialize iterators for plugins that haven't been started
        for plugin_name, plugin in self.plugins.items():
            if plugin_name not in self._plugin_iterators and plugin_name not in self._completed_plugins:
                try:
                    self._init_plugin(plugin_name, plugin)
                except Exception as e:  # pragma: no cover
                    logger.warning(f"Error initializing plugin '{plugin_name}': {e}")
                    self.add_error(plugin_name, e, self.operation)
                    self._completed_plugins.add(plugin_name)

        # Try to get next result from any plugin
        while self._plugin_iterators:
            for plugin_name in list(self._plugin_iterators.keys()):
                remaining = self._global_remaining()
                if remaining <= 0:
                    await self._kill_all_plugins(f"Global deadline exceeded ({self._global_timeout}s)")
                    raise StopAsyncIteration

                try:
                    result = await asyncio.wait_for(
                        self._plugin_iterators[plugin_name].__anext__(),
                        timeout=remaining,
                    )
                    self._results_count += 1
                    return result
                except asyncio.TimeoutError:
                    await self._kill_plugin(
                        plugin_name, TimeoutError(f"Global deadline exceeded ({self._global_timeout}s)")
                    )
                except StopAsyncIteration:
                    del self._plugin_iterators[plugin_name]
                    self._completed_plugins.add(plugin_name)
                except Exception as e:
                    await self._kill_plugin(plugin_name, e)

        # No more results from any plugin
        raise StopAsyncIteration

    def add_error(self, plugin_name: str, error: Exception, operation: str = "") -> None:
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

    def get_error_summary(self) -> ErrorSummary:
        """
        Get summary of all errors.

        Returns:
            ErrorSummary: Pydantic model containing error summary information
        """
        error_details = [
            PluginErrorDetail(
                data_hub=error.plugin_name,
                operation=error.operation,
                error=str(error.error),
                timestamp=error.timestamp.isoformat(),
            )
            for error in self.errors
        ]

        return ErrorSummary(
            total_errors=len(self.errors),
            total_results=self._results_count,
            errors=error_details,
        )


class PluginRegistry:
    """
    Registry for managing data hub plugins with automatic discovery.

    This class manages the registration and instantiation of data hub plugins,
    including automatic discovery through entry points.
    """

    def __init__(self) -> None:
        """Initialize the plugin registry."""
        self._plugins: Dict[str, Type[DataHubPlugin]] = {}
        self._instances: Dict[str, DataHubPlugin] = {}

    def register(self, plugin_class: Type[DataHubPlugin]) -> None:
        """
        Register a data hub plugin.

        Args:
            plugin_class: The plugin class to register
        """
        if not issubclass(plugin_class, DataHubPlugin):
            raise TypeError("Plugin class must inherit from DataHubPlugin")

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

    def get_plugin(self, name: str) -> Type[DataHubPlugin]:
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

    def create_instance(self, name: str, **config: Any) -> DataHubPlugin:
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
