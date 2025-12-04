"""
Main Shuttle Orchestrator
=========================

:module:: fluxnet_shuttle.core.shuttle
:synopsis: Main orchestrator for FLUXNET Shuttle operations
:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle.core.shuttle


This module provides the main FluxnetShuttle class that orchestrates
operations across multiple data hub plugins.
"""

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from fluxnet_shuttle.core.decorators import async_to_sync_generator
from fluxnet_shuttle.core.registry import ErrorCollectingIterator, registry

from ..models import ErrorSummary, FluxnetDatasetMetadata
from .base import DataHubPlugin
from .config import ShuttleConfig

logger = logging.getLogger(__name__)


class FluxnetShuttle:
    """
    Main orchestrator for FLUXNET operations with error collection.

    This class provides the main interface for interacting with multiple
    FLUXNET data hubs through their respective plugins. It handles error
    collection and provides both sync and async interfaces.
    """

    def __init__(self, data_hubs: Optional[List[str]] = None, config: Optional[ShuttleConfig] = None):
        """
        Initialize the FLUXNET Shuttle.

        Args:
            data_hubs: List of data hub names to enable. If None, all configured data hubs are used.
            config: Optional configuration object. If None, default config is loaded.
        """
        self.registry = registry
        self.config = config or ShuttleConfig.load_default()
        if data_hubs is None:
            # Use all enabled data hubs from config if none specified
            self.data_hubs = [name for name, _ in self.config.data_hubs.items()]
        else:
            self.data_hubs = [name for name, _ in self.config.data_hubs.items() if name in data_hubs]

        self._last_error_collector: Optional[ErrorCollectingIterator] = None

        logger.info(f"Initialized FluxnetShuttle with data hubs: {self.data_hubs}")

    @async_to_sync_generator
    async def get_all_sites(self, **filters: Any) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get sites from all enabled data hubs.

        Args:
            **filters: Optional filters to apply to site selection

        Yields:
            FluxnetDatasetMetadata: Site metadata objects from all data hubs

        Example:
            >>> shuttle = FluxnetShuttle()
            >>> async for site in shuttle.get_all_sites():
            ...     print(f"{site.site_info.site_id} from {site.site_info.data_hub}")
        """
        plugins = self._get_enabled_plugins()

        if not plugins:
            logger.warning("No enabled plugins found")
            # For async generators, just return without yielding anything
            return

        # Create error collecting iterator
        error_collector = ErrorCollectingIterator(plugins, "get_sites", **filters)
        self._last_error_collector = error_collector

        # Yield results using async iterator
        try:
            async for site in error_collector:
                yield site
        finally:
            # Log summary after iteration completes
            summary = error_collector.get_error_summary()
            logger.info(f"Completed get_all_sites: {summary.total_results} results, " f"{summary.total_errors} errors")

    def get_errors(self) -> ErrorSummary:
        """
        Get collected errors from last operation.

        Returns:
            ErrorSummary: Pydantic model containing error summary information
        """
        if self._last_error_collector is not None:
            summary: ErrorSummary = self._last_error_collector.get_error_summary()
            return summary
        return ErrorSummary(total_errors=0, total_results=0, errors=[])

    def list_available_data_hubs(self) -> List[str]:
        """
        List all available data hub plugins.

        Returns:
            List of available data hub names
        """
        plugins: List[str] = self.registry.list_plugins()
        return plugins

    def _get_enabled_plugins(self) -> Dict[str, Any]:
        """
        Get instances of enabled plugins.

        Returns:
            Dict mapping data hub names to plugin instances
        """
        plugins = {}
        for data_hub_name in self.data_hubs:
            plugin = self._get_plugin_instance(data_hub_name)
            plugins[data_hub_name] = plugin

        return plugins

    def _get_plugin_instance(self, data_hub_name: str) -> DataHubPlugin:
        """
        Get a plugin instance for the specified data hub.

        Args:
            data_hub_name: Name of the data hub

        Returns:
            DataHubPlugin instance

        Raises:
            ValueError: If data hub is not configured or plugin not found
        """
        if data_hub_name not in self.config.data_hubs:
            raise ValueError(f"Data hub '{data_hub_name}' not configured")

        data_hub_config = self.config.data_hubs[data_hub_name]
        if not data_hub_config.enabled:
            raise ValueError(f"Data hub '{data_hub_name}' is disabled.")

        plugin: DataHubPlugin = self.registry.create_instance(data_hub_name, **data_hub_config.__dict__)
        return plugin
