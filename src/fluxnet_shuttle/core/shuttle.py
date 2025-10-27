"""
Main Shuttle Orchestrator
=========================

:module:: fluxnet_shuttle.core.shuttle
:synopsis: Main orchestrator for FLUXNET Shuttle operations
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle.core.shuttle


This module provides the main FluxnetShuttle class that orchestrates
operations across multiple network plugins.
"""

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from fluxnet_shuttle.core.decorators import async_to_sync_generator
from fluxnet_shuttle.core.registry import ErrorCollectingIterator, registry

from ..models import ErrorSummary, FluxnetDatasetMetadata
from .config import ShuttleConfig

logger = logging.getLogger(__name__)


class FluxnetShuttle:
    """
    Main orchestrator for FLUXNET operations with error collection.

    This class provides the main interface for interacting with multiple
    FLUXNET networks through their respective plugins. It handles error
    collection and provides both sync and async interfaces.
    """

    def __init__(self, networks: Optional[List[str]] = None, config: Optional[ShuttleConfig] = None):
        """
        Initialize the FLUXNET Shuttle.

        Args:
            networks: List of network names to enable. If None, all configured networks are used.
            config: Optional configuration object. If None, default config is loaded.
        """
        self.registry = registry
        self.config = config or ShuttleConfig.load_default()
        if networks is None:
            # Use all enabled networks from config if none specified
            self.networks = [name for name, net in self.config.networks.items()]
        else:
            self.networks = [name for name, net in self.config.networks.items() if name in networks]

        self._last_error_collector: Optional[ErrorCollectingIterator] = None

        logger.info(f"Initialized FluxnetShuttle with networks: {self.networks}")

    @async_to_sync_generator
    async def get_all_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get sites from all enabled networks.

        Args:
            **filters: Optional filters to apply to site selection

        Yields:
            FluxnetDatasetMetadata: Site metadata objects from all networks

        Example:
            >>> shuttle = FluxnetShuttle()
            >>> async for site in shuttle.get_all_sites():
            ...     print(f"{site.site_info.site_id} from {site.site_info.network}")
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
        if self._last_error_collector:
            return self._last_error_collector.get_error_summary()
        return ErrorSummary(total_errors=0, total_results=0, errors=[])

    def list_available_networks(self) -> List[str]:
        """
        List all available network plugins.

        Returns:
            List of available network names
        """
        return self.registry.list_plugins()

    def _get_enabled_plugins(self) -> Dict[str, Any]:
        """
        Get instances of enabled plugins.

        Returns:
            Dict mapping network names to plugin instances
        """
        plugins = {}
        for network_name in self.networks:
            plugin = self._get_plugin_instance(network_name)
            plugins[network_name] = plugin

        return plugins

    def _get_plugin_instance(self, network_name: str):
        """
        Get a plugin instance for the specified network.

        Args:
            network_name: Name of the network

        Returns:
            NetworkPlugin instance

        Raises:
            ValueError: If network is not configured or plugin not found
        """
        if network_name not in self.config.networks:
            raise ValueError(f"Network '{network_name}' not configured")

        network_config = self.config.networks[network_name]
        if not network_config.enabled:
            raise ValueError(f"Network '{network_name}' is disabled.")

        return self.registry.create_instance(network_name, **network_config.__dict__)
