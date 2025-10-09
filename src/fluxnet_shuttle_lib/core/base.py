"""
Base Plugin Interface
=====================

:module: fluxnet_shuttle_lib.core.base
:synopsis: Base class and interfaces for network plugins in the FLUXNET Shuttle library.
:author: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.core.base
.. autosummary::
    :toctree: generated/
    NetworkPlugin


This module defines the base class and interfaces for network plugins
in the FLUXNET Shuttle library.
"""

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, Optional

from fluxnet_shuttle_lib.core.decorators import async_to_sync_generator

from ..models import FluxnetDatasetMetadata


class NetworkPlugin(ABC):
    """
    Base class for all network plugins.

    This abstract base class defines the interface that all network plugins
    must implement. It provides both async and sync versions of the get_sites
    method through the sync_from_async decorator.

    Attributes:
        config (Dict[str, Any]): Configuration dictionary for the plugin
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the network plugin.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}

    @property
    def name(self) -> str:  # pragma: no cover
        """
        Network name identifier.

        Returns:
            str: Lowercase network identifier (e.g., 'ameriflux', 'icos')
        """
        raise NotImplementedError("Subclasses must implement the 'name' property")

    @property
    @abstractmethod
    def display_name(self) -> str:
        """
        Human-readable network name.

        Returns:
            str: Display name for the network (e.g., 'AmeriFlux', 'ICOS')
        """
        pass

    @async_to_sync_generator
    @abstractmethod
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get available sites from the network.

        This is the primary method that plugins must implement.
        A synchronous version is automatically available as get_sites_sync().

        Args:
            **filters: Optional filters to apply to site selection

        Yields:
            FluxnetDatasetMetadata: Site metadata objects

        Example:
            >>> plugin = SomeNetworkPlugin()
            >>> async for site in plugin.get_sites():
            ...     print(site.site_info.site_id)

            >>> # Or use the sync version
            >>> for site in plugin.get_sites_sync():
            ...     print(site.site_info.site_id)
        """
        pass
