"""
Base Plugin Interface
=====================

:module: fluxnet_shuttle_lib.core.base
:synopsis: Base class and interfaces for network plugins in the FLUXNET Shuttle library.
:author: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.core.base

This module defines the base class and interfaces for network plugins
in the FLUXNET Shuttle library.

Network Plugin Implementation
+++++++++++++++++++++++++++++

The :class:`NetworkPlugin` abstract base class provides the interface
that all network plugins must implement. It includes methods for
retrieving site metadata and handling HTTP requests. When implementing
a new network plugin, developers should subclass :class:`NetworkPlugin`
and provide concrete implementations for the abstract methods.

The :property name must return a unique identifier for the plugin, while the
:property display_name should return a human-readable name. The primary method
for retrieving site metadata is :func:`get_sites`, which is an async
generator method. A synchronous version of this method is automatically
available through the :func:`async_to_sync_generator` decorator.

Implementation Basics
---------------------
An example implementation of a network plugin might look like this::

    from fluxnet_shuttle_lib.core.base import NetworkPlugin
    from fluxnet_shuttle_lib.models import FluxnetDatasetMetadata

    class MyNetworkPlugin(NetworkPlugin):
        @property
        def name(self) -> str:
            return "mynetwork"

        @property
        def display_name(self) -> str:
            return "My Network"

    @async_to_sync_generator
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        # Implementation to fetch and yield site metadata
        async with self._session_request("GET", "https://api.mynetwork.org/sites") as response:
            data = await response.json()
            for site in data["sites"]:
                yield FluxnetDatasetMetadata(...)  # Populate with actual data


Each plugin must implement the abstract methods defined in the
:class:`NetworkPlugin` base class.

The :class:`NetworkPlugin` class provides both asynchronous and synchronous
versions of the primary method for retrieving site metadata. The asynchronous
version is defined as :func:`get_sites`, which is an async generator method.

The synchronous version is made available through the
:func:`async_to_sync_generator` decorator, allowing users to choose
between async and sync usage based on their application's needs.

HTTP Request Handling
---------------------
Plugins should use the :func:`_session_request` helper method to make
HTTP requests. This method manages the aiohttp ClientSession and includes
error handling to ensure robust network communication.

Error Handling
--------------
Plugins should raise :class:`PluginError` exceptions when encountering
issues during execution. This allows for consistent error handling across
different plugin implementations.

"""

import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

import aiohttp

from fluxnet_shuttle_lib.core import exceptions
from fluxnet_shuttle_lib.core.decorators import async_to_sync_generator
from fluxnet_shuttle_lib.core.http_utils import session_request

from ..models import FluxnetDatasetMetadata

_logger = logging.getLogger(__name__)


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

        Raises:
            PluginError: If a shuttle plugin error occurs during site retrieval

        Example:
            >>> plugin = SomeNetworkPlugin()
            >>> async for site in plugin.get_sites():
            ...     print(site.site_info.site_id)

            >>> # Or use the sync version
            >>> for site in plugin.get_sites_sync():
            ...     print(site.site_info.site_id)
        """
        pass

    @asynccontextmanager
    async def _session_request(self, method: str, url: str, **kwargs) -> AsyncGenerator[aiohttp.ClientResponse, None]:
        """
        Make an HTTP request using an aiohttp ClientSession.

        This helper method should be used by subclasses to make HTTP requests.

        Args:
            method: HTTP method (e.g., 'GET', 'POST')
            url: URL to request
            **kwargs: Additional arguments for the request (See aiohttp.ClientSession.request)

        Returns:
            The response object from the request


        Raises:
            PluginError: If a shuttle plugin error occurs during the request

        Example:
            >>> try:
            ...     with await self._session_request('GET', 'https://api.example.com/data') as response:
            ...         data = await response.json()
            ... except PluginError as e:
            ...     print(f"Error occurred: {e}")

        Note:
            Error handling is built-in to log and re-raise as PluginError.
        """

        try:

            async with session_request(method, url, **kwargs) as response:
                response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)

                yield response
        except aiohttp.ClientError as e:
            _logger.error(f"HTTP request failed: {e}")
            raise exceptions.PluginError(
                plugin_name=self.name, message="Failed to make HTTP request", original_error=e
            ) from e
        except Exception as e:
            _logger.error(f"Unexpected error during HTTP request: {e}")
            raise exceptions.PluginError(
                plugin_name=self.name, message="Unexpected error during HTTP request", original_error=e
            ) from e
