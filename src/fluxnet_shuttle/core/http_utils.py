"""
HTTP utilities for making API requests and handling responses
=============================================================

:module:: fluxnet_shuttle.core.http_utils
:synopsis: HTTP utilities for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-15
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle.core.http_utils

"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import aiohttp

from .config import HttpTimeoutConfig

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_session(
    http_timeouts: Optional[HttpTimeoutConfig] = None,
) -> AsyncGenerator[aiohttp.ClientSession, None]:
    """
    Create and return an aiohttp ClientSession with configurable timeouts.

    Parameters
    ----------
    http_timeouts : HttpTimeoutConfig, optional
        Timeout settings to apply to the session.  When *None* the
        hardcoded defaults from :class:`HttpTimeoutConfig` are used.

    Returns
    -------
    aiohttp.ClientSession
        An instance of aiohttp ClientSession with the specified timeout.
    """
    if http_timeouts is None:
        http_timeouts = HttpTimeoutConfig()

    client_timeout = aiohttp.ClientTimeout(
        total=http_timeouts.total,
        sock_connect=http_timeouts.sock_connect,
        sock_read=http_timeouts.sock_read,
    )
    session = aiohttp.ClientSession(timeout=client_timeout)
    try:
        yield session
    finally:
        await session.close()


@asynccontextmanager
async def session_request(
    method: str,
    url: str,
    **kwargs: Any,
) -> AsyncGenerator[aiohttp.ClientResponse, None]:
    """
    Make an HTTP request using the provided aiohttp ClientSession.

    Parameters
    ----------
    method : str
        The HTTP method to use (e.g., 'GET', 'POST').
    url : str
        The URL to which the request is sent.
    **kwargs
        Additional keyword arguments to pass to the session's request method.

    Returns
    -------
    aiohttp.ClientResponse
        The response object from the HTTP request.

    Raises
    ------
    aiohttp.ClientError
        If an error occurs during the HTTP request.
    """
    try:
        async with get_session() as session:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
                yield response
    except aiohttp.ClientResponseError as e:
        _logger.error(f"HTTP response error: {e.status} - {e.message}")
        raise e
    except aiohttp.ClientConnectionError as e:
        _logger.error(f"HTTP request failed: {e}")
        raise e
