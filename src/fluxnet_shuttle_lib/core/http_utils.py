"""
HTTP utilities for making API requests and handling responses
=============================================================

:module:: fluxnet_shuttle_lib.core.http_utils
:synopsis: HTTP utilities for FLUXNET Shuttle Library
:module author:: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-15

.. currentmodule:: fluxnet_shuttle_lib.core.http_utils

"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aiohttp

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_session() -> AsyncGenerator[aiohttp.ClientSession, None]:
    """
    Create and return an aiohttp ClientSession with a specified timeout.
    The session is configured to handle slow TLS handshakes and disables the
    default 5-second idle read timeout.

    Returns
    -------
    aiohttp.ClientSession
        An instance of aiohttp ClientSession with the specified timeout.
    """
    # ----------------------------------------------------------------------
    # Choose a *very* permissive timeout to allow for large uploads, matches
    # the default requests timeout behavior.  The key setting is:
    #    - total=None → no overall deadline
    #    - sock_connect → a generous connect timeout (60 s is usually enough)
    #    - sock_read=None → wait forever for the server to finally answer
    # ----------------------------------------------------------------------
    client_timeout = aiohttp.ClientTimeout(
        total=None,  # no global deadline
        sock_connect=60,  # allow slow TLS handshakes on a busy network
        sock_read=None,  # **key** – disables the 5‑second idle read timeout
    )
    session = aiohttp.ClientSession(timeout=client_timeout)
    yield session
    await session.close()


@asynccontextmanager
async def session_request(
    method: str,
    url: str,
    **kwargs,
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
