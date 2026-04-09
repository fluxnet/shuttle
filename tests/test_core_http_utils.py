"""
Unit tests for HTTP utility functions.
======================================

These tests cover the functionality of HTTP request handling, including
successful requests and error handling.
"""

from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from fluxnet_shuttle.core.config import (
    DEFAULT_HTTP_SOCK_CONNECT,
    DEFAULT_HTTP_SOCK_READ,
    DEFAULT_HTTP_TIMEOUT_TOTAL,
    HttpTimeoutConfig,
)
from fluxnet_shuttle.core.http_utils import get_session, session_request


class TestHTTPUtils:
    """Test suite for HTTP utility functions."""

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.aiohttp.ClientSession")
    async def test_get_session_default_timeouts(self, mock_session_cls):
        """Test get_session uses HttpTimeoutConfig defaults when no arg given."""
        mock_session = AsyncMock()
        mock_session_cls.return_value = mock_session

        async with get_session() as session:
            assert session is mock_session

        call_kwargs = mock_session_cls.call_args.kwargs
        timeout = call_kwargs["timeout"]

        if DEFAULT_HTTP_TIMEOUT_TOTAL is None:
            assert timeout.total is None
        else:
            assert timeout.total == DEFAULT_HTTP_TIMEOUT_TOTAL
        assert timeout.sock_connect == DEFAULT_HTTP_SOCK_CONNECT
        assert timeout.sock_read == DEFAULT_HTTP_SOCK_READ

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.aiohttp.ClientSession")
    async def test_get_session_custom_timeouts(self, mock_session_cls):
        """Test get_session applies custom HttpTimeoutConfig values."""
        mock_session = AsyncMock()
        mock_session_cls.return_value = mock_session

        tc = HttpTimeoutConfig(total=60.0, sock_connect=5.0, sock_read=200.0)
        async with get_session(http_timeouts=tc) as session:
            assert session is mock_session

        call_kwargs = mock_session_cls.call_args.kwargs
        timeout = call_kwargs["timeout"]
        assert timeout.total == 60.0
        assert timeout.sock_connect == 5.0
        assert timeout.sock_read == 200.0

    @pytest.mark.asyncio
    async def test_session_request_forwards_http_timeouts(self):
        """Test session_request passes http_timeouts to get_session."""
        tc = HttpTimeoutConfig(total=5.0, sock_connect=2.0, sock_read=10.0)

        mock_response = AsyncMock()
        mock_response.raise_for_status.return_value = None

        mock_session = AsyncMock(spec=aiohttp.ClientSession)
        mock_session.request.return_value.__aenter__.return_value = mock_response
        mock_session.close = AsyncMock()

        with patch("fluxnet_shuttle.core.http_utils.get_session") as mock_get_session:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__.return_value = mock_session
            mock_ctx.__aexit__.return_value = None
            mock_get_session.return_value = mock_ctx

            async with session_request("GET", "https://amfcdn-dev.lbl.gov", http_timeouts=tc) as response:
                assert response is mock_response

            mock_get_session.assert_called_once_with(http_timeouts=tc)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.aiohttp.ClientSession.request")
    async def test_session_request_success(self, mock_request):
        """Test successful HTTP GET request."""
        url = "https://httpbin.org/get"
        mock_response = AsyncMock()
        mock_response.json.return_value = {"url": url}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value.__aenter__.return_value = mock_response
        async with session_request("GET", url) as response:
            data = await response.json()
            assert data["url"] == url
        mock_request.assert_called_once_with("GET", url)

    @pytest.mark.asyncio
    async def test_session_request_invalid_url(self):
        """Test HTTP request with an invalid URL."""
        url = "https://invalid.url"
        with pytest.raises(aiohttp.ClientConnectionError) as exc_info:
            async with session_request("GET", url) as response:  # noqa: F841
                pass
            assert "Failed to make HTTP request" in str(exc_info.value)
            assert isinstance(exc_info.value.original_error, aiohttp.ClientConnectionError)

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.core.http_utils.aiohttp.ClientSession.request")
    async def test_session_request_http_error(self, mock_request):
        """Test HTTP request that results in an HTTP error (e.g., 404)."""
        url = "https://httpbin.org/status/404"
        mock_response = AsyncMock()

        def mock_raise_for_status():
            raise aiohttp.ClientResponseError(
                request_info=mock_request,
                history=(),
                status=404,
                message="Not Found",
                headers=None,
            )

        mock_response.raise_for_status = mock_raise_for_status
        mock_request.return_value.__aenter__.return_value = mock_response
        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            async with session_request("GET", url) as response:  # noqa: F841
                pass
        assert exc_info.value.status == 404
        assert exc_info.value.message == "Not Found"
        mock_request.assert_called_once_with("GET", url)
