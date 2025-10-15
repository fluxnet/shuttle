"""
Unit tests for HTTP utility functions.
======================================

These tests cover the functionality of HTTP request handling, including
successful requests and error handling.
"""

from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from fluxnet_shuttle_lib.core.http_utils import session_request


class TestHTTPUtils:
    """Test suite for HTTP utility functions."""

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle_lib.core.http_utils.aiohttp.ClientSession.request")
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
    @patch("fluxnet_shuttle_lib.core.http_utils.aiohttp.ClientSession.request")
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
