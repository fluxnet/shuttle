"""
Custom Exceptions for FLUXNET Shuttle Library
=============================================

:module:: fluxnet_shuttle.core.exceptions
:synopsis: Custom exceptions for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

This module defines custom exceptions used throughout the FLUXNET Shuttle
library for error handling and debugging.
"""

from typing import Any, Dict, Optional


class FLUXNETShuttleError(Exception):
    """Base exception class for FLUXNET Shuttle operations."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class PluginError(FLUXNETShuttleError):
    """Exception raised when a plugin encounters an error."""

    def __init__(self, plugin_name: str, message: str, original_error: Optional[Exception] = None):
        super().__init__(f"Plugin '{plugin_name}': {message}")
        self.plugin_name = plugin_name
        self.original_error = original_error


class ConfigurationError(FLUXNETShuttleError):
    """Exception raised when there's a configuration issue."""

    pass


class NetworkError(PluginError):
    """Exception raised when there's a connectivity issue."""

    pass
