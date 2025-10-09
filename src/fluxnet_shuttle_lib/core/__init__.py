"""
Core framework components for FLUXNET Shuttle Library

:module:: fluxnet_shuttle_lib.core
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.core
.. autosummary::
    :toctree: generated/

    base.NetworkPlugin
    config.ShuttleConfig
    config.NetworkConfig
    decorators.async_to_sync_generator
    exceptions.FLUXNETShuttleError
    exceptions.PluginError
    registry.PluginRegistry
    registry.ErrorCollectingIterator
    shuttle.FluxnetShuttle


This module provides the core framework components for the FLUXNET Shuttle
library including plugin interfaces, decorators, and utilities.
"""

from .base import NetworkPlugin  # noqa: F401
from .config import NetworkConfig, ShuttleConfig  # noqa: F401
from .decorators import async_to_sync_generator  # noqa: F401
from .exceptions import FLUXNETShuttleError, PluginError  # noqa: F401
from .registry import ErrorCollectingIterator, PluginRegistry  # noqa: F401
from .shuttle import FluxnetShuttle  # noqa: F401

__all__ = [
    "NetworkPlugin",
    "async_to_sync_generator",
    "FLUXNETShuttleError",
    "PluginError",
    "PluginRegistry",
    "ErrorCollectingIterator",
    "FluxnetShuttle",
    "ShuttleConfig",
    "NetworkConfig",
]
