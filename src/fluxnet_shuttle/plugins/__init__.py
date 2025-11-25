"""
Data hub-specific plugins for FLUXNET Shuttle Library

:module:: fluxnet_shuttle.plugins
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle.plugins

This module contains data hub-specific plugins for accessing different
FLUXNET data sources.
"""

# Import plugins to trigger auto-registration
from .ameriflux import AmeriFluxPlugin  # noqa: F401
from .icos import ICOSPlugin  # noqa: F401

__all__ = ["AmeriFluxPlugin", "ICOSPlugin"]
