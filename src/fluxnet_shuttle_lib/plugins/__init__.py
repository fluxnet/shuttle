"""
Network-specific plugins for FLUXNET Shuttle Library

:module:: fluxnet_shuttle_lib.plugins
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.plugins
.. autosummary::
    :toctree: generated/

    ameriflux.AmeriFluxPlugin
    icos.ICOSPlugin

This module contains network-specific plugins for accessing different
FLUXNET data sources.
"""

# Import plugins to trigger auto-registration
from .ameriflux import AmeriFluxPlugin  # noqa: F401
from .icos import ICOSPlugin  # noqa: F401

__all__ = ["AmeriFluxPlugin", "ICOSPlugin"]
