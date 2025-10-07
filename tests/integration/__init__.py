"""
Integration tests for FLUXNET Shuttle Lib.

This directory contains integration tests that make real HTTP requests
to external APIs. These tests are separate from unit tests and should
be run with appropriate network connectivity.

Run integration tests with:
    pytest tests/integration/ -v

Skip integration tests with:
    pytest -m "not integration"

Run only fast integration tests:
    pytest tests/integration/ -m "not slow" -v
"""
