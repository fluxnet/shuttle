"""Shared pytest fixtures for fluxnet_shuttle_lib tests."""

import logging
import os
import tempfile
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing."""
    content = "site_id,network,filename,download_link\n"
    content += "US-TEST,AmeriFlux,test_ameriflux.zip,http://example.com/ameriflux.zip\n"
    content += "IT-TEST,ICOS,test_icos.zip,http://example.com/icos.zip\n"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        temp_file = f.name

    yield temp_file

    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


@pytest.fixture
def mock_requests_response():
    """Create a mock requests response object."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "test"}
    mock_response.iter_content.return_value = [b"test_data"]
    mock_response.raise_for_status.return_value = None
    return mock_response


@pytest.fixture
def sample_ameriflux_api_response():
    """Sample AmeriFlux API response for testing."""
    return {
        "data_urls": [
            {
                "site_id": "US-TEST1",
                "url": "http://example.com/FLX_US-TEST1_FLUXNET2015_FULLSET_" "2020-2023_1.zip",
            },
            {
                "site_id": "US-TEST2",
                "url": "http://example.com/FLX_US-TEST2_FLUXNET2015_FULLSET_" "2019-2022_2.zip",
            },
        ]
    }


@pytest.fixture
def sample_icos_api_response():
    """Sample ICOS API response for testing."""
    return {
        "results": {
            "bindings": [
                {
                    "station": {"value": "http://meta.icos-cp.eu/resources/stations/ES_ES-LM1"},
                    "dobj": {"value": "http://meta.icos-cp.eu/objects/12345"},
                    "fileName": {"value": "FLX_ES-LM1_FLUXNET2015_FULLSET_2020-2023_1.zip"},
                },
                {
                    "station": {"value": "http://meta.icos-cp.eu/resources/stations/IT_IT-CA1"},
                    "dobj": {"value": "http://meta.icos-cp.eu/objects/67890"},
                    "fileName": {"value": "FLX_IT-CA1_FLUXNET2015_FULLSET_2019-2022_2.zip"},
                },
            ]
        }
    }


@pytest.fixture
def clean_logger():
    """Provide a clean logger for testing logging functionality."""
    logger = logging.getLogger()
    original_handlers = logger.handlers[:]
    original_level = logger.level

    # Clear handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    yield logger

    # Restore original state
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    for handler in original_handlers:
        logger.addHandler(handler)
    logger.setLevel(original_level)


@pytest.fixture
def temp_directory():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture(autouse=True)
def reset_warnings():
    """Reset warnings settings after each test."""
    import warnings

    original_showwarning = warnings.showwarning
    yield
    warnings.showwarning = original_showwarning
