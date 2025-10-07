"""
Configuration and fixtures for integration tests.

This file defines pytest markers and fixtures used across integration tests.
"""

import pytest


def pytest_configure(config):
    """Configure pytest markers for integration tests."""
    config.addinivalue_line("markers", "integration: mark test as an integration test that requires network access")
    config.addinivalue_line("markers", "slow: mark test as slow-running (may take several minutes)")
    config.addinivalue_line("markers", "performance: mark test as a performance benchmark test")


@pytest.fixture(scope="session")
def network_available():
    """Check if network is available for integration tests."""
    import requests

    try:
        # Try to reach a reliable endpoint
        response = requests.get("https://httpbin.org/status/200", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


@pytest.fixture(scope="session")
def ameriflux_api_available():
    """Check if AmeriFlux API is available."""
    import requests

    from fluxnet_shuttle_lib.sources.ameriflux import AMERIFLUX_BASE_URL

    try:
        # Try to reach AmeriFlux API
        url = f"{AMERIFLUX_BASE_URL}api/v1/site_availability/AmeriFlux/FLUXNET/CCBY4.0"
        response = requests.get(url, timeout=10)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


@pytest.fixture(scope="session")
def icos_api_available():
    """Check if ICOS Carbon Portal API is available."""
    import requests

    from fluxnet_shuttle_lib.sources.icos import ICOS_API_URL

    try:
        # Try to reach ICOS SPARQL endpoint
        response = requests.get(ICOS_API_URL, timeout=10)
        # SPARQL endpoint might return 400 without proper query, but should be reachable
        return response.status_code in [200, 400, 405]
    except requests.exceptions.RequestException:
        return False


@pytest.fixture
def temp_download_dir():
    """Create a temporary directory for download tests."""
    import shutil
    import tempfile

    temp_dir = tempfile.mkdtemp(prefix="fluxnet_test_")
    yield temp_dir
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_ameriflux_sites():
    """Provide sample AmeriFlux site IDs for testing."""
    return ["US-ARM", "US-Ton", "US-Var"]


@pytest.fixture
def sample_icos_data():
    """Provide sample ICOS data structure for testing."""
    return {
        "https://meta.icos-cp.eu/objects/sample1": {
            "station": "https://meta.icos-cp.eu/resources/stations/test_station",
            "fileName": "FLX_SAMPLE_FLUXNET_ARCHIVE_2020.zip",
            "size": "1048576",
            "timeStart": "2020-01-01T00:00:00Z",
            "timeEnd": "2020-12-31T23:59:59Z",
        },
        "https://meta.icos-cp.eu/objects/sample2": {
            "station": "https://meta.icos-cp.eu/resources/stations/test_station2",
            "fileName": "FLX_SAMPLE2_FLUXNET_ARCHIVE_2021.zip",
            "size": "2097152",
            "timeStart": "2021-01-01T00:00:00Z",
            "timeEnd": "2021-12-31T23:59:59Z",
        },
    }


@pytest.fixture
def sample_ameriflux_response():
    """Provide sample AmeriFlux API response for testing."""
    return {
        "data_urls": [
            {
                "site_id": "US-ARM",
                "url": "https://example.com/FLX_US-ARM_FLUXNET2015_FULLSET_2003-2012_1.zip",
            },
            {
                "site_id": "US-Ton",
                "url": "https://example.com/FLX_US-Ton_FLUXNET2015_FULLSET_2001-2014_1.zip",
            },
        ]
    }


@pytest.fixture
def mock_requests_session():
    """Provide a mocked requests session for testing."""
    from unittest.mock import MagicMock

    session = MagicMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"results": {"bindings": []}}
    response.iter_content.return_value = [b"test data chunk"]
    response.headers = {"content-length": "100"}

    session.get.return_value = response
    session.post.return_value = response
    session.head.return_value = response

    return session


def pytest_collection_modifyitems(config, items):
    """Automatically mark integration tests based on file location."""
    for item in items:
        # Mark all tests in integration directory as integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Mark tests with "slow" in the name as slow
        if "slow" in item.name.lower() or "performance" in item.name.lower():
            item.add_marker(pytest.mark.slow)


def pytest_runtest_setup(item):
    """Skip integration tests if network is not available (when requested)."""
    if "integration" in [mark.name for mark in item.iter_markers()]:
        # Only skip if explicitly requested via command line
        if item.config.getoption("--skip-integration", default=False):
            pytest.skip("Integration tests skipped via --skip-integration option")
