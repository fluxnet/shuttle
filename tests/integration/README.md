# Integration Tests

This directory contains integration tests for the FLUXNET Shuttle Library that make actual HTTP requests to external APIs.

## Overview

Integration tests verify that the library works correctly with real external services:

- **AmeriFlux API**: Tests data retrieval from AmeriFlux CDN
- **ICOS Carbon Portal**: Tests SPARQL queries and data download from ICOS
- **Combined workflows**: Tests that combine multiple data sources

## Running Integration Tests

### Prerequisites

- Network connectivity
- Access to AmeriFlux and ICOS APIs
- Sufficient time (some tests may take several minutes)

### Basic Commands

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run only AmeriFlux integration tests
pytest tests/integration/test_ameriflux_integration.py -v

# Run only ICOS integration tests  
pytest tests/integration/test_icos_integration.py -v

# Skip slow tests (run only fast integration tests)
pytest tests/integration/ -m "not slow" -v

# Run only performance tests
pytest tests/integration/ -m "performance" -v

# Skip all integration tests (run only unit tests)
pytest -m "not integration"
```

### Running with Coverage

```bash
# Run integration tests with coverage (but skip coverage requirements)
pytest tests/integration/ --cov=src/fluxnet_shuttle_lib --cov-report=term-missing --cov-fail-under=0
```

## Test Categories

### Markers

- `@pytest.mark.integration`: All tests that require network access
- `@pytest.mark.slow`: Tests that may take several minutes to complete
- `@pytest.mark.performance`: Performance benchmark tests

### Test Types

1. **API Connectivity Tests**: Verify that APIs are accessible
2. **Data Retrieval Tests**: Test actual data fetching from services
3. **Workflow Tests**: Test complete end-to-end workflows
4. **Error Handling Tests**: Test error scenarios with real services
5. **Performance Tests**: Benchmark API response times and data processing

## API Availability

Integration tests automatically skip when APIs are not available:

```python
try:
    # Test real API
    data = get_real_api_data()
    assert data is not None
except requests.exceptions.RequestException as e:
    pytest.skip(f"API not accessible: {e}")
```

## Configuration

### Environment Variables

You can set these environment variables to customize test behavior:

- `FLUXNET_TEST_TIMEOUT`: HTTP request timeout (default: 30 seconds)
- `FLUXNET_TEST_MAX_SITES`: Maximum number of sites to test (default: 3)

### Pytest Configuration

Integration tests use these pytest configurations:

```toml
[tool.pytest.ini_options]
markers = [
    "integration: marks tests as integration tests",
    "slow: marks tests as slow", 
    "performance: marks tests as performance benchmarks",
]
```

## Fixtures

Common fixtures available for integration tests:

- `network_available`: Check if network connectivity exists
- `ameriflux_api_available`: Check if AmeriFlux API is accessible
- `icos_api_available`: Check if ICOS API is accessible
- `temp_download_dir`: Temporary directory for download tests
- `sample_ameriflux_sites`: Sample site IDs for testing
- `sample_icos_data`: Sample ICOS data structures

## Best Practices

1. **Keep tests fast**: Limit data requests to small subsets
2. **Handle failures gracefully**: Use `pytest.skip()` when APIs are unavailable
3. **Mock large downloads**: Use mocks for file downloads to avoid bandwidth issues
4. **Test error scenarios**: Verify proper error handling with real services
5. **Use appropriate markers**: Mark slow and performance tests appropriately

## Example Usage

```python
import pytest
from fluxnet_shuttle_lib.sources.ameriflux import get_ameriflux_fluxnet_sites

@pytest.mark.integration
def test_ameriflux_api_basic():
    """Test basic AmeriFlux API connectivity."""
    try:
        sites = get_ameriflux_fluxnet_sites()
        assert isinstance(sites, list)
    except requests.exceptions.RequestException as e:
        pytest.skip(f"AmeriFlux API not accessible: {e}")

@pytest.mark.integration
@pytest.mark.slow
def test_complete_workflow():
    """Test complete workflow - may take several minutes."""
    # ... implementation
```

## Troubleshooting

### Common Issues

1. **Network timeouts**: Increase timeout values or skip tests
2. **API rate limiting**: Add delays between requests
3. **Authentication errors**: Check API credentials/access
4. **Large downloads**: Mock download operations for testing

### Debug Mode

Run with verbose output to see detailed test information:

```bash
pytest tests/integration/ -v -s --tb=short
```