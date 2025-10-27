# FLUXNET Shuttle Library

![CI](https://github.com/AMF-FLX/fluxnet-shuttle-lib/actions/workflows/python-ci.yml/badge.svg)

A Python library for FLUXNET shuttle operations providing functionality for downloading and cataloging FLUXNET data from multiple networks including AmeriFlux, ICOS, and FLUXNET2015.

## Features
- **Data Download**: Download FLUXNET data from AmeriFlux and ICOS networks
- **Data Catalog**: List available datasets from multiple FLUXNET networks
- **Command Line Interface**: Easy-to-use CLI tool `fluxnet-shuttle` for common operations
- **Network Support**: 
  - AmeriFlux (via AmeriFlux API)
  - ICOS (via ICOS Carbon Portal)
  - FLUXNET2015 (placeholder for future implementation)
- **Comprehensive Logging**: Configurable logging with multiple outputs
- **Error Handling**: Custom exception handling for FLUXNET operations
- **Type Safety**: Full type hints for better development experience
- **Test Coverage**: 40+ tests with pytest and mocking support

## Installation

### From PyPI (when published)
```bash
pip install fluxnet-shuttle
```

### For Development
```bash
git clone https://github.com/AMF-FLX/fluxnet-shuttle-lib.git
cd fluxnet-shuttle-lib
pip install -e .[dev,docs]
```

### For Running Example Notebooks
To run the example Jupyter notebooks with data analysis and plotting:
```bash
pip install fluxnet-shuttle[examples]
```

### For Testing Notebooks
To run the notebook validation tests:
```bash
pip install fluxnet-shuttle[test-notebooks]
```

### All Optional Dependencies
To install all optional dependencies at once:
```bash
pip install fluxnet-shuttle[dev,docs,examples,test-notebooks]
```

## Requirements

This library officially supports Python 3.11, 3.12, and 3.13. Python 3.9 and 3.10 should work but are not officially supported. Python versions before 3.9 are not allowed.

## Command Line Interface

The library includes a command-line tool `fluxnet-shuttle` that provides easy access to core functionality:

### Basic Usage
```bash
# List all available datasets from AmeriFlux and ICOS
fluxnet-shuttle listall

# Download specific sites using a CSV file from listall
fluxnet-shuttle download -r data_availability_YYYYMMDDTHHMMSS.csv -s US-ARc IT-Niv

# Get help and see all options
fluxnet-shuttle --help
```

### CLI Commands

#### `listall`
Discover and catalog all available FLUXNET datasets:
```bash
fluxnet-shuttle listall --verbose
```
- Queries both AmeriFlux and ICOS networks
- Creates a timestamped CSV file with download information
- Takes 1-2 minutes to complete due to API processing time

#### `download`
Download data for specific sites:
```bash
fluxnet-shuttle download -r data_availability.csv -s US-ARc US-Bi1 IT-Niv
```
- Requires a CSV file from the `listall` command (`-r/--runfile`)
- Specify site IDs with `-s/--sites`
- Downloads are saved to the current directory

### CLI Options
- `-v/--verbose`: Enable detailed logging output
- `-l/--logfile`: Specify log file path (default: `fluxnet-shuttle-run.log`)
- `--no-logfile`: Disable file logging, output only to console
- `--version`: Show version information

### Example Workflow
```bash
# Step 1: Discover available data
fluxnet-shuttle listall --verbose

# Step 2: Download specific sites
fluxnet-shuttle download \
  -r data_availability_20251006T155754.csv \
  -s US-ARc IT-Niv \
  --verbose
```


#### Example/Notebook Tests
Validate documentation examples and Jupyter notebooks:
```bash
# Install test dependencies first
pip install .[test-notebooks]

# Run examples tests
pytest -m examples
```

### Coverage Report
Generate detailed coverage report:
```bash
pytest --cov=fluxnet_shuttle --cov-report=html
```

## Documentation

Documentation is built with Sphinx and includes API documentation generated from docstrings.

To build the documentation locally:
```bash
cd docs
make html
```

## License

See [LICENSE](LICENSE) for details.

## Copyright

FLUXNET Shuttle Library Copyright (c) 2025, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software, please contact Berkeley Lab's Intellectual Property Office at IPO@lbl.gov.

NOTICE. This Software was developed under funding from the U.S. Department of Energy and the U.S. Government consequently retains certain rights. As such, the U.S. Government has been granted for itself and others acting on its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software to reproduce, distribute copies to the public, prepare derivative works, and perform publicly and display publicly, and to permit other to do so.