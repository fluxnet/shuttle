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
pip install fluxnet-shuttle-lib
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
pip install fluxnet-shuttle-lib[examples]
```

### For Testing Notebooks
To run the notebook validation tests:
```bash
pip install fluxnet-shuttle-lib[test-notebooks]
```

### All Optional Dependencies
To install all optional dependencies at once:
```bash
pip install fluxnet-shuttle-lib[dev,docs,examples,test-notebooks]
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

## Quick Start

### Complete Example - From Discovery to Analysis

Here's a complete example showing how to discover, download, and analyze FLUXNET data:

```python
import os
import sys
import logging
import zipfile
import pandas as pd

from fluxnet_shuttle_lib import download, listall

# Configure logging to see progress
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### Step 1: Discover Available Data

```python
# Download a list of all available FLUXNET datasets from AmeriFlux and ICOS
# Note: listing AmeriFlux sites takes 1-2 minutes as download records are created
csv_filename = listall(ameriflux=True, icos=True)
print(f"Available data saved to: {csv_filename}")

# Load and examine the data catalog
df = pd.read_csv(csv_filename)
print(f"Found {len(df)} available datasets")
print(df.head())
```

### Step 2: Download Specific Sites

```python
# Select sites to download (examples: US-ARc from AmeriFlux, IT-Niv from ICOS)
sites = ['US-ARc', 'IT-Niv']

# Download the data files for the specified sites
downloaded_filenames = download(site_ids=sites, runfile=csv_filename)
print(f"Downloaded files: {downloaded_filenames}")
```

### Step 3: Extract and Analyze Data

```python
# Extract the first downloaded file
destination_directory = downloaded_filenames[0][:-4] + '_extracted'
with zipfile.ZipFile(downloaded_filenames[0], 'r') as zip_ref:
    zip_ref.extractall(destination_directory)
    extracted_files = zip_ref.namelist()
    print(f"Extracted {len(extracted_files)} files to {destination_directory}")

# Find and load half-hourly data
hh_file = os.path.join(destination_directory, 
                       [f for f in extracted_files if '_HH_' in f][0])

# Read the data with proper parsing
hh_data = pd.read_csv(hh_file, 
                      parse_dates=['TIMESTAMP_START'], 
                      na_values=('-9999', '-9999.0', '-9999.9'))

# Create a simple time series plot
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
hh_data.plot(x='TIMESTAMP_START', y='NEE_VUT_REF', 
            title='Half-Hourly Net Ecosystem Exchange', fontsize=10)
plt.show()
```

### Individual Function Usage

#### List Available Data
#### List Available Data
```python
from fluxnet_shuttle_lib import listall

# List data from all networks
csv_filename = listall(ameriflux=True, icos=True)
print(f"Data availability saved to: {csv_filename}")

# List data from specific networks only
ameriflux_only = listall(ameriflux=True, icos=False)
icos_only = listall(ameriflux=False, icos=True)
```

#### Download Data
```python
from fluxnet_shuttle_lib import download

#### Download Data
```python
from fluxnet_shuttle_lib import download

# Download data for specific sites using the CSV configuration file
site_ids = ["US-Ha1", "ES-LM1"]  # AmeriFlux and ICOS sites
runfile = "data_availability_20241006T120000.csv"  # From listall()

downloaded_files = download(site_ids, runfile)
print(f"Downloaded files: {downloaded_files}")
```

## Data Processing Examples

### Working with Downloaded FLUXNET Data

Once you've downloaded FLUXNET data files, here are common processing patterns:

#### Extract and Examine Data Structure
```python
import zipfile
import os

# Extract downloaded zip file
with zipfile.ZipFile('FLX_US-Ha1_FLUXNET2015_FULLSET_2020-2023_1.zip', 'r') as zip_ref:
    zip_ref.extractall('extracted_data/')
    files = zip_ref.namelist()
    print("Available data files:")
    for f in files:
        print(f"  {f}")
```

#### Load and Analyze Half-Hourly Data
```python
import pandas as pd
import matplotlib.pyplot as plt

# Load half-hourly data (HH files contain 30-minute measurements)
hh_file = 'extracted_data/FLX_US-Ha1_FLUXNET2015_FULLSET_HH_2020-2023_1-3.csv'
data = pd.read_csv(hh_file, 
                   parse_dates=['TIMESTAMP_START', 'TIMESTAMP_END'],
                   na_values=('-9999', '-9999.0', '-9999.9'))

# Examine data structure
print(f"Data shape: {data.shape}")
print(f"Date range: {data['TIMESTAMP_START'].min()} to {data['TIMESTAMP_START'].max()}")
print(f"Available variables: {list(data.columns)}")

# Plot key carbon flux variables
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Net Ecosystem Exchange
data.plot(x='TIMESTAMP_START', y='NEE_VUT_REF', ax=axes[0,0], 
          title='Net Ecosystem Exchange (NEE)', legend=False)

# Gross Primary Productivity
data.plot(x='TIMESTAMP_START', y='GPP_NT_VUT_REF', ax=axes[0,1], 
          title='Gross Primary Productivity (GPP)', legend=False)

# Ecosystem Respiration
data.plot(x='TIMESTAMP_START', y='RECO_NT_VUT_REF', ax=axes[1,0], 
          title='Ecosystem Respiration (RECO)', legend=False)

# Latent Heat Flux
data.plot(x='TIMESTAMP_START', y='LE_F_MDS', ax=axes[1,1], 
          title='Latent Heat Flux (LE)', legend=False)

plt.tight_layout()
plt.show()
```

#### Data Quality Analysis
```python
# Examine data quality flags and coverage
print("\nData Quality Summary:")
print(f"NEE data coverage: {(~data['NEE_VUT_REF'].isna()).sum() / len(data) * 100:.1f}%")
print(f"GPP data coverage: {(~data['GPP_NT_VUT_REF'].isna()).sum() / len(data) * 100:.1f}%")

# Quality flag analysis (if available)
if 'NEE_VUT_REF_QC' in data.columns:
    qc_counts = data['NEE_VUT_REF_QC'].value_counts().sort_index()
    print(f"\nNEE Quality Flag Distribution:")
    for qc, count in qc_counts.items():
        print(f"  QC {qc}: {count} records ({count/len(data)*100:.1f}%)")
```

### CSV Configuration File Format

The CSV file generated by `listall()` has the following structure:
```csv
network,publisher,site_id,first_year,last_year,version,filename,download_link
AmeriFlux,AmeriFlux,US-Ha1,2020,2023,1,FLX_US-Ha1_FLUXNET2015_FULLSET_2020-2023_1.zip,https://...
ICOS,ICOS-ETC,ES-LM1,2020,2023,1,FLX_ES-LM1_FLUXNET2015_FULLSET_2020-2023_1.zip,https://...
```

This file is automatically generated by `listall()` and contains all necessary information for downloading data with `download()`.

## Examples

### Jupyter Notebook Example
A complete Jupyter notebook example is available at [`examples/fluxnet_shuttle_example.ipynb`](examples/fluxnet_shuttle_example.ipynb) demonstrating:
- Data discovery and catalog examination
- Site selection and download
- Data extraction and quality assessment  
- Time series visualization
- Monthly aggregation and analysis

**To run the notebook, install the examples dependencies:**
```bash
pip install fluxnet-shuttle-lib[examples]
# This installs: pandas, matplotlib, jupyter, notebook
```

### Command Line Usage
```python
# Quick data discovery
python -c "from fluxnet_shuttle_lib import listall; print(listall())"

# Download specific sites
python -c "
from fluxnet_shuttle_lib import listall, download
csv_file = listall()
download(['US-Ha1', 'DE-Tha'], csv_file)
"
```

## Development
- Python 3.9+
- Dependencies: `requests` for HTTP operations
- Testing: `pytest` with `pytest-mock` for mocking
- See `pyproject.toml` for complete dependency list
- For contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md)

## Testing

The test suite is organized into different categories with appropriate markers. By default, only fast unit tests are run.

### Default Test Run (Fast Unit Tests)
Run unit tests only (excludes slow integration tests):
```bash
pytest
```

This runs 118 unit tests that complete in under 1 second and achieve 100% code coverage.

### All Tests
Run all tests including slow integration tests:
```bash
pytest -m ""
```

This runs all 158 tests including real API calls (takes 2-3 minutes).

### Test Categories

#### Unit Tests (Default)
Fast tests with mocking, run by default:
```bash
pytest -m "not integration and not slow and not performance and not examples"
```

#### Integration Tests 
Tests that make real API calls (slow):
```bash
pytest -m integration
```

#### Performance Tests
Benchmark tests for timing analysis:
```bash
pytest -m performance
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
pytest --cov=fluxnet_shuttle_lib --cov-report=html
```

### CLI Testing
Test the command-line interface:
```bash
# Test CLI help and basic functionality
fluxnet-shuttle --help
fluxnet-shuttle listall --help

# Quick integration test
fluxnet-shuttle listall --verbose --no-logfile
```

## API Reference

### Main Functions

#### `listall(ameriflux=True, icos=True)`
Lists all available FLUXNET data from specified networks and saves to CSV file.

**Parameters:**
- `ameriflux` (bool, default=True): Include AmeriFlux data in the catalog
- `icos` (bool, default=True): Include ICOS data in the catalog

**Returns:** 
- `str`: Filename of the generated CSV file containing data availability information

**Example:**
```python
# Get data from all networks
csv_file = listall(ameriflux=True, icos=True)

# Get only AmeriFlux data  
ameriflux_csv = listall(ameriflux=True, icos=False)
```

#### `download(site_ids, runfile)`
Downloads FLUXNET data for specified sites using configuration from a CSV file.

**Parameters:**
- `site_ids` (list): List of site IDs to download data for (e.g., ['US-Ha1', 'DE-Tha'])
- `runfile` (str): Path to CSV configuration file (typically generated by `listall()`)

**Returns:** 
- `list`: List of downloaded filenames (absolute paths)

**Raises:**
- `FLUXNETShuttleError`: If site_ids or runfile are invalid, or download fails

**Example:**
```python
# Download data for specific sites
sites = ['US-Ha1', 'IT-Niv']
csv_file = 'data_availability_20241006T120000.csv'
downloaded = download(site_ids=sites, runfile=csv_file)
```

### Configuration and Utilities

#### `log_config(level=logging.DEBUG, filename=None, std=True, ...)`
Configure logging for the FLUXNET Shuttle Library.

#### `FLUXNETShuttleError`
Custom exception class for FLUXNET Shuttle operations.

### Exception Handling
The library uses `FLUXNETShuttleError` for all operation-specific errors.

```python
from fluxnet_shuttle_lib import FLUXNETShuttleError, download

try:
    download(["INVALID"], "nonexistent.csv")
except FLUXNETShuttleError as e:
    print(f"Error: {e}")
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