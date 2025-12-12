# FLUXNET Shuttle Library

![CI](https://github.com/AMF-FLX/fluxnet-shuttle-lib/actions/workflows/python-ci.yml/badge.svg)

A Python library for FLUXNET shuttle to discover and download global FLUXNET data from multiple data hubs, including AmeriFlux, ICOS, and TERN.

## Features
- **Data Download**: Download FLUXNET data from data hubs
- **Metadata Snapshot**: List metadata for FLUXNET data available via the data hubs
- **Command Line Interface**: Easy-to-use CLI tool `fluxnet-shuttle` for common operations
- **Comprehensive Logging**: Configurable logging with multiple outputs
- **Error Handling**: Custom exception handling for FLUXNET operations

## Data Use Requirements

The FLUXNET data are shared under a [CC-BY-4.0 data use license](https://creativecommons.org/licenses/by/4.0/) which requires attribution for each data use.
See the data use license document contained within the FLUXNET data product (archive zip file) for details.

## Requirements

This library supports Python 3.11, 3.12, and 3.13. Python 3.9 and 3.10 should work but are not officially supported. Python versions before 3.9 are not allowed.

## Installation

### From PyPI (coming soon!)

### From GitHub using pip
```bash
pip install git+https://github.com/fluxnet/shuttle.git
```

## Example Jupyter Notebooks
Example Jupyter notebooks with data analysis and plotting are **coming soon**.

## Command Line Interface (CLI)

The library includes a command-line tool `fluxnet-shuttle` that provides easy access to core functionality:

### CLI Commands

#### `listall`
Discover all available FLUXNET data products and their metadata:
```bash
fluxnet-shuttle listall --verbose
```
- Queries all connected data hubs
- Creates a timestamped CSV file with metadata and download information

#### `download`
Download data for specific sites:
```bash
# Download specific sites
fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv -s IT-Niv PE-QFR US-NGB

# Download ALL sites from snapshot (prompts for confirmation)
fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv

# Download ALL sites without confirmation prompt and skip prompts to enter optional user information
fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv --quiet
```
- Requires a CSV snapshot file from the `listall` command (`-f/--snapshot-file`)
- Specify site IDs with `-s/--sites` to download specific sites only
- Omit `-s/--sites` to download all sites in the snapshot (will prompt for confirmation unless `--quiet` is used)
- Downloads are saved to the output directory (default: current directory, use `-o` to specify)

### CLI Options
- `-v/--verbose`: Enable detailed logging output
- `-l/--logfile`: Specify log file path (default: `fluxnet-shuttle-run.log`)
- `--no-logfile`: Disable file logging, output only to console
- `--quiet/-q`: Skip prompts to enter optional user information and confirmation prompt when downloading all sites from a snapshot file. 
- `--version`: Show version information
- `--help/-h`: Get help and see all options

### Example Workflow
```bash
# Step 1: Discover available data
fluxnet-shuttle listall --verbose

# Step 2: Download specific sites
fluxnet-shuttle download \
  -r fluxnet_shuttle_snapshot__20251006T155754.csv \
  -s PE-QFR IT-Niv \
  --verbose
```

## Development

### Installation for development
```bash
git clone https://github.com/AMF-FLX/fluxnet-shuttle-lib.git
cd fluxnet-shuttle-lib
pip install -e .[dev,docs]
```

# Run examples tests
pytest -m examples
```

### Test Coverage Report
Generate detailed test coverage report:
```bash
pytest --cov=fluxnet_shuttle --cov-report=html
```

### Documentation

Documentation is built with Sphinx and includes API documentation generated from docstrings.

To build the documentation locally:
```bash
cd docs
make html
```

## License

See [LICENSE](LICENSE) for details.

## Contributors

This library was developed by (listed in alphabetical order):
* **You-Wei Cheah** - LBL
* **Danielle S. Christianson** - LBL
* **Valerie Hendrix** - LBL
* **Sy-Toan Ngo** - LBL
* **Dario Papale** - University of Tuscia
* **Gilberto Pastorello** - LBL
* **Simone Sabbatini** - Division Impacts on Agriculture, Forests and Ecosystem Services (IAFES), Fondazione Centro Euro-Mediterraneo sui Cambiamenti Climatici

For support and questions, please contact: support@fluxnet.org