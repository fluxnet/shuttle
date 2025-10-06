# FLUXNET Shuttle Library

![CI](https://github.com/AMF-FLX/fluxnet-shuttle-lib/actions/workflows/python-ci.yml/badge.svg)

A Python library for FLUXNET shuttle operations providing core functionality for data processing and analysis.

## Features
- Core data processing utilities for FLUXNET datasets
- Pydantic models for data validation
- Type-safe interfaces for data manipulation
- Comprehensive test coverage
- Sphinx documentation with autodoc

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

## Development
- Python 3.9+
- See `pyproject.toml` for dependencies
- For contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md)

## Usage

```python
from fluxnet_shuttle_lib import main

# Run the main function
main()
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