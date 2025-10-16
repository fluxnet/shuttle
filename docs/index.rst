FLUXNET Shuttle Library Documentation
=====================================

Welcome to FLUXNET Shuttle Library's documentation!

FLUXNET Shuttle Library is a Python library for FLUXNET shuttle operations providing
functionality for discovering, downloading, and cataloging FLUXNET data from multiple
networks including AmeriFlux, ICOS, and FLUXNET2015.

Features
--------

- **Data Download**: Download FLUXNET data from AmeriFlux and ICOS networks
- **Data Catalog**: List available datasets from multiple FLUXNET networks  
- **Command Line Interface**: Easy-to-use CLI tool ``fluxnet-shuttle`` for common operations
- **Network Support**: AmeriFlux (via AmeriFlux API), ICOS (via ICOS Carbon Portal), FLUXNET2015 (placeholder)
- **Comprehensive Logging**: Configurable logging with multiple outputs
- **Error Handling**: Custom exception handling for FLUXNET operations
- **Type Safety**: Full type hints for better development experience
- **Test Coverage**: 100% test coverage with pytest and mocking support

Installation
------------

From PyPI (when published)::

    pip install fluxnet-shuttle-lib

For Development::

    git clone https://github.com/AMF-FLX/fluxnet-shuttle-lib.git
    cd fluxnet-shuttle-lib
    pip install -e .[dev,docs]

For Running Example Notebooks::

    pip install fluxnet-shuttle-lib[examples]

Quick Start
-----------

**Programmatic Usage:**

.. code-block:: python

    from fluxnet_shuttle_lib import download, listall

    # Discover available data
    csv_filename = listall(ameriflux=True, icos=True)
    print(f"Available data saved to: {csv_filename}")

    # Download specific sites
    sites = ['US-ARc', 'IT-Niv']
    downloaded_files = download(site_ids=sites, runfile=csv_filename)

For advanced usage with error handling and the developer API, see :doc:`developer_guide`.

**Command Line Usage:**

.. code-block:: bash

    # List all available datasets
    fluxnet-shuttle listall --verbose --output sites.csv

    # Download data for specific sites
    fluxnet-shuttle download -s US-Ha1 US-MMS -r sites.csv

    # Run connectivity tests
    fluxnet-shuttle test

Documentation
=============

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   developer_guide
   cli
   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`