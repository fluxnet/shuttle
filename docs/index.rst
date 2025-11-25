FLUXNET Shuttle Library Documentation
=====================================

Welcome to FLUXNET Shuttle Library's documentation!

FLUXNET Shuttle Library is a Python library for FLUXNET shuttle operations providing
functionality for discovering, downloading, and cataloging FLUXNET data from multiple
data hubs including AmeriFlux, ICOS, and FLUXNET2015.

Features
--------

- **Data Download**: Download FLUXNET data from AmeriFlux and ICOS data hubs
- **Data Catalog**: List available datasets from multiple FLUXNET data hubs  
- **Command Line Interface**: Easy-to-use CLI tool ``fluxnet-shuttle`` for common operations
- **Integrated Data Hubs**: AmeriFlux (via AmeriFlux API), ICOS (via ICOS Carbon Portal), FLUXNET2015 (placeholder)
- **Comprehensive Logging**: Configurable logging with multiple outputs
- **Error Handling**: Custom exception handling for FLUXNET operations
- **Type Safety**: Full type hints for better development experience
- **Test Coverage**: 100% test coverage with pytest and mocking support

Installation
------------

From PyPI (when published)::

    pip install fluxnet-shuttle

For Development::

    git clone https://github.com/AMF-FLX/fluxnet-shuttle-lib.git
    cd fluxnet-shuttle-lib
    pip install -e .[dev,docs]

For Running Example Notebooks::

    pip install fluxnet-shuttle[examples]

Quick Start
-----------

**Programmatic Usage:**

.. code-block:: python

    from fluxnet_shuttle import download, listall

    # Discover available data
    csv_filename = listall(ameriflux=True, icos=True)
    print(f"Available data saved to: {csv_filename}")

    # Download specific sites
    sites = ['US-ARc', 'IT-Niv']
    downloaded_files = download(site_ids=sites, snapshot_file=csv_filename)

For advanced usage with error handling and the developer API, see :doc:`developer_guide`.

**Command Line Usage:**

.. code-block:: bash

    # List all available datasets (creates snapshot file)
    fluxnet-shuttle listall

    # Download data for specific sites using snapshot file
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv -s US-Ha1 US-MMS

    # Create output directory
    mkdir /data/fluxnet
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv -s US-ARc IT-Niv -o /data/fluxnet

For complete CLI documentation, see :doc:`cli`.

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