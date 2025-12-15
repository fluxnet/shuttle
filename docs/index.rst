FLUXNET Shuttle Library Documentation
=====================================

Welcome to FLUXNET Shuttle Library's documentation!

FLUXNET Shuttle Library is a Python library for discovering and downloading global
FLUXNET data from data hubs including AmeriFlux, ICOS, and TERN.

The data hubs provide FLUXNET data organized and coordinated by many regional networks, including
AmeriFlux, AsiaFlux, ChinaFlux, JapanFlux, KoFlux, ICOS, European Flux database, OzFlux, TERN, and SAEON.

Features
--------

- **Data Download**: Download FLUXNET data from AmeriFlux, ICOS, and TERN data hubs
- **Metadata Snapshot**: List metadata for FLUXNET data available via the data hubs
- **Command Line Interface**: Easy-to-use CLI tool ``fluxnet-shuttle`` for common operations
- **Comprehensive Logging**: Configurable logging with multiple outputs
- **Error Handling**: Custom exception handling for FLUXNET operations

Data Use Requirements
---------------------

The FLUXNET data are shared under a CC-BY-4.0 data use license (https://creativecommons.org/licenses/by/4.0/) which requires attribution for each data use.
See the data use license document contained within the FLUXNET data product (archive zip file) for details.

Installation
------------

From PyPI::

    Coming soon!

From GitHub using pip::

    pip install git+https://github.com/fluxnet/shuttle.git

For Development::

    git clone https://github.com/fluxnet/shuttle.git
    cd shuttle
    pip install -e .[dev,docs]

Supported python versions: 3.11, 3.12, 3.13 (3.9, 3.10 may work but are not officially supported; <3.9 not allowed.)

Quick Start
-----------

**Programmatic Usage:**

.. code-block:: python

    from fluxnet_shuttle import download, listall

    # Discover available data
    csv_filename = listall()
    print(f"Available data saved to: {csv_filename}")

    # Download specific sites
    sites = ['PE-QFR', 'IT-Niv']
    downloaded_files = download(site_ids=sites, snapshot_file=csv_filename)

    # Download specific sites with optional user information
    # Intended Use options: 1. Synthesis, 2. Model, 3. Remote sensing, 4. Other research, 5. Education, 6. Other
    sites = ['PE-QFR', 'IT-Niv']
    user_info={'ameriflux': {'user_name': 'O2. Carbon', 'user_email': 'o2.carbon@flux.flux', 'intended_use': 1, 'description': 'Analysis of water flux'}}
    downloaded_files = download(site_ids=sites, snapshot_file=csv_filename, user_info=user_info)

For advanced usage with error handling and the developer API, see :doc:`developer_guide`.

**Command Line Usage:**

.. code-block:: bash

    # List all available datasets (creates snapshot file)
    fluxnet-shuttle listall

    # Download data for specific sites using snapshot file
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv -s US-Ha1 US-MMS

    # Create output directory
    mkdir /data/fluxnet
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv -s PE-QFR IT-Niv -o /data/fluxnet

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