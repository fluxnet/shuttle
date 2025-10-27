Command Line Interface
======================

The FLUXNET Shuttle Library provides a command-line interface (CLI) tool called ``fluxnet-shuttle`` for common data discovery and download operations.

Installation
------------

After installing the library, the CLI tool is automatically available:

.. code-block:: bash

    pip install fluxnet-shuttle
    fluxnet-shuttle --help

Commands
--------

listall
~~~~~~~

List all available FLUXNET datasets from AmeriFlux and ICOS networks.

.. code-block:: bash

    fluxnet-shuttle listall [OPTIONS]

**Options:**

- ``--output, -o TEXT``: Output CSV filename (default: timestamped)
- ``--verbose, -v``: Enable verbose logging
- ``--no-logfile``: Disable logging to file
- ``--logfile TEXT``: Custom log file path

**Examples:**

.. code-block:: bash

    # List all sites and save to timestamped CSV
    fluxnet-shuttle listall --verbose

    # Save to specific filename
    fluxnet-shuttle listall --output my_sites.csv

download
~~~~~~~~

Download FLUXNET data for specified sites using a run file with site configurations.

.. code-block:: bash

    fluxnet-shuttle download [OPTIONS]

**Options:**

- ``--sites, -s TEXT``: Space-separated list of site IDs
- ``--runfile, -r TEXT``: Path to CSV run file (from listall command)
- ``--verbose, -v``: Enable verbose logging
- ``--no-logfile``: Disable logging to file
- ``--logfile TEXT``: Custom log file path

**Examples:**

.. code-block:: bash

    # Download specific sites using runfile
    fluxnet-shuttle download -s US-Ha1 US-MMS -r sites.csv

    # Download with verbose output
    fluxnet-shuttle download -s US-ARc IT-Niv -r sites.csv --verbose

sources
~~~~~~~

List available data sources and their information.

.. code-block:: bash

    fluxnet-shuttle sources [OPTIONS]

test
~~~~

Run connectivity tests to verify API access to AmeriFlux and ICOS networks.

.. code-block:: bash

    fluxnet-shuttle test [OPTIONS]

search
~~~~~~

Search functionality (placeholder for future implementation).

.. code-block:: bash

    fluxnet-shuttle search [OPTIONS]

Global Options
--------------

All commands support these global options:

- ``--help, -h``: Show help message and exit
- ``--version``: Show version and exit
- ``--verbose, -v``: Enable verbose logging
- ``--logfile TEXT``: Specify log file path
- ``--no-logfile``: Disable file logging

Examples Workflow
-----------------

Complete workflow from discovery to download:

.. code-block:: bash

    # Step 1: Discover available data
    fluxnet-shuttle listall --verbose

    # Step 2: Review the CSV file (external step)
    # Open sites.csv and identify sites of interest

    # Step 3: Download specific sites
    fluxnet-shuttle download \
      -r data_availability_YYYYMMDDTHHMMSS.csv  \
      -s US-ARc IT-Niv DE-Tha \
      --verbose

    # Step 4: Test connectivity if needed
    fluxnet-shuttle test --verbose

Error Handling
--------------

The CLI provides clear error messages for common issues:

- Missing required arguments
- Invalid site IDs
- Network connectivity problems
- File not found errors
- Invalid CSV format

All errors are logged and return appropriate exit codes for use in scripts.