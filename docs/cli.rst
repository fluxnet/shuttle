Command Line Interface
======================

The FLUXNET Shuttle Library provides a command-line interface (CLI) tool called ``fluxnet-shuttle`` for common data discovery and download operations.

Installation
------------

After installing the library, the CLI tool is automatically available:

.. code-block:: bash

    pip install git+https://github.com/fluxnet/shuttle.git
    fluxnet-shuttle --help

Commands
--------

listall
~~~~~~~

List all available FLUXNET data from all data hubs. Creates a snapshot CSV file containing available site metadata and download links. The timestamp of the request is included in the filename.

.. code-block:: bash

    fluxnet-shuttle listall [OPTIONS]

**Options:**

- ``--output-dir, -o PATH``: Directory to save the snapshot file (default: current directory). **Note:** Directory must already exist.

**Output:**

Creates a file named ``fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv`` containing:

FLUXNET Data Information
- Product name (archive filename)
- First year of data
- Last year of data
- Download link
- ONEFLUX processing code version
- Data Citation
- Data product ID (e.g., hashtag, PID, or DOI)
- Product source network

Site General Information
- Site ID
- Site name
- Geocoordinates: Latitude and longitude
- International Geosphere-Biosphere Programme (IGBP) category
- Network affiliations
- Team Member and contact info

**Examples:**

.. code-block:: bash

    # List all sites and save snapshot file to current directory
    fluxnet-shuttle listall

    # Save snapshot file to specific directory (directory must exist)
    fluxnet-shuttle listall -o /path/to/output

    # With verbose logging
    fluxnet-shuttle --verbose listall

download
~~~~~~~~

Download FLUXNET data products (zip files) for specified sites using a snapshot file generated with the listall command.

.. code-block:: bash

    fluxnet-shuttle download -f SNAPSHOT_FILE [OPTIONS]

**Options:**

- ``--snapshot-file, -f PATH``: Path to snapshot CSV file (required)
- ``--sites, -s SITE_ID [SITE_ID ...]``: Space-separated list of site IDs to download (optional - downloads ALL if not specified)
- ``--output-dir, -o PATH``: Directory to save downloaded files (default: current directory). **Note:** Directory must already exist.
- ``--quiet, -q``: Skip prompts to enter optional user information and confirmation prompt when downloading all sites from a snapshot file.

**Behavior:**

- If ``--sites`` is specified: Downloads only those sites
- If ``--sites`` is not specified: Prompts for confirmation before downloading ALL sites from the snapshot file
- Use ``--quiet`` to skip the confirmation prompt (useful for automation)
- **File Overwriting:** If a file already exists at the download location, a warning will be logged and the file will be overwritten
- **Output Directory Validation:** The output directory must exist before running the command

**Examples:**

.. code-block:: bash

    # Download specific sites
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_20251114T113216.csv -s KE-Kpt KR-TwB AU-Lox

    # Download to specific directory (directory must exist)
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_20251114T113216.csv -s NZ-ADd IT-Niv -o /data/fluxnet

    # Download ALL sites (with confirmation prompt)
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_20251114T113216.csv

    # Download ALL sites without interactive prompts (automation)
    fluxnet-shuttle download -f fluxnet_shuttle_snapshot_20251114T113216.csv --quiet

listdatahubs
~~~~~~~~~~~~

List available FLUXNET data hub plugins dynamically registered in the system.

.. code-block:: bash

    fluxnet-shuttle listdatahubs

**Output:**

Displays all registered data hub plugins with their display names and identifiers.

**Example:**

.. code-block:: bash

    fluxnet-shuttle listdatahubs
    # Output:
    #   Available FLUXNET data hub plugins:
    #       - AmeriFlux (ameriflux)
    #       - ICOS (icos)
    #       - TERN (tern)

Global Options
--------------

All commands support these global options:

- ``--help, -h``: Show help message and exit
- ``--version``: Show version and exit
- ``--verbose, -v``: Enable verbose logging
- ``--logfile, -l TEXT``: Specify log file path
- ``--no-logfile``: Disable file logging

Examples Workflow
-----------------

Complete workflow from discovery to download:

.. code-block:: bash

    # Step 1: Check available data hub plugins
    fluxnet-shuttle listdatahubs

    # Step 2: Create directories for snapshots and downloads
    mkdir /data/snapshots /data/fluxnet

    # Step 3: Create snapshot file to discover available data
    fluxnet-shuttle --verbose listall -o /data/snapshots

    # Step 4: Review the snapshot file (external step)
    # Open fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv and identify sites of interest

    # Step 5: Download specific sites
    fluxnet-shuttle --verbose download \
      -f /data/snapshots/fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv \
      -s NZ-ADd IT-Niv DE-Tha \
      -o /data/fluxnet

    # Alternative: Download all sites (with confirmation)
    fluxnet-shuttle download \
      -f /data/snapshots/fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv \
      -o /data/fluxnet

    # Alternative: Download all sites without confirmation (automation)
    fluxnet-shuttle download \
      -f /data/snapshots/fluxnet_shuttle_snapshot_YYYYMMDDTHHMMSS.csv \
      -o /data/fluxnet \
      --quiet

Error Handling
--------------

The CLI provides error messages for common issues:

- Missing required arguments
- Invalid site IDs
- Connectivity problems
- File not found errors
- Invalid CSV format
- Output directory does not exist
- Output directory is not writable
- Output path is a file instead of a directory

All errors are logged and return appropriate exit codes for use in scripts.

Important Notes
---------------

**Output Directory Requirements:**

- The output directory specified with ``-o`` or ``--output-dir`` must already exist
- The CLI will **not** automatically create directories
- Users must create directories before running commands
- If the output directory does not exist, the command will fail with an error message

**File Overwriting:**

- When downloading files, if a file with the same name already exists, it will be overwritten
- A warning message will be logged when overwriting occurs
- No confirmation is required for individual file overwrites
- To avoid losing data, ensure you use unique output directories or backup existing files

**User Information**
- Providing user information in the download function is optional.
- Use of the user information will follow the data hub policies.
- If entered, user information may be provided to the flux team PI.
- It may also be used to send information about data updates / corrections.