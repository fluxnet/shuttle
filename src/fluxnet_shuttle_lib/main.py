"""
Command Line Interface Module
=============================

:module: fluxnet_shuttle_lib.main
:synopsis: Command-line interface for FLUXNET Shuttle Library
:moduleauthor: Generated for fluxnet-shuttle-lib
:platform: Unix, Windows
:created: 2025-10-06

.. currentmodule:: fluxnet_shuttle_lib.main

.. autosummary::
   :toctree: generated/

   setup_logging
   main
   cmd_listall
   cmd_download
   cmd_sources
   cmd_search
   cmd_test


This module provides the command-line interface for the FLUXNET Shuttle Library,
allowing users to discover and download FLUXNET data from the command line.

The CLI supports operations for listing available datasets, downloading data,
checking supported data sources, and testing network connectivity.

Commands
--------

* ``listall`` - List all available datasets from supported networks
* ``download`` - Download datasets from specified networks
* ``sources`` - Display information about supported data sources
* ``test`` - Test connectivity to data sources

Examples
--------

List all available datasets::

    fluxnet-shuttle listall

Download data from AmeriFlux::

    fluxnet-shuttle download --source ameriflux --output ./data

Version
-------

.. versionadded:: 0.1.0
   Initial CLI implementation.

"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, List, Optional

from . import FLUXNETShuttleError
from .shuttle import download, listall


# Setup logging
def setup_logging(
    level: int = logging.INFO, filename: Optional[str] = None, std: bool = True, std_level: int = logging.INFO
) -> None:
    """
    Setup logging configuration.

    :param level: Logging level for file output
    :param filename: Log file path, if None only stdout is used
    :param std: Whether to log to stdout
    :param std_level: Logging level for stdout
    """
    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    logger.handlers.clear()

    # Add file handler if filename provided
    if filename:
        file_handler = logging.FileHandler(filename)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Add stdout handler if requested
    if std:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(std_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)


DEFAULT_LOGGING_FILENAME = "fluxnet-shuttle-run.log"
COMMAND_LIST = [
    "listall",
    "download",
    "sources",
    "search",
    "test",
]


def cmd_listall(args) -> Any:
    """Execute the listall command."""
    log = logging.getLogger(__name__)
    log.debug("Running listall command")

    # Default to both AmeriFlux and ICOS
    ameriflux = True
    icos = True

    # Allow selective source querying if implemented in future
    csv_filename = listall(ameriflux=ameriflux, icos=icos)
    log.info(f"Data availability saved to: {csv_filename}")
    return csv_filename


def cmd_download(args) -> List[str]:
    """Execute the download command."""
    log = logging.getLogger(__name__)

    # Either sites or runfile must be provided, but not necessarily both
    if not args.sites and not args.runfile:
        log.error(
            "No site IDs provided for download. Use -s or --sites to specify site IDs, " "or -r to specify a run file."
        )
        sys.exit(1)

    # If runfile is provided, use it; if sites are provided, create a temporary runfile
    if args.runfile:
        if not os.path.exists(args.runfile):
            log.error(f"Run file not found: {args.runfile}")
            sys.exit(1)

        # If sites are also provided, use them; otherwise extract from runfile
        if args.sites:
            # Handle space-separated sites in a single argument
            if len(args.sites) == 1 and " " in args.sites[0]:
                sites = args.sites[0].split(" ")
            else:
                sites = args.sites
        else:
            # Extract sites from runfile
            import csv

            sites = []
            try:
                with open(args.runfile, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if "site_id" in row:
                            sites.append(row["site_id"])
                        elif "SITE_ID" in row:
                            sites.append(row["SITE_ID"])
                if not sites:
                    log.error(f"No site_id column found in runfile: {args.runfile}")
                    sys.exit(1)
            except Exception as e:
                log.error(f"Error reading runfile {args.runfile}: {e}")
                sys.exit(1)

        runfile_path = args.runfile
    else:
        # Sites provided but no runfile - this is an error based on the download function signature
        log.error("No run file provided. Use -r or --runfile to specify the run file.")
        sys.exit(1)

    log.debug(f"Running download command with site IDs: {sites} and run file: {runfile_path}")

    downloaded_files = download(site_ids=sites, runfile=runfile_path)
    log.info(f"Downloaded {len(downloaded_files)} files")
    return downloaded_files


def cmd_sources(args: Any) -> None:
    """Execute the sources command (placeholder)."""
    log = logging.getLogger(__name__)
    log.info("Available data sources:")
    log.info("  - AmeriFlux: https://ameriflux.lbl.gov")
    log.info("  - ICOS: https://www.icos-cp.eu")
    log.info("  - FLUXNET2015: (not yet implemented)")


def cmd_search(args: Any) -> None:
    """Execute the search command (placeholder)."""
    log = logging.getLogger(__name__)
    log.info("Search functionality not yet implemented.")
    log.info("Use 'listall' command to see all available sites.")


def cmd_test(args: Any) -> None:
    """Execute the test command (placeholder)."""
    log = logging.getLogger(__name__)
    log.info("Running basic connectivity tests...")

    # Test AmeriFlux and ICOS connectivity by running a minimal listall
    try:
        from .shuttle import listall

        result = listall(ameriflux=True, icos=True)
        if result:
            log.info("✓ Connectivity test passed - able to fetch site listings")
        else:
            log.warning("! Connectivity test completed but no sites returned")
    except Exception as e:
        log.error(f"✗ Connectivity test failed: {e}")


def main() -> None:
    """Main CLI entry point."""
    BEGIN_TS = datetime.now()

    # CLI argument parser
    parser = argparse.ArgumentParser(
        description="FLUXNET Shuttle Library - Download and manage FLUXNET data",
        epilog="For more information, visit: https://github.com/AMF-FLX/fluxnet-shuttle-lib",
    )

    parser.add_argument(
        "command", metavar="COMMAND", help="FLUXNET Shuttle command to run", type=str, choices=COMMAND_LIST
    )

    parser.add_argument(
        "-l",
        "--logfile",
        help=f"Logging file path (default: {DEFAULT_LOGGING_FILENAME})",
        type=str,
        dest="logfile",
        default=DEFAULT_LOGGING_FILENAME,
    )

    parser.add_argument(
        "-r",
        "--runfile",
        help="FLUXNET Shuttle run results file path (CSV from listall command)",
        type=str,
        dest="runfile",
        default="",
    )

    parser.add_argument(
        "-s",
        "--sites",
        help="Site IDs for sites to be downloaded (space-separated)",
        type=str,
        dest="sites",
        nargs="+",
        default=[],
    )

    parser.add_argument("-v", "--verbose", help="Enable verbose logging", action="store_true", dest="verbose")

    parser.add_argument("--no-logfile", help="Disable logging to file", action="store_true", dest="no_logfile")

    parser.add_argument("--version", help="Show version and exit", action="version", version="fluxnet-shuttle-lib")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_filename = None if args.no_logfile else args.logfile

    setup_logging(level=log_level, filename=log_filename, std=True, std_level=log_level)

    log = logging.getLogger(__name__)
    log.debug(f"FLUXNET SHUTTLE RUN started at {BEGIN_TS}")

    try:
        # Route to appropriate command handler
        if args.command == "listall":
            cmd_listall(args)
        elif args.command == "download":
            cmd_download(args)
        elif args.command == "sources":
            cmd_sources(args)
        elif args.command == "search":
            cmd_search(args)
        elif args.command == "test":
            cmd_test(args)
        else:
            log.error(f"Unknown command: {args.command}")
            sys.exit(1)

    except FLUXNETShuttleError as e:
        log.error(f"FLUXNET Shuttle error: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        sys.exit(1)

    END_TS = datetime.now()
    log.debug(f"FLUXNET SHUTTLE RUN finished at {END_TS}, total duration: {END_TS - BEGIN_TS}")


if __name__ == "__main__":
    main()
