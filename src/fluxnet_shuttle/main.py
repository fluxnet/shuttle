"""
Command Line Interface Module
=============================

:module: fluxnet_shuttle.main
:synopsis: Command-line interface for FLUXNET Shuttle Library
:moduleauthor: Generated for fluxnet-shuttle
:platform: Unix, Windows
:created: 2025-10-06

.. currentmodule:: fluxnet_shuttle.main


This module provides the command-line interface for the FLUXNET Shuttle Library,
allowing users to discover and download FLUXNET data from the command line.

The CLI supports operations for listing available datasets, downloading data,
checking supported data sources.

Commands
--------

* ``listall`` - List all available datasets from supported data hubs
* ``download`` - Download datasets from specified data hubs
* ``listdatahubs`` - Display information about supported data hubs
* ``test`` - Test connectivity to data hubs

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
import csv
import logging
import os
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, List, Optional

from . import FLUXNETShuttleError
from .shuttle import download, listall

# Get package version dynamically
try:
    __version__ = version("fluxnet-shuttle")
except PackageNotFoundError:
    __version__ = "unknown"


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


def _validate_output_directory(output_dir: str) -> None:
    """
    Validate output directory exists and is writable.

    :param output_dir: Directory path to validate
    :raises SystemExit: If directory does not exist or is not writable
    """
    log = logging.getLogger(__name__)
    output_path = Path(output_dir)

    if not output_path.exists():
        log.error(f"Output directory does not exist: {output_dir}")
        sys.exit(1)

    if not output_path.is_dir():
        log.error(f"Output path is not a directory: {output_dir}")
        sys.exit(1)

    if not os.access(output_dir, os.W_OK):
        log.error(f"Output directory is not writable: {output_dir}")
        sys.exit(1)


def cmd_listall(args) -> Any:
    """Execute the listall command."""
    log = logging.getLogger(__name__)
    log.debug("Running listall command")

    # Validate output directory
    output_dir = args.output_dir if hasattr(args, "output_dir") and args.output_dir else "."
    _validate_output_directory(output_dir)

    # Default to both AmeriFlux and ICOS
    ameriflux = True
    icos = True

    # Allow selective source querying if implemented in future
    csv_filename = listall(ameriflux=ameriflux, icos=icos, output_dir=output_dir)
    log.info(f"FLUXNET Shuttle snapshot written to {csv_filename}")
    return csv_filename


def cmd_download(args) -> List[str]:
    """Execute the download command."""
    log = logging.getLogger(__name__)

    # snapshot_file is required
    snapshot_file = args.snapshot_file
    if not snapshot_file:
        log.error("Snapshot file is required. Use -f or --snapshot-file to specify the snapshot file.")
        sys.exit(1)
    if not os.path.exists(snapshot_file):
        log.error(f"Snapshot file not found: {snapshot_file}")
        sys.exit(1)

    # Validate output directory
    output_dir = args.output_dir if hasattr(args, "output_dir") and args.output_dir else "."
    _validate_output_directory(output_dir)

    # If sites are provided, use them; otherwise extract all from snapshot file
    if args.sites:
        sites = args.sites
    else:
        # Extract all sites from snapshot file
        sites = []
        try:
            with open(snapshot_file, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if "site_id" in row:
                        sites.append(row["site_id"])
            if not sites:
                log.error(f"No site_id column found in snapshot file: {snapshot_file}")
                sys.exit(1)
        except Exception as e:
            log.error(f"Error reading snapshot file {snapshot_file}: {e}")
            sys.exit(1)

        # Confirmation prompt for downloading all sites (unless --quiet is used)
        quiet = hasattr(args, "quiet") and args.quiet
        if not quiet:
            log.warning(f"No site IDs specified. This will download ALL {len(sites)} sites from the snapshot file.")
            response = input("Proceed with download? [y/n]: ")
            if response.lower() not in ["y", "yes"]:
                log.info("Download cancelled by user.")
                sys.exit(0)

    log.debug(f"Running download command with {len(sites)} site IDs: {sites} and snapshot file: {snapshot_file}")

    # Build kwargs from CLI arguments for plugins
    # Plugins will extract what they need from these kwargs
    plugin_kwargs = {}
    if hasattr(args, "user_id") and args.user_id:
        plugin_kwargs["user_id"] = args.user_id
    if hasattr(args, "user_email") and args.user_email:
        plugin_kwargs["user_email"] = args.user_email
    if hasattr(args, "intended_use") and args.intended_use:
        plugin_kwargs["intended_use"] = args.intended_use
    if hasattr(args, "description") and args.description:
        plugin_kwargs["description"] = args.description

    downloaded_files: List[str] = download(
        site_ids=sites,
        snapshot_file=snapshot_file,
        output_dir=output_dir,
        **plugin_kwargs,
    )
    log.info(f"Downloaded {len(downloaded_files)} files")
    return downloaded_files


def cmd_listdatahubs(args: Any) -> None:
    """Execute the listdatahubs command - show available data hub plugins."""
    log = logging.getLogger(__name__)
    from .core.registry import registry

    log.info("Available FLUXNET data hub plugins:")

    plugin_names = registry.list_plugins()
    if not plugin_names:
        log.warning("No data hub plugins found")
        return

    for plugin_name in sorted(plugin_names):
        plugin_class = registry.get_plugin(plugin_name)
        # Instantiate to get display_name
        instance = plugin_class()
        log.info(f"  - {instance.display_name} ({plugin_name})")


def main() -> None:
    """Main CLI entry point."""
    BEGIN_TS = datetime.now()

    # Main parser
    parser = argparse.ArgumentParser(
        prog="fluxnet-shuttle",
        description="FLUXNET Shuttle - Download and manage FLUXNET data from multiple data hubs",
        epilog="For more information, visit: https://github.com/AMF-FLX/fluxnet-shuttle-lib",
    )

    # Global arguments
    parser.add_argument(
        "-l",
        "--logfile",
        help=f"Logging file path (default: {DEFAULT_LOGGING_FILENAME})",
        type=str,
        dest="logfile",
        default=DEFAULT_LOGGING_FILENAME,
    )

    parser.add_argument("-v", "--verbose", help="Enable verbose logging", action="store_true", dest="verbose")

    parser.add_argument("--no-logfile", help="Disable logging to file", action="store_true", dest="no_logfile")

    parser.add_argument("--version", action="version", version=f"fluxnet-shuttle {__version__}")

    # Subparsers for commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)

    # listall command
    parser_listall = subparsers.add_parser(
        "listall",
        help="List all available FLUXNET datasets",
        description="Fetch and save a snapshot of all available FLUXNET datasets from configured data hubs",
    )
    parser_listall.add_argument(
        "-o",
        "--output-dir",
        help="Directory to save the snapshot file (default: current directory)",
        type=str,
        dest="output_dir",
        default=".",
    )

    # download command
    parser_download = subparsers.add_parser(
        "download",
        help="Download FLUXNET datasets",
        description="Download FLUXNET data files for specified sites using a snapshot file",
    )
    parser_download.add_argument(
        "-f",
        "--snapshot-file",
        help="Path to snapshot CSV file (from listall command)",
        type=str,
        dest="snapshot_file",
        required=True,
    )
    parser_download.add_argument(
        "-s",
        "--sites",
        help="Site IDs to download (space-separated). If not provided, downloads ALL sites from snapshot file",
        type=str,
        dest="sites",
        nargs="+",
        default=None,
    )
    parser_download.add_argument(
        "-o",
        "--output-dir",
        help="Directory to save downloaded files (default: current directory)",
        type=str,
        dest="output_dir",
        default=".",
    )
    parser_download.add_argument(
        "--quiet",
        "-q",
        help="Skip confirmation prompt when downloading all sites",
        action="store_true",
        dest="quiet",
    )
    parser_download.add_argument(
        "--user-id",
        help="User ID for data hub tracking (required by some data hubs like AmeriFlux)",
        type=str,
        dest="user_id",
        default=None,
    )
    parser_download.add_argument(
        "--user-email",
        help="User email for data hub tracking (required by some data hubs like AmeriFlux)",
        type=str,
        dest="user_email",
        default=None,
    )
    parser_download.add_argument(
        "--intended-use",
        help="Intended use for data hub tracking (e.g., synthesis, model, remote_sensing)",
        type=str,
        dest="intended_use",
        default=None,
    )
    parser_download.add_argument(
        "--description",
        help="Brief description of intended use (optional, used by some data hubs)",
        type=str,
        dest="description",
        default="",
    )

    # listdatahubs command
    subparsers.add_parser(
        "listdatahubs",
        help="List available data hub plugins",
        description="Display information about available FLUXNET data hub plugins",
    )

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
        elif args.command == "listdatahubs":
            cmd_listdatahubs(args)
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
