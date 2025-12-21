"""
Command Line Interface Module
=============================

:module: fluxnet_shuttle.main
:synopsis: Command-line interface for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-06
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle.main


This module provides the command-line interface for the FLUXNET Shuttle Library,
allowing users to discover and download FLUXNET data from the command line.

The CLI supports operations for listing available datasets, downloading data,
checking supported data hubs.

Commands
--------

* ``listall`` - List all available data from supported data hubs
* ``download`` - Download data from specified data hubs
* ``listdatahubs`` - Display information about supported data hubs

Examples
--------

List all available datasets::

    fluxnet-shuttle listall

Download data::

    fluxnet-shuttle download --snapshot-file fluxnet_shuttle_snapshot_20251201T100100.csv --output-dir ./data

"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def cmd_listall(args: argparse.Namespace) -> Any:
    """Execute the listall command."""
    log = logging.getLogger(__name__)
    log.debug("Running listall command")

    # Validate output directory
    output_dir = args.output_dir if hasattr(args, "output_dir") and args.output_dir else "."
    _validate_output_directory(output_dir)

    # Get data hubs from args if provided, otherwise use None to include all available
    data_hubs = args.data_hubs if hasattr(args, "data_hubs") and args.data_hubs else None

    csv_filename = listall(data_hubs=data_hubs, output_dir=output_dir)
    log.info(f"FLUXNET Shuttle snapshot written to {csv_filename}")
    return csv_filename


def _prompt_user_info(quiet: bool) -> Dict[str, Any]:
    """
    Prompt user for AmeriFlux tracking information.

    :param quiet: If True, skip prompts and return empty user_info
    :return: Dictionary with user_info for AmeriFlux plugin (only populated fields)
    """
    log = logging.getLogger(__name__)

    # Start with empty user_info - only populate fields if user provides them
    user_info: Dict[str, Any] = {"ameriflux": {}}

    if quiet:
        log.info("Quiet mode enabled - skipping user info prompts")
        return user_info

    # Show introductory message
    print(
        "\n"
        "Please enter information about yourself and your plans to use the FLUXNET data at the following prompts.\n"
        "Providing the information is optional and collected by AmeriFlux to contact you with data updates.\n"
        "It also is provided to the AmeriFlux site teams to help them maintain research activities.\n"
        "Press Enter without typing anything to skip each prompt.\n"
    )

    # Prompt for user name
    user_name = input("Enter name: ").strip()
    if user_name:
        user_info["ameriflux"]["user_name"] = user_name

    # Prompt for user email
    user_email = input("Enter email: ").strip()
    if user_email:
        user_info["ameriflux"]["user_email"] = user_email

    # Prompt for intended use
    print(
        "\n"
        "Intended use:\n"
        "  1. Synthesis\n"
        "  2. Model\n"
        "  3. Remote sensing\n"
        "  4. Other research\n"
        "  5. Education\n"
        "  6. Other\n"
    )
    intended_use_input = input("Enter intended use (1-6): ").strip()
    if intended_use_input:
        try:
            intended_use = int(intended_use_input)
            if intended_use in range(1, 7):
                user_info["ameriflux"]["intended_use"] = intended_use
            else:
                log.warning("Invalid intended use value. Skipping this field.")
        except ValueError:
            log.warning("Invalid intended use value. Skipping this field.")

    # Prompt for description
    description = input("Enter additional information about intended use: ").strip()
    if description:
        user_info["ameriflux"]["description"] = description

    print()  # Add blank line after prompts
    return user_info


def cmd_download(args: argparse.Namespace) -> List[str]:
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

    # Check quiet flag
    quiet = hasattr(args, "quiet") and args.quiet

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
        if not quiet:
            log.warning(f"No site IDs specified. This will download ALL {len(sites)} sites from the snapshot file.")
            response = input("Proceed with download? [y/n]: ")
            if response.lower() not in ["y", "yes"]:
                log.info("Download cancelled by user.")
                sys.exit(0)

    log.debug(f"Running download command with {len(sites)} site IDs: {sites} and snapshot file: {snapshot_file}")

    # Prompt for user info (respects --quiet flag)
    user_info = _prompt_user_info(quiet)

    downloaded_files: List[str] = download(
        site_ids=sites,
        snapshot_file=snapshot_file,
        output_dir=output_dir,
        user_info=user_info,
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
        epilog="For more information, visit: https://github.com/fluxnet/shuttle",
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
        help="List all available FLUXNET data",
        description="Fetch and save a snapshot of all available FLUXNET data products from configured data hubs",
    )
    parser_listall.add_argument(
        "data_hubs",
        help=(
            "Data hub plugin names to include (space-separated, e.g., ameriflux icos). "
            "If not provided, all available data hubs are included."
        ),
        type=str,
        nargs="*",
        default=None,
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
        help="Download FLUXNET data",
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
        help="Skip confirmation prompt when downloading all sites and user info prompts",
        action="store_true",
        dest="quiet",
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
