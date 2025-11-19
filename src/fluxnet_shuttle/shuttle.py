"""
Core functionality for FLUXNET Shuttle operations

:module: fluxnet_shuttle.shuttle
:moduleauthor: Gilberto Z. Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2024-10-31

.. currentmodule:: fluxnet_shuttle.shuttle

This module provides the core functionality for FLUXNET Shuttle operations,
including data discovery, download, and source management across multiple
FLUXNET networks.

The shuttle module serves as the main interface for interacting with different
FLUXNET data sources through a unified API.


Data Requirements
-----------------

Ideal MVP fields:
    * SITE_ID
    * SITE_NAME
    * TEAM_MEMBER_NAME
    * NETWORK
    * PUBLISHER
    * LOCATION_LAT
    * LOCATION_LONG
    * IGBP
    * CLIMATE_KOEPPEN
    * GROUPING (FULLSET, SUBSET)
    * FIRST-YEAR
    * LAST-YEAR
    * DOWNLOAD-LINK

Minimal MVP fields:
    * SITE_ID
    * NETWORK
    * PUBLISHER
    * FIRST-YEAR
    * LAST-YEAR
    * DOWNLOAD-LINK

License
-------

For license information, see LICENSE file or headers in fluxnet_shuttle.__init__.py


Version
-------

.. versionadded:: 0.1.0
   Initial shuttle functionality.

"""

import csv
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse

import aiofiles
import requests

from fluxnet_shuttle import FLUXNETShuttleError
from fluxnet_shuttle.core.decorators import async_to_sync
from fluxnet_shuttle.core.shuttle import FluxnetShuttle

_log = logging.getLogger(__name__)


def _extract_filename_from_url(url: str) -> str:
    """
    Extract a clean filename from a URL by removing query parameters.

    :param url: URL to extract filename from
    :type url: str
    :return: Extracted filename without query parameters
    :rtype: str
    """
    parsed_url = urlparse(url)
    # Get the path component (without query parameters)
    path = parsed_url.path
    # Extract the last part of the path as filename
    filename = unquote(path.split("/")[-1])
    return filename


def _extract_filename_from_headers(headers: Dict[str, str]) -> Optional[str]:
    """
    Extract filename from HTTP Content-Disposition header if present.

    :param headers: HTTP response headers
    :type headers: dict
    :return: Extracted filename or None if not found
    :rtype: Optional[str]
    """
    content_disposition = headers.get("Content-Disposition", "")
    if content_disposition:
        # Parse Content-Disposition header (e.g., 'attachment; filename="file.zip"')
        parts = content_disposition.split(";")
        for part in parts:
            if "filename=" in part:
                filename = part.split("=")[1].strip().strip('"')
                return unquote(filename)
    return None


def _download_dataset(site_id: str, network: str, filename: str, download_link: str, output_dir: str = ".") -> str:
    """
    Download dataset file from any FLUXNET network.

    This is a private generic download function that works for all networks.
    Network plugins are responsible for providing ready-to-use download URLs.

    :param site_id: Site identifier
    :type site_id: str
    :param network: Network name (e.g., "AmeriFlux", "ICOS")
    :type network: str
    :param filename: Local filename to save data (may be overridden by Content-Disposition header)
    :type filename: str
    :param download_link: Ready-to-use URL to download data from
    :type download_link: str
    :param output_dir: Directory to save downloaded files (default: current directory)
    :type output_dir: str
    :return: The actual filename used to save the file
    :rtype: str
    :raises FLUXNETShuttleError: If download fails
    """
    _log.info(f"{network}: downloading site {site_id} data file: {filename}")
    try:
        response = requests.get(download_link, stream=True)
        if response.status_code == 200:
            # Try to get filename from Content-Disposition header
            actual_filename = _extract_filename_from_headers(dict(response.headers))
            if actual_filename:
                _log.debug(f"Using filename from Content-Disposition header: {actual_filename}")
                filename = actual_filename

            # Join with output directory
            filepath = os.path.join(output_dir, filename)

            # Warn if file already exists and will be overwritten
            if os.path.exists(filepath):
                _log.warning(f"{network}: file already exists and will be overwritten: {filepath}")

            with open(filepath, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            _log.info(f"{network}: file downloaded successfully to {filepath}")
            return filepath
        else:
            msg = f"Failed to download {network} file. Status code: {response.status_code}"
            _log.error(msg)
            raise FLUXNETShuttleError(msg)
    except requests.RequestException as e:
        msg = f"Failed to download {network} file: {e}"
        _log.error(msg)
        raise FLUXNETShuttleError(msg)


def download(site_ids: Optional[List[str]] = None, snapshot_file: str = "", output_dir: str = ".") -> List[str]:
    """
    Download FLUXNET data for specified sites using configuration from a snapshot file.

    .. versionadded:: 0.1.0
       Initial download functionality for AmeriFlux and ICOS networks.

    :param site_ids: List of site IDs to download data for. If None or empty, downloads all sites from snapshot file.
    :type site_ids: Optional[List[str]]
    :param snapshot_file: Path to CSV snapshot file containing site configuration
    :type snapshot_file: str
    :param output_dir: Directory to save downloaded files (default: current directory)
    :type output_dir: str
    :return: List of downloaded filenames
    :rtype: list
    :raises FLUXNETShuttleError: If snapshot_file is invalid or sites not found
    """
    if not snapshot_file:
        msg = "No snapshot file provided."
        _log.error(msg)
        raise FLUXNETShuttleError(msg)

    # Load CSV snapshot file
    if not os.path.exists(snapshot_file):
        msg = f"Snapshot file {snapshot_file} does not exist."
        _log.error(msg)
        raise FLUXNETShuttleError(msg)
    with open(snapshot_file, "r") as f:
        run_data: List[Any] = f.readlines()
    run_data = [line.strip().split(",") for line in run_data]
    fields = run_data[0]
    sites = {}
    for line in run_data[1:]:
        site = {}
        for i, field in enumerate(fields):
            site[field] = line[i]
        sites[site["site_id"]] = site
    _log.debug(f"Loaded {len(sites)} sites from snapshot file")

    # If no site IDs specified, download all sites from snapshot
    if not site_ids:
        site_ids = list(sites.keys())
        _log.info(f"No site IDs specified. Will download all {len(site_ids)} sites from snapshot file.")

    _log.info(f"Starting download with {len(site_ids)} site IDs: {site_ids} and snapshot file: {snapshot_file}")

    # Check if site IDs are in the snapshot file
    for site_id in site_ids:
        if site_id not in sites:
            msg = f"Site ID {site_id} not found in snapshot file."
            _log.error(msg)
            raise FLUXNETShuttleError(msg)
    _log.debug("All site IDs found in snapshot file")

    # Download data for each site
    downloaded_filenames = []
    for site_id in site_ids:
        site = sites[site_id]
        network = site["network"]
        download_link = site["download_link"]
        # Extract clean filename from URL (without query parameters)
        filename = _extract_filename_from_url(download_link)
        _log.info(f"Downloading data for site {site_id} from network {network}")

        # _download_dataset may override filename from Content-Disposition header
        actual_filename = _download_dataset(
            site_id=site_id, network=network, filename=filename, download_link=download_link, output_dir=output_dir
        )
        downloaded_filenames.append(actual_filename)
    _log.info(f"Downloaded data for {len(site_ids)} sites: {site_ids}")
    return downloaded_filenames


@async_to_sync
async def listall(*args, **kwargs) -> str:
    """
    List all available FLUXNET data from specified networks.

    .. versionadded:: 0.1.0
       Initial data discovery functionality for AmeriFlux and ICOS networks.
    .. versionchanged:: 0.2.0
       Refactored to use FluxnetShuttle class for unified data retrieval.

    :param ameriflux: Whether to include AmeriFlux data
    :type ameriflux: bool
    :param icos: Whether to include ICOS data
    :type icos: bool
    :param output_dir: Directory to save the snapshot file (default: current directory)
    :type output_dir: str
    :return: CSV filename containing data availability information
    :rtype: str
    """
    _log.debug(f"Starting listall with {args}, {kwargs}")

    # Extract output_dir from kwargs if present
    output_dir = kwargs.pop("output_dir", ".")

    networks = [k for k, v in kwargs.items() if v]
    _log.debug(f"Networks to include: {networks}")
    shuttle = FluxnetShuttle(networks=networks)

    # FLUXNET2015 TODO: add FLUXNET2015 data

    # Combine data from all networks
    fields = [
        "network",
        # "publisher",  # don't have this
        "site_id",
        "first_year",
        "last_year",
        # "version",  # don't have this
        # "filename", # don't have this
        "download_link",
    ]

    csv_filename = f"fluxnet_shuttle_snapshot_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
    csv_filepath = os.path.join(output_dir, csv_filename)
    counts = await _write_data_availability(shuttle, fields, csv_filepath)

    _log.info(f"Wrote data availability to {csv_filepath}")
    _log.info(f"Network counts: {counts}")
    return csv_filepath


@async_to_sync
async def _write_data_availability(shuttle, fields, csv_filename):
    """
    Write data availability information to a CSV file.

    :param shuttle: FluxnetShuttle instance
    :param fields: List of fields to include in the CSV
    :param csv_filename: Output CSV filename
    :return: Dictionary with counts of sites per network
    """
    counts: Dict[str, int] = {}
    # map expansion for network counts
    # Write to CSV file, using asyncio file operations
    async with aiofiles.open(csv_filename, "w") as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fields)
        await csv_writer.writeheader()
        async for site in shuttle.get_all_sites():
            counts.setdefault(site.site_info.network, 0)
            counts[site.site_info.network] += 1
            site_dict = site.site_info.model_dump(include={"network", "site_id"})
            site_dict.update(site.product_data.model_dump(include={"first_year", "last_year", "download_link"}))
            await csv_writer.writerow(site_dict)
    return counts


# main function
if __name__ == "__main__":
    sys.exit("ERROR: cannot run independently")
