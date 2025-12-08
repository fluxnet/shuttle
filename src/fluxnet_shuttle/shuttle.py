"""
Core functionality for FLUXNET Shuttle operations

:module: fluxnet_shuttle.shuttle
:moduleauthor: Gilberto Z. Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2024-10-31

.. currentmodule:: fluxnet_shuttle.shuttle

This module provides the core functionality for FLUXNET Shuttle operations,
including data discovery, download, and source management across multiple
FLUXNET data hubs.

The shuttle module serves as the main interface for interacting with different
FLUXNET data sources through a unified API.


Data Requirements
-----------------

Ideal MVP fields:
    * SITE_ID
    * SITE_NAME
    * TEAM_MEMBER_NAME
    * DATA_HUB
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
    * DATA_HUB
    * PUBLISHER
    * FIRST-YEAR
    * LAST-YEAR
    * DOWNLOAD-LINK

License
-------

For license information, see LICENSE file or headers in fluxnet_shuttle.__init__.py

"""

import csv
import logging
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse

import aiofiles

from fluxnet_shuttle import FLUXNETShuttleError
from fluxnet_shuttle.core.decorators import async_to_sync
from fluxnet_shuttle.core.registry import registry
from fluxnet_shuttle.core.shuttle import FluxnetShuttle

_log = logging.getLogger(__name__)

# FLUXNET filename pattern: <network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.zip
# Capture groups: 1=network_id, 2=site_id, 3=first_year, 4=last_year, 5=version, 6=run
_FLUXNET_ZIP_PATTERN = r"^([A-Z]{2,10})_([A-Z]{2}-[A-Za-z0-9]{3})_FLUXNET_(\d{4})-(\d{4})_(v\d+(?:\.\d+)?)_(r\d+)\.zip$"

# Delimiter for concatenating multiple values in CSV (e.g., team members)
CSV_MULTI_VALUE_DELIMITER = ";"


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


def extract_fluxnet_filename_metadata(filename: str) -> tuple[str, str]:
    """
    Extract both product source network and code version from FLUXNET filename.

    FLUXNET filenames follow the archive format (ZIP):
       <network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.zip

    Args:
        filename: The filename or URL to extract metadata from

    Returns:
        Tuple of (product_source_network, oneflux_code_version). Returns ("", "") if filename is invalid.

    Examples:
        >>> extract_fluxnet_filename_metadata("AMF_US-Ha1_FLUXNET_1991-2020_v1.2_r2.zip")
        ('AMF', 'v1.2')
        >>> extract_fluxnet_filename_metadata("invalid_filename.zip")
        ('', '')
    """
    if not filename:
        return ("", "")

    filename_only = _extract_filename_from_url(filename)

    # ZIP format: <network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.zip
    zip_match = re.match(_FLUXNET_ZIP_PATTERN, filename_only, re.IGNORECASE)
    if zip_match:
        # Extract network_id from group 1 and version from group 5
        product_source_network = zip_match.group(1)
        oneflux_code_version = zip_match.group(5)
        return (product_source_network, oneflux_code_version)

    return ("", "")


def validate_fluxnet_filename_format(filename: str) -> bool:
    """
    Validate that a filename follows the standard FLUXNET filename format.

    Valid format (ZIP archive):
       <network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.zip

    Examples:
    - AMF_US-Ha1_FLUXNET_1991-2020_v1.2_r2.zip
    - ICOSETC_BE-Bra_FLUXNET_2020-2024_v1.4_r1.zip

    Args:
        filename: The filename (or URL containing filename) to validate

    Returns:
        True if the filename matches the expected format, False otherwise

    Examples:
        >>> validate_fluxnet_filename_format("AMF_US-Ha1_FLUXNET_1991-2020_v1.2_r2.zip")
        True
        >>> validate_fluxnet_filename_format("invalid_filename.zip")
        False
        >>> validate_fluxnet_filename_format("AMF_US-Ha1 FLUXNET_1991-2020_v1.2_r2.zip")
        False
    """
    if not filename:
        return False

    filename_only = _extract_filename_from_url(filename)

    # ZIP format: <network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.zip
    return bool(re.match(_FLUXNET_ZIP_PATTERN, filename_only, re.IGNORECASE))


@async_to_sync
async def _download_dataset(
    site_id: str,
    data_hub: str,
    filename: str,
    download_link: str,
    output_dir: str = ".",
    **kwargs: Any,
) -> str:
    """
    Download a FLUXNET dataset for a specific site using plugin's download_stream method.

    This function delegates to the appropriate plugin's download_stream method,
    which handles data hub-specific logic (e.g., AmeriFlux user tracking, ICOS filename validation).
    The shuttle orchestrator receives only the content stream, then handles file I/O using the
    filename from the snapshot metadata.

    :param site_id: Site identifier
    :type site_id: str
    :param data_hub: Data hub name (e.g., "AmeriFlux", "ICOS")
    :type data_hub: str
    :param filename: Filename from snapshot metadata
    :type filename: str
    :param download_link: Ready-to-use URL to download data from
    :type download_link: str
    :param output_dir: Directory to save downloaded files (default: current directory)
    :type output_dir: str
    :param kwargs: Additional keyword arguments. Special handling for:
        - user_info: Dictionary with plugin-specific user tracking info (e.g., {"ameriflux": {...}})
        Other kwargs are passed through to the plugin's download_stream method.
    :return: The filepath where the file was saved
    :rtype: str
    :raises FLUXNETShuttleError: If download fails
    """
    _log.info(f"{data_hub}: downloading site {site_id} data file: {filename}")

    try:
        # Get plugin instance
        plugin_class = registry.get_plugin(data_hub.lower())
        if not plugin_class:
            msg = f"Data hub plugin {data_hub} not found for site {site_id}"
            _log.error(msg)
            raise FLUXNETShuttleError(msg)

        plugin_instance = plugin_class()

        # Add filename to kwargs and pass everything to the plugin
        kwargs["filename"] = filename

        # Use plugin's download_file method to get the content stream
        async with plugin_instance.download_file(site_id=site_id, download_link=download_link, **kwargs) as stream:
            # Join with output directory
            filepath = os.path.join(output_dir, filename)

            # Warn if file already exists and will be overwritten
            if os.path.exists(filepath):
                _log.warning(f"{data_hub}: file already exists and will be overwritten: {filepath}")

            # Write the stream to file
            with open(filepath, "wb") as file:
                async for chunk in stream.iter_chunked(8192):
                    file.write(chunk)

            _log.info(f"{data_hub}: file downloaded successfully to {filepath}")
            return filepath

    except Exception as e:
        msg = f"Failed to download {data_hub} file for site {site_id}: {e}"
        _log.error(msg)
        raise FLUXNETShuttleError(msg) from e


@async_to_sync
async def download(
    site_ids: Optional[List[str]] = None,
    snapshot_file: str = "",
    output_dir: str = ".",
    **kwargs: Any,
) -> List[str]:
    """
    Download FLUXNET data for specified sites using configuration from a snapshot file.

    :param site_ids: List of site IDs to download data for. If None or empty, downloads all sites from snapshot file.
    :type site_ids: Optional[List[str]]
    :param snapshot_file: Path to CSV snapshot file containing site configuration
    :type snapshot_file: str
    :param output_dir: Directory to save downloaded files (default: current directory)
    :type output_dir: str
    :param kwargs: Additional keyword arguments passed to _download_dataset. Special handling for:
        - user_info: Dictionary with plugin-specific user tracking info (e.g., {"ameriflux": {...}})
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
        data_hub = site["data_hub"]
        download_link = site["download_link"]
        filename = site.get("fluxnet_product_name")

        if not filename:
            _log.error(f"No filename found for site {site_id} from data hub {data_hub}. Skipping download.")
            continue

        _log.info(f"Downloading data for site {site_id} from data hub {data_hub}")

        actual_filename = await _download_dataset(
            site_id=site_id,
            data_hub=data_hub,
            filename=filename,
            download_link=download_link,
            output_dir=output_dir,
            **kwargs,
        )
        downloaded_filenames.append(actual_filename)
    _log.info(f"Downloaded data for {len(site_ids)} sites: {site_ids}")
    return downloaded_filenames


@async_to_sync
async def listall(data_hubs: Optional[List[str]] = None, output_dir: str = ".") -> str:
    """
    List all available FLUXNET data from specified data hubs.

    .. versionadded:: 0.1.0
       Initial data discovery functionality for AmeriFlux and ICOS data hubs.
    .. versionchanged:: 0.2.0
       Refactored to use FluxnetShuttle class for unified data retrieval.

    :param data_hubs: List of data hub plugin names to include (e.g., ["ameriflux", "icos"]).
                      If None or empty, all available data hub plugins are included.
    :type data_hubs: Optional[List[str]]
    :param output_dir: Directory to save the snapshot file (default: current directory)
    :type output_dir: str
    :return: CSV filename containing data availability information
    :rtype: str
    """

    # If data_hubs is None or empty list, pass None to FluxnetShuttle to use all available plugins
    if data_hubs is not None and len(data_hubs) == 0:
        data_hubs = None

    _log.debug(f"Data hubs to include: {data_hubs if data_hubs else 'all available'}")
    shuttle = FluxnetShuttle(data_hubs=data_hubs)

    # FLUXNET2015 TODO: add FLUXNET2015 data

    # Combine data from all data hubs
    fields = [
        # Site information fields
        "data_hub",
        "site_id",
        "site_name",
        "location_lat",
        "location_long",
        "igbp",
        "network",
        # Team member fields (concatenated with delimiter)
        "team_member_name",
        "team_member_role",
        "team_member_email",
        # Product data fields
        "first_year",
        "last_year",
        "download_link",
        "fluxnet_product_name",
        "product_citation",
        "product_id",
        "oneflux_code_version",
        "product_source_network",
    ]

    csv_filename = f"fluxnet_shuttle_snapshot_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
    csv_filepath = os.path.join(output_dir, csv_filename)
    counts = await _write_snapshot_file(shuttle, fields, csv_filepath)

    _log.info(f"Wrote FLUXNET dataset snapshot to {csv_filepath}")
    _log.info(f"Data hub counts: {counts}")
    return csv_filepath


@async_to_sync
async def _write_snapshot_file(shuttle: FluxnetShuttle, fields: List[str], csv_filename: str) -> Dict[str, int]:
    """
    Write FLUXNET dataset snapshot to a CSV file.

    Creates a snapshot file containing complete metadata for all available
    FLUXNET datasets from configured data hubs, including site information,
    team members, and product details.

    :param shuttle: FluxnetShuttle instance
    :param fields: List of fields to include in the CSV
    :param csv_filename: Output CSV filename path
    :return: Dictionary with counts of sites per data hub
    """
    counts: Dict[str, int] = {}
    # map expansion for data hub counts
    # Write to CSV file, using asyncio file operations
    async with aiofiles.open(csv_filename, "w") as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=fields)
        await csv_writer.writeheader()
        async for site in shuttle.get_all_sites():
            counts.setdefault(site.site_info.data_hub, 0)
            counts[site.site_info.data_hub] += 1

            # Get site info fields (excluding team members and network which need special handling)
            site_dict = site.site_info.model_dump(exclude={"group_team_member", "network"})

            # Concatenate network values with delimiter
            network_list = site.site_info.network
            site_dict["network"] = CSV_MULTI_VALUE_DELIMITER.join(network_list) if network_list else ""

            # Concatenate team member fields with delimiter
            team_members = site.site_info.group_team_member
            site_dict["team_member_name"] = (
                CSV_MULTI_VALUE_DELIMITER.join([tm.team_member_name for tm in team_members]) if team_members else ""
            )
            site_dict["team_member_role"] = (
                CSV_MULTI_VALUE_DELIMITER.join([tm.team_member_role for tm in team_members]) if team_members else ""
            )
            site_dict["team_member_email"] = (
                CSV_MULTI_VALUE_DELIMITER.join([tm.team_member_email for tm in team_members]) if team_members else ""
            )

            # Add product data fields
            product_dict = site.product_data.model_dump()
            # Convert HttpUrl to string for CSV
            product_dict["download_link"] = str(product_dict["download_link"])
            site_dict.update(product_dict)

            await csv_writer.writerow(site_dict)
    return counts


# main function
if __name__ == "__main__":
    sys.exit("ERROR: cannot run independently")
