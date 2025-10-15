"""
Core functionality for FLUXNET Shuttle operations

:module: fluxnet_shuttle_lib.shuttle
:moduleauthor: Gilberto Z. Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2024-10-31

.. currentmodule:: fluxnet_shuttle_lib.shuttle

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

For license information, see LICENSE file or headers in fluxnet_shuttle_lib.__init__.py


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
from typing import Any, Dict, List

import aiofiles

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.core.decorators import async_to_sync
from fluxnet_shuttle_lib.core.shuttle import FluxnetShuttle
from fluxnet_shuttle_lib.sources.ameriflux import download_ameriflux_data
from fluxnet_shuttle_lib.sources.icos import download_icos_data

_log = logging.getLogger(__name__)


def download(site_ids: List[str], runfile: str) -> List[str]:
    """
    Download FLUXNET data for specified sites using configuration from a run file.

    .. versionadded:: 0.1.0
       Initial download functionality for AmeriFlux and ICOS networks.

    :param site_ids: List of site IDs to download data for
    :type site_ids: list
    :param runfile: Path to CSV file containing site configuration
    :type runfile: str
    :return: List of downloaded filenames
    :rtype: list
    :raises FLUXNETShuttleError: If site_ids or runfile are invalid
    """
    if not site_ids:
        msg = "No site IDs provided for download."
        _log.error(msg)
        raise FLUXNETShuttleError(msg)
    if not runfile:
        msg = "No run file provided."
        _log.error(msg)
        raise FLUXNETShuttleError(msg)

    _log.info(f"Starting download with site IDs: {site_ids} and run file: {runfile}")

    # Load CSV run file
    if not os.path.exists(runfile):
        msg = f"Run file {runfile} does not exist."
        _log.error(msg)
        raise FLUXNETShuttleError(msg)
    with open(runfile, "r") as f:
        run_data: List[Any] = f.readlines()
    run_data = [line.strip().split(",") for line in run_data]
    fields = run_data[0]
    sites = {}
    for line in run_data[1:]:
        site = {}
        for i, field in enumerate(fields):
            site[field] = line[i]
        sites[site["site_id"]] = site
    _log.debug(f"Loaded {len(sites)} sites from run file")

    # Check if site IDs are in the run file
    for site_id in site_ids:
        if site_id not in sites:
            msg = f"Site ID {site_id} not found in run file."
            _log.error(msg)
            raise FLUXNETShuttleError(msg)
    _log.debug("All site IDs found in run file")

    # Download data for each site
    downloaded_filenames = []
    for site_id in site_ids:
        site = sites[site_id]
        network = site["network"]
        download_link = site["download_link"]
        filename = download_link.split("/")[-1]
        _log.info(f"Downloading data for site {site_id} from network {network}")

        if network == "AmeriFlux":
            download_ameriflux_data(site_id=site_id, filename=filename, download_link=download_link)
        elif network == "ICOS":
            download_icos_data(site_id=site_id, filename=filename, download_link=download_link)
        else:
            msg = f"Network {network} not supported for download."
            _log.error(msg)
            raise FLUXNETShuttleError(msg)
        downloaded_filenames.append(filename)
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
    :return: CSV filename containing data availability information
    :rtype: str
    """
    _log.debug(f"Starting listall with {args}, {kwargs}")
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

    csv_filename = f"data_availability_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
    counts = await _write_data_availability(shuttle, fields, csv_filename)

    _log.info(f"Wrote data availability to {csv_filename}")
    _log.info(f"Network counts: {counts}")
    return csv_filename


def test(*args, **kwargs) -> bool:
    """
    Test connectivity to all configured FLUXNET networks.

    .. versionadded:: 0.1.0
       Initial connectivity test functionality for AmeriFlux and ICOS networks.
    .. versionchanged:: 0.2.0
       Refactored to be a stub until plugins implement connectivity tests.

    :return: True if connectivity test passed, False otherwise
    :rtype: bool

    :raises NotImplementedError: If connectivity test is not implemented in plugins
    """
    _log.debug("Starting connectivity test")

    # Stub this method until a helper method is implemented in each plugin
    _log.info("Testing connectivity for networks is not fully implemented yet.")
    raise NotImplementedError("Connectivity test not implemented in plugins yet.")


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
