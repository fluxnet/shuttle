"""
Core Shuttle Functionality
===========================

:module: fluxnet_shuttle_lib.shuttle
:synopsis: Core functionality for FLUXNET Shuttle operations
:moduleauthor: Gilberto Z. Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2024-10-31

.. currentmodule:: fluxnet_shuttle_lib.shuttle

.. autosummary::
   :toctree: generated/

   listall
   download


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

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

from fluxnet_shuttle_lib import FLUXNETShuttleError
from fluxnet_shuttle_lib.sources.ameriflux import download_ameriflux_data, get_ameriflux_data
from fluxnet_shuttle_lib.sources.icos import download_icos_data, get_icos_data

_log = logging.getLogger(__name__)

AMERIFLUX_OUTPUT_FILENAME = "data_availability_ameriflux.json"
ICOS_OUTPUT_FILENAME = "data_availability_icos.json"


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
        filename = site["filename"]
        download_link = site["download_link"]

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


def listall(ameriflux=True, icos=True):
    """
    List all available FLUXNET data from specified networks.

    .. versionadded:: 0.1.0
       Initial data discovery functionality for AmeriFlux and ICOS networks.

    :param ameriflux: Whether to include AmeriFlux data
    :type ameriflux: bool
    :param icos: Whether to include ICOS data
    :type icos: bool
    :return: CSV filename containing data availability information
    :rtype: str
    """
    _log.debug(f"Starting listall with ameriflux={ameriflux}, icos={icos}")
    ameriflux_data, icos_data = {}, {}

    # AmeriFlux
    if ameriflux:
        ameriflux_data: Any = get_ameriflux_data(ameriflux_output_filename=AMERIFLUX_OUTPUT_FILENAME)

    # ICOS
    if icos:
        icos_data: Any = get_icos_data(icos_output_filename=ICOS_OUTPUT_FILENAME)

    # FLUXNET2015 TODO: add FLUXNET2015 data

    # Combine data from all networks
    sites: Dict[str, Any] = {}
    fields = [
        "network",
        "publisher",
        "site_id",
        "first_year",
        "last_year",
        "version",
        "filename",
        "download_link",
    ]
    sites_csv = [
        fields,
    ]
    if ameriflux:
        sites.update(ameriflux_data)
        for _, site in ameriflux_data.items():
            sites_csv.append([site.get(field, "") for field in fields])
    if icos:
        sites.update(icos_data)
        for _, site in icos_data.items():
            sites_csv.append([site.get(field, "") for field in fields])

    # Write to CSV file
    csv_filename = f"data_availability_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
    with open(csv_filename, "w") as csv_file:
        for row in sites_csv:
            csv_file.write(",".join(map(str, row)) + "\n")
    _log.info(f"Wrote data availability to {csv_filename}")
    _log.debug(
        f"Finished listall with {len(ameriflux_data)} AmeriFlux sites, "
        f"{len(icos_data)} ICOS sites, total {len(sites)} sites"
    )
    return csv_filename


# main function
if __name__ == "__main__":
    sys.exit("ERROR: cannot run independently")
