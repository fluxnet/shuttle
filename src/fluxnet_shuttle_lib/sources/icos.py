"""
ICOS Data Source Module
=======================

:module: fluxnet_shuttle_lib.sources.icos
:synopsis: ICOS Carbon Portal data source implementation
:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2025-01-25

.. currentmodule:: fluxnet_shuttle_lib.sources.icos

This module provides functionality for accessing and downloading data from the
ICOS (Integrated Carbon Observation System) network through the ICOS Carbon Portal.

ICOS is a European research infrastructure that provides observations of greenhouse
gas concentrations and fluxes. This module interfaces with the ICOS SPARQL endpoint
to discover and download available ecosystem flux datasets.

API Interface
-------------

The module uses the ICOS Carbon Portal SPARQL endpoint:

* Endpoint: ``https://meta.icos-cp.eu/sparql``
* Query Language: SPARQL 1.1
* Response Format: JSON

The queries focus on ecosystem flux measurements and provide site information,
temporal coverage, and download links.


License
-------

For license information, see LICENSE file or headers in fluxnet_shuttle_lib.__init__.py


Version
-------

.. versionadded:: 0.1.0
   Initial ICOS support.

"""

import logging
import sys

import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError

_log = logging.getLogger(__name__)


def download_icos_data(site_id, filename, download_link):
    """
    Download ICOS data for specified site.

    .. versionadded:: 0.1.0
       Initial ICOS data download functionality.

    :param site_id: Site identifier
    :type site_id: str
    :param filename: Local filename to save data
    :type filename: str
    :param download_link: URL to download data from
    :type download_link: str
    :raises FLUXNETShuttleError: If download fails
    """
    _log.info(f"ICOS Carbon Portal: Downloading ICOS site {site_id} data file: {filename}")
    # Parse ID from download link
    download_id = download_link.strip().split("/")[-1].strip()

    # Build new download link with license acceptance
    # download_link = f'https://data.icos-cp.eu/objects/{download_id}'
    download_link = f'https://data.icos-cp.eu/licence_accept?ids=["{download_id}"]'

    # Download the file
    response = requests.get(download_link, stream=True)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        _log.info(f"ICOS Carbon Portal: ICOS file downloaded successfully to {filename}")
    else:
        msg = f"Failed to download ICOS file. Status code: {response.status_code}"
        _log.error(msg)
        raise FLUXNETShuttleError(msg)


# main function
if __name__ == "__main__":
    sys.exit("ERROR: cannot run independently")
