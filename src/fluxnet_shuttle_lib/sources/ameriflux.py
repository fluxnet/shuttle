"""
AmeriFlux Data Source Module
============================

:module: fluxnet_shuttle_lib.sources.ameriflux
:synopsis: AmeriFlux network data source implementation
:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2025-01-25

.. currentmodule:: fluxnet_shuttle_lib.sources.ameriflux

.. autosummary::
   :toctree: generated/

   get_ameriflux_data
   download_ameriflux_data


This module provides functionality for accessing and downloading data from the
AmeriFlux network through the AmeriFlux API.

AmeriFlux is a network of ecosystem-level carbon, water, and energy flux measurements
across the Americas. This module interfaces with the AmeriFlux CDN API to discover
and download available datasets.

API Endpoints
-------------

The module uses the following AmeriFlux API endpoints:

* Site availability: ``/api/v1/site_availability/AmeriFlux/FLUXNET/CCBY4.0``
* Data download: Direct links provided by the API


License
-------

For license information, see LICENSE file or headers in fluxnet_shuttle_lib.__init__.py

Version
-------

.. versionadded:: 0.1.0
   Initial AmeriFlux support.

"""

import logging
import sys

import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError

_log = logging.getLogger(__name__)


def download_ameriflux_data(site_id, filename, download_link):
    """
    Download AmeriFlux data for specified site.

    .. versionadded:: 0.1.0
       Initial AmeriFlux data download functionality.

    :param site_id: Site identifier
    :type site_id: str
    :param filename: Local filename to save data
    :type filename: str
    :param download_link: URL to download data from
    :type download_link: str
    :raises FLUXNETShuttleError: If download fails
    """
    _log.info(f"AmeriFlux Portal: downloading AmeriFlux site {site_id} data file: {filename}")
    response = requests.get(download_link, stream=True)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        _log.info(f"AmeriFlux Portal: AmeriFlux file downloaded successfully to {filename}")
    else:
        msg = f"Failed to download AmeriFlux file. Status code: {response.status_code}"
        _log.error(msg)
        raise FLUXNETShuttleError(msg)


# main function
if __name__ == "__main__":
    sys.exit("ERROR: cannot run independently")
