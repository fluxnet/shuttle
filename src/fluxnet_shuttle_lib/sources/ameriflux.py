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

import json
import logging
import sys

import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError

_log = logging.getLogger(__name__)

# AMERIFLUX_BASE_URL = 'https://amfcdn.lbl.gov/api/v1/site_availability/...'
# AMERIFLUX_BASE_URL = 'https://amfcdn.lbl.gov/api/v1/data_availability/...'
# AMERIFLUX_BASE_URL = 'https://amfcdn.lbl.gov/api/v1'

AMERIFLUX_BASE_URL = "https://amfcdn.lbl.gov/"
AMERIFLUX_BASE_PATH = "api/v1/"
AMERIFLUX_AVAILABILITY_PATH = "site_availability/AmeriFlux/FLUXNET/CCBY4.0"
AMERIFLUX_DOWNLOAD_PATH = "data_download"
AMERIFLUX_HEADERS = {"accept": "application/json", "content-type": "application/json"}


def get_ameriflux_fluxnet_sites(
    api_url,
    url_get_query_endpoint=AMERIFLUX_AVAILABILITY_PATH,
    headers=AMERIFLUX_HEADERS,
):
    """
    Get list of site IDs for AmeriFlux sites with FLUXNET data from the AmeriFlux API.

    :param api_url: Base API URL
    :type api_url: str
    :param url_get_query_endpoint: API endpoint for site availability
    :type url_get_query_endpoint: str
    :param headers: HTTP headers for the request
    :type headers: dict
    :return: List of site IDs or None if error
    :rtype: list or None
    """

    # get list of all AmeriFlux sites with FLUXNET data
    url_get_query = f"{api_url}{url_get_query_endpoint}"

    try:
        _log.debug(f"Starting request for list of AmeriFlux sites: {url_get_query}")
        response = requests.get(url=url_get_query, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the JSON response
        ameriflux_with_data_site_list = [e[0] for e in response.json()]
        _log.info(
            f"Found {len(ameriflux_with_data_site_list)} AmeriFlux sites "
            f"with FLUXNET data available: {ameriflux_with_data_site_list}"
        )

        return ameriflux_with_data_site_list

    except requests.exceptions.RequestException as e:
        _log.critical(f"Error fetching AmeriFlux data: {e}")
        return None


def get_ameriflux_download_links(
    api_url,
    site_ids=None,
    url_post_query_endpoint=AMERIFLUX_DOWNLOAD_PATH,
    headers=AMERIFLUX_HEADERS,
    output_file=None,
):
    """
    Get download links for AmeriFlux data from the AmeriFlux API for specified
    sites; if site_ids=None gets links for all sites with available data.

    :param api_url: Base API URL
    :type api_url: str
    :param site_ids: List of site IDs to get links for
    :type site_ids: list or None
    :param url_post_query_endpoint: API endpoint for download requests
    :type url_post_query_endpoint: str
    :param headers: HTTP headers for the request
    :type headers: dict
    :param output_file: Optional file to save response
    :type output_file: str or None
    :return: API response data or None if error
    :rtype: dict or None
    """

    # if no site_ids are provided, get links for all sites with available data
    if site_ids is None:
        site_ids = get_ameriflux_fluxnet_sites(api_url=api_url)

    # get links to site data
    url_post_query = f"{api_url}{url_post_query_endpoint}"
    json_query = {
        "user_id": "fluxnetshuttle",
        "user_email": "1color-censure@icloud.com",
        "data_product": "FLUXNET",
        "data_variant": "FULLSET",
        "data_policy": "CCBY4.0",
        "site_ids": site_ids,
        "intended_use": "Other",
        "description": "Testing FLUXNET Shuttle",
        "is_test": True,
    }

    try:
        # Make a POST request to the API
        _log.debug(
            f"Starting request for list of AmeriFlux download links "
            f"(this might take a few minutes): {url_post_query}"
        )
        response = requests.post(url=url_post_query, headers=headers, json=json_query)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the JSON response
        data = response.json()

        # Save the response to a file if specified
        if output_file is not None:
            with open(output_file, "w") as f:
                f.write(json.dumps(data, indent=4))
            _log.info(f"Saved AmeriFlux results to file: {output_file}")
        return data

    except requests.exceptions.RequestException as e:
        _log.critical(f"Error fetching AmeriFlux data: {e}")
        return None


def parse_ameriflux_response(data):
    """
    Parse the AmeriFlux API response to extract site information and download links.

    :param data: AmeriFlux API response data
    :type data: dict
    :return: Dictionary of sites with extracted information
    :rtype: dict
    """
    sites = {}
    network = "AmeriFlux"
    publisher = "AMP"
    for s in data["data_urls"]:
        entries = {
            "network": network,
            "publisher": publisher,
            "site_id": None,
            "first_year": None,
            "last_year": None,
            "download_link": None,
            "filename": None,
            "version": None,
        }
        site_id = s["site_id"]
        entries["site_id"] = site_id
        entries["download_link"] = s["url"]
        entries["filename"] = s["url"].split("/")[-1].split("?")[0]
        entries["version"] = entries["filename"].split("_")[-1].split(".")[0]
        entries["first_year"] = entries["filename"].split("_")[-2].split("-")[0]
        entries["last_year"] = entries["filename"].split("_")[-2].split("-")[1]
        sites[site_id] = entries
    return sites


def get_ameriflux_data(
    base_url=AMERIFLUX_BASE_URL,
    endpoint=AMERIFLUX_BASE_PATH,
    ameriflux_output_filename=None,
):
    """
    Fetch AmeriFlux data from the AmeriFlux API.

    .. versionadded:: 0.1.0
       Initial AmeriFlux API integration with site discovery and data availability.

    :param base_url: Base URL for AmeriFlux API
    :type base_url: str
    :param endpoint: API endpoint path
    :type endpoint: str
    :param ameriflux_output_filename: Optional file to save response
    :type ameriflux_output_filename: str or None
    :return: Dictionary of sites with data or None if error
    :rtype: dict or None
    """
    # base URL and endpoint for the AmeriFlux API
    api_url = f"{base_url}{endpoint}"

    # get list of AmeriFlux sites with FLUXNET data
    ameriflux_with_data_site_list = get_ameriflux_fluxnet_sites(api_url=api_url)
    if not ameriflux_with_data_site_list:
        _log.error("No AmeriFlux sites with FLUXNET data found.")
        return None

    # get download links for AmeriFlux data
    download_links = get_ameriflux_download_links(
        api_url=api_url,
        site_ids=ameriflux_with_data_site_list,
        output_file=ameriflux_output_filename,
    )
    if not download_links:
        _log.error("No AmeriFlux download links found.")
        return None
    sites = parse_ameriflux_response(download_links)

    _log.info(f"Found FLUXNET data for {len(sites)} AmeriFlux sites.")
    return sites


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
