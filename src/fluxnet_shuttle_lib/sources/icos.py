"""
ICOS Data Source Module
=======================

:module: fluxnet_shuttle_lib.sources.icos
:synopsis: ICOS Carbon Portal data source implementation
:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:platform: Unix, Windows
:created: 2025-01-25

.. currentmodule:: fluxnet_shuttle_lib.sources.icos

.. autosummary::
   :toctree: generated/

   get_icos_data
   download_icos_data

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

import json
import logging
import sys

import requests

from fluxnet_shuttle_lib import FLUXNETShuttleError

_log = logging.getLogger(__name__)

ICOS_API_URL = "https://meta.icos-cp.eu/sparql"
ICOS_HEADERS = {"Accept": "application/json"}
ICOS_QUERY = """
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix geo: <http://www.opengis.net/ont/geosparql#>
select ?dobj ?hasNextVersion ?spec ?station ?fileName ?size ?submTime ?timeStart ?timeEnd
where {
    VALUES ?spec {<http://meta.icos-cp.eu/resources/cpmeta/miscFluxnetArchiveProduct>}
    ?dobj cpmeta:hasObjectSpec ?spec .
    BIND(EXISTS{[] cpmeta:isNextVersionOf ?dobj} AS ?hasNextVersion)
    ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
    ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
    FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
}
order by desc(?fileName)
"""


def parse_icos_response(response):
    """
    Parse the ICOS API response and extract relevant information.

    :param response: HTTP response object from ICOS API
    :type response: requests.Response
    :return: Dictionary of sites with extracted information
    :rtype: dict
    """
    sites_json_list = response.json()["results"]["bindings"]
    sites = {}
    network = "ICOS"
    publisher = "ICOS-ETC"
    for s in sites_json_list:
        entries = {
            "network": network,
            "publisher": publisher,
            "first_year": None,
            "last_year": None,
            "download_link": None,
            "site_id": None,
            "filename": None,
            "version": None,
        }
        site_id = None
        for k, v in s.items():
            if k == "station":
                site_id = v["value"][-6:]
                entries["site_id"] = site_id
            if k == "dobj":
                entries["download_link"] = v["value"]
            if k == "fileName":
                filename = v["value"]
                entries["filename"] = filename
                entries["first_year"] = filename.split("_")[-2].split("-")[0]
                entries["last_year"] = filename.split("_")[-2].split("-")[1]
                entries["version"] = filename.split("_")[-1].split(".")[0]
        sites[site_id] = entries
    return sites


def get_icos_sites_and_links(
    api_url=ICOS_API_URL,
    headers=ICOS_HEADERS,
    query=ICOS_QUERY,
    icos_output_filename=None,
):
    """
    Get ICOS sites and download links from the ICOS Carbon Portal API.

    :param api_url: ICOS API URL
    :type api_url: str
    :param headers: HTTP headers for the request
    :type headers: dict
    :param query: SPARQL query string
    :type query: str
    :param icos_output_filename: Optional file to save response
    :type icos_output_filename: str or None
    :return: Dictionary of sites with data or None if error
    :rtype: dict or None
    """
    try:
        # Make a POST request to the API
        response = requests.post(url=api_url, headers=headers, data=bytes(query, "utf-8"))
        response.raise_for_status()  # Raise an error for bad responses

        # Save the response to a file if specified
        if icos_output_filename is not None:
            with open(icos_output_filename, "w") as f:
                f.write(json.dumps(response.json(), indent=4))
            _log.info(f"Saved ICOS results to file: {icos_output_filename}")

        # Parse the JSON response
        sites = parse_icos_response(response)
        return sites

    except requests.exceptions.RequestException as e:
        _log.error(f"Error fetching ICOS data: {e}")
        return None


def get_icos_data(api_url=ICOS_API_URL, icos_output_filename=None):
    """
    Fetch ICOS ETC data from the ICOS Carbon Portal API.

    .. versionadded:: 0.1.0
       Initial ICOS Carbon Portal integration with SPARQL queries.

    :param api_url: ICOS API URL
    :type api_url: str
    :param icos_output_filename: Optional file to save response
    :type icos_output_filename: str or None
    :return: Dictionary of sites with data or None if error
    :rtype: dict or None
    """

    # get list of ICOS sites with FLUXNET data and download links
    sites = get_icos_sites_and_links(api_url=api_url, icos_output_filename=icos_output_filename)
    if not sites:
        _log.error("No ICOS sites or download links found.")
        return None

    _log.info(f"Found FLUXNET data for {len(sites)} ICOS sites.")
    return sites


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
