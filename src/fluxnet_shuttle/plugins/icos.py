"""
ICOS Data Hub Plugin
====================

ICOS Carbon Portal data hub implementation for the FLUXNET Shuttle plugin system.

:moduleauthor: Gilberto Pastorello <gzpastorello@lbl.gov>
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:platform: Unix, Windows
:created: 2025-01-09
:updated: 2025-12-09
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Generator, List, Optional, Tuple

from ..core.base import DataHubPlugin
from ..core.decorators import async_to_sync_generator
from ..models import (
    BadmSiteGeneralInfo,
    DataFluxnetProduct,
    FluxnetDatasetMetadata,
    TeamMember,
)
from ..shuttle import (
    extract_fluxnet_filename_metadata,
    validate_fluxnet_filename_format,
)

logger = logging.getLogger(__name__)

# Constants from original ICOS module
ICOS_API_URL = "https://meta.icos-cp.eu/sparql"
ICOS_SPARQL_QUERY = """
prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix geo: <http://www.opengis.net/ont/geosparql#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select ?dobj ?hasNextVersion ?spec ?station ?stationName ?fileName ?size ?submTime
       ?timeStart ?timeEnd ?lat ?lon ?ecosystemType ?citationString
       ?firstName ?lastName ?email ?roleName ?orgName
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

    # Get station name
    OPTIONAL {
        ?station cpmeta:hasName ?stationName .
    }

    # Get station location
    OPTIONAL {
        ?station cpmeta:hasLatitude ?lat .
        ?station cpmeta:hasLongitude ?lon .
    }

    # Get ecosystem/vegetation type if available
    OPTIONAL {
        ?station cpmeta:hasEcosystemType ?ecosystemType .
    }

    # Get citation string
    OPTIONAL {
        ?dobj cpmeta:hasCitationString ?citationString .
    }

    # Get team member information
    OPTIONAL {
        ?membership cpmeta:atOrganization ?station .
        ?person cpmeta:hasMembership ?membership .
        OPTIONAL { ?person cpmeta:hasFirstName ?firstName . }
        OPTIONAL { ?person cpmeta:hasLastName ?lastName . }
        OPTIONAL { ?person cpmeta:hasEmail ?email . }
        OPTIONAL {
            ?membership cpmeta:hasRole ?role .
            ?role rdfs:label ?roleName .
        }
        OPTIONAL {
            ?membership cpmeta:hasAttributingOrganization ?org .
            ?org cpmeta:hasName ?orgName .
        }
    }

    FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
}
order by desc(?fileName)
"""


class ICOSPlugin(DataHubPlugin):
    """ICOS Carbon Portal data hub plugin implementation."""

    @property
    def name(self) -> str:
        return "icos"

    @property
    def display_name(self) -> str:
        return "ICOS"

    @async_to_sync_generator
    async def get_sites(self) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Fetch ICOS sites with FLUXNET data from the ICOS Carbon Portal SPARQL endpoint.

        Yields site metadata objects using the FluxnetDatasetMetadata model.
        All available sites are returned; filtering is not currently supported.

        This method is an async generator. The :func:`async_to_sync_generator`
        decorator allows usage in both asynchronous and synchronous contexts.

        Configuration:
            api_url (str): Optional. Override the default ICOS API URL.
            timeout (int): Optional. Request timeout in seconds.

        Yields:
            FluxnetDatasetMetadata: Site metadata objects.
        """
        logger.info("Fetching ICOS sites...")

        # Get configuration parameters
        api_url = self.config.get("api_url", ICOS_API_URL)

        async with self._session_request(
            "POST", api_url, data={"query": ICOS_SPARQL_QUERY}, headers={"Accept": "application/json"}
        ) as response:
            data = await response.json()

            # Parse and yield site metadata
            for site_data in self._parse_sparql_response(data):
                await asyncio.sleep(0.001)  # Yield control to event loop
                yield site_data

    def _group_sparql_bindings(self, bindings: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Group SPARQL bindings by data object URI and collect team members."""
        sites_data: Dict[str, Dict[str, Any]] = {}

        for binding in bindings:
            try:
                dobj_uri = binding["dobj"]["value"]

                # Initialize site data if first time seeing this dobj
                if dobj_uri not in sites_data:
                    station_uri = binding["station"]["value"][-6:]
                    station_id = station_uri.split("/")[-1]

                    sites_data[dobj_uri] = {
                        "station_id": station_id,
                        "station_name": binding.get("stationName", {}).get("value", station_id),
                        "time_start": binding.get("timeStart", {}).get("value", ""),
                        "time_end": binding.get("timeEnd", {}).get("value", ""),
                        "location_lat": binding.get("lat", {}).get("value"),
                        "location_long": binding.get("lon", {}).get("value"),
                        "ecosystem_type": binding.get("ecosystemType", {}).get("value", ""),
                        "citation": binding.get("citationString", {}).get("value", ""),
                        "filename": binding.get("fileName", {}).get("value", ""),
                        "dobj_uri": dobj_uri,
                        "team_members": [],
                    }

                # Extract and add team member if present
                team_member = self._extract_team_member(binding)
                if team_member:
                    sites_data[dobj_uri]["team_members"].append(team_member)

            except Exception as e:
                logger.warning(f"Error grouping ICOS site data: {e}")
                continue

        return sites_data

    def _extract_team_member(self, binding: Dict[str, Any]) -> Optional[TeamMember]:
        """Extract team member information from SPARQL binding."""
        first_name = binding.get("firstName", {}).get("value", "")
        last_name = binding.get("lastName", {}).get("value", "")

        if not (first_name or last_name):
            return None

        full_name = f"{first_name} {last_name}".strip()
        if not full_name:
            return None

        return TeamMember(
            team_member_name=full_name,
            team_member_role=binding.get("roleName", {}).get("value", ""),
            team_member_email=binding.get("email", {}).get("value", ""),
        )

    def _parse_coordinates(self, station_id: str, lat_value: Any, lon_value: Any) -> Tuple[float, float]:
        """Parse and validate latitude and longitude coordinates."""
        location_lat = 0.0
        location_long = 0.0

        try:
            if lat_value is not None:
                location_lat = float(lat_value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid latitude for station {station_id}")

        try:
            if lon_value is not None:
                location_long = float(lon_value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid longitude for station {station_id}")

        return location_lat, location_long

    def _parse_year_range(self, time_start: str, time_end: str) -> Tuple[int, int]:
        """Parse year range from time strings."""
        first_year = 2000  # Default
        last_year = 2020  # Default

        if time_start:
            try:
                first_year = int(time_start[:4])
            except (ValueError, IndexError):
                pass

        if time_end:
            try:
                last_year = int(time_end[:4])
            except (ValueError, IndexError):
                pass

        return first_year, last_year

    def _parse_sparql_response(self, data: Dict[str, Any]) -> Generator[FluxnetDatasetMetadata, None, None]:
        """
        Parse ICOS SPARQL response to extract site information.

        Args:
            data: SPARQL response data

        Yields:
            FluxnetDatasetMetadata objects with citation information and team members from SPARQL query
        """
        bindings = data.get("results", {}).get("bindings", [])
        sites_data = self._group_sparql_bindings(bindings)

        # Yield one FluxnetDatasetMetadata per site
        for dobj_uri, site_data in sites_data.items():
            try:
                station_id = site_data["station_id"]
                filename = site_data["filename"]

                # Validate filename format
                if not validate_fluxnet_filename_format(filename):
                    logger.info(
                        f"Skipping site {station_id} - filename does not follow standard format "
                        f"(<network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.<extension>): "
                        f"{filename}"
                    )
                    continue

                location_lat, location_long = self._parse_coordinates(
                    station_id, site_data["location_lat"], site_data["location_long"]
                )
                first_year, last_year = self._parse_year_range(site_data["time_start"], site_data["time_end"])

                igbp = self._map_ecosystem_to_igbp(site_data["ecosystem_type"])
                download_id = dobj_uri.split("/")[-1]
                download_link = f"https://data.icos-cp.eu/licence_accept?ids=%5B%22{download_id}%22%5D"
                # Extract both product source network and code version from filename in one pass
                # Note: We ignore the year range and run here since ICOS provides years via the SPARQL API
                product_source_network, oneflux_code_version, _, _, _ = extract_fluxnet_filename_metadata(filename)
                citation = site_data["citation"]

                # Skip site if citation is not available
                if not citation:
                    logger.warning(
                        f"Skipping site {station_id} - no citation available. "
                        f"Please contact FLUXNET support at support@fluxnet.org."
                    )
                    continue

                site_info = BadmSiteGeneralInfo(
                    site_id=station_id,
                    site_name=site_data["station_name"],
                    data_hub="ICOS",
                    location_lat=location_lat,
                    location_long=location_long,
                    igbp=igbp,
                    network=[],  # Update when network information is available from SPARQL
                    group_team_member=site_data["team_members"],
                )

                product_data = DataFluxnetProduct(
                    first_year=first_year,
                    last_year=last_year,
                    download_link=download_link,  # type: ignore[arg-type]
                    product_citation=citation,
                    product_id=download_id,
                    oneflux_code_version=oneflux_code_version,
                    product_source_network=product_source_network,
                    fluxnet_product_name=filename,
                )

                yield FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

            except Exception as e:
                logger.warning(f"Error parsing ICOS site data: {e}")
                continue

    def _map_ecosystem_to_igbp(self, ecosystem_type: str) -> str:
        """
        Map ICOS ecosystem type to IGBP land cover classification.

        ICOS provides ecosystem types as URIs in the format:
        http://meta.icos-cp.eu/ontologies/cpmeta/igbp_XXX
        where XXX is the IGBP code (ENF, GRA, CRO, etc.)

        Args:
            ecosystem_type: ICOS ecosystem type string (URI)

        Returns:
            IGBP classification code (e.g., "ENF", "GRA", "CRO", "UNK")
        """
        if not ecosystem_type:
            return "UNK"

        # Extract the last part of URI if it's a URI
        if "/" in ecosystem_type:
            ecosystem_type = ecosystem_type.split("/")[-1]

        # Check if it's in IGBP format (igbp_XXX)
        if ecosystem_type.startswith("igbp_"):
            igbp_code = ecosystem_type[5:].upper()  # Extract XXX from igbp_XXX
            return igbp_code

        # If not recognized, return unknown
        logger.debug(f"Unknown ecosystem type for IGBP mapping: {ecosystem_type}")
        return "UNK"


# Auto-register the plugin
from ..core.registry import registry

registry.register(ICOSPlugin)
