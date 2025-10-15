"""
ICOS Network Plugin
===================

ICOS Carbon Portal network implementation for the FLUXNET Shuttle plugin system.
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Generator

from ..core.base import NetworkPlugin
from ..core.decorators import async_to_sync_generator
from ..models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata

logger = logging.getLogger(__name__)

# Constants from original ICOS module
ICOS_API_URL = "https://meta.icos-cp.eu/sparql"
ICOS_SPARQL_QUERY = """
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


class ICOSPlugin(NetworkPlugin):
    """ICOS Carbon Portal network plugin implementation."""

    @property
    def name(self) -> str:
        return "icos"

    @property
    def display_name(self) -> str:
        return "ICOS"

    @async_to_sync_generator
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get ICOS sites with FLUXNET data.

        Args:
            **filters: Optional filters (not used in this implementation)

        Yields:
            FluxnetDatasetMetadata: Site metadata objects
        """
        logger.info("Fetching ICOS sites...")

        # Get configuration parameters
        api_url = self.config.get("api_url", ICOS_API_URL)
        timeout = self.config.get("timeout", 45)

        async with self._session_request(
            "POST", api_url, data={"query": ICOS_SPARQL_QUERY}, headers={"Accept": "application/json"}, timeout=timeout
        ) as response:
            data = await response.json()

            # Parse and yield site metadata
            for site_data in self._parse_sparql_response(data):
                await asyncio.sleep(0.1)  # Yield control to event loop
                yield site_data

    def _parse_sparql_response(self, data: Dict[str, Any]) -> Generator[FluxnetDatasetMetadata, None, None]:
        """
        Parse ICOS SPARQL response to extract site information.

        Args:
            data: SPARQL response data

        Returns:
            List of FluxnetDatasetMetadata objects
        """
        # Group results by station to avoid duplicates
        for binding in data.get("results", {}).get("bindings", []):
            try:
                station_uri = binding["station"]["value"][-6:]

                # Extract station ID from URI (e.g., last part after /)
                station_id = station_uri.split("/")[-1]

                # Extract year from time strings if available
                time_start = binding.get("timeStart", {}).get("value", "")
                time_end = binding.get("timeEnd", {}).get("value", "")

                location_lat = binding.get("lat", {}).get("value", 0.0)
                location_long = binding.get("lon", {}).get("value", 0.0)
                location_lat = float(location_lat)
                location_long = float(location_long)

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

                # Extract download URL from data object URI
                dobj_uri = binding["dobj"]["value"]
                download_link = dobj_uri.replace("/meta/", "/objects/")

                site_info = BadmSiteGeneralInfo(
                    site_id=station_id,
                    network="ICOS",
                    location_lat=location_lat,
                    location_long=location_long,
                    igbp="UNK",  # ICOS doesn't provide IGBP in this query
                )

                product_data = DataFluxnetProduct(
                    first_year=first_year, last_year=last_year, download_link=download_link
                )

                metadata = FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

                yield metadata

            except Exception as e:
                logger.warning(f"Error parsing ICOS site data: {e}")
                continue


# Auto-register the plugin
from ..core.registry import registry

registry.register(ICOSPlugin)
