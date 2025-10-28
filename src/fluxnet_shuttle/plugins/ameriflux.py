"""
AmeriFlux Network Plugin
========================

AmeriFlux network implementation for the FLUXNET Shuttle plugin system.
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Generator, List, cast

from pydantic import HttpUrl

from fluxnet_shuttle.core.exceptions import PluginError

from ..core.base import NetworkPlugin
from ..core.decorators import async_to_sync_generator
from ..models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata

logger = logging.getLogger(__name__)

# Constants from original ameriflux module
AMERIFLUX_BASE_URL = "https://amfcdn.lbl.gov/"
AMERIFLUX_BASE_PATH = "api/v2/"
AMERIFLUX_SITE_INFO_PATH = "site_info_display/AmeriFlux"
AMERIFLUX_DOWNLOAD_PATH = "amf_shuttle_data_files_and_manifest"
AMERIFLUX_HEADERS = {"Content-Type": "application/json"}


class AmeriFluxPlugin(NetworkPlugin):
    """AmeriFlux network plugin implementation."""

    @property
    def name(self) -> str:
        return __name__.split(".")[-1]

    @property
    def display_name(self) -> str:
        return "AmeriFlux"

    @async_to_sync_generator
    async def get_sites(self, **filters) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get AmeriFlux sites with FLUXNET data.

        Args:
            **filters: Optional filters (not used in this implementation)

        Yields:
            FluxnetDatasetMetadata: Site metadata objects
        """
        logger.info("Fetching AmeriFlux sites...")

        api_url = f"{AMERIFLUX_BASE_URL}{AMERIFLUX_BASE_PATH}"

        try:
            site_metadata = await self._get_site_metadata(api_url, timeout=30)
        except Exception as e:
            logger.exception("Failed to retrieve AmeriFlux data: %s", e)
            raise PluginError(self.name, f"Failed to retrieve data from API: {e}", original_error=e)

        # Validate site metadata
        if not site_metadata:
            logger.warning("No AmeriFlux site metadata found")
        else:
            logger.info(f"Retrieved metadata for {len(site_metadata)} AmeriFlux sites")

        if site_metadata:
            try:
                # Filter for sites with FLUXNET data (non-empty grp_publish_fluxnet)
                sites_with_data: Dict[str, List[int]] = {
                    site_id: cast(List[int], site.get("grp_publish_fluxnet", []))
                    for site_id, site in site_metadata.items()
                    if site.get("grp_publish_fluxnet")
                }

                if not sites_with_data:
                    logger.warning("No AmeriFlux sites with FLUXNET data found")
                else:
                    logger.info(f"Found {len(sites_with_data)} AmeriFlux sites with FLUXNET data")

                    # Get download links for sites with data
                    site_ids = list(sites_with_data.keys())
                    download_data = await self._get_download_links(api_url, site_ids, timeout=30)

                    if not download_data or not download_data.get("data_urls"):
                        logger.warning("No AmeriFlux download links found")
                    else:
                        logger.info(f"Retrieved download links for {len(download_data.get('data_urls', []))} sites")
                        for site_data in self._parse_response(download_data, site_metadata, sites_with_data):
                            await asyncio.sleep(0.1)
                            yield site_data

            except PluginError:
                # Re-raise PluginError without wrapping
                raise
            except Exception as e:
                logger.exception("Error processing AmeriFlux data: %s", e)
                raise PluginError(self.name, f"Error processing data: {e}", original_error=e)

    async def _get_site_metadata(self, api_url: str, timeout: int) -> Dict[str, Any]:
        """Get site metadata including lat, lon, IGBP from v2 site_info_display endpoint."""
        try:
            async with self._session_request(
                "GET", f"{api_url}{AMERIFLUX_SITE_INFO_PATH}", timeout=timeout
            ) as response:
                data = await response.json()
                # Create a dictionary indexed by site_id for quick lookup
                site_dict = {}
                for site in data.get("values", []):
                    await asyncio.sleep(0.1)
                    site_id = site.get("site_id")
                    if site_id:
                        site_dict[site_id] = site

                return site_dict

        except PluginError:
            # Re-raise PluginError - site metadata is critical for plugin operation
            raise

    async def _get_download_links(self, base_url: str, site_ids: list, timeout: int) -> Dict[str, Any]:
        """Get download links for specified AmeriFlux sites using v2 shuttle endpoint."""
        url_post_query = f"{base_url}{AMERIFLUX_DOWNLOAD_PATH}"

        # V2 endpoint requires only: user_id, data_product, data_variant, site_ids
        json_query = {
            "user_id": "fluxnetshuttle",
            "data_product": "FLUXNET",
            "data_variant": "FULLSET",
            "site_ids": site_ids,
        }

        try:
            async with self._session_request(
                "POST", url_post_query, headers=AMERIFLUX_HEADERS, json=json_query, timeout=timeout
            ) as response:
                data: Dict[str, Any] = await response.json()
                return data

        except PluginError:
            # Re-raise PluginError - download links are critical for plugin operation
            raise

    @staticmethod
    def _build_site_info(site_id: str, site_metadata: Dict[str, Any]) -> BadmSiteGeneralInfo:
        """
        Build BadmSiteGeneralInfo model from site metadata.

        Args:
            site_id: Site identifier
            site_metadata: Dictionary containing site metadata from site_info_display endpoint

        Returns:
            BadmSiteGeneralInfo: Validated site information model

        Raises:
            ValueError: If site metadata is invalid or incomplete
        """
        site_meta = site_metadata.get(site_id, {})
        grp_location = site_meta.get("grp_location", {})
        grp_igbp = site_meta.get("grp_igbp", {})

        # Extract lat, lon, and IGBP from metadata
        try:
            location_lat = float(grp_location.get("location_lat", 0.0))
            location_long = float(grp_location.get("location_long", 0.0))
        except (ValueError, TypeError):
            location_lat = 0.0
            location_long = 0.0
            logger.warning(f"Invalid lat/lon for site {site_id}")

        igbp = grp_igbp.get("igbp", "UNK")

        return BadmSiteGeneralInfo(
            site_id=site_id,
            network="AmeriFlux",
            location_lat=location_lat,
            location_long=location_long,
            igbp=igbp,
        )

    @staticmethod
    def _build_product_data(publish_years: List[int], download_link: str) -> DataFluxnetProduct:
        """
        Build DataFluxnetProduct model from publish years and download link.

        Args:
            publish_years: List of years with published data
            download_link: URL to download the data

        Returns:
            DataFluxnetProduct: Validated product data model

        Raises:
            ValueError: If publish_years is empty or data is invalid
        """
        if not publish_years:
            raise ValueError("publish_years cannot be empty")

        first_year = min(publish_years)
        last_year = max(publish_years)

        # Pydantic will validate and convert the string to HttpUrl
        return DataFluxnetProduct(
            first_year=first_year, last_year=last_year, download_link=cast(HttpUrl, download_link)
        )

    def _parse_response(
        self, data: Dict[str, Any], site_metadata: Dict[str, Any], sites_with_data: Dict[str, List[int]]
    ) -> Generator[FluxnetDatasetMetadata, None, None]:
        """
        Parse AmeriFlux API response to extract site information with complete metadata.

        Args:
            data: AmeriFlux API response data with download links
            site_metadata: Dictionary of site metadata indexed by site_id
            sites_with_data: Dictionary of site_id to publish_years

        Returns:
            Generator yielding FluxnetDatasetMetadata objects
        """
        for s in data.get("data_urls", []):
            try:
                site_id = s["site_id"]
                download_link = s["url"]

                # Get years from sites_with_data (from data_availability endpoint)
                publish_years = sites_with_data.get(site_id, [])
                if not publish_years:
                    logger.debug(f"Skipping site {site_id} - no publish years available")
                    continue

                # Build site info and product data models using helper functions
                site_info = self._build_site_info(site_id, site_metadata)
                product_data = self._build_product_data(publish_years, download_link)

                metadata = FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

                yield metadata

            except Exception as e:
                site_id = s.get("site_id", "unknown")
                logger.warning(f"Error parsing site data for {site_id}: {e}. Skipping this site.")
                continue


# Auto-register the plugin
from fluxnet_shuttle.core.registry import registry

registry.register(AmeriFluxPlugin)
