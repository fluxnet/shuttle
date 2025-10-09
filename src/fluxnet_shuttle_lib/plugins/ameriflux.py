"""
AmeriFlux Network Plugin
========================

AmeriFlux network implementation for the FLUXNET Shuttle plugin system.
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Generator, Optional

import aiohttp

from ..core.base import NetworkPlugin
from ..core.decorators import async_to_sync_generator
from ..models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata

logger = logging.getLogger(__name__)

# Constants from original ameriflux module
AMERIFLUX_BASE_URL = "https://amfcdn.lbl.gov/"
AMERIFLUX_BASE_PATH = "api/v1/"
AMERIFLUX_AVAILABILITY_PATH = "site_availability/AmeriFlux/FLUXNET/CCBY4.0"
AMERIFLUX_DOWNLOAD_PATH = "data_download"
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

        # Get configuration parameters

        api_url = f"{AMERIFLUX_BASE_URL}{AMERIFLUX_BASE_PATH}"

        try:
            # First, get list of sites with FLUXNET data
            site_ids = await self._get_fluxnet_sites(api_url, timeout=30)
            if not site_ids:
                logger.warning("No AmeriFlux sites with FLUXNET data found")
                return

            logger.info(f"Found {len(site_ids)} AmeriFlux sites with FLUXNET data")

            # Then get download links for those sites
            download_data = await self._get_download_links(api_url, site_ids, timeout=30)
            if not download_data:
                logger.warning("No AmeriFlux download links found")
                return

            # Parse and yield site metadata
            for site_data in self._parse_response(download_data):
                await asyncio.sleep(0.1)  # Yield control to event loop
                yield site_data

        except Exception as e:
            logger.error(f"Error fetching AmeriFlux data: {e}")
            raise

    async def _get_fluxnet_sites(self, api_url: str, timeout: int) -> Optional[list]:
        """Get list of AmeriFlux sites that have FLUXNET data available."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.get(f"{api_url}{AMERIFLUX_AVAILABILITY_PATH}") as response:
                    response.raise_for_status()
                    site_ids = []
                    for site in await response.json():
                        await asyncio.sleep(0.1)  # Yield control to event loop
                        if isinstance(site, list) and len(site) == 2:
                            site_ids.append(site[0])

                    return site_ids

        except Exception as e:
            logger.error(f"Error fetching AmeriFlux FLUXNET sites: {e}")
            return None

    async def _get_download_links(self, base_url: str, site_ids: list, timeout: int) -> Optional[Dict[str, Any]]:
        """Get download links for specified AmeriFlux sites."""
        url_post_query = f"{base_url}{AMERIFLUX_DOWNLOAD_PATH}"

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
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=None)
            ) as session:
                async with session.post(url_post_query, headers=AMERIFLUX_HEADERS, json=json_query) as response:
                    response.raise_for_status()
                    data: Dict[str, Any] = await response.json()
                    return data

        except Exception as e:
            logger.error(f"Error fetching AmeriFlux download links: {e}")
            return None

    def _parse_response(self, data: Dict[str, Any]) -> Generator[Any, Any, Any]:
        """
        Parse AmeriFlux API response to extract site information.

        Args:
            data: AmeriFlux API response data

        Returns:
            Generator yielding FluxnetDatasetMetadata objects
        """
        for s in data.get("data_urls", []):
            try:
                site_id = s["site_id"]
                download_link = s["url"]
                filename = download_link.split("/")[-1].split("?")[0]

                # Extract metadata from filename (e.g., AMF_US-Ha1_FLUXNET_FULLSET_2005-2012_3-5.zip)
                parts = filename.split("_")
                if len(parts) >= 5:
                    year_range = parts[-2].split("-")
                    first_year = int(year_range[0])
                    last_year = int(year_range[1])
                else:
                    # Fallback if filename format is unexpected
                    first_year = 2000
                    last_year = 2020

                # For now, use placeholder values for missing geographic data
                # In a real implementation, you'd need another API call to get lat/lon
                site_info = BadmSiteGeneralInfo(
                    site_id=site_id,
                    network="AmeriFlux",
                    location_lat=0.0,  # Placeholder - would need additional API call
                    location_long=0.0,  # Placeholder - would need additional API call
                    igbp="UNK",  # Placeholder - would need additional API call
                )

                product_data = DataFluxnetProduct(
                    first_year=first_year, last_year=last_year, download_link=download_link
                )

                metadata = FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

                yield metadata

            except Exception as e:
                logger.warning(f"Error parsing site data for {s.get('site_id', 'unknown')}: {e}")
                continue


# Auto-register the plugin
from fluxnet_shuttle_lib.core.registry import registry

registry.register(AmeriFluxPlugin)
