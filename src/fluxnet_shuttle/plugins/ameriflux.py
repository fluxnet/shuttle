"""
AmeriFlux Data Hub Plugin
=========================

AmeriFlux data hub implementation for the FLUXNET Shuttle plugin system.
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Generator, List, cast

from pydantic import HttpUrl

from fluxnet_shuttle.core.exceptions import PluginError

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

# Constants from original ameriflux module
AMERIFLUX_BASE_URL = "https://amfcdn.lbl.gov/"
AMERIFLUX_BASE_PATH = "api/v2/"
AMERIFLUX_SITE_INFO_PATH = "site_info_display/AmeriFlux"
AMERIFLUX_MANIFEST_PATH = "amf_shuttle_data_files_and_manifest"  # For listall - returns manifest
AMERIFLUX_DOWNLOAD_PATH = "data_download"  # For download - requires full user tracking
AMERIFLUX_CITATIONS_PATH = "citations/FLUXNET"
AMERIFLUX_HEADERS = {"Content-Type": "application/json"}

AMERIFLUX_DEFAULT_USER_ID = "fluxnetshuttle"
AMERIFLUX_DEFAULT_USER_EMAIL = "support@fluxnet.org"
AMERIFLUX_DEFAULT_INTENDED_USE = "synthesis"
AMERIFLUX_DEFAULT_DATA_POLICY = "CCBY4.0"
AMERIFLUX_DEFAULT_DESCRIPTION = "FLUXNET data download via fluxnet-shuttle"

# AmeriFlux API request constants
AMERIFLUX_DATA_PRODUCT = "FLUXNET"
AMERIFLUX_DATA_VARIANT = "FULLSET"

# Valid intended_use values (AmeriFlux API enum)
AMERIFLUX_INTENDED_USE_OPTIONS = {
    "synthesis": "Multi-site synthesis",
    "model": "Land model/Earth system model",
    "remote_sensing": "Remote sensing research",
    "other_research": "Other research",
    "education": "Education (Teacher or Student)",
    "other": "Other",
}


class AmeriFluxPlugin(DataHubPlugin):
    """AmeriFlux data hub plugin implementation."""

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
            site_metadata = await self._get_site_metadata(api_url)
        except Exception as e:
            logger.exception("Failed to retrieve AmeriFlux data: %s", e)
            raise PluginError(self.name, f"Failed to retrieve data from API: {e}", original_error=e)

        # Validate site metadata
        if not site_metadata:
            logger.warning("No AmeriFlux sites with FLUXNET data found")
        else:
            logger.info(f"Retrieved metadata for {len(site_metadata)} AmeriFlux sites")

            try:

                # Get manifest with download links for sites with data (used by listall)
                site_ids = list(site_metadata.keys())
                download_data = await self._get_manifest(api_url, site_ids)

                if not download_data or not download_data.get("data_urls"):
                    logger.warning("No AmeriFlux download links found")
                else:
                    logger.info(f"Retrieved download links for {len(download_data.get('data_urls', []))} sites")

                    # Fetch citations for all sites
                    citations = await self._get_citations(api_url, site_ids)
                    logger.info(f"Retrieved citations for {len(citations)} sites")

                    for site_data in self._parse_response(download_data, site_metadata, citations):
                        await asyncio.sleep(0.001)  # Yield control to event loop
                        yield site_data

            except PluginError:
                # Re-raise PluginError without wrapping
                raise
            except Exception as e:
                logger.exception("Error processing AmeriFlux data: %s", e)
                raise PluginError(self.name, f"Error processing data: {e}", original_error=e)

    async def _get_site_metadata(self, api_url: str) -> Dict[str, Any]:
        """Get site metadata including lat, lon, IGBP from v2 site_info_display endpoint."""
        try:
            async with self._session_request("GET", f"{api_url}{AMERIFLUX_SITE_INFO_PATH}") as response:
                data = await response.json()
                # Create a dictionary indexed by site_id for quick lookup
                site_dict = {}
                for site in data.get("values", []):
                    await asyncio.sleep(0.001)  # Yield control to event loop
                    site_id = site.get("site_id")
                    if site_id is not None and site.get("grp_publish_fluxnet", False):
                        site_dict[site_id] = site

                return site_dict

        except PluginError:
            # Re-raise PluginError - site metadata is critical for plugin operation
            raise

    async def _get_manifest(
        self,
        base_url: str,
        site_ids: list,
    ) -> Dict[str, Any]:
        """
        Get manifest for specified AmeriFlux sites using v2 manifest endpoint.

        This endpoint is used by listall to get download URLs without full user tracking.
        It requires only: user_id, data_product, data_variant, site_ids
        """
        url_post_query = f"{base_url}{AMERIFLUX_MANIFEST_PATH}"

        # V2 manifest endpoint requires minimal info
        json_query = {
            "user_id": AMERIFLUX_DEFAULT_USER_ID,
            "data_product": AMERIFLUX_DATA_PRODUCT,
            "data_variant": AMERIFLUX_DATA_VARIANT,
            "site_ids": site_ids,
        }

        try:
            async with self._session_request(
                "POST", url_post_query, headers=AMERIFLUX_HEADERS, json=json_query
            ) as response:
                data: Dict[str, Any] = await response.json()
                return data

        except PluginError:
            # Re-raise PluginError - manifest is critical for plugin operation
            raise

    async def _get_download_url(
        self,
        base_url: str,
        site_ids: list,
        user_id: str = AMERIFLUX_DEFAULT_USER_ID,
        user_email: str = AMERIFLUX_DEFAULT_USER_EMAIL,
        intended_use: str = AMERIFLUX_DEFAULT_INTENDED_USE,
        description: str = AMERIFLUX_DEFAULT_DESCRIPTION,
    ) -> Dict[str, Any]:
        """
        Get download URL for specified AmeriFlux sites using v2 data_download endpoint.

        This endpoint is used by download command to track user information.
        It requires: user_id, user_email, data_policy, data_product, data_variant,
        site_ids, intended_use, description
        """
        # Use test server for data_download endpoint during development
        url_post_query = f"https://ameriflux-data-test.lbl.gov/api/v2/{AMERIFLUX_DOWNLOAD_PATH}"

        json_query = {
            "user_id": user_id,
            "user_email": user_email,
            "data_policy": AMERIFLUX_DEFAULT_DATA_POLICY,
            "data_product": AMERIFLUX_DATA_PRODUCT,
            "data_variant": AMERIFLUX_DATA_VARIANT,
            "site_ids": site_ids,
            "intended_use": intended_use,
            "description": description,
        }

        try:
            async with self._session_request(
                "POST", url_post_query, headers=AMERIFLUX_HEADERS, json=json_query
            ) as response:
                data: Dict[str, Any] = await response.json()
                return data

        except PluginError:
            # Re-raise PluginError - download URL is critical for download operation
            raise

    async def _get_citations(self, base_url: str, site_ids: List[str]) -> Dict[str, str]:
        """
        Get citations for specified AmeriFlux sites using v2 citations endpoint.

        Args:
            base_url: Base API URL
            site_ids: List of site IDs to get citations for

        Returns:
            Dictionary mapping site_id to citation string
        """
        url_post_query = f"{base_url}{AMERIFLUX_CITATIONS_PATH}"

        json_query = {"site_ids": site_ids}

        try:
            async with self._session_request(
                "POST", url_post_query, headers=AMERIFLUX_HEADERS, json=json_query
            ) as response:
                data: Dict[str, Any] = await response.json()

                # Build dictionary mapping site_id to citation
                citations_dict = {}
                for item in data.get("values", []):
                    site_id = item.get("site_id")
                    citation = item.get("citation", "")
                    if site_id:
                        citations_dict[site_id] = citation

                return citations_dict

        except PluginError:
            # Log warning but don't fail - citations are optional
            logger.warning(f"Failed to fetch citations for {len(site_ids)} sites")
            return {}
        except Exception as e:
            # Log warning but don't fail - citations are optional
            logger.warning(f"Error fetching citations: {e}")
            return {}

    async def prepare_download(self, site_id: str, download_link: str, **kwargs) -> str:
        """
        Prepare AmeriFlux download URL with user tracking.

        Args:
            site_id: Site identifier
            download_link: Original download link (unused - we generate fresh URL)
            **kwargs: Additional parameters including user_info dictionary with
                     user_id, user_email, intended_use, description

        Returns:
            str: Fresh download URL with user tracking

        Raises:
            PluginError: If URL generation fails
        """
        # Extract user_info from kwargs, use empty dict if not provided
        user_info = kwargs.get("user_info", {})

        # Interactive prompts for missing user info
        user_id = user_info.get("user_id")
        if not user_id:
            print("\n" + "=" * 60)
            print("AmeriFlux User Tracking Information Required")
            print("=" * 60)
            user_id = input("Enter your user name: ").strip()
            if not user_id:
                logger.warning("No user name provided, using default")
                user_id = AMERIFLUX_DEFAULT_USER_ID

        user_email = user_info.get("user_email")
        if not user_email:
            user_email = input("Enter your email address: ").strip()
            if not user_email:
                logger.warning("No email provided, using default")
                user_email = AMERIFLUX_DEFAULT_USER_EMAIL

        intended_use = user_info.get("intended_use")
        if not intended_use:
            print("\nIntended use options:")
            print("  1. synthesis - Multi-site synthesis")
            print("  2. model - Land model/Earth system model")
            print("  3. remote_sensing - Remote sensing research")
            print("  4. other_research - Other research")
            print("  5. education - Education (Teacher or Student)")
            print("  6. other - Other")
            intended_use_choice = input("Select intended use [1-6, default: 1]: ").strip() or "1"

            intended_use_map = {
                "1": "synthesis",
                "2": "model",
                "3": "remote_sensing",
                "4": "other_research",
                "5": "education",
                "6": "other",
            }
            intended_use = intended_use_map.get(intended_use_choice, "synthesis")

        # Validate intended_use
        if intended_use not in AMERIFLUX_INTENDED_USE_OPTIONS:
            logger.warning(f"Invalid intended_use '{intended_use}', using default '{AMERIFLUX_DEFAULT_INTENDED_USE}'")
            intended_use = AMERIFLUX_DEFAULT_INTENDED_USE

        description = user_info.get("description")
        if not description:
            description = input("Enter a brief description of your intended use (optional): ").strip()
            if not description:
                description = AMERIFLUX_DEFAULT_DESCRIPTION
            print("=" * 60 + "\n")

        logger.info(f"Getting personalized download URL for AmeriFlux site {site_id} with user_id={user_id}")

        api_url = f"{AMERIFLUX_BASE_URL}{AMERIFLUX_BASE_PATH}"

        try:
            # Get fresh download URL from AmeriFlux API with user tracking
            download_data = await self._get_download_url(
                api_url, [site_id], user_id, user_email, intended_use, description
            )

            # Extract URL for this specific site
            for item in download_data.get("data_urls", []):
                if item.get("site_id") == site_id:
                    fresh_url = item.get("url")
                    if fresh_url and isinstance(fresh_url, str):
                        logger.info(f"Generated personalized URL for {site_id} with user tracking")
                        return str(fresh_url)  # Cast to str to satisfy mypy

            # If we get here, site wasn't found in response
            logger.warning(f"No download URL found for site {site_id}, using original link")
            return download_link

        except Exception as e:
            logger.error(f"Failed to get fresh download URL for {site_id}: {e}")
            logger.warning("Using original download link as fallback")
            return download_link

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

        # Extract site name
        site_name = site_meta.get("site_name", "")

        # Extract lat, lon, and IGBP from metadata
        try:
            location_lat = float(grp_location.get("location_lat", 0.0))
            location_long = float(grp_location.get("location_long", 0.0))
        except (ValueError, TypeError):
            location_lat = 0.0
            location_long = 0.0
            logger.warning(f"Invalid lat/lon for site {site_id}")

        igbp = grp_igbp.get("igbp", "UNK")

        # Extract network information from grp_network (list of strings)
        grp_network = site_meta.get("grp_network", [])
        network = grp_network if isinstance(grp_network, list) else []

        # Extract team member information
        team_members = []
        grp_team_member = site_meta.get("grp_team_member", [])
        if isinstance(grp_team_member, list):
            for member in grp_team_member:
                try:
                    team_member = TeamMember(
                        team_member_name=member.get("team_member_name", ""),
                        team_member_role=member.get("team_member_role", ""),
                        team_member_email=member.get("team_member_email", ""),
                    )
                    team_members.append(team_member)
                except Exception as e:
                    logger.warning(f"Error parsing team member for site {site_id}: {e}")
                    continue

        return BadmSiteGeneralInfo(
            site_id=site_id,
            site_name=site_name,
            data_hub="AmeriFlux",
            location_lat=location_lat,
            location_long=location_long,
            igbp=igbp,
            network=network,
            group_team_member=team_members,
        )

    @staticmethod
    def _build_product_data(
        publish_years: List[int],
        download_link: str,
        product_id: str,
        citation: str,
        code_version: str,
        product_source_network: str,
    ) -> DataFluxnetProduct:
        """
        Build DataFluxnetProduct model from publish years and download link.

        Args:
            publish_years: List of years with published data
            download_link: URL to download the data
            product_id: Product identifier (e.g., hashtag, DOI, PID)
            citation: Citation string for the data product
            code_version: Code version extracted from filename
            product_source_network: Source network identifier extracted from filename

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
            first_year=first_year,
            last_year=last_year,
            download_link=cast(HttpUrl, download_link),
            product_citation=citation,
            product_id=product_id,
            code_version=code_version,
            product_source_network=product_source_network,
        )

    def _parse_response(
        self, data: Dict[str, Any], site_metadata: Dict[str, Any], citations: Dict[str, str]
    ) -> Generator[FluxnetDatasetMetadata, None, None]:
        """
        Parse AmeriFlux API response to extract site information with complete metadata.

        Args:
            data: AmeriFlux API response data with download links
            site_metadata: Dictionary of site metadata indexed by site_id
            citations: Dictionary mapping site_id to citation string

        Returns:
            Generator yielding FluxnetDatasetMetadata objects
        """
        for s in data.get("data_urls", []):
            try:
                site_id = s["site_id"]
                download_link = s["url"]

                # Validate filename format
                if not validate_fluxnet_filename_format(download_link):
                    logger.info(
                        f"Skipping site {site_id} - filename does not follow standard format "
                        f"(<network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.<extension>): "
                        f"{download_link}"
                    )
                    continue

                # Get years from site_metadata (from data_availability endpoint)
                publish_years = site_metadata.get(site_id, {}).get("grp_publish_fluxnet", [])
                if not publish_years:
                    logger.debug(f"Skipping site {site_id} - no publish years available")
                    continue

                # Extract FLUXNET DOI from site metadata
                doi_info = site_metadata.get(site_id, {}).get("doi", {})
                product_id = doi_info.get("FLUXNET", "") if isinstance(doi_info, dict) else ""

                # Extract both product source network and code version from download URL in one pass
                # URL path typically contains the filename at the end
                product_source_network, code_version = extract_fluxnet_filename_metadata(download_link)

                # Get citation for this site
                citation = citations.get(site_id, "")

                # Skip site if citation is not available
                if not citation:
                    logger.warning(
                        f"Skipping site {site_id} - no citation available. "
                        f"Please contact AmeriFlux Management Project (AMP) at ameriflux-support@lbl.gov."
                    )
                    continue

                # Build site info and product data models using helper functions
                site_info = self._build_site_info(site_id, site_metadata)
                product_data = self._build_product_data(
                    publish_years,
                    download_link,
                    product_id=product_id,
                    citation=citation,
                    code_version=code_version,
                    product_source_network=product_source_network,
                )

                metadata = FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

                yield metadata

            except Exception as e:
                site_id = s.get("site_id", "unknown")
                logger.warning(f"Error parsing site data for {site_id}: {e}. Skipping this site.")
                continue


# Auto-register the plugin
from fluxnet_shuttle.core.registry import registry

registry.register(AmeriFluxPlugin)
