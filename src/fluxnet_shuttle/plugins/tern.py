"""
TERN Data Hub Plugin
====================

TERN (Terrestrial Ecosystem Research Network) data hub implementation
for the FLUXNET Shuttle plugin system.

This plugin handles:
- BADM SGI metadata from BIF (BADM Interchange Format) files
- FLUXNET Data Product information from separate text files
- Parsing grouped metadata elements (e.g., team members)
"""

import asyncio
import csv
import logging
from collections import defaultdict
from io import StringIO
from typing import Any, AsyncGenerator, Dict, List, Optional

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
    _extract_filename_from_url,
    extract_fluxnet_filename_metadata,
)

logger = logging.getLogger(__name__)

# TERN data endpoints
TERN_BASE_URL = "https://dap.tern.org.au/thredds/fileServer/ecosystem_process/fluxnet/"
TERN_BIF_METADATA_URL = f"{TERN_BASE_URL}BIF_all_sites.csv"
TERN_PRODUCT_METADATA_URL = f"{TERN_BASE_URL}TERN_THREDDS_catalogue.csv"

# BIF file column names
BIF_COLUMNS = ["SITE_ID", "GROUP_ID", "VARIABLE_GROUP", "VARIABLE", "DATAVALUE"]


def _is_newer_product(
    current_version: tuple[int, ...],
    current_run: int,
    latest_version: Optional[tuple[int, ...]],
    latest_run: Optional[int],
) -> bool:
    """
    Determine if a product is newer than the current latest product.

    Comparison logic:
    1. Compare major version numbers first (e.g., v2 > v1)
    2. If major versions equal, compare minor versions (e.g., v1.3 > v1.2)
    3. Continue for all version parts (patch, etc.)
    4. If all version parts equal, compare run numbers (e.g., r2 > r1)

    Args:
        current_version: Version tuple of current product (e.g., (1, 3) for v1.3)
        current_run: Run number of current product (e.g., 2 for r2)
        latest_version: Version tuple of latest product found so far (None if first)
        latest_run: Run number of latest product found so far (None if first)

    Returns:
        True if current product is newer than latest, False otherwise

    Examples:
        >>> _is_newer_product((1, 3), 1, (1, 2), 1)  # v1.3 > v1.2
        True
        >>> _is_newer_product((1, 3), 2, (1, 3), 1)  # v1.3.r2 > v1.3.r1
        True
        >>> _is_newer_product((2, 0), 1, (1, 9), 1)  # v2.0 > v1.9
        True
    """
    # If no latest product exists yet, current is always newer
    if latest_version is None or latest_run is None:
        return True

    # FLUXNET versions currently follow a 2-part pattern (major.minor)
    # e.g., v1.3, v2.0, etc. Compare each part explicitly.

    # Compare major version (index 0)
    if len(current_version) > 0 and len(latest_version) > 0:
        current_major = current_version[0]
        latest_major = latest_version[0]

        if current_major > latest_major:
            return True
        elif current_major < latest_major:
            return False
        # If major versions equal, continue to minor

    # Compare minor version (index 1) if both versions have it
    if len(current_version) > 1 and len(latest_version) > 1:
        current_minor = current_version[1]
        latest_minor = latest_version[1]

        if current_minor > latest_minor:
            return True
        elif current_minor < latest_minor:
            return False
        # If minor versions equal, continue to run number

    # All version parts are equal, compare run numbers
    return current_run > latest_run


class BIFParser:
    """
    Parser for BADM Interchange Format (BIF) files.

    BIF files contain BADM metadata in a CSV format with columns:
    SITE_ID, GROUP_ID, VARIABLE_GROUP, VARIABLE, DATAVALUE

    The GROUP_ID identifies related elements (e.g., all fields for one team member).
    """

    @staticmethod
    def parse_bif_content(content: str) -> Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]:
        """
        Parse BIF file content into structured metadata.

        Args:
            content: BIF file content as string

        Returns:
            Nested dictionary structure:
            {
                site_id: {
                    group_id: {
                        variable_group: [
                            {variable: datavalue, ...},
                            ...
                        ]
                    }
                }
            }
        """
        reader = csv.DictReader(StringIO(content))

        # Validate header
        if not reader.fieldnames or set(reader.fieldnames) != set(BIF_COLUMNS):
            raise ValueError(f"Invalid BIF file format. Expected columns: {BIF_COLUMNS}, got: {reader.fieldnames}")

        # Structure: site_id -> group_id -> variable_group -> list of {variable: datavalue}
        parsed_data: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for row in reader:
            site_id = row["SITE_ID"]
            group_id = row["GROUP_ID"]
            variable_group = row["VARIABLE_GROUP"]
            variable = row["VARIABLE"]
            datavalue = row["DATAVALUE"]

            # Combine site_id with group_id to ensure uniqueness across sites
            # This handles the case where GROUP_IDs might only be unique within a site
            unique_group_key = f"{site_id}_{group_id}"

            parsed_data[site_id][unique_group_key][variable_group].append({variable: datavalue})

        return parsed_data

    @staticmethod
    def extract_site_metadata(  # noqa: C901
        site_id: str, site_data: Dict[str, Dict[str, List[Dict[str, str]]]]
    ) -> Dict[str, Any]:
        """
        Extract structured metadata for a single site from parsed BIF data.

        Args:
            site_id: Site identifier
            site_data: Parsed BIF data for this site (group_id -> variable_group -> data)

        Returns:
            Dictionary with structured site metadata including:
            - site_name
            - location_lat, location_long
            - igbp
            - network (list)
            - team_members (list of dicts)
            - utc_offset
        """
        metadata: Dict[str, Any] = {
            "site_id": site_id,
            "site_name": "",
            "location_lat": 0.0,
            "location_long": 0.0,
            "igbp": "UNK",
            "network": [],
            "team_members": [],
            "utc_offset": None,
        }

        # Iterate through all groups for this site
        for group_id, group_data in site_data.items():
            # Process HEADER group
            if "HEADER" in group_data:
                for item in group_data["HEADER"]:
                    if "SITE_NAME" in item:
                        metadata["site_name"] = item["SITE_NAME"]

            # Process LOCATION group
            if "LOCATION" in group_data:
                for item in group_data["LOCATION"]:
                    # Process latitude and longitude fields
                    for field_name, metadata_key, display_name in [
                        ("LOCATION_LAT", "location_lat", "latitude"),
                        ("LOCATION_LONG", "location_long", "longitude"),
                    ]:
                        if field_name in item:
                            try:
                                metadata[metadata_key] = float(item[field_name])
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid {display_name} for site {site_id}: {item[field_name]}")

            # Process IGBP group
            if "IGBP" in group_data:
                for item in group_data["IGBP"]:
                    if "IGBP" in item:
                        metadata["igbp"] = item["IGBP"]

            # Process NETWORK group (can have multiple entries)
            if "NETWORK" in group_data:
                for item in group_data["NETWORK"]:
                    network = item.get("NETWORK")
                    if network and network not in metadata["network"]:
                        metadata["network"].append(network)

            # Process TEAM_MEMBER group (grouped by GROUP_ID)
            # Note: A group can contain multiple team members (rows with same GROUP_ID)
            # We need to group by each occurrence of TEAM_MEMBER_NAME to separate individuals
            if "TEAM_MEMBER" in group_data:
                current_member: Dict[str, str] = {}
                for item in group_data["TEAM_MEMBER"]:
                    # When we encounter a name, it signals a new team member
                    if "TEAM_MEMBER_NAME" in item:
                        # Save previous member if exists
                        if current_member.get("name"):
                            metadata["team_members"].append(current_member)
                        # Start new member
                        current_member = {"name": item["TEAM_MEMBER_NAME"], "role": "", "email": ""}
                    elif "TEAM_MEMBER_ROLE" in item:
                        if current_member:
                            current_member["role"] = item["TEAM_MEMBER_ROLE"]
                    elif "TEAM_MEMBER_EMAIL" in item:
                        if current_member:
                            current_member["email"] = item["TEAM_MEMBER_EMAIL"]

                # Add the last team member
                if current_member.get("name"):
                    metadata["team_members"].append(current_member)

            # Process UTC_OFFSET group
            if "UTC_OFFSET" in group_data:
                for item in group_data["UTC_OFFSET"]:
                    if "UTC_OFFSET" in item:
                        try:
                            metadata["utc_offset"] = float(item["UTC_OFFSET"])
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid UTC offset for site {site_id}: {item['UTC_OFFSET']}")

        return metadata


class TERNPlugin(DataHubPlugin):
    """TERN data hub plugin implementation."""

    @property
    def name(self) -> str:
        return __name__.split(".")[-1]

    @property
    def display_name(self) -> str:
        return "TERN"

    @async_to_sync_generator
    async def get_sites(self, **filters: Any) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Get TERN sites with FLUXNET data.

        This method:
        1. Fetches BIF file containing BADM Site General Information
        2. Fetches FLUXNET product metadata file
        3. Combines the data to yield FluxnetDatasetMetadata objects

        Args:
            **filters: Optional filters (not used in this implementation)

        Yields:
            FluxnetDatasetMetadata: Site metadata objects
        """
        logger.info("Fetching TERN sites...")

        try:
            # Fetch BIF metadata
            bif_metadata = await self._fetch_bif_metadata()

            # Fetch product metadata
            product_metadata = await self._fetch_product_metadata()

            # Combine and yield results
            async for site_data in self._combine_metadata(bif_metadata, product_metadata):
                await asyncio.sleep(0.001)  # Yield control to event loop
                yield site_data

        except Exception as e:
            logger.exception("Failed to retrieve TERN data: %s", e)
            raise PluginError(self.name, f"Failed to retrieve data: {e}", original_error=e)

    async def _fetch_bif_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch and parse BIF metadata file.

        Returns:
            Dictionary mapping site_id to parsed site metadata

        Raises:
            PluginError: If fetching or parsing fails
        """
        logger.info(f"Fetching BIF metadata from {TERN_BIF_METADATA_URL}")

        try:
            async with self._session_request("GET", TERN_BIF_METADATA_URL) as response:
                content = await response.text()

                # Parse BIF content
                parser = BIFParser()
                parsed_data = parser.parse_bif_content(content)

                # Extract structured metadata for each site
                site_metadata = {}
                for site_id, site_data in parsed_data.items():
                    site_metadata[site_id] = parser.extract_site_metadata(site_id, site_data)

                logger.info(f"Successfully parsed BIF metadata for {len(site_metadata)} sites")
                return site_metadata

        except PluginError:
            raise
        except Exception as e:
            logger.error(f"Error fetching BIF metadata: {e}")
            raise PluginError(self.name, f"Failed to fetch BIF metadata: {e}", original_error=e)

    async def _fetch_product_metadata(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch FLUXNET product metadata file.

        File format:
        SITE_ID,PRODUCT_URL,PRODUCT_ID,PRODUCT_CITATION
        AU-Lox,https://...,doi:...,Citation text...

        Returns:
            Dictionary mapping site_id to list of all available products for that site.
            Selection of the best product happens later in _combine_metadata.

        Raises:
            PluginError: If fetching or parsing fails
        """
        logger.info(f"Fetching product metadata from {TERN_PRODUCT_METADATA_URL}")

        try:
            async with self._session_request("GET", TERN_PRODUCT_METADATA_URL) as response:
                content = await response.text()

                # Parse product metadata (returns all products per site, no selection yet)
                product_data = self._parse_products(content)

                logger.info(f"Successfully parsed product metadata for {len(product_data)} sites")
                return product_data

        except PluginError:
            raise
        except Exception as e:
            logger.error(f"Error fetching product metadata: {e}")
            raise PluginError(self.name, f"Failed to fetch product metadata: {e}", original_error=e)

    @staticmethod
    def _parse_products(content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse product metadata file and group by site.

        File format:
        SITE_ID,PRODUCT_URL,PRODUCT_ID,PRODUCT_CITATION

        Args:
            content: File content as string

        Returns:
            Dictionary mapping site_id to list of all products for that site
        """
        reader = csv.DictReader(StringIO(content))

        # Group products by site_id
        products_by_site: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for row in reader:
            site_id = row.get("SITE_ID", "").strip()
            product_url = row.get("PRODUCT_URL", "").strip()
            product_id = row.get("PRODUCT_ID", "").strip()
            product_citation = row.get("PRODUCT_CITATION", "").strip()

            if not site_id or not product_url:
                continue

            products_by_site[site_id].append(
                {
                    "product_url": product_url,
                    "product_id": product_id,
                    "product_citation": product_citation,
                }
            )

        return dict(products_by_site)

    @staticmethod
    def _select_latest_product_version(products: List[Dict[str, Any]], site_id: str) -> Optional[Dict[str, Any]]:
        """
        Select the latest run of the most recent version from a list of products
        and return the product with parsed filename components.

        Selection criteria (in order):
        1. Valid FLUXNET filename format
        2. Highest version number (e.g., v1.3 > v1.2)
        3. Highest run number within that version (e.g., r2 > r1)

        Args:
            products: List of product dictionaries with 'product_url' key
            site_id: Site identifier (for logging)

        Returns:
            Dictionary containing:
            - product: Original product dict
            - filename: Extracted filename
            - product_source_network: Network code from filename
            - oneflux_code_version: Version string
            - first_year: First year of data
            - last_year: Last year of data
            - version_tuple: Parsed version tuple (for comparison)
            - run_num: Run number (for comparison)
            Returns None if no valid products found
        """
        latest_product: Optional[Dict[str, Any]] = None

        for product in products:
            url = product["product_url"]
            filename = _extract_filename_from_url(url)

            # Extract all metadata from filename (validates format internally)
            product_source_network, version, first_year, last_year, run = extract_fluxnet_filename_metadata(filename)

            # Skip if validation failed
            if not version or not run:
                logger.debug(
                    f"Skipping product for {site_id} - filename does not follow standard format "
                    f"(<network_id>_<site_id>_FLUXNET_<year_range>_<version>_<run>.<extension>): {filename}"
                )
                continue

            # Parse version number (e.g., "v1.3" -> (1, 3))
            version_parts = version.lower().replace("v", "").split(".")
            version_tuple = tuple(int(p) for p in version_parts)

            # Parse run number (e.g., "r2" -> 2)
            run_num = int(run.lower().replace("r", ""))

            # Keep track of the latest product found so far
            if _is_newer_product(
                current_version=version_tuple,
                current_run=run_num,
                latest_version=latest_product["version_tuple"] if latest_product else None,
                latest_run=latest_product["run_num"] if latest_product else None,
            ):
                latest_product = {
                    "product": product,
                    "version_tuple": version_tuple,
                    "run_num": run_num,
                    "filename": filename,
                    "product_source_network": product_source_network,
                    "oneflux_code_version": version,
                    "first_year": first_year,
                    "last_year": last_year,
                }

        if latest_product is None:
            logger.debug(
                f"No valid production-ready products found for {site_id} (currently only beta products available)"
            )
            return None

        logger.debug(
            f"Selected product for {site_id}: {latest_product['filename']} "
            f"(version: {latest_product['version_tuple']}, run: {latest_product['run_num']})"
        )

        return latest_product

    async def _combine_metadata(
        self, bif_metadata: Dict[str, Dict[str, Any]], product_metadata: Dict[str, List[Dict[str, Any]]]
    ) -> AsyncGenerator[FluxnetDatasetMetadata, None]:
        """
        Combine BIF metadata and product metadata to create FluxnetDatasetMetadata objects.

        This method selects the best product for each site and extracts filename metadata
        in a single pass.

        Args:
            bif_metadata: Dictionary of site metadata from BIF file
            product_metadata: Dictionary mapping site_id to list of available products

        Yields:
            FluxnetDatasetMetadata objects
        """
        # Find sites that have both BIF and product metadata
        common_sites = set(bif_metadata.keys()) & set(product_metadata.keys())

        if not common_sites:
            logger.warning("No sites found with both BIF and product metadata")
            return

        logger.info(f"Processing {len(common_sites)} sites with complete metadata")

        for site_id in common_sites:
            try:
                site_meta = bif_metadata[site_id]
                products_list = product_metadata[site_id]

                # Select best product and extract metadata (single extraction point!)
                latest_product = self._select_latest_product_version(products_list, site_id)
                if not latest_product:
                    logger.debug(f"No valid product found for site {site_id}, skipping")
                    continue

                # Extract metadata from the selection result
                selected_product = latest_product.get("product", {})
                filename = latest_product.get("filename", "")
                product_source_network = latest_product.get("product_source_network", "")
                oneflux_code_version = latest_product.get("oneflux_code_version", "")
                first_year = latest_product.get("first_year", 0)
                last_year = latest_product.get("last_year", 0)

                # Build BadmSiteGeneralInfo
                team_members = []
                for tm_data in site_meta.get("team_members", []):
                    try:
                        team_member = TeamMember(
                            team_member_name=tm_data.get("name", ""),
                            team_member_role=tm_data.get("role", ""),
                            team_member_email=tm_data.get("email", ""),
                        )
                        team_members.append(team_member)
                    except Exception as e:
                        logger.warning(f"Error parsing team member for site {site_id}: {e}")
                        continue

                site_info = BadmSiteGeneralInfo(
                    site_id=site_id,
                    site_name=site_meta.get("site_name", ""),
                    data_hub="TERN",
                    location_lat=site_meta.get("location_lat", 0.0),
                    location_long=site_meta.get("location_long", 0.0),
                    igbp=site_meta.get("igbp", "UNK"),
                    network=site_meta.get("network", []),
                    group_team_member=team_members,
                )

                # Get citation
                product_citation = selected_product.get("product_citation", "")

                # Skip site if citation is not available
                if not product_citation:
                    logger.warning(f"Skipping site {site_id} - no citation available. Please contact TERN support.")
                    continue

                product_data = DataFluxnetProduct(
                    first_year=first_year,
                    last_year=last_year,
                    download_link=selected_product.get("product_url", ""),
                    product_citation=product_citation,
                    product_id=selected_product.get("product_id", ""),
                    oneflux_code_version=oneflux_code_version,
                    product_source_network=product_source_network,
                    fluxnet_product_name=filename,
                )

                metadata = FluxnetDatasetMetadata(site_info=site_info, product_data=product_data)

                yield metadata

            except Exception as e:
                logger.warning(f"Error processing site {site_id}: {e}. Skipping this site.")
                continue


# Auto-register the plugin
from fluxnet_shuttle.core.registry import registry

registry.register(TERNPlugin)
