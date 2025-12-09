"""Test suite for fluxnet_shuttle.plugins.tern module."""

from unittest.mock import AsyncMock, patch

import pytest

from fluxnet_shuttle.core.exceptions import PluginError
from fluxnet_shuttle.plugins import tern


class TestBIFParser:
    """Test cases for BIFParser."""

    def test_parse_bif_content_basic(self):
        """Test basic BIF content parsing."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton Eddy Covariance Site
AU-Lox,8,IGBP,IGBP,DBF
AU-Lox,10,LOCATION,LOCATION_LAT,-34.47035
AU-Lox,10,LOCATION,LOCATION_LONG,140.65512"""

        parser = tern.BIFParser()
        result = parser.parse_bif_content(content)

        assert "AU-Lox" in result
        assert "AU-Lox_6" in result["AU-Lox"]
        assert "AU-Lox_8" in result["AU-Lox"]
        assert "AU-Lox_10" in result["AU-Lox"]

        # Check HEADER group
        assert "HEADER" in result["AU-Lox"]["AU-Lox_6"]
        assert {"SITE_NAME": "Loxton Eddy Covariance Site"} in result["AU-Lox"]["AU-Lox_6"]["HEADER"]

        # Check IGBP group
        assert "IGBP" in result["AU-Lox"]["AU-Lox_8"]
        assert {"IGBP": "DBF"} in result["AU-Lox"]["AU-Lox_8"]["IGBP"]

        # Check LOCATION group
        assert "LOCATION" in result["AU-Lox"]["AU-Lox_10"]
        location_items = result["AU-Lox"]["AU-Lox_10"]["LOCATION"]
        assert {"LOCATION_LAT": "-34.47035"} in location_items
        assert {"LOCATION_LONG": "140.65512"} in location_items

    def test_parse_bif_content_team_members(self):
        """Test parsing team member groups."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_NAME,Robert Stevens
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_ROLE,PI
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_EMAIL,rmstevens.water@gmail.com
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_NAME,Cacilia Ewenz
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_ROLE,Technician
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_EMAIL,cacilia.ewenz@internode.on.net"""

        parser = tern.BIFParser()
        result = parser.parse_bif_content(content)

        # Two different GROUP_IDs for team members
        assert "AU-Lox_16" in result["AU-Lox"]
        assert "AU-Lox_17" in result["AU-Lox"]

        # Check first team member
        team1_items = result["AU-Lox"]["AU-Lox_16"]["TEAM_MEMBER"]
        assert {"TEAM_MEMBER_NAME": "Robert Stevens"} in team1_items
        assert {"TEAM_MEMBER_ROLE": "PI"} in team1_items
        assert {"TEAM_MEMBER_EMAIL": "rmstevens.water@gmail.com"} in team1_items

        # Check second team member
        team2_items = result["AU-Lox"]["AU-Lox_17"]["TEAM_MEMBER"]
        assert {"TEAM_MEMBER_NAME": "Cacilia Ewenz"} in team2_items
        assert {"TEAM_MEMBER_ROLE": "Technician"} in team2_items

    def test_parse_bif_content_multiple_sites(self):
        """Test parsing BIF content with multiple sites."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton Site
AU-Lox,8,IGBP,IGBP,DBF
AU-Cow,6,HEADER,SITE_NAME,Cowombat Site
AU-Cow,8,IGBP,IGBP,EBF"""

        parser = tern.BIFParser()
        result = parser.parse_bif_content(content)

        assert "AU-Lox" in result
        assert "AU-Cow" in result

        # Check that GROUP_IDs are unique per site
        assert "AU-Lox_6" in result["AU-Lox"]
        assert "AU-Cow_6" in result["AU-Cow"]

        # Verify site names are different
        lox_name = result["AU-Lox"]["AU-Lox_6"]["HEADER"][0]["SITE_NAME"]
        cow_name = result["AU-Cow"]["AU-Cow_6"]["HEADER"][0]["SITE_NAME"]
        assert lox_name == "Loxton Site"
        assert cow_name == "Cowombat Site"

    def test_parse_bif_content_invalid_header(self):
        """Test parsing BIF content with invalid header."""
        content = """SITE_ID,GROUP_ID,INVALID_COLUMN,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton"""

        parser = tern.BIFParser()
        with pytest.raises(ValueError, match="Invalid BIF file format"):
            parser.parse_bif_content(content)

    def test_extract_site_metadata_complete(self):
        """Test extracting complete site metadata."""
        # First parse a complete BIF structure
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton Eddy Covariance Site
AU-Lox,8,IGBP,IGBP,DBF
AU-Lox,10,LOCATION,LOCATION_LAT,-34.47035
AU-Lox,10,LOCATION,LOCATION_LONG,140.65512
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_NAME,Robert Stevens
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_ROLE,PI
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_EMAIL,rmstevens.water@gmail.com
AU-Lox,19,UTC_OFFSET,UTC_OFFSET,9.5"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        assert metadata["site_id"] == "AU-Lox"
        assert metadata["site_name"] == "Loxton Eddy Covariance Site"
        assert metadata["location_lat"] == -34.47035
        assert metadata["location_long"] == 140.65512
        assert metadata["igbp"] == "DBF"
        assert metadata["utc_offset"] == 9.5

        # Check team members
        assert len(metadata["team_members"]) == 1
        assert metadata["team_members"][0]["name"] == "Robert Stevens"
        assert metadata["team_members"][0]["role"] == "PI"
        assert metadata["team_members"][0]["email"] == "rmstevens.water@gmail.com"

    def test_extract_site_metadata_multiple_team_members(self):
        """Test extracting metadata with multiple team members."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton Site
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_NAME,Robert Stevens
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_ROLE,PI
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_EMAIL,rmstevens.water@gmail.com
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_NAME,Cacilia Ewenz
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_ROLE,Technician
AU-Lox,17,TEAM_MEMBER,TEAM_MEMBER_EMAIL,cacilia.ewenz@internode.on.net"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        assert len(metadata["team_members"]) == 2
        assert metadata["team_members"][0]["name"] == "Robert Stevens"
        assert metadata["team_members"][0]["role"] == "PI"
        assert metadata["team_members"][1]["name"] == "Cacilia Ewenz"
        assert metadata["team_members"][1]["role"] == "Technician"

    def test_extract_site_metadata_invalid_coordinates(self):
        """Test handling of invalid coordinate values."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,10,LOCATION,LOCATION_LAT,invalid_lat
AU-Lox,10,LOCATION,LOCATION_LONG,invalid_long"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        # Should default to 0.0 for invalid values
        assert metadata["location_lat"] == 0.0
        assert metadata["location_long"] == 0.0

    def test_extract_site_metadata_network_list(self):
        """Test extracting network list."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,20,NETWORK,NETWORK,TERN
AU-Lox,21,NETWORK,NETWORK,OzFlux"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        assert "TERN" in metadata["network"]
        assert "OzFlux" in metadata["network"]
        assert len(metadata["network"]) == 2


class TestTERNPlugin:
    """Test cases for TERNPlugin."""

    def test_plugin_properties(self):
        """Test TERNPlugin properties."""
        plugin = tern.TERNPlugin()

        assert plugin.name == "tern"
        assert plugin.display_name == "TERN"

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_success(self, mock_fetch_product, mock_fetch_bif):
        """Test successful retrieval of sites."""
        # Mock BIF metadata
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Eddy Covariance Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": ["TERN"],
                "team_members": [
                    {
                        "name": "Robert Stevens",
                        "role": "PI",
                        "email": "rmstevens.water@gmail.com",
                    }
                ],
                "utc_offset": 9.5,
            }
        }

        # Mock product metadata
        mock_fetch_product.return_value = {
            "AU-Lox": [
                {
                    "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip",
                    "product_id": "doi:10.1234/tern.au-lox",
                    "product_citation": "Stevens et al. (2020). Loxton Site FLUXNET Data.",
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 1
        assert sites[0].site_info.site_id == "AU-Lox"
        assert sites[0].site_info.site_name == "Loxton Eddy Covariance Site"
        assert sites[0].site_info.data_hub == "TERN"
        assert sites[0].site_info.location_lat == -34.47035
        assert sites[0].site_info.location_long == 140.65512
        assert sites[0].site_info.igbp == "DBF"
        assert "TERN" in sites[0].site_info.network
        assert len(sites[0].site_info.group_team_member) == 1
        assert sites[0].site_info.group_team_member[0].team_member_name == "Robert Stevens"

        # Check product data
        assert sites[0].product_data.product_id == "doi:10.1234/tern.au-lox"
        assert sites[0].product_data.product_citation == "Stevens et al. (2020). Loxton Site FLUXNET Data."
        assert sites[0].product_data.fluxnet_product_name == "TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip"

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_no_common_sites(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites when BIF and product metadata have no common sites."""
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                "team_members": [],
                "utc_offset": None,
            }
        }

        mock_fetch_product.return_value = {
            "AU-Cow": [
                {  # Different site
                    "product_url": "https://data.tern.org.au/TERN_AU-Cow_FLUXNET_2010-2020_v1_r1.zip",
                    "product_id": "doi:10.1234/tern.au-cow",
                    "product_citation": "Citation for AU-Cow",
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_invalid_filename_format(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites skips sites with invalid filename format.

        When _fetch_product_metadata is called, it internally uses _parse_and_select_products
        which filters out invalid filenames. So if a site has only invalid filenames,
        _fetch_product_metadata won't return an entry for that site.
        """
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                "team_members": [],
                "utc_offset": None,
            }
        }

        # No products returned because all filenames were invalid
        # (filtered out by _parse_and_select_products)
        mock_fetch_product.return_value = {}

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        # Should skip the site due to no valid products
        assert len(sites) == 0

    def test_parse_products(self):
        """Test parsing product metadata file into lists per site."""
        content = """SITE_ID,PRODUCT_URL,PRODUCT_ID,PRODUCT_CITATION
AU-Lox,https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip,doi:10.1234/tern.au-lox,Stevens et al. (2020)
AU-Cow,https://data.tern.org.au/TERN_AU-Cow_FLUXNET_2010-2020_v1_r1.zip,doi:10.1234/tern.au-cow,Jones et al. (2021)"""

        result = tern.TERNPlugin._parse_products(content)

        assert "AU-Lox" in result
        assert "AU-Cow" in result

        # Now returns lists of products
        assert len(result["AU-Lox"]) == 1
        assert result["AU-Lox"][0]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip"
        assert result["AU-Lox"][0]["product_id"] == "doi:10.1234/tern.au-lox"
        assert result["AU-Lox"][0]["product_citation"] == "Stevens et al. (2020)"

        assert len(result["AU-Cow"]) == 1
        assert result["AU-Cow"][0]["product_url"] == "https://data.tern.org.au/TERN_AU-Cow_FLUXNET_2010-2020_v1_r1.zip"
        assert result["AU-Cow"][0]["product_id"] == "doi:10.1234/tern.au-cow"
        assert result["AU-Cow"][0]["product_citation"] == "Jones et al. (2021)"

    def test_select_latest_product_latest_version(self):
        """Test selecting the latest version when multiple versions exist."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.2_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.2",
                "product_citation": "Citation v1.2",
            },
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.3",
                "product_citation": "Citation v1.3",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        assert result is not None
        assert result["product"]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip"
        assert result["filename"] == "TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip"
        assert result["product_source_network"] == "TERN"
        assert result["oneflux_code_version"] == "v1.3"
        assert result["first_year"] == 2008
        assert result["last_year"] == 2020

    def test_select_latest_product_latest_run(self):
        """Test selecting the latest run when multiple runs exist for the same version."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.r1",
                "product_citation": "Citation r1",
            },
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r2.zip",
                "product_id": "doi:10.1234/tern.au-lox.r2",
                "product_citation": "Citation r2",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        assert result is not None
        assert result["product"]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r2.zip"
        assert result["filename"] == "TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r2.zip"

    def test_select_latest_product_newer_version_first(self):
        """Test when newer version comes before older version in the list."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.3",
                "product_citation": "Citation v1.3",
            },
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.2_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.2",
                "product_citation": "Citation v1.2",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        assert result is not None
        assert result["product"]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_r1.zip"

    def test_select_latest_product_major_version_comparison(self):
        """Test comparison when major versions differ (older major version first)."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.9_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.9",
                "product_citation": "Citation v1.9",
            },
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v2.0_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v2.0",
                "product_citation": "Citation v2.0",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        assert result is not None
        assert result["product"]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v2.0_r1.zip"
        assert result["oneflux_code_version"] == "v2.0"

    def test_select_latest_product_major_version_newer_first(self):
        """Test comparison when newer major version is encountered first."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v2.0_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v2.0",
                "product_citation": "Citation v2.0",
            },
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.9_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox.v1.9",
                "product_citation": "Citation v1.9",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        assert result is not None
        assert result["product"]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v2.0_r1.zip"
        assert result["oneflux_code_version"] == "v2.0"

    def test_select_latest_product_skip_beta(self):
        """Test that beta products are skipped."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rbeta.zip",
                "product_id": "doi:10.1234/tern.au-lox.beta",
                "product_citation": "Citation beta",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # Should return None since only beta product is available
        assert result is None

    def test_select_latest_product_skip_rbeta_in_filename(self):
        """Test that products with 'rbeta' in run are skipped after version extraction."""
        products = [
            {
                # This has rbeta in the extracted run field
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rbeta.zip",
                "product_id": "doi:10.1234/tern.au-lox",
                "product_citation": "Citation",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # Should skip the rbeta run
        assert result is None

    def test_select_latest_product_all_error_paths(self):
        """Test all error/warning paths in _select_latest_product_version."""
        products = [
            # This one has invalid version format (non-numeric)
            # Filtered out because version does not match regex pattern
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_vABC.DEF_r1.zip",
                "product_id": "doi:1",
                "product_citation": "Citation 1",
            },
            # This one has invalid run format (non-numeric) - filtered out because version does not match regex pattern
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rXYZ.zip",
                "product_id": "doi:2",
                "product_citation": "Citation 2",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # All products should be filtered out
        assert result is None

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_bif_metadata_success(self, mock_session_request):
        """Test successful BIF metadata fetch."""
        bif_content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,6,HEADER,SITE_NAME,Loxton Site
AU-Lox,8,IGBP,IGBP,DBF
AU-Lox,10,LOCATION,LOCATION_LAT,-34.47035
AU-Lox,10,LOCATION,LOCATION_LONG,140.65512"""

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value=bif_content)
        mock_session_request.return_value.__aenter__.return_value = mock_response

        plugin = tern.TERNPlugin()
        result = await plugin._fetch_bif_metadata()

        assert "AU-Lox" in result
        assert result["AU-Lox"]["site_name"] == "Loxton Site"
        assert result["AU-Lox"]["igbp"] == "DBF"
        assert result["AU-Lox"]["location_lat"] == -34.47035
        assert result["AU-Lox"]["location_long"] == 140.65512

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_product_metadata_success(self, mock_session_request):
        """Test successful product metadata fetch."""
        product_content = """SITE_ID,PRODUCT_URL,PRODUCT_ID,PRODUCT_CITATION
AU-Lox,https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip,doi:10.1234/tern.au-lox,Citation text"""

        mock_response = AsyncMock()
        mock_response.text = AsyncMock(return_value=product_content)
        mock_session_request.return_value.__aenter__.return_value = mock_response

        plugin = tern.TERNPlugin()
        result = await plugin._fetch_product_metadata()

        assert "AU-Lox" in result
        # Now returns a list of products
        assert len(result["AU-Lox"]) == 1
        assert result["AU-Lox"][0]["product_url"] == "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip"
        assert result["AU-Lox"][0]["product_id"] == "doi:10.1234/tern.au-lox"

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_bif_metadata_failure(self, mock_session_request):
        """Test BIF metadata fetch failure."""
        mock_session_request.side_effect = PluginError("tern", "Network error")

        plugin = tern.TERNPlugin()
        with pytest.raises(PluginError):
            await plugin._fetch_bif_metadata()

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_bif_metadata_generic_exception(self, mock_session_request):
        """Test BIF metadata fetch with generic exception."""
        mock_session_request.side_effect = ValueError("Unexpected error")

        plugin = tern.TERNPlugin()
        with pytest.raises(PluginError, match="Failed to fetch BIF metadata"):
            await plugin._fetch_bif_metadata()

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_product_metadata_generic_exception(self, mock_session_request):
        """Test product metadata fetch with generic exception."""
        mock_session_request.side_effect = ValueError("Unexpected error")

        plugin = tern.TERNPlugin()
        with pytest.raises(PluginError, match="Failed to fetch product metadata"):
            await plugin._fetch_product_metadata()

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._session_request")
    async def test_fetch_product_metadata_plugin_error(self, mock_session_request):
        """Test product metadata fetch with PluginError (should re-raise)."""
        mock_session_request.side_effect = PluginError("tern", "Connection failed")

        plugin = tern.TERNPlugin()
        with pytest.raises(PluginError, match="Connection failed"):
            await plugin._fetch_product_metadata()

    @pytest.mark.asyncio
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    async def test_get_sites_generic_exception(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites with generic exception."""
        mock_fetch_bif.side_effect = ValueError("Unexpected error")

        plugin = tern.TERNPlugin()
        with pytest.raises(PluginError, match="Failed to retrieve data"):
            # Get the async generator and try to iterate
            gen = plugin.get_sites.__wrapped__(plugin)
            async for _ in gen:
                pass

    def test_extract_site_metadata_invalid_utc_offset(self):
        """Test extracting metadata with invalid UTC offset."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,19,UTC_OFFSET,UTC_OFFSET,invalid_offset"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        # Should default to None for invalid values
        assert metadata["utc_offset"] is None

    def test_extract_site_metadata_team_member_within_group(self):
        """Test extracting team member metadata where multiple fields appear in one group."""
        content = """SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_NAME,First Member
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_ROLE,PI
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_EMAIL,first@example.com
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_NAME,Second Member
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_ROLE,Technician
AU-Lox,16,TEAM_MEMBER,TEAM_MEMBER_EMAIL,second@example.com"""

        parser = tern.BIFParser()
        parsed_data = parser.parse_bif_content(content)
        metadata = parser.extract_site_metadata("AU-Lox", parsed_data["AU-Lox"])

        # Should have two team members from the same group
        assert len(metadata["team_members"]) == 2
        assert metadata["team_members"][0]["name"] == "First Member"
        assert metadata["team_members"][1]["name"] == "Second Member"

    def test_select_latest_product_no_version_match(self):
        """Test selecting product when version/run can't be extracted."""
        products = [
            {
                "product_url": "https://data.tern.org.au/invalid_format.zip",
                "product_id": "doi:10.1234/tern.au-lox",
                "product_citation": "Citation",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # Should return None since version/run couldn't be extracted
        assert result is None

    def test_select_latest_product_invalid_version_format(self):
        """Test selecting product with invalid version format."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_vABC_r1.zip",
                "product_id": "doi:10.1234/tern.au-lox",
                "product_citation": "Citation",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # Should return None due to invalid version format
        assert result is None

    def test_select_latest_product_invalid_run_format(self):
        """Test selecting product with invalid run format."""
        products = [
            {
                "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1.3_rXYZ.zip",
                "product_id": "doi:10.1234/tern.au-lox",
                "product_citation": "Citation",
            },
        ]

        result = tern.TERNPlugin._select_latest_product_version(products, "AU-Lox")

        # Should return None due to invalid run format
        assert result is None

    def test_parse_products_empty_site_id(self):
        """Test parsing products with empty site_id or product_url."""
        content = """SITE_ID,PRODUCT_URL,PRODUCT_ID,PRODUCT_CITATION
,https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip,doi:10.1234/tern.au-lox,Citation
AU-Lox,,doi:10.1234/tern.au-lox2,Citation2"""

        result = tern.TERNPlugin._parse_products(content)

        # Should skip rows with empty site_id or product_url
        assert len(result) == 0

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_invalid_team_member_data(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites with invalid team member data that causes exception."""
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                # Invalid team member data - missing required fields
                "team_members": [{"invalid_field": "value"}],
                "utc_offset": None,
            }
        }

        mock_fetch_product.return_value = {
            "AU-Lox": [
                {
                    "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip",
                    "product_id": "doi:10.1234/tern.au-lox",
                    "product_citation": "Citation text",
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        # Should still yield site but skip the invalid team member
        assert len(sites) == 1
        assert len(sites[0].site_info.group_team_member) == 0

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_no_citation(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites skips sites without citation."""
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                "team_members": [],
                "utc_offset": None,
            }
        }

        # Product with empty citation
        mock_fetch_product.return_value = {
            "AU-Lox": [
                {
                    "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_2008-2020_v1_r1.zip",
                    "product_id": "doi:10.1234/tern.au-lox",
                    "product_citation": "",  # Empty citation
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        # Should skip site due to missing citation
        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_no_year_in_filename(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites with filename missing year range - should skip the site."""
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                "team_members": [],
                "utc_offset": None,
            }
        }

        # Product with filename that doesn't have year range (invalid format)
        mock_fetch_product.return_value = {
            "AU-Lox": [
                {
                    "product_url": "https://data.tern.org.au/TERN_AU-Lox_FLUXNET_NOYEAR_v1_r1.zip",
                    "product_id": "doi:10.1234/tern.au-lox",
                    "product_citation": "Citation text",
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        # Should skip sites with invalid year data (no year range in filename)
        assert len(sites) == 0

    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_bif_metadata")
    @patch("fluxnet_shuttle.plugins.tern.TERNPlugin._fetch_product_metadata")
    def test_get_sites_exception_during_processing(self, mock_fetch_product, mock_fetch_bif):
        """Test get_sites handles exceptions during site processing."""
        mock_fetch_bif.return_value = {
            "AU-Lox": {
                "site_id": "AU-Lox",
                "site_name": "Loxton Site",
                "location_lat": -34.47035,
                "location_long": 140.65512,
                "igbp": "DBF",
                "network": [],
                "team_members": [],
                "utc_offset": None,
            }
        }

        # Return invalid data that will cause an exception
        mock_fetch_product.return_value = {
            "AU-Lox": [
                {
                    "product_url": None,  # This will cause an error
                    "product_id": "doi:10.1234/tern.au-lox",
                    "product_citation": "Citation",
                }
            ]
        }

        plugin = tern.TERNPlugin()
        sites = list(plugin.get_sites())

        # Should skip the site and log warning
        assert len(sites) == 0
