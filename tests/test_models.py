"""
Tests for Pydantic Schema Models
===============================

This module contains comprehensive tests for the Pydantic models defined
in the fluxnet_shuttle_service.schema module, ensuring proper validation,
serialization, and error handling for the minimum requirement models.
"""

import pytest
from pydantic import ValidationError

from fluxnet_shuttle_lib.models import BadmSiteGeneralInfo, DataFluxnetProduct, FluxnetDatasetMetadata


# Fixtures for minimum requirement models
@pytest.fixture
def sample_site_info():
    """Fixture providing sample site general information."""
    return BadmSiteGeneralInfo(
        site_id="US-ARc",
        network="AmeriFlux",
        location_lat=68.1396,
        location_long=-149.5892,
        igbp="WET",
    )


@pytest.fixture
def sample_product_data():
    """Fixture providing sample product data."""
    return DataFluxnetProduct(first_year=2018, last_year=2023, download_link="https://example.com/dataset.zip")


@pytest.fixture
def sample_metadata(sample_site_info, sample_product_data):
    """Fixture providing sample complete metadata."""
    return FluxnetDatasetMetadata(
        site_info=sample_site_info,
        product_data=sample_product_data,
    )


# Tests for BadmSiteGeneralInfo
def test_badm_site_general_info_valid_creation(sample_site_info):
    """Test creating a valid BadmSiteGeneralInfo instance."""
    assert sample_site_info.site_id == "US-ARc"
    assert sample_site_info.network == "AmeriFlux"
    assert sample_site_info.location_lat == 68.1396
    assert sample_site_info.location_long == -149.5892
    assert sample_site_info.igbp == "WET"


def test_badm_site_general_info_site_id_validation():
    """Test site ID format validation."""
    # Valid site ID formats
    valid_site_ids = ["US-ARc", "IT-Niv", "FR-Hes", "DE-Hai"]
    for site_id in valid_site_ids:
        site_info = BadmSiteGeneralInfo(
            site_id=site_id,
            network="AmeriFlux",
            location_lat=45.0,
            location_long=2.0,
            igbp="ENF",
        )
        assert site_info.site_id == site_id

    # Invalid site ID formats
    invalid_site_ids = ["INVALID", "US_ARc", "USARc", "US-", "-ARc", "123-ARc"]
    for site_id in invalid_site_ids:
        with pytest.raises(ValidationError):
            BadmSiteGeneralInfo(
                site_id=site_id,
                network="AmeriFlux",
                location_lat=45.0,
                location_long=2.0,
                igbp="ENF",
            )


def test_badm_site_general_info_latitude_validation():
    """Test latitude range validation."""
    # Valid latitudes
    valid_lats = [-90.0, -45.5, 0.0, 45.5, 90.0]
    for lat in valid_lats:
        site_info = BadmSiteGeneralInfo(
            site_id="US-ARc",
            network="AmeriFlux",
            location_lat=lat,
            location_long=0.0,
            igbp="WET",
        )
        assert site_info.location_lat == lat

    # Invalid latitudes
    invalid_lats = [-90.1, -91.0, 90.1, 91.0, 180.0]
    for lat in invalid_lats:
        with pytest.raises(ValidationError):
            BadmSiteGeneralInfo(
                site_id="US-ARc",
                network="AmeriFlux",
                location_lat=lat,
                location_long=0.0,
                igbp="WET",
            )


def test_badm_site_general_info_longitude_validation():
    """Test longitude range validation."""
    # Valid longitudes
    valid_lons = [-180.0, -90.5, 0.0, 90.5, 180.0]
    for lon in valid_lons:
        site_info = BadmSiteGeneralInfo(
            site_id="US-ARc",
            network="AmeriFlux",
            location_lat=0.0,
            location_long=lon,
            igbp="WET",
        )
        assert site_info.location_long == lon

    # Invalid longitudes
    invalid_lons = [-180.1, -181.0, 180.1, 181.0, 360.0]
    for lon in invalid_lons:
        with pytest.raises(ValidationError):
            BadmSiteGeneralInfo(
                site_id="US-ARc",
                network="AmeriFlux",
                location_lat=0.0,
                location_long=lon,
                igbp="WET",
            )


def test_badm_site_general_info_required_fields():
    """Test that all required fields are enforced."""
    with pytest.raises(ValidationError):
        BadmSiteGeneralInfo()

    with pytest.raises(ValidationError):
        BadmSiteGeneralInfo(site_id="US-ARc")

    with pytest.raises(ValidationError):
        BadmSiteGeneralInfo(site_id="US-ARc", network="AmeriFlux")


# Tests for DataFluxnetProduct
def test_data_fluxnet_product_valid_creation(sample_product_data):
    """Test creating a valid DataFluxnetProduct instance."""
    assert sample_product_data.first_year == 2018
    assert sample_product_data.last_year == 2023
    assert str(sample_product_data.download_link) == "https://example.com/dataset.zip"


def test_data_fluxnet_product_year_validation():
    """Test year range and value validation."""
    # Valid year ranges
    valid_ranges = [(2000, 2005), (2018, 2023), (1900, 1905), (2095, 2100)]
    for first_year, last_year in valid_ranges:
        product = DataFluxnetProduct(
            first_year=first_year,
            last_year=last_year,
            download_link="https://example.com/dataset.zip",
        )
        assert product.first_year == first_year
        assert product.last_year == last_year

    # Invalid year values (out of range)
    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=1899,  # Below minimum
            last_year=2000,
            download_link="https://example.com/dataset.zip",
        )

    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=2000,
            last_year=2101,  # Above maximum
            download_link="https://example.com/dataset.zip",
        )

    # Invalid year range (last_year < first_year)
    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=2023,
            last_year=2020,
            download_link="https://example.com/dataset.zip",
        )


def test_data_fluxnet_product_url_validation():
    """Test download link URL validation."""
    # Valid URLs
    valid_urls = [
        "https://example.com/dataset.zip",
        "http://ameriflux.lbl.gov/data/dataset.tar.gz",
        "https://icos-cp.eu/data/file.nc",
    ]
    for url in valid_urls:
        product = DataFluxnetProduct(first_year=2018, last_year=2023, download_link=url)
        assert str(product.download_link) == url

    # Invalid URLs
    invalid_urls = [
        "not-a-url",
        "just-text",
        "www.example.com",  # Missing protocol
        "ftp://server.com/data.zip",  # FTP not supported by HttpUrl
        "",
    ]
    for url in invalid_urls:
        with pytest.raises(ValidationError):
            DataFluxnetProduct(first_year=2018, last_year=2023, download_link=url)


# Tests for FluxnetDatasetMetadata
def test_fluxnet_dataset_metadata_valid_creation(sample_metadata, sample_site_info, sample_product_data):
    """Test creating a valid FluxnetDatasetMetadata instance."""
    assert sample_metadata.site_info == sample_site_info
    assert sample_metadata.product_data == sample_product_data


def test_fluxnet_dataset_metadata_nested_validation():
    """Test that nested model validation works."""
    # Invalid site_info should raise ValidationError
    with pytest.raises(ValidationError):
        FluxnetDatasetMetadata(
            site_info=BadmSiteGeneralInfo(
                site_id="INVALID",  # Bad format
                network="AmeriFlux",
                location_lat=68.1396,
                location_long=-149.5892,
                igbp="WET",
            ),
            product_data=DataFluxnetProduct(
                first_year=2018,
                last_year=2023,
                download_link="https://example.com/dataset.zip",
            ),
        )

    # Invalid product_data should raise ValidationError
    with pytest.raises(ValidationError):
        FluxnetDatasetMetadata(
            site_info=BadmSiteGeneralInfo(
                site_id="US-ARc",
                network="AmeriFlux",
                location_lat=68.1396,
                location_long=-149.5892,
                igbp="WET",
            ),
            product_data=DataFluxnetProduct(
                first_year=2023,
                last_year=2020,  # Invalid range
                download_link="https://example.com/dataset.zip",
            ),
        )


# Tests for JSON serialization and deserialization
def test_site_info_json_serialization(sample_site_info):
    """Test JSON serialization of BadmSiteGeneralInfo."""
    json_data = sample_site_info.model_dump()
    assert json_data["site_id"] == "US-ARc"
    assert json_data["network"] == "AmeriFlux"
    assert json_data["location_lat"] == 68.1396
    assert json_data["location_long"] == -149.5892
    assert json_data["igbp"] == "WET"

    # Test round-trip
    reconstructed = BadmSiteGeneralInfo(**json_data)
    assert reconstructed == sample_site_info


def test_product_data_json_serialization(sample_product_data):
    """Test JSON serialization of DataFluxnetProduct."""
    json_data = sample_product_data.model_dump(mode="json")
    assert json_data["first_year"] == 2018
    assert json_data["last_year"] == 2023
    assert json_data["download_link"] == "https://example.com/dataset.zip"

    # Test round-trip
    reconstructed = DataFluxnetProduct(**json_data)
    assert reconstructed.first_year == sample_product_data.first_year
    assert reconstructed.last_year == sample_product_data.last_year
    assert reconstructed.download_link == sample_product_data.download_link


def test_metadata_json_serialization(sample_metadata):
    """Test JSON serialization of complete metadata."""
    json_str = sample_metadata.model_dump_json(indent=2)
    json_str_no_spaces = json_str.replace(" ", "").replace("\n", "")
    assert '"site_id":"US-ARc"' in json_str_no_spaces
    assert '"first_year":2018' in json_str_no_spaces
