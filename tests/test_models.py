"""
Tests for Pydantic Schema Models
===============================

This module contains comprehensive tests for the Pydantic models defined
in the fluxnet_shuttle_service.schema module, ensuring proper validation,
serialization, and error handling for the minimum requirement models.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from fluxnet_shuttle.models import (
    BadmSiteGeneralInfo,
    DataFluxnetProduct,
    ErrorSummary,
    FluxnetDatasetMetadata,
    PluginErrorDetail,
)


# Fixtures for minimum requirement models
@pytest.fixture
def sample_site_info():
    """Fixture providing sample site general information."""
    return BadmSiteGeneralInfo(
        site_id="US-ARc",
        site_name="Utqiaġvik (Barrow)",
        data_hub="AmeriFlux",
        location_lat=68.1396,
        location_long=-149.5892,
        igbp="WET",
    )


@pytest.fixture
def sample_product_data():
    """Fixture providing sample product data."""
    return DataFluxnetProduct(
        first_year=2018,
        last_year=2023,
        download_link="https://example.com/dataset.zip",
        product_citation="Test citation",
        product_id="test-id-123",
        oneflux_code_version="v1",
        product_source_network="AMF",
        fluxnet_product_name="test_file.zip",
    )


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
    assert sample_site_info.data_hub == "AmeriFlux"
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
            site_name="Test Site",
            data_hub="AmeriFlux",
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
                site_name="Test Site",
                data_hub="AmeriFlux",
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
            site_name="Test Site",
            data_hub="AmeriFlux",
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
                site_name="Test Site",
                data_hub="AmeriFlux",
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
            site_name="Test Site",
            data_hub="AmeriFlux",
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
                site_name="Test Site",
                data_hub="AmeriFlux",
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
        BadmSiteGeneralInfo(site_id="US-ARc", data_hub="AmeriFlux")


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
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="AMF",
            fluxnet_product_name="test_file.zip",
        )
        assert product.first_year == first_year
        assert product.last_year == last_year

    # Invalid year values (out of range)
    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=1899,  # Below minimum
            last_year=2000,
            download_link="https://example.com/dataset.zip",
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="AMF",
            fluxnet_product_name="test_file.zip",
        )

    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=2000,
            last_year=2101,  # Above maximum
            download_link="https://example.com/dataset.zip",
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="AMF",
            fluxnet_product_name="test_file.zip",
        )

    # Invalid year range (last_year < first_year)
    with pytest.raises(ValidationError):
        DataFluxnetProduct(
            first_year=2023,
            last_year=2020,
            download_link="https://example.com/dataset.zip",
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="AMF",
            fluxnet_product_name="test_file.zip",
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
        product = DataFluxnetProduct(
            first_year=2018,
            last_year=2023,
            download_link=url,
            product_citation="Test citation",
            product_id="test-id",
            oneflux_code_version="v1",
            product_source_network="AMF",
            fluxnet_product_name="test_file.zip",
        )
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
            DataFluxnetProduct(
                first_year=2018,
                last_year=2023,
                download_link=url,
                product_citation="Test citation",
                product_id="test-id",
                oneflux_code_version="v1",
                product_source_network="AMF",
                fluxnet_product_name="test_file.zip",
            )


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
                site_name="Test Site",
                data_hub="AmeriFlux",
                location_lat=68.1396,
                location_long=-149.5892,
                igbp="WET",
            ),
            product_data=DataFluxnetProduct(
                first_year=2018,
                last_year=2023,
                download_link="https://example.com/dataset.zip",
                product_citation="Test citation",
                product_id="test-id",
                oneflux_code_version="v1",
                product_source_network="AMF",
                fluxnet_product_name="test_file.zip",
            ),
        )

    # Invalid product_data should raise ValidationError
    with pytest.raises(ValidationError):
        FluxnetDatasetMetadata(
            site_info=BadmSiteGeneralInfo(
                site_id="US-ARc",
                site_name="Test Site",
                data_hub="AmeriFlux",
                location_lat=68.1396,
                location_long=-149.5892,
                igbp="WET",
            ),
            product_data=DataFluxnetProduct(
                first_year=2023,
                last_year=2020,  # Invalid range
                download_link="https://example.com/dataset.zip",
                product_citation="Test citation",
                product_id="test-id",
                oneflux_code_version="v1",
                product_source_network="AMF",
                fluxnet_product_name="test_file.zip",
            ),
        )


# Tests for JSON serialization and deserialization
def test_site_info_json_serialization(sample_site_info):
    """Test JSON serialization of BadmSiteGeneralInfo."""
    json_data = sample_site_info.model_dump()
    assert json_data["site_id"] == "US-ARc"
    assert json_data["data_hub"] == "AmeriFlux"
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


# Tests for PluginErrorDetail
def test_plugin_error_detail_valid_creation():
    """Test creating a valid PluginErrorDetail instance."""
    timestamp = datetime.now().isoformat()
    error_detail = PluginErrorDetail(
        data_hub="ameriflux",
        operation="get_sites",
        error="Connection timeout",
        timestamp=timestamp,
    )
    assert error_detail.data_hub == "ameriflux"
    assert error_detail.operation == "get_sites"
    assert error_detail.error == "Connection timeout"
    assert error_detail.timestamp == timestamp


def test_plugin_error_detail_timestamp_validation():
    """Test timestamp format validation."""
    # Valid ISO format timestamps
    valid_timestamps = [
        "2025-10-16T12:00:00",
        "2025-10-16T12:00:00.123456",
        "2025-10-16T12:00:00+00:00",
        "2025-10-16T12:00:00.123456+00:00",
        datetime.now().isoformat(),
    ]
    for timestamp in valid_timestamps:
        error_detail = PluginErrorDetail(
            data_hub="ameriflux",
            operation="get_sites",
            error="Test error",
            timestamp=timestamp,
        )
        assert error_detail.timestamp == timestamp

    # Invalid timestamp formats
    invalid_timestamps = [
        "not-a-timestamp",
        "12:00:00",  # Missing date component
        "2025/10/16 12:00:00",  # Wrong separator
        "invalid-date-format",
    ]
    for timestamp in invalid_timestamps:
        with pytest.raises(ValidationError) as exc_info:
            PluginErrorDetail(
                data_hub="ameriflux",
                operation="get_sites",
                error="Test error",
                timestamp=timestamp,
            )
        assert "timestamp must be in ISO format" in str(exc_info.value)

    # Empty string should fail with min_length validation
    with pytest.raises(ValidationError):
        PluginErrorDetail(
            data_hub="ameriflux",
            operation="get_sites",
            error="Test error",
            timestamp="",
        )


def test_plugin_error_detail_required_fields():
    """Test that all required fields are enforced."""
    with pytest.raises(ValidationError):
        PluginErrorDetail()

    with pytest.raises(ValidationError):
        PluginErrorDetail(data_hub="ameriflux")

    with pytest.raises(ValidationError):
        PluginErrorDetail(data_hub="ameriflux", operation="get_sites")


def test_plugin_error_detail_min_length_validation():
    """Test minimum length validation for string fields."""
    timestamp = datetime.now().isoformat()

    # Empty strings should fail
    with pytest.raises(ValidationError):
        PluginErrorDetail(data_hub="", operation="get_sites", error="Test error", timestamp=timestamp)

    with pytest.raises(ValidationError):
        PluginErrorDetail(data_hub="ameriflux", operation="", error="Test error", timestamp=timestamp)

    with pytest.raises(ValidationError):
        PluginErrorDetail(data_hub="ameriflux", operation="get_sites", error="", timestamp=timestamp)


# Tests for ErrorSummary
def test_error_summary_valid_creation():
    """Test creating a valid ErrorSummary instance."""
    timestamp = datetime.now().isoformat()
    error_detail = PluginErrorDetail(
        data_hub="ameriflux",
        operation="get_sites",
        error="Connection timeout",
        timestamp=timestamp,
    )
    error_summary = ErrorSummary(
        total_errors=1,
        total_results=10,
        errors=[error_detail],
    )
    assert error_summary.total_errors == 1
    assert error_summary.total_results == 10
    assert len(error_summary.errors) == 1
    assert error_summary.errors[0] == error_detail


def test_error_summary_empty_errors():
    """Test ErrorSummary with no errors."""
    error_summary = ErrorSummary(
        total_errors=0,
        total_results=10,
        errors=[],
    )
    assert error_summary.total_errors == 0
    assert error_summary.total_results == 10
    assert len(error_summary.errors) == 0


def test_error_summary_multiple_errors():
    """Test ErrorSummary with multiple errors."""
    timestamp = datetime.now().isoformat()
    errors = [
        PluginErrorDetail(
            data_hub="ameriflux",
            operation="get_sites",
            error="Connection timeout",
            timestamp=timestamp,
        ),
        PluginErrorDetail(
            data_hub="icos",
            operation="get_sites",
            error="API rate limit exceeded",
            timestamp=timestamp,
        ),
    ]
    error_summary = ErrorSummary(
        total_errors=2,
        total_results=5,
        errors=errors,
    )
    assert error_summary.total_errors == 2
    assert error_summary.total_results == 5
    assert len(error_summary.errors) == 2


def test_error_summary_non_negative_validation():
    """Test that total_errors and total_results must be non-negative."""
    timestamp = datetime.now().isoformat()
    error_detail = PluginErrorDetail(
        data_hub="ameriflux",
        operation="get_sites",
        error="Test error",
        timestamp=timestamp,
    )

    # Negative total_errors should fail
    with pytest.raises(ValidationError):
        ErrorSummary(total_errors=-1, total_results=10, errors=[error_detail])

    # Negative total_results should fail
    with pytest.raises(ValidationError):
        ErrorSummary(total_errors=1, total_results=-1, errors=[error_detail])


def test_error_summary_json_serialization():
    """Test JSON serialization of ErrorSummary."""
    timestamp = datetime.now().isoformat()
    error_detail = PluginErrorDetail(
        data_hub="ameriflux",
        operation="get_sites",
        error="Connection timeout",
        timestamp=timestamp,
    )
    error_summary = ErrorSummary(
        total_errors=1,
        total_results=10,
        errors=[error_detail],
    )

    # Test serialization
    json_data = error_summary.model_dump()
    assert json_data["total_errors"] == 1
    assert json_data["total_results"] == 10
    assert len(json_data["errors"]) == 1
    assert json_data["errors"][0]["data_hub"] == "ameriflux"

    # Test round-trip
    reconstructed = ErrorSummary(**json_data)
    assert reconstructed.total_errors == error_summary.total_errors
    assert reconstructed.total_results == error_summary.total_results
    assert len(reconstructed.errors) == len(error_summary.errors)
