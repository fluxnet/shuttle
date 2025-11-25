"""
Pydantic Schema Models for FLUXNET Shuttle Library
==================================================

:module:: fluxnet_shuttle.models
:synopsis: Pydantic models for FLUXNET dataset metadata and validation
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:versionadded: 1.0.0
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle.models

This module defines Pydantic models for data validation and serialization
in the FLUXNET Shuttle Library. These models ensure type safety and provide
automatic validation for FLUXNET dataset metadata and operations.

Classes:
    BadmSiteGeneralInfo: Site general information from BADM format
    DataFluxnetProduct: FLUXNET product data information
    FluxnetDatasetMetadata: Combined model for complete dataset metadata
    PluginErrorDetail: Individual plugin error information
    ErrorSummary: Summary of errors collected during operations

The models are designed to work with the FLUXNET data format and provide
validation for:
    - Data hub and publisher information
    - Site identifiers and temporal coverage
    - Dataset versions and file metadata
    - Download URLs with validation
    - Error tracking and reporting

Example:
    >>> from fluxnet_shuttle.models.schema import FluxnetDatasetMetadata
    >>> site_info = BadmSiteGeneralInfo(
    ...     site_id="US-Ha1",
    ...     data_hub="AmeriFlux",
    ...     location_lat=42.5378,
    ...     location_long=-72.1715,
    ...     igbp="DBF"
    ... )
    >>> product_data = DataFluxnetProduct(
    ...     first_year=2005,
    ...     last_year=2012,
    ...     download_link="https://example.com/data.zip"
    ... )
    >>> metadata = FluxnetDatasetMetadata(
    ...     site_info=site_info,
    ...     product_data=product_data
    ... )

Note:
    All models use Pydantic v2 syntax and are compatible with FastAPI
    automatic API documentation generation.

.. moduleauthor:: FLUXNET Shuttle Library Team
.. versionadded:: 1.0.0
"""

import re
from datetime import datetime
from typing import List

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


class BadmSiteGeneralInfo(BaseModel):
    """
    Pydantic model for BADM Site General Information.

    This model represents the minimum required fields for site general information
    in the BADM (Biological, Ancillary, Disturbance and Metadata) format.

    Attributes:
        site_id (str): Site identifier by country using first two chars or clusters
        data_hub (str): Data hub name (e.g., AmeriFlux, ICOS)
        location_lat (float): Site latitude in decimal degrees
        location_long (float): Site longitude in decimal degrees
        igbp (str): IGBP land cover type classification
    """

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    site_id: str = Field(
        ...,
        description="Site identifier by country using first two chars or clusters",
        min_length=1,
        max_length=20,
    )

    # data_hub is not part of the BADM Standard but including in the BADM SGI model"
    data_hub: str = Field(
        ...,
        description="Data hub name (e.g., AmeriFlux, ICOS, NEON)",
        min_length=1,
        max_length=50,
    )

    location_lat: float = Field(..., description="Site latitude in decimal degrees", ge=-90.0, le=90.0)

    location_long: float = Field(..., description="Site longitude in decimal degrees", ge=-180.0, le=180.0)

    igbp: str = Field(
        ...,
        description="IGBP land cover type classification",
        min_length=1,
        max_length=10,
    )

    @field_validator("site_id")
    @classmethod
    def validate_site_id_format(cls: type, v: str) -> str:
        """Validate that site_id follows the country code pattern."""
        if not re.match(r"^[A-Z_]+-[A-Za-z0-9]+$", v):
            raise ValueError("site_id must follow format: XX-YYYY where XX is country code")
        return v


class DataFluxnetProduct(BaseModel):
    """
    Pydantic model for FLUXNET Product Data Information.

    This model represents the minimum required fields for FLUXNET data products,
    including temporal coverage and download information.

    Attributes:
        first_year (int): First year of data coverage (YYYY format)
        last_year (int): Last year of data coverage (YYYY format)
        download_link (HttpUrl): URL for downloading the data product
    """

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    first_year: int = Field(..., description="First year of data coverage in YYYY format", ge=1900, le=2100)

    last_year: int = Field(..., description="Last year of data coverage in YYYY format", ge=1900, le=2100)

    download_link: HttpUrl = Field(..., description="URL for downloading the data product")

    @model_validator(mode="after")
    def validate_year_range(self) -> "DataFluxnetProduct":
        """Validate that last_year is not before first_year."""
        if self.last_year < self.first_year:
            raise ValueError("last_year must be greater than or equal to first_year")
        return self


class FluxnetDatasetMetadata(BaseModel):
    """
    Combined model for complete FLUXNET dataset metadata.

    This model combines both site general information and product data
    to represent a complete FLUXNET dataset entry.

    Attributes:
        site_info (BadmSiteGeneralInfo): Site general information
        product_data (DataFluxnetProduct): Product data information
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="allow",  # Allow additional fields for extensibility
    )

    site_info: BadmSiteGeneralInfo = Field(..., description="Site general information from BADM")

    product_data: DataFluxnetProduct = Field(..., description="FLUXNET product data information")


class PluginErrorDetail(BaseModel):
    """
    Pydantic model for individual plugin error details.

    This model represents an error that occurred during plugin execution,
    including context about which data hub/plugin encountered the error.

    Attributes:
        data_hub (str): Data hub/plugin name where the error occurred
        operation (str): Operation being performed when the error occurred
        error (str): Error message or description
        timestamp (str): ISO format timestamp when the error occurred
    """

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    data_hub: str = Field(..., description="Data hub/plugin name where the error occurred", min_length=1)

    operation: str = Field(..., description="Operation being performed when the error occurred", min_length=1)

    error: str = Field(..., description="Error message or description", min_length=1)

    timestamp: str = Field(..., description="ISO format timestamp when the error occurred")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp_format(cls: type, v: str) -> str:
        """Validate that timestamp is in ISO format."""
        try:
            datetime.fromisoformat(v)
        except ValueError as e:
            raise ValueError(f"timestamp must be in ISO format: {e}") from e
        return v


class ErrorSummary(BaseModel):
    """
    Pydantic model for error summary information.

    This model represents a summary of errors collected during FLUXNET Shuttle
    operations, including total counts and detailed error information.

    Attributes:
        total_errors (int): Total number of errors encountered
        total_results (int): Total number of successful results retrieved
        errors (List[PluginErrorDetail]): List of detailed error information
    """

    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")

    total_errors: int = Field(..., description="Total number of errors encountered", ge=0)

    total_results: int = Field(..., description="Total number of successful results retrieved", ge=0)

    errors: List[PluginErrorDetail] = Field(..., description="List of detailed error information")
