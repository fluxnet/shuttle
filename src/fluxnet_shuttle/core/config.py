"""
Configuration System
====================

:module:: fluxnet_shuttle.core.config
:synopsis: Configuration system for FLUXNET Shuttle library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle.core.config


This module provides the configuration system for the FLUXNET Shuttle
library, including loading default and custom configurations.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class DataHubConfig:
    """
    Configuration for a specific data hub.

    Attributes:
        enabled: Whether this data hub is enabled
        user_info: Optional dictionary containing user-specific information for this data hub.
                   For example, AmeriFlux may use: {"user_name": "...", "user_email": "...",
                   "intended_use": 1, "description": "..."}
    """

    enabled: bool = True
    user_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShuttleConfig:
    """
    Main shuttle configuration.

    Attributes:
        data_hubs: Dictionary mapping data hub names to their configurations
    """

    data_hubs: Dict[str, DataHubConfig] = field(default_factory=dict)
    parallel_requests: int = 2

    @classmethod
    def get_user_config_path(cls) -> Optional[Path]:
        """
        Get the path to the user's config file from environment variable.

        Checks FLUXNET_SHUTTLE_CONFIG environment variable only.

        Returns:
            Path to config file if environment variable is set and file exists, None otherwise
        """
        # Check environment variable
        env_config = os.environ.get("FLUXNET_SHUTTLE_CONFIG")
        if env_config:
            config_path = Path(env_config).expanduser()
            if config_path.exists():
                logger.info(f"Using config from FLUXNET_SHUTTLE_CONFIG: {config_path}")
                return config_path
            else:
                logger.warning(f"Config file specified in FLUXNET_SHUTTLE_CONFIG not found: {config_path}")

        return None

    @classmethod
    def load_user_config(cls) -> "ShuttleConfig":
        """
        Load configuration from user's config file if it exists, otherwise use defaults.

        Returns:
            ShuttleConfig: Configuration object
        """
        user_config_path = cls.get_user_config_path()
        if user_config_path:
            return cls.load_from_file(user_config_path)
        else:
            logger.info("No user config file found, using default configuration")
            return cls.load_default()

    @classmethod
    def load_default(cls) -> "ShuttleConfig":
        """
        Load default configuration from packaged config.yaml.

        Returns:
            ShuttleConfig: Configuration object with default settings
        """
        try:
            # Try to load from package data first
            try:
                # Don't use deprecated pkg_resources if possible
                import importlib.resources

                config_data = importlib.resources.read_text("fluxnet_shuttle.plugins", "config.yaml")
                config_dict = yaml.safe_load(config_data)
                logger.info("Loaded default configuration from package")
            except (ImportError, FileNotFoundError):  # pragma: no cover
                # Fallback to file path if pkg_resources fails
                config_path = Path(__file__).parent.parent / "plugins" / "config.yaml"
                if config_path.exists():
                    with open(config_path) as f:
                        config_dict = yaml.safe_load(f)
                    logger.info(f"Loaded default configuration from {config_path}")
                else:
                    logger.warning("Default config file not found, using hardcoded defaults")
                    config_dict = cls._get_hardcoded_defaults()

            # Parse configuration
            config = cls()
            if "data_hubs" in config_dict:
                for data_hub_name, data_hub_data in config_dict["data_hubs"].items():
                    config.data_hubs[data_hub_name] = DataHubConfig(**data_hub_data)

            # Update other settings (currently unused, reserved for future config fields)
            for key, value in config_dict.items():  # pragma: no cover
                if key != "data_hubs" and hasattr(config, key):  # pragma: no cover
                    setattr(config, key, value)  # pragma: no cover

            return config

        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to load default config: {e}, using hardcoded defaults")
            return cls._create_default_config()

    @classmethod
    def load_from_file(cls, config_path: Path) -> "ShuttleConfig":
        """
        Load configuration from external YAML file.

        Args:
            config_path: Path to the configuration file

        Returns:
            ShuttleConfig: Configuration object
        """
        if not config_path.exists():
            logger.warning(f"Config file {config_path} not found, using defaults")
            return cls.load_default()

        try:
            with open(config_path) as f:
                config_dict = yaml.safe_load(f)

            # Start with default config and override with file config
            config = cls.load_default()

            if "data_hubs" in config_dict:
                for data_hub_name, data_hub_data in config_dict["data_hubs"].items():
                    # Ensure data_hub_data is a dict
                    if isinstance(data_hub_data, dict):
                        # Get existing config for this data hub (from shuttle config)
                        existing_config = config.data_hubs.get(data_hub_name, DataHubConfig())

                        # Merge user config with shuttle config
                        # User config takes precedence, but defaults to shuttle config values
                        enabled = data_hub_data.get("enabled", existing_config.enabled)

                        # Handle user_info field if present
                        user_info = data_hub_data.get("user_info", {})
                        if not isinstance(user_info, dict):
                            logger.warning(
                                f"Invalid user_info for {data_hub_name}, expected dict, got {type(user_info)}"
                            )
                            user_info = {}

                        # Merge user_info from both configs
                        merged_user_info = {**existing_config.user_info, **user_info}

                        config.data_hubs[data_hub_name] = DataHubConfig(enabled=enabled, user_info=merged_user_info)
                    else:
                        logger.warning(
                            f"Invalid config for data hub {data_hub_name}, expected dict, got {type(data_hub_data)}"
                        )

            # Update other settings (currently unused, reserved for future config fields)
            for key, value in config_dict.items():  # pragma: no cover
                if key != "data_hubs" and hasattr(config, key):  # pragma: no cover
                    setattr(config, key, value)  # pragma: no cover

            logger.info(f"Loaded configuration from {config_path}")
            return config

        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}, using defaults")
            return cls.load_default()

    @classmethod
    def _get_hardcoded_defaults(cls) -> Dict[str, Any]:
        """Get hardcoded default configuration."""
        return {
            "parallel_requests": 2,
            "data_hubs": {
                "ameriflux": {"enabled": True},
                "icos": {"enabled": True},
                "fluxnet2015": {"enabled": False},
            },
        }

    @classmethod
    def _create_default_config(cls) -> "ShuttleConfig":
        """Create default configuration object."""
        config_dict = cls._get_hardcoded_defaults()
        config = cls()

        for data_hub_name, data_hub_data in config_dict["data_hubs"].items():
            config.data_hubs[data_hub_name] = DataHubConfig(**data_hub_data)

        # Update other settings (currently unused, reserved for future config fields)
        for key, value in config_dict.items():  # pragma: no cover
            if key != "data_hubs" and hasattr(config, key):  # pragma: no cover
                setattr(config, key, value)  # pragma: no cover

        return config
