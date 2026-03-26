"""
Configuration System
====================

:module:: fluxnet_shuttle.core.config
:synopsis: Configuration system for FLUXNET Shuttle library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:moduleauthor: Sy-Toan Ngo <sytoanngo@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09
:updated: 2025-12-09

.. currentmodule:: fluxnet_shuttle.core.config


This module provides the configuration system for the FLUXNET Shuttle
library, including loading default and custom configurations.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

# Default configuration values (overridden by config.yaml at runtime)
DEFAULT_PARALLEL_REQUESTS = 3
DEFAULT_GLOBAL_TIMEOUT = 60.0
DEFAULT_FLUXNET_SHUTTLE_REFERER = "local_shuttle"


@dataclass
class DataHubConfig:
    """Configuration for a specific data hub.

    Stores the ``enabled`` flag plus any additional plugin-specific
    settings (e.g. ``base_url``, ``api_url``).  Extra keys supplied
    at construction time are kept in the ``settings`` dict and are
    forwarded to the plugin instance via ``self.config``.
    """

    enabled: bool = True
    settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShuttleConfig:
    """Main shuttle configuration."""

    data_hubs: Dict[str, DataHubConfig] = field(default_factory=dict)
    parallel_requests: int = DEFAULT_PARALLEL_REQUESTS
    global_timeout: float = DEFAULT_GLOBAL_TIMEOUT
    fluxnet_shuttle_referer: str = DEFAULT_FLUXNET_SHUTTLE_REFERER

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
                    config.data_hubs[data_hub_name] = cls._parse_data_hub_config(data_hub_data)

            # Update other settings
            for key, value in config_dict.items():
                if key != "data_hubs" and hasattr(config, key):
                    setattr(config, key, value)

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
                    if data_hub_name in config.data_hubs:
                        # Merge: override only the keys specified in the file
                        existing = config.data_hubs[data_hub_name]
                        known_fields = {"enabled"}
                        if "enabled" in data_hub_data:
                            existing.enabled = data_hub_data["enabled"]
                        override_settings = {k: v for k, v in data_hub_data.items() if k not in known_fields}
                        existing.settings.update(override_settings)
                    else:
                        config.data_hubs[data_hub_name] = cls._parse_data_hub_config(data_hub_data)

            for key, value in config_dict.items():
                if key != "data_hubs" and hasattr(config, key):
                    setattr(config, key, value)

            logger.info(f"Loaded configuration from {config_path}")
            return config

        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}, using defaults")
            return cls.load_default()

    @classmethod
    def _get_hardcoded_defaults(cls) -> Dict[str, Any]:
        """Get hardcoded default configuration."""
        return {
            "parallel_requests": DEFAULT_PARALLEL_REQUESTS,
            "global_timeout": DEFAULT_GLOBAL_TIMEOUT,
            "fluxnet_shuttle_referer": DEFAULT_FLUXNET_SHUTTLE_REFERER,
            "data_hubs": {
                "ameriflux": {"enabled": True},
                "icos": {"enabled": True},
                "tern": {"enabled": True},
            },
        }

    @classmethod
    def _create_default_config(cls) -> "ShuttleConfig":
        """Create default configuration object."""
        config_dict = cls._get_hardcoded_defaults()
        config = cls()

        for data_hub_name, data_hub_data in config_dict["data_hubs"].items():
            config.data_hubs[data_hub_name] = cls._parse_data_hub_config(data_hub_data)

        for key, value in config_dict.items():
            if key != "data_hubs" and hasattr(config, key):
                setattr(config, key, value)

        return config

    @staticmethod
    def _parse_data_hub_config(data: Dict[str, Any]) -> "DataHubConfig":
        """Parse a data hub config dict into a DataHubConfig.

        Known fields (``enabled``) are set as dataclass attributes.
        All other keys are stored in the ``settings`` dict so they
        can be forwarded to the plugin instance.
        """
        known_fields = {"enabled"}
        enabled = data.get("enabled", True)
        settings = {k: v for k, v in data.items() if k not in known_fields}
        return DataHubConfig(enabled=enabled, settings=settings)
