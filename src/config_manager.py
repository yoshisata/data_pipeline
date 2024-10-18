from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.utils import resolve_config_paths, resolve_path


class ConfigError(Exception):
    """Custom exception for configuration errors."""


class ConfigManager:
    """Manage configuration parameters loaded from a YAML file."""

    def __init__(self, config_file: str | None = None) -> None:
        """Initialize the ConfigManager."""
        self.config = self._load_config(config_file)

    @staticmethod
    def _load_config(config_file: str | None) -> dict[str, Any]:
        """Load configuration from a YAML file."""

        def raise_file_error(file_path: str) -> None:
            error_msg = f'Configuration file not found: {file_path}'

            raise FileNotFoundError(error_msg)

        try:
            if config_file is None:
                config_file = 'config/config.yaml'

            config_file = resolve_path(config_file)

            # Check if the config file exists
            if not Path(config_file).exists():
                raise_file_error(config_file)

            # Open and load the YAML file
            with Path(config_file).open() as f:
                config = yaml.safe_load(f)

            # Resolve paths in the configuration
            keys_to_resolve = ['data_paths', 'csv_paths', 'export_paths', 'logging_config']
            return resolve_config_paths(config, keys_to_resolve)

        except FileNotFoundError as e:
            error_msg = f'Configuration file not found: {config_file}'
            raise FileNotFoundError(error_msg) from e

        except yaml.YAMLError as e:
            error_msg = f'Error parsing configuration file: {config_file}'
            raise ConfigError(error_msg) from e

    def get(self, key: str, default: Any | None = None) -> Any:
        """Retrieve a configuration value."""
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value
