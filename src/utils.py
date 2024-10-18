from pathlib import Path
from typing import Any


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    # Assuming this file is in src/, and project root is one level up
    return Path(__file__).resolve().parent.parent


def resolve_path(relative_path: str) -> Path:
    """Resolve a relative path to an absolute path based on the project root."""
    return get_project_root() / relative_path


def resolve_config_paths(config: dict[str, Any], keys: list) -> dict[str, Any]:
    """Resolve relative paths to absolute paths in specified keys of the configuration dictionary."""
    for key in keys:
        if key in config:
            value = config[key]
            if isinstance(value, dict):
                config[key] = _resolve_dict_paths(value)
            elif isinstance(value, str):
                config[key] = resolve_path(value)

    return config


def _resolve_dict_paths(path_dict: dict[str, str]) -> dict[str, str]:
    """Resolve paths in a dictionary of paths."""
    for subkey, path in path_dict.items():
        if isinstance(path, str):
            path_dict[subkey] = resolve_path(path)

    return path_dict


def resolve_logging_paths(logging_config: dict[str, Any]) -> dict[str, Any]:
    """Resolve the paths in the logging configuration, specifically for file handlers."""
    handlers = logging_config.get('handlers', {})
    for handler in handlers.values():
        if 'filename' in handler and isinstance(handler['filename'], str):
            handler['filename'] = resolve_path(handler['filename'])

    return logging_config
