import logging
import logging.config
import os
from pathlib import Path

import yaml

from src.utils import resolve_logging_paths, resolve_path


def setup_logging(
    default_path: str = 'config/logging.yaml',
    default_level: int = logging.INFO,
    env_key: str = 'LOG_CFG',
) -> None:
    """Set up logging configuration."""
    path = default_path
    value = os.getenv(env_key)
    if value:
        path = value

    # Resolve the logging configuration file path
    path = resolve_path(path)

    if path and Path(path).exists():
        try:
            with Path(path).open() as f:
                config = yaml.safe_load(f.read())
        except yaml.YAMLError:
            logging.basicConfig(level=default_level)
            logger = logging.getLogger(__name__)
            logger.exception('Error parsing logging configuration file.')
            raise

        # Resolve paths in the logging configuration
        config = resolve_logging_paths(config)

        # Ensure the directories for log files exist
        for handler in config.get('handlers', {}).values():
            if 'filename' in handler:
                log_dir = Path(handler['filename']).parent

                if not log_dir.exists():
                    log_dir.mkdir(parents=True)

        logging.config.dictConfig(config)

    else:
        logging.basicConfig(level=default_level)
        logger = logging.getLogger(__name__)
        error_msg = f'Configuration file not found: {path}'
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
