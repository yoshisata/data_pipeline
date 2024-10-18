import logging
from unittest.mock import mock_open, patch

import pytest
from yaml import YAMLError

from src.logger import setup_logging


def test_setup_logging_success() -> None:
    """Test successful logging setup."""
    mock_logging_config = {
        'version': 1,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'stream': 'ext://sys.stdout',
            },
        },
        'formatters': {
            'detailed': {
                'format': '%(asctime)s %(name)-15s %(levelname)-8s %(message)s',
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
    }

    with patch('builtins.open', mock_open(read_data='')), \
         patch('yaml.safe_load', return_value=mock_logging_config):
        setup_logging(default_path='config/logging.yaml')
        logger = logging.getLogger(__name__)

        assert logger.isEnabledFor(logging.DEBUG)


def test_setup_logging_file_not_found() -> None:
    """Test logging setup when configuration file is not found."""
    with patch('os.path.exists', return_value=False):
        with pytest.raises(FileNotFoundError) as excinfo:
            setup_logging(default_path='config/missing_logging.yaml')

        assert 'Configuration file not found' in str(excinfo.value)


def test_setup_logging_yaml_error() -> None:
    """Test logging setup with invalid YAML."""
    invalid_yaml = 'invalid: [unclosed_list'

    with patch('builtins.open', mock_open(read_data=invalid_yaml)), \
         patch('yaml.safe_load', side_effect=YAMLError('Invalid YAML')):
        with pytest.raises(YAMLError) as excinfo:
            setup_logging(default_path='config/logging.yaml')

        assert 'Invalid YAML' in str(excinfo.value)
