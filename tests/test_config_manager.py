from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
import yaml

from src.config_manager import ConfigError, ConfigManager


def test_load_config_success() -> None:
    """Test loading configuration successfully without accessing the file system."""
    mock_config = {
        'data_paths': {
            'transactions': 'data/raw/transactions.parquet',
            'accounts': 'data/raw/accounts.parquet',
            'customers': 'data/raw/customers.parquet',
        },
        'export_paths': {
            'classified_data': 'data/processed/classified_data.delta',
            'weekly_aggregates': 'data/processed/weekly_aggregates.delta',
            'largest_transactions': 'data/processed/largest_transactions.delta',
            'total_deposits_withdrawals': 'data/processed/total_deposits_withdrawals.delta',
            'total_balance_by_account_type': 'data/processed/total_balance_by_account_type.delta',
            'avg_transaction_per_customer': 'data/processed/avg_transaction_per_customer.delta',
        },
        'logging_config': 'config/logging.yaml',
        'spark': {
            'app_name': 'DataPipelineTest',
            'master': 'local[*]',
            'configs': {},
        },
    }

    with patch.object(ConfigManager, '_load_config', return_value=mock_config):
        config_manager = ConfigManager('config/config.yaml')

        assert Path(config_manager.get('data_paths.transactions')).resolve() == Path(
            'data/raw/transactions.parquet').resolve()
        assert Path(config_manager.get('export_paths.total_deposits_withdrawals')).resolve() == Path(
            'data/processed/total_deposits_withdrawals.delta').resolve()
        assert Path(config_manager.get('logging_config')).resolve() == Path('config/logging.yaml').resolve()
        assert config_manager.get('spark.app_name') == 'DataPipelineTest'


def test_load_config_file_not_found() -> None:
    """Test loading configuration when file is not found."""
    with patch('os.path.exists', return_value=False), \
         pytest.raises(FileNotFoundError) as excinfo:
        ConfigManager('config/missing_config.yaml')

    assert 'Configuration file not found' in str(excinfo.value)


def test_load_config_yaml_error() -> None:
    """Test loading configuration with invalid YAML."""
    invalid_yaml = 'invalid: [unclosed_list'

    with patch('builtins.open', mock_open(read_data=invalid_yaml)), \
            patch('yaml.safe_load', side_effect=yaml.YAMLError('Error parsing configuration file')), \
            patch('src.utils.resolve_path', return_value=Path('config/config.yaml').resolve()), \
            pytest.raises(ConfigError, match='Error parsing configuration file'):
        ConfigManager('config/config.yaml')
