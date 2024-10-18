from pathlib import Path

from src.utils import get_project_root, resolve_config_paths, resolve_logging_paths, resolve_path


def test_get_project_root() -> None:
    """Test retrieving the project root directory."""
    current_file = Path(__file__).resolve()
    expected_root = current_file.parent.parent

    assert get_project_root() == expected_root


def test_resolve_path() -> None:
    """Test resolving a relative path to an absolute path."""
    relative_path = 'data/transactions.parquet'
    expected_path = get_project_root() / relative_path
    resolved = resolve_path(relative_path)

    assert resolved == expected_path


def test_resolve_config_paths() -> None:
    """Test resolving paths in the configuration dictionary."""
    config = {
        'data_paths': {
            'transactions': 'data/transactions.parquet',
            'accounts': 'data/accounts.parquet',
            'customers': 'data/customers.parquet',
        },
        'export_paths': {
            'test_export': 'export/test_export.delta',
        },
        'logging_config': 'config/logging.yaml',
    }

    expected_resolved = {
        'data_paths': {
            'transactions': get_project_root() / 'data/transactions.parquet',
            'accounts': get_project_root() / 'data/accounts.parquet',
            'customers': get_project_root() / 'data/customers.parquet',
        },
        'export_paths': {
            'test_export': get_project_root() / 'export/test_export.delta',
        },
        'logging_config': get_project_root() / 'config/logging.yaml',
    }

    resolved_config = resolve_config_paths(config, ['data_paths', 'export_paths', 'logging_config'])

    assert resolved_config == expected_resolved


def test_resolve_logging_paths() -> None:
    """Test resolving logging paths in the logging configuration."""
    logging_config = {
        'handlers': {
            'file_handler': {
                'class': 'logging.FileHandler',
                'filename': 'logs/app.log',
                'level': 'INFO',
                'formatter': 'simple',
            },
            'console_handler': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
            },
        },
    }

    expected_resolved = {
        'handlers': {
            'file_handler': {
                'class': 'logging.FileHandler',
                'filename': get_project_root() / 'logs/app.log',
                'level': 'INFO',
                'formatter': 'simple',
            },
            'console_handler': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
            },
        },
    }

    resolved_logging = resolve_logging_paths(logging_config)

    assert resolved_logging == expected_resolved
