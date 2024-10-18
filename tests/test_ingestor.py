from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from src.ingestor import DataIngestor
from src.schemas import SCHEMAS
from tests.conftest import MockConfigManager

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def test_load_data_success(spark: SparkSession, tmp_path: Path) -> None:
    """Test successful data loading."""
    transactions_path = tmp_path / 'transactions.parquet'
    accounts_path = tmp_path / 'accounts.parquet'
    customers_path = tmp_path / 'customers.parquet'

    transactions_data = [
        ('trans_001', 'acc_001', date(2024, 1, 15), Decimal('1500.00'), 'deposit'),
        ('trans_002', 'acc_002', date(2024, 2, 20), Decimal('500.50'), 'withdrawal'),
    ]
    accounts_data = [
        ('acc_001', 'cust_001', 'checking', Decimal('1000.00')),
        ('acc_002', 'cust_002', 'savings', Decimal('2000.00')),
    ]
    customers_data = [
        ('cust_001', 'Alice Smith', date(2023, 12, 1)),
        ('cust_002', 'Bob Johnson', date(2024, 1, 15)),
    ]

    expected_count = 2

    spark.createDataFrame(transactions_data, schema=SCHEMAS['transactions']).write.parquet(str(transactions_path))
    spark.createDataFrame(accounts_data, schema=SCHEMAS['accounts']).write.parquet(str(accounts_path))
    spark.createDataFrame(customers_data, schema=SCHEMAS['customers']).write.parquet(str(customers_path))

    config = {
        'data_paths': {
            'transactions': str(transactions_path),
            'accounts': str(accounts_path),
            'customers': str(customers_path),
        },
    }

    ingestor = DataIngestor(MockConfigManager(config), spark)
    data = ingestor.load_data()

    assert 'transactions' in data
    assert 'accounts' in data
    assert 'customers' in data

    assert data['transactions'].count() == expected_count
    assert data['accounts'].count() == expected_count
    assert data['customers'].count() == expected_count


def test_load_data_missing_path(spark: SparkSession) -> None:
    """Test data loading when a data path is missing."""
    config = {
        'data_paths': {
            'transactions': 'non_existent_transactions.parquet',
            'accounts': 'data/accounts.parquet',
            'customers': 'data/customers.parquet',
        },
    }

    ingestor = DataIngestor(MockConfigManager(config), spark)
    with pytest.raises(FileNotFoundError) as excinfo:
        ingestor.load_data()

    assert 'File not found: non_existent_transactions.parquet' in str(excinfo.value)


def test_load_data_file_not_found(spark: SparkSession, tmp_path: Path) -> None:
    """Test data loading when a specific file does not exist."""
    accounts_path = tmp_path / 'accounts.parquet'
    customers_path = tmp_path / 'customers.parquet'

    accounts_data = [
        ('acc_001', 'cust_001', 'checking', Decimal('1000.00')),
        ('acc_002', 'cust_002', 'savings', Decimal('2000.00')),
    ]
    customers_data = [
        ('cust_001', 'Alice Smith', date(2023, 12, 1)),
        ('cust_002', 'Bob Johnson', date(2024, 1, 15)),
    ]

    spark.createDataFrame(accounts_data, schema=SCHEMAS['accounts']).write.parquet(str(accounts_path))
    spark.createDataFrame(customers_data, schema=SCHEMAS['customers']).write.parquet(str(customers_path))

    config = {
        'data_paths': {
            'transactions': 'non_existent_transactions.parquet',  # File does not exist
            'accounts': str(accounts_path),
            'customers': str(customers_path),
        },
    }

    ingestor = DataIngestor(MockConfigManager(config), spark)
    with pytest.raises(FileNotFoundError) as excinfo:
        ingestor.load_data()

    assert 'File not found: non_existent_transactions.parquet' in str(excinfo.value)
