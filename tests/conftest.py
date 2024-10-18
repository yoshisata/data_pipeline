from __future__ import annotations

from collections.abc import Generator
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.schemas import SCHEMAS
from src.transformer import DataTransformer

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(scope='session')
def spark() -> Generator[SparkSession, None, None]:
    """Fixture for creating a SparkSession with Delta Lake support for testing."""
    spark = SparkSession.builder \
        .appName('DataPipelineTest') \
        .master('local[*]') \
        .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture(scope='module')
def sample_transactions_df(spark: SparkSession) -> DataFrame:
    """Create a sample transactions DataFrame with correct data types."""
    data = [
        ('trans_001', 'acc_001', date(2024, 1, 15), Decimal('1500.00'), 'deposit'),
        ('trans_002', 'acc_002', date(2024, 2, 20), Decimal('500.50'), 'withdrawal'),
        ('trans_003', 'acc_003', date(2024, 3, 10), Decimal('250.75'), 'deposit'),
        ('trans_004', 'acc_001', date(2024, 1, 25), Decimal('100.00'), 'withdrawal'),
        ('trans_005', 'acc_002', date(2024, 2, 28), Decimal('300.25'), 'deposit'),
    ]

    return spark.createDataFrame(data, schema=SCHEMAS['transactions'])


@pytest.fixture(scope='module')
def sample_accounts_df(spark: SparkSession) -> DataFrame:
    """Create a sample accounts DataFrame with correct data types."""
    data = [
        ('acc_001', 'cust_001', 'checking', Decimal('1000.00')),
        ('acc_002', 'cust_002', 'savings', Decimal('2000.00')),
        ('acc_003', 'cust_003', 'checking', Decimal('1500.00')),
    ]

    return spark.createDataFrame(data, schema=SCHEMAS['accounts'])


@pytest.fixture(scope='module')
def sample_customers_df(spark: SparkSession) -> DataFrame:
    """Create a sample customers DataFrame with correct data types."""
    data = [
        ('cust_001', 'Alice Smith', date(2023, 12, 1)),
        ('cust_002', 'Bob Johnson', date(2024, 1, 15)),
        ('cust_003', 'Charlie Lee', date(2024, 2, 20)),
    ]

    return spark.createDataFrame(data, schema=SCHEMAS['customers'])


@pytest.fixture(scope='module')
def sample_merged_df(
    sample_transactions_df: DataFrame,
    sample_accounts_df: DataFrame,
    sample_customers_df: DataFrame,
) -> DataFrame:
    """Create a merged DataFrame for testing."""
    mock_logger = MagicMock()
    transformer = DataTransformer()
    transformer.logger = mock_logger

    data = {
        'transactions': sample_transactions_df,
        'accounts': sample_accounts_df,
        'customers': sample_customers_df,
    }
    transformed = transformer.transform_data(data)

    return transformed['merged_data']


class MockConfigManager:
    """Mock ConfigManager for testing purposes."""

    def __init__(self, config: dict[str, dict[str, str]]) -> None:
        """Initialize MockConfigManager with a configuration dictionary."""
        self.config = config

    def get(self, key: str, default: str | None = None) -> dict[str, str]:
        """Retrieve a configuration value by key."""
        return self.config.get(key, default)
