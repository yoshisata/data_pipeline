from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from src.exporter import DataExporter
from src.schemas import SCHEMAS
from tests.conftest import MockConfigManager

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def test_export_data_success(spark: SparkSession, tmp_path: Path) -> None:
    """Test successful data export to Delta format."""
    data = [
        ('checking', '2024-03', Decimal('1153.43')),
        ('savings', '2024-03', Decimal('406.69')),
    ]

    schema = StructType([
        StructField('account_type', StringType(), nullable=True),
        StructField('year_month', StringType(), nullable=True),
        StructField('total_balance', DecimalType(18, 2), nullable=True),
    ])
    df = spark.createDataFrame(data, schema=schema)

    config = {
        'export_paths': {
            'test_export': str(tmp_path / 'test_export.delta'),
        },
    }

    exporter = DataExporter(MockConfigManager(config))
    exporter.export_data({'test_export': df})

    # Verify that the Delta table was created
    assert (tmp_path / 'test_export.delta').exists()

    expected_count = 2
    filtered_count = 1

    # Read the Delta table and verify contents
    exported_df = spark.read.format('delta').load(str(tmp_path / 'test_export.delta'))

    assert exported_df.count() == expected_count
    assert exported_df.filter(col('account_type') == 'checking').count() == filtered_count
    assert exported_df.filter(col('account_type') == 'savings').count() == filtered_count


def test_export_data_missing_export_path(spark: SparkSession) -> None:
    """Test data export when export path is missing."""
    config = {
        'export_paths': {
            # 'test_export' path is missing
        },
    }

    exporter = DataExporter(MockConfigManager(config))
    data = {
        'test_export': spark.createDataFrame([], SCHEMAS['transactions']),
    }

    with pytest.raises(ValueError, match='Export path for "test_export" is not specified.'):
        exporter.export_data(data)


def test_export_to_delta_no_data(spark: SparkSession, tmp_path: Path) -> None:
    """Test exporting an empty DataFrame."""
    empty_df = spark.createDataFrame([], SCHEMAS['transactions'])

    config = {
        'export_paths': {
            'empty_export': str(tmp_path / 'empty_export.delta'),
        },
    }

    exporter = DataExporter(MockConfigManager(config))
    exporter.export_data({'empty_export': empty_df})

    # Verify that the Delta table was not created
    assert not (tmp_path / 'empty_export.delta').exists()
