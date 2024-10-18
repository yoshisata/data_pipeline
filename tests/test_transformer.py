from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format

from src.schemas import SCHEMAS
from src.transformer import DataTransformer


def test_transform_data_success(
    sample_transactions_df: DataFrame,
    sample_accounts_df: DataFrame,
    sample_customers_df: DataFrame,
) -> None:
    """Test successful data transformation."""
    mock_logger = MagicMock()
    transformer = DataTransformer()
    transformer.logger = mock_logger

    data = {
        'transactions': sample_transactions_df,
        'accounts': sample_accounts_df,
        'customers': sample_customers_df,
    }

    transformed = transformer.transform_data(data)
    merged_df = transformed['merged_data']

    # Check if 'year_month' column exists
    assert 'year_month' in merged_df.columns

    # Check data types of date columns
    transaction_date_type = dict(merged_df.dtypes)['transaction_date']
    join_date_type = dict(merged_df.dtypes)['join_date']

    assert transaction_date_type == 'timestamp', f"Expected 'timestamp', but got '{transaction_date_type}'"
    assert join_date_type == 'timestamp', f"Expected 'timestamp', but got '{join_date_type}'"

    # Check no nulls in merged DataFrame
    assert merged_df.filter(col('customer_id').isNull()).count() == 0
    assert merged_df.filter(col('account_type').isNull()).count() == 0
    assert merged_df.filter(col('balance').isNull()).count() == 0
    assert merged_df.filter(col('customer_name').isNull()).count() == 0
    assert merged_df.filter(col('join_date').isNull()).count() == 0


def test_transform_data_missing_dataframe(
    sample_transactions_df: DataFrame,
    sample_accounts_df: DataFrame,
) -> None:
    """Test data transformation when a required DataFrame is missing."""
    transformer = DataTransformer()
    data = {
        'transactions': sample_transactions_df,
        'accounts': sample_accounts_df,
        # 'customers' DataFrame is missing
    }

    with pytest.raises(ValueError, match='DataFrame "customers" is missing.') as excinfo:
        transformer.transform_data(data)
    assert 'DataFrame "customers" is missing.' in str(excinfo.value)


def test_transform_data_empty_dataframe(
    spark: SparkSession,
    sample_accounts_df: DataFrame,
    sample_customers_df: DataFrame,
) -> None:
    """Test data transformation when a DataFrame is empty."""
    empty_df = spark.createDataFrame([], SCHEMAS['transactions'])
    transformer = DataTransformer()
    data = {
        'transactions': empty_df,
        'accounts': sample_accounts_df,
        'customers': sample_customers_df,
    }

    with pytest.raises(ValueError, match='DataFrame "transactions" is empty.') as excinfo:
        transformer.transform_data(data)

    assert 'DataFrame "transactions" is empty.' in str(excinfo.value)


def test_add_year_month(sample_transactions_df: DataFrame) -> None:
    """Test the addition of 'year_month' column."""
    transformer = DataTransformer()
    transformed_data = transformer._add_year_month(sample_transactions_df)

    # Extract unique year_month values from the sample data
    expected_year_month = sample_transactions_df.withColumn(
        'year_month', date_format(col('transaction_date'), 'yyyy-MM'),
    ).select('year_month').distinct().collect()
    expected_year_month = [row['year_month'] for row in expected_year_month]

    actual_year_month = [row['year_month'] for row in transformed_data.select('year_month').distinct().collect()]

    # Sort lists to ensure order doesn't affect the assertion
    assert sorted(actual_year_month) == sorted(expected_year_month), \
        f"Expected year_month values {expected_year_month}, but got {actual_year_month}"
