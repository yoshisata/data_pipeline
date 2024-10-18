from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.analyzer import DataAnalyzer
from src.schemas import SCHEMAS


def test_analyze_data_success(sample_merged_df: DataFrame) -> None:
    """Test successful data analysis."""
    analyzer = DataAnalyzer()
    analysis = analyzer.analyze_data({'merged_data': sample_merged_df})

    # Check keys in analysis result
    assert 'total_deposits_withdrawals' in analysis
    assert 'total_balance_by_account_type' in analysis
    assert 'avg_transaction_per_customer' in analysis

    # Check if DataFrames are not empty
    assert analysis['total_deposits_withdrawals'].count() > 0
    assert analysis['total_balance_by_account_type'].count() > 0
    assert analysis['avg_transaction_per_customer'].count() > 0

    # Check rounding in total_balance_by_account_type
    balance_df = analysis['total_balance_by_account_type']
    for row in balance_df.select('total_balance').collect():
        total_balance = row['total_balance']

        assert float(total_balance) == pytest.approx(round(total_balance, 2))


def test_analyze_data_missing_merged_data() -> None:
    """Test data analysis when merged_data is missing."""
    analyzer = DataAnalyzer()
    with pytest.raises(KeyError) as excinfo:
        analyzer.analyze_data({})

    assert "'merged_data'" in str(excinfo.value)


def test_analyze_data_empty_merged_data(spark: SparkSession) -> None:
    """Test data analysis when merged_data is empty."""
    empty_df = spark.createDataFrame([], SCHEMAS['transactions'])
    analyzer = DataAnalyzer()

    with pytest.raises(ValueError, match='DataFrame is missing required columns'):
        analyzer.analyze_data({'merged_data': empty_df})


def test_calculate_total_deposits_withdrawals(sample_merged_df: DataFrame) -> None:
    """Test the calculation of total deposits and withdrawals."""
    analyzer = DataAnalyzer()
    result_df = analyzer._calculate_total_deposits_withdrawals(sample_merged_df)

    expected_columns = {'account_id', 'year_month', 'total_deposit', 'total_withdrawal'}

    assert set(result_df.columns) == expected_columns, \
        f"Expected columns {expected_columns}, but got {set(result_df.columns)}"

    expected_data = {
        ('acc_001', '2024-01'): {'total_deposit': Decimal('1500.00'), 'total_withdrawal': Decimal('100.00')},
        ('acc_002', '2024-02'): {'total_deposit': Decimal('300.25'), 'total_withdrawal': Decimal('500.50')},
        ('acc_003', '2024-03'): {'total_deposit': Decimal('250.75'), 'total_withdrawal': Decimal('0.00')},
    }

    for row in result_df.collect():
        key = (row['account_id'], row['year_month'])

        assert key in expected_data, f"Unexpected key: {key}"
        assert row['total_deposit'] == expected_data[key]['total_deposit'], \
            (f"Mismatch in total_deposit for {key}: "
             f"expected {expected_data[key]['total_deposit']}, got {row['total_deposit']}")
        assert row['total_withdrawal'] == expected_data[key]['total_withdrawal'], \
            (f"Mismatch in total_withdrawal for {key}: "
             f"expected {expected_data[key]['total_withdrawal']}, got {row['total_withdrawal']}")


def test_calculate_avg_transaction_per_customer(sample_merged_df: DataFrame) -> None:
    """Test the calculation of average transaction per customer."""
    analyzer = DataAnalyzer()
    result_df = analyzer._calculate_avg_transaction_per_customer(sample_merged_df)

    expected_columns = {'customer_id', 'avg_transaction_amount'}

    assert set(result_df.columns) == expected_columns

    expected_data = {
        'cust_001': Decimal('800.00'),
        'cust_002': Decimal('400.375'),
        'cust_003': Decimal('250.75'),
    }

    for row in result_df.collect():
        customer_id = row['customer_id']
        avg_amount = row['avg_transaction_amount']

        assert customer_id in expected_data
        assert avg_amount == expected_data[customer_id]
