import logging
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lower, when
from pyspark.sql.functions import sum as _sum


class DataAnalyzer:
    """Perform data analysis tasks on the transformed data."""

    def __init__(self) -> None:
        """Initialize the DataAnalyzer."""
        self.logger = logging.getLogger(__name__)

    def analyze_data(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Perform analysis on the transformed data."""
        self.logger.info('Starting data analysis.')

        try:
            merged_df = data['merged_data'].cache()

            # Validate that required columns exist
            required_columns = [
                'account_id', 'year_month', 'transaction_type',
                'amount', 'account_type', 'customer_id', 'balance',
            ]
            self._validate_columns(merged_df, required_columns)

            # Perform analysis
            total_deposits_withdrawals = self._calculate_total_deposits_withdrawals(merged_df)
            total_balance_by_account_type = self._calculate_balance_by_account_type(merged_df)
            avg_transaction_per_customer = self._calculate_avg_transaction_per_customer(merged_df)

            # Unpersist the DataFrame after analysis
            merged_df.unpersist()

            self.logger.info('Data analysis completed successfully.')

            return {
                'total_deposits_withdrawals': total_deposits_withdrawals,
                'total_balance_by_account_type': total_balance_by_account_type,
                'avg_transaction_per_customer': avg_transaction_per_customer,
            }

        except Exception:
            self.logger.exception('Error during data analysis.')
            raise

    def _validate_columns(self, df: DataFrame, required_columns: list) -> None:
        """Validate that the DataFrame contains the required columns."""
        missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]

        if missing_columns:
            error_msg = f'DataFrame is missing required columns: {missing_columns}'
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def _calculate_total_deposits_withdrawals(self, df: DataFrame) -> DataFrame:
        """Calculate total deposit and withdrawal amounts per account per 'year_month'."""
        self.logger.info('Calculating total deposits and withdrawals per account per year_month.')

        return df.groupBy('account_id', 'year_month') \
            .agg(
                _sum(
                    when(lower(col('transaction_type')) == 'deposit', col('amount'))
                    .otherwise(Decimal('0.00')),
                ).alias('total_deposit'),
                _sum(
                    when(lower(col('transaction_type')) == 'withdrawal', col('amount'))
                    .otherwise(Decimal('0.00')),
                ).alias('total_withdrawal'),
            )

    def _calculate_balance_by_account_type(self, df: DataFrame) -> DataFrame:
        """Calculate total balance per account type per 'year_month'."""
        self.logger.info('Calculating total balance per account type per year_month.')

        return df.groupBy('account_type', 'year_month').agg(
            _sum('balance').alias('total_balance'),
        )

    def _calculate_avg_transaction_per_customer(self, df: DataFrame) -> DataFrame:
        """Calculate the average transaction amount per customer."""
        self.logger.info('Calculating average transaction amount per customer.')

        return df.groupBy('customer_id').agg(
            avg('amount').alias('avg_transaction_amount'),
        )
