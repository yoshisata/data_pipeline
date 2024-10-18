import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, date_format, to_timestamp
from pyspark.sql.types import DateType

from src.schemas import SCHEMAS


class DataTransformer:
    """Handle data cleaning and transformation tasks."""

    def __init__(self) -> None:
        """Initialize the DataTransformer."""
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Clean, transform, and merge the data."""
        self.logger.info('Starting data transformation.')

        try:
            # Validate input DataFrames
            self._validate_dataframes(data)

            # Proceed with transformation steps
            transformed_data = {}
            for dataset_name in ['transactions', 'accounts', 'customers']:
                df = data[dataset_name]
                df = self._clean_data(df, dataset_name)
                df = self._convert_date_columns(df, dataset_name)
                transformed_data[dataset_name] = df

            # Specific transformation for transactions
            transactions_df = transformed_data['transactions']
            transactions_df = self._add_year_month(transactions_df)
            transformed_data['transactions'] = transactions_df

            # Merge datasets
            merged_df = self._merge_datasets(
                transformed_data['transactions'],
                transformed_data['accounts'],
                transformed_data['customers'],
            )

            self.logger.info('Data transformation completed successfully.')

            return {'merged_data': merged_df}

        except Exception:
            self.logger.exception('Error during data transformation.')
            raise

    def _validate_dataframes(self, data: dict[str, DataFrame]) -> None:
        """Validate that the required DataFrames are present and not empty."""
        required_dfs = ['transactions', 'accounts', 'customers']
        for df_name in required_dfs:
            df = data.get(df_name)

            if df is None:
                error_msg = f'DataFrame "{df_name}" is missing.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            if df.rdd.isEmpty():
                error_msg = f'DataFrame "{df_name}" is empty.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)

    def _clean_data(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Clean DataFrame by handling missing data."""
        self.logger.info('Cleaning %s data.', dataset_name)

        return df.dropna()

    def _convert_date_columns(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Convert date columns to datetime objects."""
        self.logger.info('Converting date columns in %s data.', dataset_name)

        date_columns = self._get_date_columns(dataset_name)
        for date_col in date_columns:
            if date_col in df.columns:
                df = df.withColumn(date_col, to_timestamp(col(date_col)))

        return df

    @staticmethod
    def _get_date_columns(dataset_name: str) -> list[str]:
        """Return a list of date columns for a given dataset based on SCHEMAS."""
        schema = SCHEMAS.get(dataset_name)

        return [
            field.name for field in schema.fields
            if isinstance(field.dataType, DateType)
        ]

    def _add_year_month(self, df: DataFrame) -> DataFrame:
        """Add 'year_month' column in 'YYYY-MM' format to the transactions DataFrame."""
        self.logger.info('Adding year_month column to transactions data.')

        return df.withColumn(
            'year_month',
            date_format(col('transaction_date'), 'yyyy-MM'),
        )

    def _merge_datasets(self, transactions_df: DataFrame, accounts_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        """Merge the transactions, accounts, and customers DataFrames using inner joins."""
        self.logger.info('Merging datasets.')

        merged_df = transactions_df.join(
            accounts_df, on='account_id', how='inner',
        ).join(
            broadcast(customers_df), on='customer_id', how='inner',
        )

        # Drop any remaining nulls
        merged_df = merged_df.dropna()

        self.logger.info('Datasets merged successfully with no null values.')

        return merged_df
