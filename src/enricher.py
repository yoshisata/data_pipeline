import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, row_number, weekofyear, when
from pyspark.sql.window import Window

HIGH_AMOUNT_THRESHOLD = 1000
MEDIUM_AMOUNT_THRESHOLD = 500


class DataEnricher:
    """Perform data enrichment tasks on the analyzed data."""

    def __init__(self) -> None:
        """Initialize the DataEnricher."""
        self.logger = logging.getLogger(__name__)

    def enrich_data(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Perform enrichment on the analyzed data."""
        self.logger.info('Starting data enrichment.')

        def raise_value_error(msg: str) -> None:
            self.logger.error(msg)
            raise ValueError(msg)

        try:
            merged_df = data.get('merged_data')
            if merged_df is None or merged_df.rdd.isEmpty():
                raise_value_error('Merged data is missing or empty.')

            # Proceed with enrichment steps
            classified_df = self._classify_transactions(merged_df)
            weekly_aggregates = self._aggregate_transactions_per_week(classified_df)
            largest_transactions = self._find_largest_transactions(classified_df)

            self.logger.info('Data enrichment completed successfully.')
            return {
                'classified_data': classified_df,
                'weekly_aggregates': weekly_aggregates,
                'largest_transactions': largest_transactions,
            }

        except Exception:
            self.logger.exception('Error during data enrichment.')
            raise

    def _classify_transactions(self, df: DataFrame) -> DataFrame:
        """Add a classification column to the transactions DataFrame."""
        self.logger.info('Classifying transactions based on amount.')

        # Add 'classification' column
        return df.withColumn(
            'classification',
            when(col('amount') > HIGH_AMOUNT_THRESHOLD, 'high')
            .when((col('amount') >= MEDIUM_AMOUNT_THRESHOLD) & (col('amount') <= HIGH_AMOUNT_THRESHOLD), 'medium')
            .otherwise('low'),
        )

    def _aggregate_transactions_per_week(self, df: DataFrame) -> DataFrame:
        """Aggregate the number of transactions per classification per week."""
        self.logger.info('Aggregating number of transactions per classification per week.')

        # Extract week number from 'transaction_date'
        df = df.withColumn('week_number', weekofyear(col('transaction_date')))

        # Group by 'classification' and 'week_number', count transactions
        return df.groupBy('classification', 'week_number').agg(
            count('*').alias('transaction_count'),
        )

    def _find_largest_transactions(self, df: DataFrame) -> DataFrame:
        """Identify the transactions with the largest amount per classification."""
        self.logger.info('Finding transactions with the largest amount per classification.')

        # Rank transactions within each classification
        window_spec = Window.partitionBy('classification').orderBy(col('amount').desc())

        ranked_df = df.withColumn('rank', row_number().over(window_spec))

        # Filter to get the top transaction per classification
        return ranked_df.filter(col('rank') == 1).drop('rank')
