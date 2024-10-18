from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.enricher import DataEnricher
from src.schemas import SCHEMAS


def test_enrich_data_success(sample_merged_df: DataFrame) -> None:
    """Test successful data enrichment."""
    enricher = DataEnricher()
    enriched = enricher.enrich_data({'merged_data': sample_merged_df})

    # Check keys in enrichment result
    assert 'classified_data' in enriched
    assert 'weekly_aggregates' in enriched
    assert 'largest_transactions' in enriched

    # Check classification
    classified_df = enriched['classified_data']

    assert 'classification' in classified_df.columns

    # Check classification values
    classifications = classified_df.select('classification').distinct().collect()
    classifications = [row['classification'] for row in classifications]

    assert set(classifications).issubset({'low', 'medium', 'high'})

    # Check weekly aggregates
    weekly_df = enriched['weekly_aggregates']

    assert 'classification' in weekly_df.columns
    assert 'week_number' in weekly_df.columns
    assert 'transaction_count' in weekly_df.columns

    # Check largest transactions
    largest_df = enriched['largest_transactions']

    assert 'transaction_id' in largest_df.columns
    assert 'amount' in largest_df.columns

    # Verify that largest transactions have the highest amount per classification
    for classification in classifications:
        class_transactions = largest_df.filter(col('classification') == classification).collect()
        if class_transactions:
            max_amount = class_transactions[0]['amount']

            assert all(row['amount'] == max_amount for row in class_transactions)


def test_enrich_data_missing_merged_data() -> None:
    """Test data enrichment when merged_data is missing."""
    enricher = DataEnricher()

    with pytest.raises(ValueError, match='Merged data is missing or empty.'):
        enricher.enrich_data({})


def test_enrich_data_empty_merged_data(spark: SparkSession) -> None:
    """Test data enrichment when merged_data is empty."""
    empty_df = spark.createDataFrame([], SCHEMAS['transactions'])
    enricher = DataEnricher()

    with pytest.raises(ValueError, match='Merged data is missing or empty.'):
        enricher.enrich_data({'merged_data': empty_df})


def test_classify_transactions(sample_merged_df: DataFrame) -> None:
    """Test the classification of transactions based on amount."""
    enricher = DataEnricher()
    classified_df = enricher._classify_transactions(sample_merged_df)

    # Check if 'classification' column exists
    assert 'classification' in classified_df.columns

    for row in classified_df.collect():
        amount = row['amount']
        if amount > Decimal('1000'):
            assert row['classification'] == 'high'
        elif Decimal('500') <= amount <= Decimal('1000'):
            assert row['classification'] == 'medium'
        else:
            assert row['classification'] == 'low'
