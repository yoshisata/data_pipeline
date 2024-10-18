import logging

from pyspark.sql import SparkSession

from analyzer import DataAnalyzer
from config_manager import ConfigManager
from enricher import DataEnricher
from exporter import DataExporter
from ingestor import DataIngestor
from logger import setup_logging
from transformer import DataTransformer


def main() -> None:
    """Orchestrate the data pipeline."""
    config_manager = ConfigManager()

    setup_logging(default_path=config_manager.get('logging_config'))
    logger = logging.getLogger(__name__)

    logger.info('Starting the data pipeline.')

    spark = None

    try:
        # Initialize Spark session with Delta Lake configurations
        spark_config = config_manager.get('spark', {})
        spark_builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'DataPipeline')) \
            .master(spark_config.get('master', 'local[*]'))

        # Apply additional Spark configurations
        for key, value in spark_config.get('configs', {}).items():
            spark_builder = spark_builder.config(key, value)

        # Stop any existing Spark sessions
        existing_spark = SparkSession._instantiatedSession
        if existing_spark is not None:
            existing_spark.stop()

        # Create a new Spark session with the configurations
        spark = spark_builder.getOrCreate()

        ingestor = DataIngestor(config_manager, spark)
        data = ingestor.load_data()

        transformer = DataTransformer()
        transformed_data = transformer.transform_data(data)

        analyzer = DataAnalyzer()
        analysis_results = analyzer.analyze_data(transformed_data)

        enricher = DataEnricher()
        enriched_data = enricher.enrich_data(transformed_data)

        # Combine all data to export
        export_data = {**analysis_results, **enriched_data}

        exporter = DataExporter(config_manager)
        exporter.export_data(export_data)

        logger.info('Data pipeline completed successfully.')

    except Exception:
        logger.exception('Pipeline execution failed.')
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
