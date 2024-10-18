import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from src.config_manager import ConfigManager
from src.logger import setup_logging
from src.schemas import SCHEMAS


class CSVToParquetConverter:
    """Convert CSV files to Parquet format using specified schemas."""

    def __init__(self, config_manager: ConfigManager) -> None:
        """Initialize the CSVToParquetConverter."""
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)

        # Initialize Spark session
        spark_config = self.config_manager.get('spark', {})
        spark_builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'CSVToParquetConverter')) \
            .master(spark_config.get('master', 'local[*]'))

        # Apply additional Spark configurations
        for key, value in spark_config.get('configs', {}).items():
            spark_builder = spark_builder.config(key, value)

        self.spark = spark_builder.getOrCreate()

    def convert_all(self) -> None:
        """Convert all CSV files to Parquet format."""
        try:
            data_paths = self.config_manager.get('data_paths', {})
            csv_paths = self.config_manager.get('csv_paths', {})
            datasets = ['transactions', 'accounts', 'customers']

            for dataset in datasets:
                csv_path = csv_paths.get(dataset)
                parquet_path = data_paths.get(dataset)
                schema = SCHEMAS.get(dataset)

                if not csv_path or not parquet_path or not schema:
                    error_msg = f'Paths or schema missing for dataset "{dataset}".'
                    self.logger.error(error_msg)
                    continue

                self.convert_csv_to_parquet(csv_path, parquet_path, schema)
        finally:
            self.spark.stop()

    def convert_csv_to_parquet(self, csv_path: str, parquet_path: str, schema: StructType) -> None:
        """Convert a CSV file to Parquet format."""
        self.logger.info('Starting conversion of %s to %s.', csv_path, parquet_path)

        if not Path(csv_path).exists():
            error_msg = f'CSV file not found: {csv_path}'
            self.logger.error(error_msg)
            return

        try:
            df = self.spark.read.csv(str(csv_path), header=True, schema=schema)
            df.write.parquet(str(parquet_path), mode='overwrite')

            self.logger.info('Successfully converted %s to %s.', csv_path, parquet_path)

        except Exception:
            self.logger.exception('Error converting %s to Parquet.', csv_path)
            raise


if __name__ == '__main__':
    config_manager = ConfigManager()

    setup_logging(default_path=config_manager.get('logging_config'))

    converter = CSVToParquetConverter(config_manager)
    converter.convert_all()
