import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config_manager import ConfigManager
from src.schemas import SCHEMAS


class DataIngestionError(Exception):
    """Custom exception for data ingestion errors."""


class DataIngestor:
    """Handle data loading and conversion to Spark DataFrames."""

    def __init__(self, config_manager: ConfigManager, spark: SparkSession) -> None:
        """Initialize the DataIngestor."""
        self.config_manager = config_manager
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def load_data(self) -> dict[str, DataFrame]:
        """Load datasets into Spark DataFrames."""
        self.logger.info('Loading data.')

        data_paths = self.config_manager.get('data_paths', {})
        required_files = ['transactions', 'accounts', 'customers']
        data = {}

        for file_key in required_files:
            file_path = data_paths.get(file_key)
            if not file_path:
                error_msg = f'Data path for "{file_key}" is not specified in the configuration.'
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            try:
                schema = SCHEMAS[file_key]
                df = self._load_data(str(file_path), schema)
                data[file_key] = df

            except FileNotFoundError:
                self.logger.exception('File not found: %s', file_path)
                raise

            except Exception:
                self.logger.exception('Unexpected error loading "%s".', file_key)
                raise

        self.logger.info('Data loaded successfully.')
        return data

    def _load_data(self, file_path: str, schema: StructType) -> DataFrame:
        """Load data into a Spark DataFrame with the specified schema."""
        if not Path(file_path).exists():
            error_msg = f'File not found: {file_path}'
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        try:
            df = self.spark.read.format('parquet').schema(schema).load(file_path)
            self.logger.debug('Loaded "%s" into Spark DataFrame.', file_path)
            return df

        except Exception as e:
            self.logger.exception('Error loading data from "%s".', file_path)
            error_msg = f'Error loading data from "{file_path}": {e}'
            raise DataIngestionError(error_msg) from e
