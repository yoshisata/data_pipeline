import logging

from pyspark.sql import DataFrame

from src.config_manager import ConfigManager


class DataExporter:
    """Handle exporting data to Delta files."""

    def __init__(self, config_manager: ConfigManager) -> None:
        """Initialize the DataExporter."""
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)

    def export_data(self, data: dict[str, DataFrame]) -> None:
        """Export data to Delta files."""
        self.logger.info('Starting data export.')

        def raise_value_error_for_key(key: str) -> None:
            error_msg = f'Export path for "{key}" is not specified.'
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            export_paths = self.config_manager.get('export_paths', {})
            for key, df in data.items():
                export_path = export_paths.get(key)
                if not export_path:
                    raise_value_error_for_key(key)

                self._export_to_delta(df, str(export_path))

            self.logger.info('Data export completed successfully.')

        except Exception:
            self.logger.exception('Error during data export.')
            raise

    def _export_to_delta(self, df: DataFrame, path: str) -> None:
        """Export a DataFrame to a Delta file."""
        self.logger.info('Exporting data to %s.', path)

        try:
            if df is None or df.rdd.isEmpty():
                self.logger.warning('No data to export to %s. Skipping.', path)
                return

            df.write.format('delta').mode('overwrite').save(path)
            self.logger.info('Data exported to Delta table at %s.', path)

        except Exception:
            self.logger.exception('Failed to export data to %s.', path)
            raise
