# Data Pipeline Project

## Overview
This project is a data pipeline designed to ingest, process, enrich, and export data. It is modular and uses PySpark for distributed processing and Delta Lake for handling large datasets. The pipeline includes configurable steps to transform data, analyze its content, and export it in Delta format.

## Features
- Data ingestion and transformation
- Enrichment and analysis of data
- Configurable through YAML files
- Logging and error handling
- Automated testing for each module

## Installation

### Requirements
- Python 3.9+
- PySpark
- Delta Lake
- Java (required for PySpark)

The dependencies are listed in the `requirements.txt` file. To install them, run:

```bash
pip install -r requirements.txt
```

### Directory Structure

```
data_pipeline/
├── src/
│   ├── __init__.py
│   ├── analyzer.py
│   ├── config_manager.py
│   ├── enricher.py
│   ├── exporter.py
│   ├── ingestor.py
│   ├── logger.py
│   ├── main.py
│   ├── schemas.py
│   ├── transformer.py
│   └── utils.py
├── scripts/
│   ├── __init__.py
│   └── converter.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_analyzer.py
│   ├── test_config_manager.py
│   ├── test_enricher.py
│   ├── test_exporter.py
│   ├── test_ingestor.py
│   ├── test_logger.py
│   ├── test_transformer.py
│   └── test_utils.py
├── config/
│   ├── config.yaml
│   └── logging.yaml
├── .ruff.toml
├── requirements.txt
└── .gitignore
```

## Usage

### Step 1: Data Conversion
Before running the main pipeline, you must convert your input data into a Parquet format using the `converter.py` script.

To run the converter:

```bash
python scripts/converter.py
```

### Step 2: Running the Pipeline
Once the data has been converted to Parquet format, you can execute the pipeline:

```bash
python src/main.py
```

This will initiate the full pipeline, including ingestion, transformation, enrichment, analysis, and exporting of the data. All configurations (e.g., input/output paths, logging levels) are defined in `config/config.yaml`.

### Logging
Logging is configured via `config/logging.yaml`. To customize log levels or output formats, modify this file.

## Testing
Automated tests are available for all key modules in the `tests/` directory. To run the tests, execute:

```bash
pytest
```

Ensure that your environment is properly configured with all necessary dependencies before running tests.

## Linting
Code linting and formatting are handled by `ruff`. To check for linting issues:

```bash
ruff check
```

## Conclusion
This pipeline is designed for scalable data processing and can be easily extended to handle more complex use cases. Please refer to the specific scripts in the `src/` directory for further customization.
