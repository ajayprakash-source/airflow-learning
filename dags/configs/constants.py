"""
Project-wide constants for the Production Sales Platform.
Keeping configuration values in one place makes the codebase
easier to maintain and avoids hardcoding strings.
"""

# Airflow Connection ID
AWS_CONN_ID = "AWS_S3"

# S3 Buckets
INPUT_BUCKET = "jp-airflow-sales-input-896586841767-eu-north-1-an"
OUTPUT_BUCKET = "jp-airflow-sales-output"
FAILED_BUCKET = "jp-airflow-sales-failed"

# DAG Information
DAG_ID = "production_sales_pipeline"

# Default Retry Configuration
DEFAULT_RETRIES = 2
RETRY_DELAY_MINUTES = 5