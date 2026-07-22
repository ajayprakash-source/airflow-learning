import os
import time

import pandas as pd

from airflow.exceptions import AirflowException
from airflow.sdk.observability.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

REQUIRED_COLUMNS = [
    "order_id",
    "customer",
    "product",
    "quantity",
    "price",
]


def validate_sales_file(file_path: str) -> str:
    """
    Validate the downloaded sales CSV.

    Returns
    -------
    str
        Same file path if validation succeeds.
    """

    logger.info("Validating file: %s", file_path)

    start_time = time.time()

    try:
        if not os.path.exists(file_path):
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(f"File not found: {file_path}")

        df = pd.read_csv(file_path)

        logger.info("Rows: %d", len(df))

        # Record number of rows processed
        Stats.gauge(
            "sales_pipeline.rows.processed",
            len(df),
        )

        missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)

        if missing_columns:
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(
                f"Missing columns: {missing_columns}"
            )

        if df[REQUIRED_COLUMNS].isnull().any().any():
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(
                "CSV contains missing values."
            )

        if df["order_id"].duplicated().any():
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(
                "Duplicate order_id found."
            )

        if (df["quantity"] <= 0).any():
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(
                "Quantity must be greater than zero."
            )

        if (df["price"] <= 0).any():
            Stats.incr("sales_pipeline.validation.failed")
            raise AirflowException(
                "Price must be greater than zero."
            )

        logger.info("Validation successful.")

        # Record successful validation
        Stats.incr("sales_pipeline.validation.success")

        return file_path

    finally:
        elapsed_ms = int((time.time() - start_time) * 1000)

        Stats.timing(
            "sales_pipeline.validation.time_ms",
            elapsed_ms,
        )

        logger.info(
            "Validation completed in %d ms",
            elapsed_ms,
        )