import os

import pandas as pd

from airflow.exceptions import AirflowException
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

    if not os.path.exists(file_path):
        raise AirflowException(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    logger.info("Rows: %d", len(df))

    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)

    if missing_columns:
        raise AirflowException(
            f"Missing columns: {missing_columns}"
        )

    if df[REQUIRED_COLUMNS].isnull().any().any():
        raise AirflowException(
            "CSV contains missing values."
        )

    if df["order_id"].duplicated().any():
        raise AirflowException(
            "Duplicate order_id found."
        )

    if (df["quantity"] <= 0).any():
        raise AirflowException(
            "Quantity must be greater than zero."
        )

    if (df["price"] <= 0).any():
        raise AirflowException(
            "Price must be greater than zero."
        )

    logger.info("Validation successful.")

    return file_path