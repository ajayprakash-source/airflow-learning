from pathlib import Path

import pandas as pd

from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def transform_sales_file(file_path: str) -> str:
    """
    Transform the validated sales CSV.

    Steps:
    1. Standardize column names
    2. Convert data types
    3. Add business columns
    4. Add processing metadata
    5. Save processed CSV

    Returns
    -------
    str
        Path to processed CSV.
    """

    logger.info("Reading file: %s", file_path)

    df = pd.read_csv(file_path)

    # ---------------------------------------------------------
    # Standardize column names
    # ---------------------------------------------------------
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # ---------------------------------------------------------
    # Convert datatypes
    # ---------------------------------------------------------
    df["quantity"] = df["quantity"].astype(int)
    df["price"] = df["price"].astype(float)

    # ---------------------------------------------------------
    # Business transformation
    # ---------------------------------------------------------
    df["total_amount"] = df["quantity"] * df["price"]

    # ---------------------------------------------------------
    # Processing metadata
    # ---------------------------------------------------------
    df["processed_timestamp"] = pd.Timestamp.utcnow().isoformat()

    output_path = (
        Path(file_path).parent
        / f"processed_{Path(file_path).name}"
    )

    logger.info("Writing processed file: %s", output_path)

    df.to_csv(output_path, index=False)

    logger.info("Transformation completed successfully.")

    return str(output_path)