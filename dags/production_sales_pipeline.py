from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.log.logging_mixin import LoggingMixin

from callbacks import (
    dag_success_callback,
    dag_failure_callback,
)

from configs.constants import (
    DAG_ID,
    DEFAULT_RETRIES,
    RETRY_DELAY_MINUTES,
    AWS_CONN_ID,
    INPUT_BUCKET,
    INPUT_FILE,
    SENSOR_TIMEOUT,
    SENSOR_POKE_INTERVAL,
    LOCAL_DOWNLOAD_PATH,
    PROCESSED_PREFIX,
)

from tasks.load import (
    download_sales_file,
    upload_processed_file,
)
from tasks.validation import validate_sales_file
from tasks.transformation import transform_sales_file

logger = LoggingMixin().log

default_args = {
    "owner": "platform-engineering",
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
}

with DAG(
    dag_id=DAG_ID,
    description="Production Sales Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
    tags=["production", "aws", "etl"],
):

    # ------------------------------------------------------------------
    # Wait until the expected sales file arrives in S3
    # ------------------------------------------------------------------
    wait_for_sales_file = S3KeySensor(
        task_id="wait_for_sales_file",
        bucket_name=INPUT_BUCKET,
        bucket_key=INPUT_FILE,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=SENSOR_POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        mode="reschedule",
    )

    # ------------------------------------------------------------------
    # Download the sales file from S3
    # ------------------------------------------------------------------
    @task
    def download_file() -> str:
        return download_sales_file(
            bucket_name=INPUT_BUCKET,
            object_key=INPUT_FILE,
            download_directory=LOCAL_DOWNLOAD_PATH,
        )

    # ------------------------------------------------------------------
    # Validate the downloaded CSV
    # ------------------------------------------------------------------
    @task
    def validate_file(file_path: str) -> str:
        return validate_sales_file(file_path)

    # ------------------------------------------------------------------
    # Transform the validated CSV
    # ------------------------------------------------------------------
    @task
    def transform_file(file_path: str) -> str:
        return transform_sales_file(file_path)

    # ------------------------------------------------------------------
    # Upload the processed CSV back to S3
    # ------------------------------------------------------------------
    @task
    def upload_file(processed_file: str) -> str:

        object_key = (
            PROCESSED_PREFIX
            + Path(processed_file).name
        )

        return upload_processed_file(
            local_file_path=processed_file,
            bucket_name=INPUT_BUCKET,
            object_key=object_key,
        )

    # ------------------------------------------------------------------
    # Task Dependencies
    # ------------------------------------------------------------------
    downloaded_file = download_file()

    validated_file = validate_file(downloaded_file)

    processed_file = transform_file(validated_file)

    uploaded_file = upload_file(processed_file)

    (
        wait_for_sales_file
        >> downloaded_file
        >> validated_file
        >> processed_file
        >> uploaded_file
    )