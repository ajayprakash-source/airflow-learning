from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.log.logging_mixin import LoggingMixin

from configs.constants import (
    DAG_ID,
    DEFAULT_RETRIES,
    RETRY_DELAY_MINUTES,
    AWS_CONN_ID,
    INPUT_BUCKET,
    INPUT_FILE,
    SENSOR_TIMEOUT,
    SENSOR_POKE_INTERVAL,
)
from utils.aws import list_objects

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
    tags=["production", "aws", "s3"],
):

    # -------------------------------------------------------------------------
    # Wait until the expected sales file arrives in S3
    # -------------------------------------------------------------------------
    wait_for_sales_file = S3KeySensor(
        task_id="wait_for_sales_file",
        bucket_name=INPUT_BUCKET,
        bucket_key=INPUT_FILE,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=SENSOR_POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        mode="reschedule",
    )

    # -------------------------------------------------------------------------
    # List files available in the bucket
    # -------------------------------------------------------------------------
    @task
    def list_sales_files():
        logger.info("Listing objects in bucket: %s", INPUT_BUCKET)

        objects = list_objects(INPUT_BUCKET)

        logger.info("Found %d object(s)", len(objects))

        for obj in objects:
            logger.info("Object: %s", obj)

        return objects

    list_files = list_sales_files()

    wait_for_sales_file >> list_files