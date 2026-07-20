from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin

from configs.constants import (
    DAG_ID,
    DEFAULT_RETRIES,
    INPUT_BUCKET,
    RETRY_DELAY_MINUTES,
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
    description="Verify AWS S3 connectivity",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["production", "aws", "verification"],
):

    @task
    def verify_s3_connectivity():

        logger.info("Connecting to bucket: %s", INPUT_BUCKET)

        objects = list_objects(INPUT_BUCKET)

        logger.info("Found %d object(s)", len(objects))

        if not objects:
            logger.warning("Bucket is empty.")
            return

        for obj in objects:
            logger.info("Object: %s", obj)

    verify_s3_connectivity()