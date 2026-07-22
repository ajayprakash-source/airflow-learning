import os
import time

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.observability.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

from configs.constants import AWS_CONN_ID

logger = LoggingMixin().log


def download_sales_file(
    bucket_name: str,
    object_key: str,
    download_directory: str,
) -> str:
    """
    Download a file from S3.
    """

    start_time = time.time()

    try:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        logger.info(
            "Downloading %s from bucket %s",
            object_key,
            bucket_name,
        )

        downloaded_file = hook.download_file(
            key=object_key,
            bucket_name=bucket_name,
            local_path=download_directory,
            preserve_file_name=True,
        )

        if os.path.isabs(downloaded_file):
            full_path = downloaded_file
        else:
            full_path = os.path.join(
                download_directory,
                downloaded_file,
            )

        logger.info("Downloaded file: %s", full_path)

        Stats.incr("sales_pipeline.download.success")

        return full_path

    except Exception:
        Stats.incr("sales_pipeline.download.failed")
        raise

    finally:
        elapsed_ms = int((time.time() - start_time) * 1000)

        Stats.timing(
            "sales_pipeline.download.time_ms",
            elapsed_ms,
        )


def upload_processed_file(
    local_file_path: str,
    bucket_name: str,
    object_key: str,
) -> str:
    """
    Upload a processed file to S3.
    """

    start_time = time.time()

    try:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        logger.info(
            "Uploading %s to s3://%s/%s",
            local_file_path,
            bucket_name,
            object_key,
        )

        hook.load_file(
            filename=local_file_path,
            key=object_key,
            bucket_name=bucket_name,
            replace=True,
        )

        logger.info(
            "Upload completed successfully."
        )

        # Business metrics
        Stats.incr("sales_pipeline.upload.success")
        Stats.incr("sales_pipeline.files.processed")

        return object_key

    except Exception:
        Stats.incr("sales_pipeline.upload.failed")
        raise

    finally:
        elapsed_ms = int((time.time() - start_time) * 1000)

        Stats.timing(
            "sales_pipeline.upload.time_ms",
            elapsed_ms,
        )