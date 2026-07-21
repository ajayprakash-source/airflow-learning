import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

    Returns
    -------
    str
        Absolute path of the downloaded file.
    """

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

    full_path = os.path.join(download_directory, downloaded_file)

    logger.info("Downloaded file: %s", full_path)

    return full_path