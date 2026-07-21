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

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket.
    object_key : str
        Object key in S3.
    download_directory : str
        Local directory where the file will be downloaded.

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

    # Some provider versions return only the filename,
    # while others return the absolute path.
    if os.path.isabs(downloaded_file):
        full_path = downloaded_file
    else:
        full_path = os.path.join(download_directory, downloaded_file)

    logger.info("Downloaded file: %s", full_path)

    return full_path


def upload_processed_file(
    local_file_path: str,
    bucket_name: str,
    object_key: str,
) -> str:
    """
    Upload a processed file to S3.

    Parameters
    ----------
    local_file_path : str
        Local path of the processed file.
    bucket_name : str
        Destination S3 bucket.
    object_key : str
        Destination object key.

    Returns
    -------
    str
        Uploaded S3 object key.
    """

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

    return object_key