from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin

from configs.constants import AWS_CONN_ID

logger = LoggingMixin().log


def download_sales_file(
    bucket_name: str,
    object_key: str,
    local_path: str,
) -> str:
    """
    Download a file from S3.

    Returns:
        Local path of the downloaded file.
    """

    logger.info(
        "Downloading %s from bucket %s",
        object_key,
        bucket_name,
    )

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    hook.download_file(
        key=object_key,
        bucket_name=bucket_name,
        local_path=local_path,
        preserve_file_name=True,
    )

    logger.info("Download completed.")

    return local_path