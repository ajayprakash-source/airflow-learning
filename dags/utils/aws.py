"""
Utility functions for interacting with AWS S3.

This module centralizes all AWS operations so that DAGs and task
modules remain clean and focused on orchestration.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.configs.constants import AWS_CONN_ID


def list_objects(bucket_name: str) -> list[str]:
    """
    List all objects present in an S3 bucket.

    Args:
        bucket_name: Name of the S3 bucket.

    Returns:
        List of object keys. Returns an empty list if the bucket is empty.
    """

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    objects = hook.list_keys(bucket_name=bucket_name)

    return objects or []