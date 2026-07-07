from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="wait_for_s3_file",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "sensor", "learning"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )
    wait_for_file = S3KeySensor(
    task_id="wait_for_file",
    bucket_name="jayp-airflow",
    bucket_key="sample.txt",
    aws_conn_id="AWS_S3",
    poke_interval=10,
    timeout=300,
    )

    success = EmptyOperator(
        task_id = "success"
    )

    start >> wait_for_file >> success
