from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def start():
    print("Workflow Started")


def extract():
    print("Extracting data...")
    time.sleep(3)


def validate():
    print("Validating data...")
    time.sleep(4)


def transform():
    print("Transforming data...")
    time.sleep(5)


def upload():
    print("Uploading processed data...")
    time.sleep(2)


def end():
    print("Workflow Completed")


with DAG(
    dag_id="parallel_workflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "parallel"],
) as dag:

    t1 = PythonOperator(
        task_id="start",
        python_callable=start,
    )

    t2 = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t3 = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t4 = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t5 = PythonOperator(
        task_id="upload",
        python_callable=upload,
    )

    t6 = PythonOperator(
        task_id="end",
        python_callable=end,
    )

    t1 >> [t2, t3]
    [t2, t3] >> t4
    t4 >> t5 >> t6
