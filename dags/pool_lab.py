from datetime import datetime
import time

from airflow.sdk import DAG
from airflow.decorators import task

with DAG(
    dag_id="pool_lab",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pool", "lab"],
):

    @task(pool="database_pool")
    def simulate_task(task_number):
        print(f"Task {task_number} started")
        time.sleep(120)
        print(f"Task {task_number} finished")

    for i in range(20):
        simulate_task.override(
            task_id=f"task_{i}"
        )(task_number=i)
