from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id="kubernetes_batch_job",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kubernetes"],
):

    KubernetesPodOperator(
        task_id="hello_from_pod",
        namespace="airflow",
        image="busybox",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from Kubernetes Pod'; sleep 5"],
        get_logs=True,
        is_delete_operator_pod=True,
    )
