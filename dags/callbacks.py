from airflow.sdk.observability.stats import Stats


def dag_success_callback(context):
    """
    Called when the DAG run completes successfully.
    """
    Stats.incr("sales_pipeline.pipeline.success")


def dag_failure_callback(context):
    """
    Called when the DAG run fails.
    """
    Stats.incr("sales_pipeline.pipeline.failed")