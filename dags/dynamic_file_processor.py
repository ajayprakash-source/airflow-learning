from airflow.decorators import dag, task
from datetime import datetime
import logging


@dag(
    dag_id="dynamic_file_processor",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dynamic", "mapping"],
)
def dynamic_file_processor():

    @task
    def discover_files():
        return [
            "employees.csv",
            "sales.csv",
            "products.csv",
            "inventory.csv",
            "customers.csv",
        ]

    @task
    def process_file(file_name):

        logging.info(f"Processing {file_name}")

        return {
            "file": file_name,
            "status": "Processed",
            "rows": len(file_name) * 100,
        }

    @task
    def combine_results(results):

        logging.info("Processing Summary")

        total_rows = 0

        for result in results:
            logging.info(result)
            total_rows += result["rows"]

        logging.info(f"Total Rows Processed: {total_rows}")

    files = discover_files()

    processed = process_file.expand(file_name=files)

    combine_results(processed)


dynamic_file_processor()
