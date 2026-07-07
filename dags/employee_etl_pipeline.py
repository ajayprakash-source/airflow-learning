from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging


default_args = {
    "owner": "Jayprakash",
    "retries": 2,
}


@dag(
    dag_id="employee_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "taskflow", "intermediate"],
)
def employee_etl_pipeline():

    @task
    def extract():
        logging.info("Extracting employee records...")

        return [
            {"id": 1, "name": "alice", "salary": 52000},
            {"id": 2, "name": "bob", "salary": 61000},
            {"id": 3, "name": "charlie", "salary": 73000},
        ]

    @task
    def validate(records):

        logging.info("Validating records...")

        valid = []

        for record in records:
            if record["salary"] > 0:
                valid.append(record)

        return valid

    with TaskGroup("transformation_tasks"):

        @task
        def clean_names(records):

            for record in records:
                record["name"] = record["name"].title()

            logging.info("Names cleaned.")

            return records

        @task
        def normalize_salary(records):

            for record in records:
                record["salary_lpa"] = round(record["salary"] / 100000, 2)

            logging.info("Salary normalized.")

            return records

        @task
        def add_department(records):

            for record in records:
                record["department"] = "Engineering"

            logging.info("Department added.")

            return records

    @task
    def summary(records):

        logging.info("Pipeline Summary")

        logging.info(f"Employees Processed : {len(records)}")

        for employee in records:
            logging.info(employee)

    extracted = extract()

    validated = validate(extracted)

    cleaned = clean_names(validated)

    normalized = normalize_salary(cleaned)

    enriched = add_department(normalized)

    summary(enriched)


employee_etl_pipeline()
