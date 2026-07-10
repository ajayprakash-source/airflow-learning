from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import logging
from collections import Counter


default_args = {
    "owner": "Jayprakash",
    "retries": 2,
}


@dag(
    dag_id="sales_analytics_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "analytics", "sales"],
)
def sales_analytics_pipeline():

    # -------------------------
    # Extract
    # -------------------------

    @task
    def extract_sales():

        logging.info("Extracting today's sales data...")

        sales = [
            {
                "order_id": 101,
                "product": "Laptop",
                "quantity": 1,
                "price": 55000,
            },
            {
                "order_id": 102,
                "product": "Mouse",
                "quantity": 2,
                "price": 700,
            },
            {
                "order_id": 103,
                "product": "Keyboard",
                "quantity": 1,
                "price": 1500,
            },
            {
                "order_id": 104,
                "product": "Monitor",
                "quantity": 1,
                "price": 12000,
            },
        ]

        logging.info(f"Extracted {len(sales)} sales records.")

        return sales

    # -------------------------
    # Validate
    # -------------------------

    @task
    def validate_sales(sales):

        logging.info("Validating sales records...")

        valid_sales = []

        for sale in sales:

            if (
                sale["quantity"] > 0
                and sale["price"] > 0
                and sale["product"]
            ):
                valid_sales.append(sale)

        logging.info(f"{len(valid_sales)} valid records found.")

        return valid_sales

    # -------------------------
    # Transformations
    # -------------------------

    with TaskGroup(group_id="transformation_tasks"):

        @task
        def clean_sales(sales):

            logging.info("Cleaning sales records...")

            for sale in sales:
                sale["product"] = sale["product"].strip().title()

            logging.info("Sales records cleaned.")

            return sales

        @task
        def calculate_revenue(sales):

            total_revenue = 0

            for sale in sales:
                total_revenue += sale["price"] * sale["quantity"]

            logging.info(f"Total Revenue : ₹{total_revenue}")

            return {
                "sales": sales,
                "revenue": total_revenue,
            }

        @task
        def calculate_top_product(data):

            sales = data["sales"]

            counter = Counter()

            for sale in sales:
                counter[sale["product"]] += sale["quantity"]

            top_product = counter.most_common(1)[0][0]

            data["top_product"] = top_product

            logging.info(f"Top Product : {top_product}")

            return data

    # -------------------------
    # Report
    # -------------------------

    @task
    def generate_summary(data):

        sales = data["sales"]

        revenue = data["revenue"]

        top_product = data["top_product"]

        avg_order_value = revenue / len(sales)

        logging.info("========== SALES SUMMARY ==========")

        logging.info(f"Orders Processed : {len(sales)}")

        logging.info(f"Total Revenue : ₹{revenue}")

        logging.info(f"Average Order Value : ₹{avg_order_value:.2f}")

        logging.info(f"Best Selling Product : {top_product}")

        logging.info("===================================")

        return data

    # -------------------------
    # Load
    # -------------------------

    @task
    def store_results(data):

        logging.info("Storing analytics results...")

        logging.info(data)

        logging.info("Pipeline completed successfully.")

    # -------------------------
    # Workflow
    # -------------------------

    sales = extract_sales()

    validated_sales = validate_sales(sales)

    cleaned_sales = clean_sales(validated_sales)

    revenue_data = calculate_revenue(cleaned_sales)

    analytics_data = calculate_top_product(revenue_data)

    summary = generate_summary(analytics_data)

    store_results(summary)


sales_analytics_pipeline()