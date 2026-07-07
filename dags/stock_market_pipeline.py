from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="stock_market_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stock-market", "learning"],
)
def stock_market_pipeline():
    
    @task
    def is_api_available():
        print("checking api")

    @task
    def fetch_stock_prices():
        print("fetching prices")

    @task
    def store_prices():
        print("storing raw prices")

    @task
    def format_prices():
        print("formatting prices")

    @task
    def get_formatted_csv():
        print("generating csv")

    @task
    def load_to_dw():
        print("loading into data warehouse")

    (
        is_api_available()
        >> fetch_stock_prices()
        >> store_prices()
        >> format_prices()
        >> get_formatted_csv()
        >> load_to_dw()
    )

stock_market_pipeline()    
