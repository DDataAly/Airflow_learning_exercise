import logging
import pendulum
from airflow.sdk import dag, task
from src.utils.helpers import set_up_exchanges, get_request_time
from src.utils.price_extract import get_price
from src.utils.price_transform import choose_best_price
from src.utils.db_load import load_quotes_to_db

@dag(
    schedule=pendulum.duration(minutes=5), 
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/London"),
    catchup=False,
    tags=["personal projects"],
)
def extract_btc_snapshot():
    
    exchanges = set_up_exchanges()

    @task
    def fetch_snapshots(exchange):
        request_time = get_request_time()
        result = get_price(exchange, request_time)
        if result is None:
            logging.warning(f"{exchange.name}: failed to fetch snapshot at {request_time}")
        else:
            logging.info(f"{exchange.name}: snapshot fetched successfully at {request_time}")
        return result
    # Dynamic mapping - Airflow creates an instance of fetch snapshot for each exchange in the exchanges
    fetched_data = fetch_snapshots.expand(exchange=exchanges)

    @task
    def transform_snapshots(fetched_data, vol_per_order: float):
        result = choose_best_price(exchanges, fetched_data, vol_per_order)
        return result
    vol_per_order = 0.01
    transformed = transform_snapshots(fetched_data, vol_per_order)

    @task
    def load_to_db(transformed, vol_per_order):
        return load_quotes_to_db(transformed, vol_per_order)
    vol_per_order = 0.01
    load_to_db(transformed, vol_per_order)


dag_instance = extract_btc_snapshot()

