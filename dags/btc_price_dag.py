import pendulum
import json
from airflow.sdk import dag, task
from airflow.models import Variable
from src.utils.helpers import set_up_exchanges, get_request_time
from src.utils.price_extract import get_price
from src.utils.price_transform import choose_best_price
from src.utils.db_load import load_quotes_to_db

# This DAG uses two variables
# vol_per_order - the amount of BTC we get quotes for
# exchanges_list - the list of exchanges we get quotes from 
vol_per_order = float(Variable.get("vol_per_order", default_var=0.01)) # Airflow variable returns a string but float() converts it to the numerical format
exchanges_variable = Variable.get('exchanges_list', default_var = ['kraken','binance', 'coinbase'])
exchanges_names = json.loads(exchanges_variable) # Need json loads as variable is returned as json string

@dag(
    schedule=pendulum.duration(minutes=5), 
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/London"),
    catchup=False,
    tags=["personal projects"],
)
def extract_btc_snapshot():
    
    exchanges = set_up_exchanges(exchanges_names)

    @task
    def fetch_snapshots(exchange):
        request_time = get_request_time()
        return get_price(exchange, request_time)
    # Dynamic mapping - Airflow creates an instance of fetch snapshot for each exchange in the exchanges
    fetched_data = fetch_snapshots.expand(exchange=exchanges)

    @task
    def transform_snapshots(fetched_data, vol_per_order):
        return choose_best_price(exchanges, fetched_data, vol_per_order)
    transformed = transform_snapshots(fetched_data, vol_per_order)

    @task
    def load_to_db(transformed, vol_per_order):
        return load_quotes_to_db(transformed, vol_per_order)
    load_to_db(transformed, vol_per_order)

dag_instance = extract_btc_snapshot()

