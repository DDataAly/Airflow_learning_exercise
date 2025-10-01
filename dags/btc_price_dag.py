import os
import pendulum
from src.utils.helpers import set_up_exchanges, get_request_time
from src.utils.price_extract import get_price
from airflow.sdk import Variable, dag, task

@dag(
    schedule=pendulum.duration(days=1),
    start_date=pendulum.datetime(2025, 9, 30, 11, 30, tz="Europe/London"),
    catchup=False,
    tags=["personal projects"],
)
def get_best_price_for_required_order_volume():
    """
    Returns best price for the total order volume
    """
    order_volume = float(Variable.get("order_volume", default=0.5))
    num_trans = int(Variable.get("num_trans", default=3))
    vol_per_order = order_volume / num_trans
    
    @task()
    def get_individual_quotes():    
        """
        Make individual API calls to 3 exchanges; repeat num_trans times
        """
        exchanges = set_up_exchanges()
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "data")

        for i in range(0, num_trans):
            print("Fetching the prices across exchanges...")
            request_time = get_request_time()
            print(request_time)

            get_price (exchanges, path, request_time)

    get_individual_quotes()

dag_instance = get_best_price_for_required_order_volume()    
