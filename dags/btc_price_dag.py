import os
import pendulum
from src.utils.helpers import set_up_exchanges, get_request_time
from src.utils.price_extract import get_price
from airflow.sdk import Variable, dag, task


def compute_schedule(trans_time: int, num_trans: int):
    interval_minutes = max(1, (trans_time // num_trans) // 60)
    return pendulum.duration(minutes=interval_minutes)

@dag(
    schedule=compute_schedule(
        int(Variable.get("trans_time", default=180)), #time in sec
        int(Variable.get("num_trans", default=3))
    ),
    start_date=pendulum.datetime(2025, 9, 30, 11, 30, tz="Europe/London"),
    catchup=False,
    tags=["personal projects"],
)

def get_order_book_snapshots():
    
    exchanges = set_up_exchanges()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, "data")

    @task
    def fetch_snapshot(exchange):
        request_time = get_request_time()
        get_price(exchange, path, request_time)
        return f"{exchange.name} snapshot at {request_time}"

    fetch_snapshot.expand(exchange=exchanges)

dag_instance = get_order_book_snapshots()


















# import os
# import pendulum
# from src.utils.helpers import set_up_exchanges, get_request_time
# from src.utils.price_extract import get_price
# from airflow.sdk import Variable, dag, task

# @dag(
#     schedule=pendulum.duration(days=1),
#     start_date=pendulum.datetime(2025, 9, 30, 11, 30, tz="Europe/London"),
#     catchup=False,
#     tags=["personal projects"],
# )
# def get_best_price_for_required_order_volume():
#     """
#     Returns best price for the total order volume
#     """
#     order_volume = float(Variable.get("order_volume", default=0.5))
#     num_trans = int(Variable.get("num_trans", default=3))
#     vol_per_order = order_volume / num_trans

#     exchanges = set_up_exchanges()
        
#     base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     path = os.path.join(base_dir, "data")
 
#     @task
#     def prepare_exchange_calls(exchanges, num_trans):
#         return [(exchange, i) for i in range(num_trans) for exchange in exchanges]

#     @task
#     def get_request_time_task():
#         rt = get_request_time()
#         return rt

#     @task
#     def make_api_call(params, path, request_time):
#         exchange, iteration = params
#         return get_price(exchange, path, request_time)

#     calls = prepare_exchange_calls(exchanges, num_trans)
#     request_time = get_request_time_task()
#     make_api_call.expand(params=calls, path=path, request_time=request_time)

# dag_instance = get_best_price_for_required_order_volume()





    
#     @task()
#     def get_individual_quotes():    
#         """
#         Make individual API calls to 3 exchanges; repeat num_trans times
#         """
#         exchanges = set_up_exchanges()
        
#         base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#         path = os.path.join(base_dir, "data")

#         for i in range(0, num_trans):
#             print("Fetching the prices across exchanges...")
#             request_time = get_request_time()
#             print(request_time)

#             for exchange in exchanges:
#                 get_price (exchange, path, request_time)

#     get_individual_quotes()

# dag_instance = get_best_price_for_required_order_volume()    

