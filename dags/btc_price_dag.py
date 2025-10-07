import os
import pendulum
from src.utils.helpers import set_up_exchanges, get_request_time
from src.utils.price_extract import get_price
from airflow.sdk import dag, task

@dag(
    schedule=None,  # No automatic scheduling; triggered externally
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/London"),
    catchup=False,
    tags=["personal projects"],
)
def extract_btc_snapshot():
    exchanges = set_up_exchanges()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, "data")

    @task
    def fetch_snapshot(exchange):
        request_time = get_request_time()
        get_price(exchange, path, request_time)
        return {"exchange": exchange.name, "request_time": request_time, "file_path": path}

    fetch_snapshot.expand(exchange=exchanges)

dag_instance = extract_btc_snapshot()


# import os
# import pendulum
# from src.utils.helpers import set_up_exchanges, get_request_time
# from src.utils.price_extract import get_price
# from airflow.sdk import Variable, dag, task

# NUM_TRANS = int(Variable.get("num_trans", default=3))
# TRANS_TIME = int(Variable.get("trans_time", default=180))
# START_DATE=pendulum.datetime(2025, 9, 30, 11, 30, tz="Europe/London")
# END_DATE = START_DATE.add(seconds=TRANS_TIME + 110)


# def compute_schedule(trans_time: int, num_trans: int):
#     interval_minutes = max(1, (trans_time // num_trans) // 60)
#     return pendulum.duration(minutes=interval_minutes)

# @dag(
#     schedule=compute_schedule(TRANS_TIME,NUM_TRANS),
#     start_date=START_DATE,
#     end_date = END_DATE,
#     catchup=False,
#     tags=["personal projects"],
# )

# def get_order_book_snapshots():
#     NUM_TRANS = int(Variable.get("num_trans", default=3))
#     current_iter = int(Variable.get("current_iteration", default=0))

#     if current_iter >= NUM_TRANS:
#         print(f"Reached max iterations ({NUM_TRANS}). Exiting.")
#         return

#     exchanges = set_up_exchanges()
#     base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     path = os.path.join(base_dir, "data")

#     @task
#     def fetch_snapshot(exchange):
#         request_time = get_request_time()
#         get_price(exchange, path, request_time)
#         return f"{exchange.name} snapshot at {request_time}"

#     fetch_snapshot.expand(exchange=exchanges)
    
#     Variable.set("current_iteration", str(current_iter + 1))
#     print(f"Incremented iteration: {str(current_iter + 1)} / {NUM_TRANS}")

# dag_instance = get_order_book_snapshots()




# import os
# import pendulum
# from src.utils.helpers import set_up_exchanges, get_request_time
# from src.utils.price_extract import get_price
# from airflow.sdk import Variable, dag, task


# def compute_schedule(trans_time: int, num_trans: int):
#     interval_minutes = max(1, (trans_time // num_trans) // 60)
#     return pendulum.duration(minutes=interval_minutes)

# @dag(
#     schedule=compute_schedule(
#         int(Variable.get("trans_time", default=180)), #time in sec
#         int(Variable.get("num_trans", default=3))
#     ),
#     start_date=pendulum.datetime(2025, 9, 30, 11, 30, tz="Europe/London"),
#     catchup=False,
#     tags=["personal projects"],
# )

# def get_order_book_snapshots(num_trans):

#     num_trans = int(Variable.get("num_trans", default=3))
#     current_iter = int(Variable.get("current_iteration", default=0))

#     if current_iter >= num_trans:
#         print(f"Reached max iterations ({num_trans}). Exiting.")
#         return
    
#     exchanges = set_up_exchanges()
#     base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     path = os.path.join(base_dir, "data")

#     @task
#     def fetch_snapshot(exchange):
#         request_time = get_request_time()
#         get_price(exchange, path, request_time)
#         return f"{exchange.name} snapshot at {request_time}"

#     fetch_snapshot.expand(exchange=exchanges)
    
#     Variable.set("current_iteration", current_iter + 1)
#     print(f"Incremented iteration: {current_iter + 1} / {num_trans}")

# dag_instance = get_order_book_snapshots()














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

