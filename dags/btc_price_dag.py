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

    exchanges = set_up_exchanges()
        
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, "data")

    @task
    def api_calls_per_exchange(num_trans):
        return list(range(num_trans))  #[0, 1, 2]

    # @task
    # def get_request_time():
    #     print("Fetching the prices across exchanges...")
    #     request_time = get_request_time()
    #     print(request_time)
    #     return(request_time)

    @task
    def prepare_exchange_calls(exchanges, num_trans):
        return [(exchange, i) for i in range(num_trans) for exchange in exchanges]
    
    @task
    def make_api_call(exchange, path, request_time):
        return get_price (exchange, path, request_time)
    
    iterations = api_calls_per_exchange(num_trans = num_trans) #using task(param=value) Airflow syntax 
    # request_time = get_request_time()
    make_api_call.expand(exchange = exchanges, iteration = iterations) #mapping over multiple parameters -  nested loop logic but order of execution is not guaranteed; only keyword args are allowed











    
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

