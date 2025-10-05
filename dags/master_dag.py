# import pendulum
# from airflow.sdk import dag, task
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.sdk import Variable

# # NUM_TRANS = int(Variable.get("num_trans", default=3))
# # TRANS_TIME = int(Variable.get("trans_time", default=180))

# @dag(
#     schedule=None,  # manually or via another schedule
#     start_date=pendulum.datetime(2025, 10, 4, 12, 0, tz="UTC"),
#     catchup=False,
#     tags=["personal projects"],
# )

# def master_btc_etl():

#     trigger_extract = TriggerDagRunOperator(
#         task_id="trigger_extract_dag",
#         trigger_dag_id="extract_btc_snapshot",  # The target DAG to trigger
#         wait_for_completion=True,                # Wait until extract DAG finishes
#         poke_interval=30,                        # Check every 30s (optional)
#     )

#     trigger_extract


# # def master_btc_etl():

# #     # Dynamically generate iteration numbers
# #     iterations = list(range(1, NUM_TRANS + 1))

# #     # Dynamically map TriggerDagRunOperator over each iteration
# #     trigger_extract = TriggerDagRunOperator.partial(
# #         task_id="trigger_extract_dag",
# #         trigger_dag_id="extract_btc_snapshot",  # your extract DAG id
# #         wait_for_completion=True,                # wait for each DAG to finish
# #     ).expand(
# #         conf=[{"iteration": i} for i in iterations]
# #     )

# master_dag_instance = master_btc_etl()

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 5, 12, 0, tz="UTC"),
    catchup=False,
)
def master_btc_etl():
    for i in range(3):
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_extract_{i+1}",
            trigger_dag_id="extract_btc_snapshot",
            wait_for_completion=True,
        )

        if i < 2:  # No wait after the last trigger
            wait = TimeDeltaSensor(
                task_id=f"wait_between_runs_{i+1}",
                delta=timedelta(minutes=1),
            )
            trigger >> wait