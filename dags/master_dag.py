# import pendulum
# from airflow.sdk import dag
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# @dag(
#     schedule=None,  # Trigger manually
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

# master_dag_instance = master_btc_etl()

# This is working version
# import time
# import logging
# import pendulum
# from airflow.decorators import dag, task
# from airflow.sdk import Variable
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# NUM_TRANS = int(Variable.get("num_trans", default=2))
# TRANS_TIME = int(Variable.get("trans_time", default=120))

# @dag(
#     schedule=None,
#     start_date=pendulum.datetime(2025, 10, 4, 12, 0, tz="UTC"),
#     catchup=False,
#     tags=["personal projects"],
# )
# def master_btc_etl():

#     trigger_extract = TriggerDagRunOperator(
#         task_id="trigger_extract_dag",
#         trigger_dag_id="extract_btc_snapshot",
#         wait_for_completion=True,
#         poke_interval=TRANS_TIME/NUM_TRANS,
#     )

#     trigger_extract_2 = TriggerDagRunOperator(
#         task_id="trigger_extract_dag_2",
#         trigger_dag_id="extract_btc_snapshot",
#         wait_for_completion=True,
#         poke_interval=TRANS_TIME/NUM_TRANS,
#     )

#     trigger_extract_3 = TriggerDagRunOperator(
#         task_id="trigger_extract_dag_3",
#         trigger_dag_id="extract_btc_snapshot",
#         wait_for_completion=True,
#         poke_interval=TRANS_TIME/NUM_TRANS,
#     )

#     trigger_extract >> trigger_extract_2 >> trigger_extract_3

# master_dag_instance = master_btc_etl()


import json
import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable
import os

NUM_TRANS = int(Variable.get("num_trans", default=3))
TRANS_TIME = int(Variable.get("trans_time", default=180))
BASE_PATH = "/home/alyona/personal_projects/airflow_learning_exercise/data"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 4, 12, 0, tz="UTC"),
    catchup=False,
    tags=["personal projects"],
)
def master_btc_etl():

    def trigger_extract(task_id_suffix):
        return TriggerDagRunOperator(
            task_id=f"trigger_extract_{task_id_suffix}",
            trigger_dag_id="extract_btc_snapshot",
            wait_for_completion=True,
            poke_interval=TRANS_TIME / NUM_TRANS,
        )

    trigger_1 = trigger_extract("1")
    trigger_2 = trigger_extract("2")
    trigger_3 = trigger_extract("3")

    @task
    def log_request_times():
        all_request_times = []

        # Read metadata files from each extract run
        for i in range(1, 4):
            metadata_file = os.path.join(BASE_PATH, f"snapshot_metadata.json")
            try:
                with open(metadata_file, "r") as f:
                    snapshots = json.load(f)
                    times = [snap["request_time"] for snap in snapshots]
                    all_request_times.extend(times)
            except FileNotFoundError:
                logging.warning(f"Metadata file not found for extract {i}: {metadata_file}")

        logging.info(f"These are request-times from all extracts: {all_request_times}")
        return all_request_times

    # Sequential execution
    trigger_1 >> trigger_2 >> trigger_3 >> log_request_times()

master_dag_instance = master_btc_etl()