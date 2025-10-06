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


import time
import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 10, 4, 12, 0, tz="UTC"),
    catchup=False,
    tags=["personal projects"],
)
def master_btc_etl():

    @task
    def wait_90_seconds():
        logging.info("Waiting 90 seconds as requested...")
        time.sleep(90)
        logging.info("Wait complete.")

    # Trigger operator goes directly here, not in a @task function
    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract_dag",
        trigger_dag_id="extract_btc_snapshot",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Set dependencies using TaskFlow-style >> operator
    trigger_extract >> wait_90_seconds()

master_dag_instance = master_btc_etl()