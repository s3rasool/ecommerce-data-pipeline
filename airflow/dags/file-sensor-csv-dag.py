import sys
sys.path.append('/opt/scripts')

import os
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.sdk import Variable



def track_csv_file(file_path: str, variable_key: str):
    last_processed_count = int(Variable.get(variable_key, default="0"))

    try:
        new_df = pd.read_csv(
            file_path,
            skiprows=range(1, last_processed_count + 1)
        )
    except pd.errors.EmptyDataError:
        print(f"[ERROR] {file_path} is empty.")
        return False

    if new_df.empty:
        print(f"[WARN] No new rows in {file_path}")
        return False

    print(f"[INFO] Found {len(new_df)} new raw rows in {file_path}")
    print(new_df)

    return True


def track_orders_changes():
    return track_csv_file(
        file_path="/opt/airflow/data/orders.csv",
        variable_key="orders_csv_last_row",
    )


def track_users_changes():
    return track_csv_file(
        file_path="/opt/airflow/data/users.csv",
        variable_key="users_csv_last_row",
    )


def track_any_csv_changes():
    users_changed = track_users_changes()
    orders_changed = track_orders_changes()

    if users_changed or orders_changed:
        print("[INFO] At least one CSV file has new rows. Triggering load DAG.")
        return True

    print("[WARN] No new rows found in users.csv or orders.csv.")
    return False


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="wait_for_csv_file_changes",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["csv", "tracker", "extractor"],
) as dag:

    track_any_changes_task = ShortCircuitOperator(
        task_id="track_any_csv_changes",
        python_callable=track_any_csv_changes,
    )

    trigger_load_to_postgres_dag = TriggerDagRunOperator(
        task_id="trigger_load_transitional_to_postgres",
        trigger_dag_id="load_transitional_to_postgres",
        wait_for_completion=False,
        reset_dag_run=False,
        conf={
            "triggered_by": "wait_for_csv_file_changes"
        },
    )

    track_any_changes_task >> trigger_load_to_postgres_dag
