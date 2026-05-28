from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator


# just for test
def done():
    print("All CSV files detected.")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="wait_for_all_csv_files",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_products = FileSensor(
        task_id="wait_for_products_csv",
        filepath="/opt/airflow/data/products.csv",
        poke_interval=300,
        timeout=600,
        mode="poke",
    )

    wait_for_orders = FileSensor(
        task_id="wait_for_orders_csv",
        filepath="/opt/airflow/data/orders.csv",
        poke_interval=300,
        timeout=600,
        mode="poke",
    )

    wait_for_users = FileSensor(
        task_id="wait_for_users_csv",
        filepath="/opt/airflow/data/users.csv",
        poke_interval=300,
        timeout=600,
        mode="poke",
    )

    finish = PythonOperator(
        task_id="finish",
        python_callable=done,
    )

    [wait_for_products, wait_for_orders, wait_for_users] >> finish
