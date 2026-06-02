import sys
sys.path.append('/opt/scripts')

from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

from csv_extraction import extract_users, extract_orders, extract_products


def track_csv_file(file_path: str, variable_key: str):
    last_processed_count = int(Variable.get(variable_key, default_var=0))

    try:
        new_df = pd.read_csv(
            file_path,
            skiprows=range(1, last_processed_count + 1)
        )
    except pd.errors.EmptyDataError:
        print(f"[ERROR] {file_path} is empty.")
        return False

    if new_df.empty:
        print(f"[INFO] No new rows in {file_path}")
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


def extract_new_users():
    last_row = int(Variable.get("users_csv_last_row", default_var=0))

    new_users_df, new_last_row = extract_users(
        file_path="/opt/airflow/data/users.csv",
        last_processed_count=last_row
    )

    if not new_users_df.empty:
        print(f"[INFO] {len(new_users_df)} new users extracted.")
    else:
        print("[INFO] No new valid users.")

    Variable.set("[INFO] users_csv_last_row", new_last_row)


def extract_new_orders():
    last_row = int(Variable.get("orders_csv_last_row", default_var=0))

    new_orders_df, new_last_row = extract_orders(
        file_path="/opt/airflow/data/orders.csv",
        last_processed_count=last_row
    )

    if not new_orders_df.empty:
        print(f"[INFO] {len(new_orders_df)} new orders extracted.")
    else:
        print("[INFO] No new valid orders.")

    Variable.set("orders_csv_last_row", new_last_row)


def extract_all_products():
    products_df = extract_products(
        file_path="/opt/airflow/data/products.csv"
    )

    if not products_df.empty:
        print(f"[INFO] {len(products_df)} products extracted.")
        print(products_df)
    else:
        print("[INFO] No products available.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="wait_for_csv_file_changes",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["csv", "tracker", "extractor"],
) as dag:

    track_users_task = ShortCircuitOperator(
        task_id="track_users_changes",
        python_callable=track_users_changes,
    )

    extract_users_task = PythonOperator(
        task_id="extract_new_users",
        python_callable=extract_new_users,
    )

    track_orders_task = ShortCircuitOperator(
        task_id="track_orders_changes",
        python_callable=track_orders_changes,
    )

    extract_orders_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract_new_orders,
    )

    extract_products_task = PythonOperator(
        task_id="extract_all_products",
        python_callable=extract_all_products,
    )

    track_users_task >> extract_users_task
    track_orders_task >> extract_orders_task
