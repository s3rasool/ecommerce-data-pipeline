import sys
sys.path.append('/opt/scripts')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from csv_extraction import extract_users, extract_orders, extract_products



# for test
def done():
    print('Done')

def track_csv_file(file_path: str, variable_key: str):
    last_offset = int(Variable.get(variable_key, default_var=0))

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            f.seek(last_offset)
            new_data = f.read()
            new_offset = f.tell()

        if not new_data.strip():
            print(f"[INFO] No new content in {file_path}")
            print(f"[INFO] Last offset remains: {last_offset}")
            return

        print(f"[INFO] New content detected in {file_path}")
        print(f"[INFO] Previous offset: {last_offset}")
        print(f"[INFO] New offset: {new_offset}")
        print("[INFO] Newly appended content:")
        print(new_data)

        Variable.set(variable_key, new_offset)
        print(f"[INFO] Updated Airflow Variable '{variable_key}' to {new_offset}")

    except FileNotFoundError:
        print(f"[ERROR] File not found: {file_path}")
        raise
    except Exception as e:
        print(f"[ERROR] Unexpected error while processing {file_path}: {e}")
        raise


def process_products():
    track_csv_file(
        file_path="/opt/airflow/data/products.csv",
        variable_key="products_csv_last_offset",
    )


def process_orders():
    track_csv_file(
        file_path="/opt/airflow/data/orders.csv",
        variable_key="orders_csv_last_offset",
    )


def process_users():
    track_csv_file(
        file_path="/opt/airflow/data/users.csv",
        variable_key="users_csv_last_offset",
    )


def extract_new_users():
    last_offset = int(Variable.get("last_user_offset", default=0))
    new_users_df, new_last_offset = extract_users(file_path="/opt/airflow/data/users.csv", last_processed_count=last_offset)
    if not new_users_df.empty:
        print(f"{len(new_users_df)} new users extracted.")
        Variable.set("last_user_offset", new_last_offset)
    else:
        print("no new users.")


def extract_new_orders():
    last_offset = int(Variable.get("last_order_offset", default=0))
    new_orders_df, new_last_offset = extract_orders(file_path="/opt/airflow/data/orders.csv", last_processed_count=last_offset)
    if not new_orders_df.empty:
        print(f"{len(new_orders_df)} new orders extracted.")
        Variable.set("last_order_offset", new_last_offset)
    else:
        print("no new orders.")


def extract_all_products():
    products_df = extract_products(file_path="/opt/airflow/data/products.csv")
    if not products_df.empty:
        print(f"{len(products_df)} products extracted.")
    else:
        print("no products available.")


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

    process_products_task = PythonOperator(
        task_id="process_products_csv",
        python_callable=process_products,
    )

    process_orders_task = PythonOperator(
        task_id="process_orders_csv",
        python_callable=process_orders,
    )

    process_users_task = PythonOperator(
        task_id="process_users_csv",
        python_callable=process_users,
    )

    extract_new_users_task = PythonOperator(
        task_id="extract_new_users",
        python_callable=extract_new_users,
    )
    
    extract_new_orders_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract_new_orders,
    )

    extract_all_products_task = PythonOperator(
        task_id="extract_all_products",
        python_callable=extract_all_products,
    )

    done = PythonOperator(
        task_id="done",
        python_callable=done,
    )


    [process_products_task, process_orders_task, process_users_task] >> done
