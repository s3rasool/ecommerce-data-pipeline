from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable



# for test
def done():
    print('Done')

def process_file_incrementally(file_path: str, variable_key: str):
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
    process_file_incrementally(
        file_path="/opt/airflow/data/products.csv",
        variable_key="products_csv_last_offset",
    )


def process_orders():
    process_file_incrementally(
        file_path="/opt/airflow/data/orders.csv",
        variable_key="orders_csv_last_offset",
    )


def process_users():
    process_file_incrementally(
        file_path="/opt/airflow/data/users.csv",
        variable_key="users_csv_last_offset",
    )


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
    
    done = PythonOperator(
        task_id="done",
        python_callable=done,
    )


    [process_products_task, process_orders_task, process_users_task] >> done
