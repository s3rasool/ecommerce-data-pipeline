import sys
sys.path.append('/opt/scripts')
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from csv_extraction import extract_users, extract_orders, extract_products


def done():
    print('Done')


def track_csv_file(file_path: str, variable_key: str) -> bool:
    """
    Track new content in CSV file, reading only complete lines.
    Returns True if new content detected, False otherwise.
    """
    last_offset = int(Variable.get(variable_key, default_var=0))

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            f.seek(last_offset)
            new_data = f.read()
            
            # Only keep complete lines (ending with \n)
            if new_data and not new_data.endswith('\n'):
                last_newline = new_data.rfind('\n')
                if last_newline == -1:
                    print(f"[INFO] Incomplete line detected in {file_path}, skipping")
                    return False
                new_data = new_data[:last_newline + 1]
            
            new_offset = last_offset + len(new_data)

        if not new_data.strip():
            print(f"[INFO] No new content in {file_path}")
            return False

        print(f"[INFO] New content detected in {file_path}")
        Variable.set(variable_key, new_offset)
        return True

    except FileNotFoundError:
        print(f"[ERROR] File not found: {file_path}")
        raise
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise


def track_products_changes(**context):
    has_changes = track_csv_file(
        file_path="/opt/airflow/data/products.csv",
        variable_key="products_csv_last_offset",
    )
    return "extract_all_products" if has_changes else "done"


def track_orders_changes(**context):
    has_changes = track_csv_file(
        file_path="/opt/airflow/data/orders.csv",
        variable_key="orders_csv_last_offset",
    )
    return "extract_new_orders" if has_changes else "done"


def track_users_changes(**context):
    has_changes = track_csv_file(
        file_path="/opt/airflow/data/users.csv",
        variable_key="users_csv_last_offset",
    )
    return "extract_new_users" if has_changes else "done"


def extract_new_users():
    last_offset = int(Variable.get("last_user_offset", default=0))
    new_users_df, new_last_offset = extract_users(
        file_path="/opt/airflow/data/users.csv",
        last_processed_count=last_offset
    )
    if not new_users_df.empty:
        print(f"{len(new_users_df)} new users extracted.")
        Variable.set("last_user_offset", new_last_offset)
    else:
        print("No new users.")


def extract_new_orders():
    last_offset = int(Variable.get("last_order_offset", default=0))
    new_orders_df, new_last_offset = extract_orders(
        file_path="/opt/airflow/data/orders.csv",
        last_processed_count=last_offset
    )
    if not new_orders_df.empty:
        print(f"{len(new_orders_df)} new orders extracted.")
        Variable.set("last_order_offset", new_last_offset)
    else:
        print("No new orders.")


def extract_all_products():
    products_df = extract_products(file_path="/opt/airflow/data/products.csv")
    if not products_df.empty:
        print(f"{len(products_df)} products extracted.")
    else:
        print("No products available.")


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

    track_products_task = BranchPythonOperator(
        task_id="track_products_changes",
        python_callable=track_products_changes,
    )

    track_orders_task = BranchPythonOperator(
        task_id="track_orders_changes",
        python_callable=track_orders_changes,
    )

    track_users_task = BranchPythonOperator(
        task_id="track_users_changes",
        python_callable=track_users_changes,
    )

    extract_products_task = PythonOperator(
        task_id="extract_all_products",
        python_callable=extract_all_products,
        trigger_rule="none_failed",
    )

    extract_orders_task = PythonOperator(
        task_id="extract_new_orders",
        python_callable=extract_new_orders,
        trigger_rule="none_failed",
    )

    extract_users_task = PythonOperator(
        task_id="extract_new_users",
        python_callable=extract_new_users,
        trigger_rule="none_failed",
    )

    done_task = PythonOperator(
        task_id="done",
        python_callable=done,
        trigger_rule="none_failed",
    )

    # Branching: track → extract or skip to done
    track_products_task >> [extract_products_task, done_task]
    extract_products_task >> done_task

    track_orders_task >> [extract_orders_task, done_task]
    extract_orders_task >> done_task

    track_users_task >> [extract_users_task, done_task]
    extract_users_task >> done_task