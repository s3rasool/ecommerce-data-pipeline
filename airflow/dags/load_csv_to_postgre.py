import os
import time
import io
import logging
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2026, 5, 29),
}


USERS_FILE = "/opt/airflow/data/users.csv"
ORDERS_FILE = "/opt/airflow/data/orders.csv"
PRODUCTS_FILE = "/opt/airflow/data/products.csv"


def is_file_complete(file_path, stable_time=5):
    if not os.path.exists(file_path):
        logging.info("File does not exist yet: %s", file_path)
        return False

    size1 = os.path.getsize(file_path)
    time.sleep(stable_time)
    size2 = os.path.getsize(file_path)

    complete = size1 == size2 and size1 > 0

    logging.info(
        "File completeness check for %s: size1=%s, size2=%s, complete=%s",
        file_path,
        size1,
        size2,
        complete,
    )

    return complete


def count_csv_data_rows(file_path):
    if not os.path.exists(file_path):
        return 0

    try:
        df = pd.read_csv(file_path)
        return len(df)
    except pd.errors.EmptyDataError:
        return 0


def track_csv_file(file_path: str, variable_key: str):
    last_processed_count = int(Variable.get(variable_key, default="0"))

    try:
        new_df = pd.read_csv(
            file_path,
            skiprows=range(1, last_processed_count + 1),
        )
    except pd.errors.EmptyDataError:
        logging.warning("%s is empty.", file_path)
        return False
    except FileNotFoundError:
        logging.warning("%s does not exist.", file_path)
        return False

    if new_df.empty:
        logging.info("No new rows in %s", file_path)
        return False

    logging.info("Found %s new raw rows in %s", len(new_df), file_path)
    return True


def check_any_changes(**kwargs):
    users_changed = track_csv_file(
        file_path=USERS_FILE,
        variable_key="users_csv_last_row",
    )

    orders_changed = track_csv_file(
        file_path=ORDERS_FILE,
        variable_key="orders_csv_last_row",
    )

    products_loaded = (
        Variable.get("products_csv_loaded", default="false").lower() == "true"
    )

    kwargs["ti"].xcom_push(key="users_changed", value=users_changed)
    kwargs["ti"].xcom_push(key="orders_changed", value=orders_changed)
    kwargs["ti"].xcom_push(key="products_loaded", value=products_loaded)

    should_continue = users_changed or orders_changed or not products_loaded

    logging.info(
        "check_any_changes => users_changed=%s, orders_changed=%s, products_loaded=%s, should_continue=%s",
        users_changed,
        orders_changed,
        products_loaded,
        should_continue,
    )

    return should_continue


def branch_products_load(**kwargs):
    products_loaded = kwargs["ti"].xcom_pull(
        task_ids="check_any_csv_changes",
        key="products_loaded",
    )

    if products_loaded:
        logging.info("Products already loaded. Skipping products branch.")
        return "skip_products_load"

    return "wait_for_products_file"


def branch_users_load(**kwargs):
    users_changed = kwargs["ti"].xcom_pull(
        task_ids="check_any_csv_changes",
        key="users_changed",
    )

    if users_changed:
        return "wait_for_users_file"

    logging.info("Users unchanged. Skipping users load.")
    return "skip_users_load"


def branch_orders_load(**kwargs):
    orders_changed = kwargs["ti"].xcom_pull(
        task_ids="check_any_csv_changes",
        key="orders_changed",
    )

    if orders_changed:
        return "wait_for_orders_file"

    logging.info("Orders unchanged. Skipping orders load.")
    return "skip_orders_load"


def load_csv_to_postgres(
    table_name,
    file_path,
    columns,
    conflict_columns,
    variable_key=None,
):
    """
    Loads only new CSV rows into PostgreSQL.

    For users/orders:
      - variable_key is used to read the last processed row count.
      - only rows after that count are loaded.

    For products:
      - variable_key can be None.
      - the full file is loaded once.
    """

    last_processed_count = 0

    if variable_key is not None:
        last_processed_count = int(Variable.get(variable_key, default="0"))

    try:
        new_df = pd.read_csv(
            file_path,
            skiprows=range(1, last_processed_count + 1),
        )
    except pd.errors.EmptyDataError:
        logging.warning("%s is empty. Nothing to load.", file_path)
        return 0
    except FileNotFoundError:
        logging.warning("%s does not exist. Nothing to load.", file_path)
        return 0

    if new_df.empty:
        logging.info("No new rows to load from %s into raw.%s", file_path, table_name)
        return 0

    logging.info(
        "Loading %s new rows from %s into raw.%s",
        len(new_df),
        file_path,
        table_name,
    )

    pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn = pg_hook.get_conn()
    conn.autocommit = False
    cursor = conn.cursor()

    temp_table = f"temp_{table_name}_{int(time.time())}"
    column_names = ", ".join(columns)
    conflict_clause = ", ".join(conflict_columns)

    try:
        cursor.execute(
            f"""
            CREATE TEMP TABLE {temp_table}
            (LIKE raw.{table_name} INCLUDING DEFAULTS)
            ON COMMIT DROP;
            """
        )

        csv_buffer = io.StringIO()
        new_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        copy_sql = f"""
            COPY {temp_table} ({column_names})
            FROM STDIN
            WITH (FORMAT CSV, HEADER true, DELIMITER ',')
        """

        cursor.copy_expert(copy_sql, csv_buffer)

        insert_sql = f"""
            INSERT INTO raw.{table_name} ({column_names})
            SELECT {column_names}
            FROM {temp_table}
            ON CONFLICT ({conflict_clause}) DO NOTHING;
        """

        cursor.execute(insert_sql)

        inserted_or_attempted_rows = cursor.rowcount

        conn.commit()

        logging.info(
            "Finished loading %s. New CSV rows=%s, inserted rows=%s",
            table_name,
            len(new_df),
            inserted_or_attempted_rows,
        )

        return len(new_df)

    except Exception as e:
        conn.rollback()
        logging.error("Error loading %s into raw.%s: %s", file_path, table_name, e)
        raise

    finally:
        cursor.close()
        conn.close()


def load_users(**kwargs):
    return load_csv_to_postgres(
        table_name="users",
        file_path=USERS_FILE,
        columns=[
            "user_id",
            "name",
            "email",
            "signup_date",
            "device",
            "loyalty_tier",
            "location",
        ],
        conflict_columns=["user_id"],
        variable_key="users_csv_last_row",
    )


def load_products(**kwargs):
    return load_csv_to_postgres(
        table_name="products",
        file_path=PRODUCTS_FILE,
        columns=[
            "product_id",
            "name",
            "price",
            "category",
            "inventory",
            "popularity_score",
        ],
        conflict_columns=["product_id"],
        variable_key=None,
    )


def load_orders(**kwargs):
    return load_csv_to_postgres(
        table_name="orders",
        file_path=ORDERS_FILE,
        columns=[
            "order_id",
            "user_id",
            "timestamp",
            "total",
            "status",
            "payment_method",
        ],
        conflict_columns=["order_id"],
        variable_key="orders_csv_last_row",
    )


def update_users_last_row(**kwargs):
    row_count = count_csv_data_rows(USERS_FILE)
    Variable.set("users_csv_last_row", str(row_count))
    logging.info("Updated users_csv_last_row to %s", row_count)


def update_orders_last_row(**kwargs):
    row_count = count_csv_data_rows(ORDERS_FILE)
    Variable.set("orders_csv_last_row", str(row_count))
    logging.info("Updated orders_csv_last_row to %s", row_count)


def mark_products_loaded(**kwargs):
    Variable.set("products_csv_loaded", "true")
    logging.info("Marked products_csv_loaded=true")


with DAG(
    dag_id="load_transitional_to_postgres",
    default_args=default_args,
    description="Track CSV changes and load transitional data into PostgreSQL",
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["postgresql", "etl", "load", "csv"],
) as dag:


    # for better flow graph
    start = EmptyOperator(task_id="start")

    products_done = EmptyOperator(
        task_id="products_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    users_done = EmptyOperator(
        task_id="users_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    orders_done = EmptyOperator(
        task_id="orders_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    skip_products_load = EmptyOperator(task_id="skip_products_load")
    skip_users_load = EmptyOperator(task_id="skip_users_load")
    skip_orders_load = EmptyOperator(task_id="skip_orders_load")
    
    
    # main tasks

    check_any_changes_task = ShortCircuitOperator(
        task_id="check_any_csv_changes",
        python_callable=check_any_changes,
    )

    branch_products_task = BranchPythonOperator(
        task_id="branch_products_load",
        python_callable=branch_products_load,
    )

    wait_for_products_file = PythonSensor(
        task_id="wait_for_products_file",
        python_callable=is_file_complete,
        op_kwargs={
            "file_path": PRODUCTS_FILE,
            "stable_time": 5,
        },
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )

    mark_products_loaded_task = PythonOperator(
        task_id="mark_products_loaded",
        python_callable=mark_products_loaded,
    )

    branch_users_task = BranchPythonOperator(
        task_id="branch_users_load",
        python_callable=branch_users_load,
    )

    wait_for_users_file = PythonSensor(
        task_id="wait_for_users_file",
        python_callable=is_file_complete,
        op_kwargs={
            "file_path": USERS_FILE,
            "stable_time": 5,
        },
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    load_users_task = PythonOperator(
        task_id="load_users",
        python_callable=load_users,
    )

    update_users_last_row_task = PythonOperator(
        task_id="update_users_csv_last_row",
        python_callable=update_users_last_row,
    )

    branch_orders_task = BranchPythonOperator(
        task_id="branch_orders_load",
        python_callable=branch_orders_load,
    )

    wait_for_orders_file = PythonSensor(
        task_id="wait_for_orders_file",
        python_callable=is_file_complete,
        op_kwargs={
            "file_path": ORDERS_FILE,
            "stable_time": 5,
        },
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    update_orders_last_row_task = PythonOperator(
        task_id="update_orders_csv_last_row",
        python_callable=update_orders_last_row,
    )

    # dependency
    start >> check_any_changes_task

    check_any_changes_task >> branch_products_task
    branch_products_task >> wait_for_products_file >> load_products_task >> mark_products_loaded_task >> products_done
    branch_products_task >> skip_products_load >> products_done

    check_any_changes_task >> branch_users_task
    branch_users_task >> wait_for_users_file >> load_users_task >> update_users_last_row_task >> users_done
    branch_users_task >> skip_users_load >> users_done

    users_done >> branch_orders_task
    branch_orders_task >> wait_for_orders_file >> load_orders_task >> update_orders_last_row_task >> orders_done
    branch_orders_task >> skip_orders_load >> orders_done

    [products_done, orders_done] >> end
