import logging
import time
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 5, 29)
}


def is_file_complete(file_path, stable_time=5):
    if not os.path.exists(file_path):
        return False
    size1 = os.path.getsize(file_path)
    time.sleep(stable_time)
    size2 = os.path.getsize(file_path)
    return size1 == size2 and size1 > 0


def load_csv_to_postgres(table_name, file_path, columns, conflict_column=None, **kwargs):
    """Load CSV data into PostgreSQL table using COPY"""

    if not is_file_complete(file_path):
        raise ValueError(f"File {file_path} is still being transferred or incomplete.")
    
    #use-pg_hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    column_names = ','.join(columns)
    
    #HEADER true
    sql = f"""
        COPY raw.{table_name} ({column_names})
        FROM STDIN
        WITH (FORMAT CSV, HEADER true, DELIMITER ',')
    """
    
    try:
        pg_hook.copy_expert(sql, file_path)
        logging.info(f"Successfully loaded {file_path} into raw.{table_name} using COPY")
        return f"Loaded {table_name} via COPY"
    except Exception as e:
        logging.error(f"Error in COPY for {table_name}: {e}")
        raise

def load_users(**kwargs):
    return load_csv_to_postgres(
        table_name='users',
        file_path='/tmp/users.csv',  #Confirm path
        columns=['user_id', 'name', 'email', 'signup_date', 'device', 'loyalty_tier', 'location'],
        conflict_column='user_id'
    )

def load_products(**kwargs):
    return load_csv_to_postgres(
        table_name='products',
        file_path='/tmp/products.csv',  #Confirm path
        columns=['product_id', 'name', 'price', 'category', 'inventory', 'popularity_score'],
        conflict_column='product_id'
    )


def load_orders(**kwargs):
    return load_csv_to_postgres(
        table_name='orders',
        file_path='/tmp/orders.csv',  #Confirm path
        columns=['order_id', 'user_id', 'timestamp', 'total', 'status', 'payment_method'],
        conflict_column='order_id'
    )

with DAG(
    'load_transitional_to_postgres',
    default_args=default_args,
    description='Load transitional data (users, products, orders) into PostgreSQL',
    schedule=None,
    catchup=False,
    tags=['postgresql', 'etl', 'load']
) as dag:

    wait_for_users = FileSensor(
        task_id='wait_for_users_file',
        filepath='/tmp/users.csv',  #confirm path 
        poke_interval=30,        
        timeout=3600,            #1 houre 
        mode='poke'
    )
    wait_for_products = FileSensor(
        task_id='wait_for_products_file',
        filepath='/tmp/products.csv',  #confirm path 
        poke_interval=30,
        timeout=3600,           #1 houre 
        mode='poke'
    )
    wait_for_orders = FileSensor(
        task_id='wait_for_orders_file',
        filepath='/tmp/orders.csv',  #confirm path 
        poke_interval=30,
        timeout=3600,           #1 houre 
        mode='poke'
    )

    load_users_task = PythonOperator(
        task_id='load_users',
        python_callable=load_users
    )

    load_products_task = PythonOperator(
        task_id='load_products',
        python_callable=load_products
    )

    load_orders_task = PythonOperator(
        task_id='load_orders',
        python_callable=load_orders
    )

    wait_for_users >> load_users_task
    wait_for_products >> load_products_task
    wait_for_orders >> load_orders_task