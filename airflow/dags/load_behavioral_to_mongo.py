import json
import logging
import time 
import os 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
from airflow.sensors.python import PythonSensor


default_args = {
    'start_date': datetime(2026, 5, 29)
}

def is_file_complete(file_path, stable_time=5):
    # Check if file is fully written (not being transferred)
    # Returns True if file size remains stable for 'stable_time' seconds
    if not os.path.exists(file_path):
        return False
    size1 = os.path.getsize(file_path)
    time.sleep(stable_time)
    size2 = os.path.getsize(file_path)
    
    return size1 == size2 and size1 > 0

def check_for_transformed_file():
    """Check if any .jsonl file exists in transformed directory"""
    transformed_dir = "/opt/airflow/data/transformed"
    if not os.path.exists(transformed_dir):
        return False
    files = [f for f in os.listdir(transformed_dir) if f.endswith('.jsonl')]
    return len(files) > 0

def get_latest_transformed_file():
    """Get the most recent .jsonl file from transformed directory"""
    transformed_dir = "/opt/airflow/data/transformed"
    if not os.path.exists(transformed_dir):
        return None
    
    files = [os.path.join(transformed_dir, f) for f in os.listdir(transformed_dir) 
             if f.endswith('.jsonl')]
    
    if not files:
        return None
    
    return max(files, key=os.path.getmtime)


def load_behavioral_to_mongo(**kwargs):
    transformed_dir = "/opt/airflow/data/transformed"
    file_path = get_latest_transformed_file()
    
    if not file_path:
        raise ValueError(f"No transformed .jsonl file found in {transformed_dir}")

    if not is_file_complete(file_path):
        raise ValueError(f"File {file_path} is still being transferred or incomplete.")

    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        
        if not data:
            logging.info("No Data To Load")
            return "No data to insert"
        
        mongo_hook = MongoHook(mongo_conn_id='mongo_default')
        client = mongo_hook.get_conn()
        db = client['behavioral']
        collection = db['events']
        
        result = collection.insert_many(data)
        logging.info(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB.")
        return f"Inserted {len(result.inserted_ids)} records"
    
    except Exception as e:
        logging.error(f"Error loading data into MongoDB: {e}")
        raise


with DAG(
    'load_behavioral_to_mongo',
    default_args=default_args,
    description='Load behavioral data into MongoDB',
    schedule_interval='*/1 * * * *',  # هر 1 دقیقه یکبار اجرا شود
    catchup=False,
    tags=['mongodb', 'etl', 'load']
) as dag:
    
    wait_for_file = PythonSensor(
        task_id='wait_for_behavioral_file',
        python_callable=check_for_transformed_file,
        poke_interval=30,
        timeout=3600,
        mode='poke'
    )
    
    load_task = PythonOperator(
        task_id='load_behavioral_data',
        python_callable=load_behavioral_to_mongo
    )
    
    wait_for_file >> load_task
