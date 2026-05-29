import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 5, 29)
}

def load_behavioral_to_mongo(**kwargs):
    ti = kwargs.get('ti')
    file_path = ti.xcom_pull(task_ids='json_transform_task')
    
    if not file_path:
        raise ValueError("where is path file ?!")
    
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

#with DAG(
#    'load_behavioral_to_mongo',
#    default_args=default_args,
#    description='Load behavioral data into MongoDB',
#    schedule_interval=None,
#    catchup=False,
#    tags=['mongodb', 'etl', 'load']
#) as dag:
#    
#    load_task = PythonOperator(
#        task_id='load_behavioral_data',
#        python_callable=load_behavioral_to_mongo
#    )
#    
#    load_task

