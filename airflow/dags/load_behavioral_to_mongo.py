import sys
import json
import logging
import time
import os
import shutil

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sensors.python import PythonSensor

sys.path.append("/opt/scripts")

PROJECT_ROOT = "/opt/airflow/dags/load_mongo_docs_to_kafka"
sys.path.append(PROJECT_ROOT)

from json_transformation import parse_timestamp


TRANSFORMED_DIR = "/opt/airflow/data/transformed"
PROCESSED_DIR = "/opt/airflow/data/processed"


default_args = {
    "start_date": datetime(2026, 5, 29),
}


def is_file_complete(file_path, stable_time=5):
    if not os.path.exists(file_path):
        return False

    size1 = os.path.getsize(file_path)
    time.sleep(stable_time)
    size2 = os.path.getsize(file_path)

    return size1 == size2 and size1 > 0


def get_latest_transformed_file():
    if not os.path.exists(TRANSFORMED_DIR):
        return None

    files = [
        os.path.join(TRANSFORMED_DIR, f)
        for f in os.listdir(TRANSFORMED_DIR)
        if f.endswith(".jsonl")
    ]

    if not files:
        return None

    return max(files, key=os.path.getmtime)


def check_for_transformed_file():
    file_path = get_latest_transformed_file()

    if not file_path:
        return False

    return is_file_complete(file_path)


def load_behavioral_to_mongo(**kwargs):
    file_path = get_latest_transformed_file()

    if not file_path:
        raise ValueError(f"No transformed .jsonl file found in {TRANSFORMED_DIR}")

    if not is_file_complete(file_path):
        raise ValueError(f"File {file_path} is still being transferred or incomplete.")

    try:
        data = []

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue

                doc = json.loads(line)

                if "timestamp" in doc:
                    doc["timestamp"] = parse_timestamp(doc["timestamp"])

                if "transformed_at" in doc:
                    doc["transformed_at"] = parse_timestamp(doc["transformed_at"])

                # Important checkpoint field.
                # Newly inserted docs will be picked up by Kafka producer.
                doc["kafka_produced"] = False
                doc["source_file"] = os.path.basename(file_path)

                data.append(doc)

        if not data:
            logging.info("No data to insert.")
            return {
                "inserted_count": 0,
                "file_path": file_path,
            }

        mongo_hook = MongoHook(mongo_conn_id="mongo_default")
        client = mongo_hook.get_conn()
        db = client["behavioral"]
        collection = db["events"]

        result = collection.insert_many(data)
        inserted_count = len(result.inserted_ids)

        logging.info(f"Successfully inserted {inserted_count} records into MongoDB.")

        os.makedirs(PROCESSED_DIR, exist_ok=True)

        processed_path = os.path.join(
            PROCESSED_DIR,
            os.path.basename(file_path)
        )

        shutil.move(file_path, processed_path)

        logging.info(f"Moved processed file to {processed_path}")

        return {
            "inserted_count": inserted_count,
            "processed_file": processed_path,
        }

    except Exception as e:
        logging.error(f"Error loading data into MongoDB: {e}")
        raise


def produce_behavioural_data(**kwargs):
    ti = kwargs["ti"]
    load_result = ti.xcom_pull(task_ids="load_behavioral_data")

    logging.info(f"Load task result: {load_result}")

    old_cwd = os.getcwd()

    try:
        os.chdir(PROJECT_ROOT)

        from app.producer import EventProducer

        result = EventProducer().run()

        logging.info(f"Kafka producer result: {result}")

        return result

    finally:
        os.chdir(old_cwd)


with DAG(
    "load_behavioral_to_mongo",
    default_args=default_args,
    description="Load behavioral data into MongoDB and publish unproduced Mongo documents to Kafka",
    schedule="*/1 * * * *",
    catchup=False,
    tags=["mongodb", "etl", "load", "kafka"],
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_behavioral_file",
        python_callable=check_for_transformed_file,
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    load_task = PythonOperator(
        task_id="load_behavioral_data",
        python_callable=load_behavioral_to_mongo,
    )

    kafka_task = PythonOperator(
        task_id="produce_behavioural_data",
        python_callable=produce_behavioural_data,
    )

    wait_for_file >> load_task >> kafka_task
