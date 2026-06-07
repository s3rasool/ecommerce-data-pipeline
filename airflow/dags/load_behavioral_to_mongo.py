import sys
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sensors.python import PythonSensor

sys.path.append("/opt/scripts")
from json_transformation import parse_timestamp

PROJECT_ROOT = "/opt/airflow/dags/load_mongo_docs_to_kafka"
sys.path.append(PROJECT_ROOT)

TRANSFORMED_DIR = "/opt/airflow/data/transformed"
PROCESSED_DIR = "/opt/airflow/data/processed"


def is_file_complete(file_path, stable_seconds=10):
    if not os.path.exists(file_path):
        return False

    if os.path.getsize(file_path) == 0:
        return False

    age_seconds = time.time() - os.path.getmtime(file_path)
    return age_seconds >= stable_seconds


def get_ready_transformed_files():
    if not os.path.exists(TRANSFORMED_DIR):
        return []

    files = []
    for file_name in os.listdir(TRANSFORMED_DIR):
        if not file_name.endswith(".jsonl"):
            continue

        file_path = os.path.join(TRANSFORMED_DIR, file_name)
        if is_file_complete(file_path):
            files.append(file_path)

    return files


def check_for_transformed_files():
    files = get_ready_transformed_files()

    if not files:
        print("[INFO] No complete transformed files found.")
        return False

    print("[INFO] Ready transformed files:")
    for file_path in files:
        print(f" - {file_path}")

    return True


def load_behavioral_to_mongo(**kwargs):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    files = get_ready_transformed_files()
    if not files:
        raise ValueError("No complete transformed files available for loading.")

    hook = MongoHook(mongo_conn_id="mongo_default")
    collection = hook.get_collection("events", mongo_db="behavioral")

    loaded_files = []
    total_inserted = 0

    for file_path in files:
        file_name = os.path.basename(file_path)
        docs = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue

                    doc = json.loads(line)

                    if "timestamp" in doc and doc["timestamp"]:
                        doc["timestamp"] = parse_timestamp(doc["timestamp"])

                    if "transformed_at" in doc and doc["transformed_at"]:
                        doc["transformed_at"] = parse_timestamp(doc["transformed_at"])

                    doc["kafka_produced"] = False
                    doc["transformed_file"] = file_name

                    docs.append(doc)

            if not docs:
                print(f"[WARN] No documents found in transformed file: {file_path}")
                continue

            result = collection.insert_many(docs)
            inserted_count = len(result.inserted_ids)

            processed_path = os.path.join(PROCESSED_DIR, file_name)
            shutil.move(file_path, processed_path)

            print(f"[INFO] Inserted {inserted_count} docs from {file_name}")
            print(f"[INFO] Moved to processed: {processed_path}")

            loaded_files.append(file_name)
            total_inserted += inserted_count

        except Exception as e:
            print(f"[ERROR] Failed loading file {file_name}: {e}")
            continue

    if not loaded_files:
        raise ValueError("No transformed files were successfully loaded into MongoDB.")

    kwargs["ti"].xcom_push(key="loaded_files", value=loaded_files)
    print(f"[INFO] Total inserted docs: {total_inserted}")
    print(f"[INFO] Loaded files: {loaded_files}")


def produce_behavioural_data():
    old_cwd = os.getcwd()

    try:
        os.chdir(PROJECT_ROOT)

        from app.producer import EventProducer

        result = EventProducer().run()
        logging.info(f"Kafka producer result: {result}")
        return result

    finally:
        os.chdir(old_cwd)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="load_behavioral_to_mongo",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["mongo", "load", "behavioral"],
) as dag:

    wait_for_behavioral_file = PythonSensor(
        task_id="wait_for_behavioral_file",
        python_callable=check_for_transformed_files,
        poke_interval=30,
        timeout=120,
        mode="reschedule",
    )

    load_behavioral_data = PythonOperator(
        task_id="load_behavioral_data",
        python_callable=load_behavioral_to_mongo,
    )

    produce_behavioral_data = PythonOperator(
        task_id="produce_behavioral_data",
        python_callable=produce_behavioural_data,
    )

    wait_for_behavioral_file >> load_behavioral_data >> produce_behavioral_data
