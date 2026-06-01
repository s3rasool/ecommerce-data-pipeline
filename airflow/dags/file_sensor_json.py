from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

RAW_DIR = "/opt/airflow/data/behavioral"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def valid_jsonl_file_exists():
    if not os.path.exists(RAW_DIR):
        print(f"Directory does not exist: {RAW_DIR}")
        return False

    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]

    for file_name in files:
        path = os.path.join(RAW_DIR, file_name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                found_line = False
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    found_line = True
                    json.loads(line)

            if found_line:
                print(f"Valid JSONL file found: {file_name}")
                return True
            else:
                print(f"Empty file: {file_name}")

        except Exception as e:
            print(f"Invalid JSONL file {file_name}: {e}")

    return False

with DAG(
    dag_id="json_file_sensor_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["json", "sensor"],
) as dag:

    json_file_sensor = PythonSensor(
        task_id="json_file_sensor",
        python_callable=valid_jsonl_file_exists,
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )

    trigger_extract_dag = TriggerDagRunOperator(
        task_id="trigger_extract_dag",
        trigger_dag_id="json_extract_dag",
        conf={"raw_dir": RAW_DIR},
        wait_for_completion=False,
    )

    json_file_sensor >> trigger_extract_dag
