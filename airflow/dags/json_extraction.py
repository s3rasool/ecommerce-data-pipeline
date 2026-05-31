from datetime import datetime, timedelta
import os
import glob
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.json_transformation import transform_json_files



RAW_DIR = "/opt/airflow/data/raw"
EXTRACTED_DIR = "/opt/airflow/data/extracted"
TRANSFORMED_DIR = "/opt/airflow/data/transformed"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
def extract_first_valid_jsonl(**context):
    """ change jsonl format to json format."""
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    raw_dir = conf.get("raw_dir", "/opt/airflow/data/raw")

    os.makedirs(EXTRACTED_DIR, exist_ok=True)

    files = sorted(glob.glob(os.path.join(raw_dir, "*.json")))
    if not files:
        raise ValueError(f"No .json files found in {raw_dir}")

    for input_path in files:
        try:
            rows = []
            with open(input_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))

            if not rows:
                continue

            base = os.path.basename(input_path)              
            stem, _ = os.path.splitext(base)                 
            output_path = os.path.join(EXTRACTED_DIR, f"{stem}_extracted.json")

            with open(output_path, "w", encoding="utf-8") as out:
                json.dump(rows, out, ensure_ascii=False, indent=2)

            print(f"Extracted {len(rows)} records from {input_path} -> {output_path}")
            # return output_path

        except Exception as e:
            print(f"Skipping invalid file {input_path}: {e}")

    raise ValueError("No valid JSONL files found for extraction.")
	

with DAG(
    dag_id="json_extract_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["json", "extract", "transform"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_first_valid_jsonl,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_json_files,
        op_kwargs={
            "extracted_dir": EXTRACTED_DIR,
            "transformed_dir": TRANSFORMED_DIR,
            "skip_existing": True,
            "cleanup_extracted": True,
        },
    )

    extract_task >> transform_task
