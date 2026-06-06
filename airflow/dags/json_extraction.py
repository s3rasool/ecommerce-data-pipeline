import sys
sys.path.append("/opt/scripts")

import json
import os
import re
import shutil
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from json_transformation import transform_json_files

RAW_DIR = "/opt/airflow/data/behavioral"
RAW_ARCHIVE_DIR = "/opt/airflow/data/raw_archive"
RAW_ARCHIVE_ZIP = "/opt/airflow/data/raw_archive.zip"
EXTRACTED_DIR = "/opt/airflow/data/extracted"
TRANSFORMED_DIR = "/opt/airflow/data/transformed"


def safe_name(value):
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


def extract_and_advance_raw_files(**context):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    raw_dir = conf.get("raw_dir", RAW_DIR)

    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    os.makedirs(RAW_ARCHIVE_DIR, exist_ok=True)

    if not os.path.exists(raw_dir):
        raise ValueError(f"Raw directory does not exist: {raw_dir}")

    raw_files = [
        os.path.join(raw_dir, f)
        for f in os.listdir(raw_dir)
        if f.endswith(".jsonl") or f.endswith(".json")
    ]

    if not raw_files:
        raise ValueError(f"No raw files found in {raw_dir}")

    processed_count = 0

    for input_path in raw_files:
        file_name = os.path.basename(input_path)

        try:
            rows = []

            with open(input_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))

            if not rows:
                print(f"[WARN] Empty raw file skipped: {input_path}")
                continue

            stem, _ = os.path.splitext(file_name)
            output_path = os.path.join(EXTRACTED_DIR, f"{stem}_extracted.json")
            temp_output_path = output_path + ".tmp"

            with open(temp_output_path, "w", encoding="utf-8") as out:
                json.dump(rows, out, ensure_ascii=False, indent=2)

            os.replace(temp_output_path, output_path)

            archive_path = os.path.join(RAW_ARCHIVE_DIR, file_name)
            if os.path.exists(archive_path):
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                archive_path = os.path.join(
                    RAW_ARCHIVE_DIR,
                    f"{stem}_{timestamp}{os.path.splitext(file_name)[1]}",
                )

            shutil.move(input_path, archive_path)

            print(f"[INFO] Extracted {len(rows)} rows: {input_path} -> {output_path}")
            print(f"[INFO] Archived raw file: {input_path} -> {archive_path}")

            processed_count += 1

        except Exception as e:
            print(f"[ERROR] Failed to extract {input_path}: {e}")
            continue

    if processed_count == 0:
        raise ValueError("No raw files were successfully extracted.")


def zip_and_clean_raw_archive(**context):
    os.makedirs(RAW_ARCHIVE_DIR, exist_ok=True)

    files_to_zip = []
    for root, _, files in os.walk(RAW_ARCHIVE_DIR):
        for file_name in files:
            files_to_zip.append(os.path.join(root, file_name))

    if not files_to_zip:
        print("[INFO] raw_archive directory is empty. Nothing to zip.")
        return

    dag_run = context.get("dag_run")
    run_id = safe_name(dag_run.run_id) if dag_run else datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    os.makedirs(os.path.dirname(RAW_ARCHIVE_ZIP), exist_ok=True)

    with zipfile.ZipFile(
        RAW_ARCHIVE_ZIP,
        mode="a",
        compression=zipfile.ZIP_DEFLATED,
    ) as zipf:
        for file_path in files_to_zip:
            relative_name = os.path.relpath(file_path, RAW_ARCHIVE_DIR)
            archive_name = os.path.join("raw_archive", run_id, relative_name)
            zipf.write(file_path, archive_name)
            print(f"[INFO] Added to zip: {file_path} -> {archive_name}")

    print(f"[INFO] Raw archive zip updated: {RAW_ARCHIVE_ZIP}")

    shutil.rmtree(RAW_ARCHIVE_DIR)
    os.makedirs(RAW_ARCHIVE_DIR, exist_ok=True)

    print(f"[INFO] Raw archive directory cleaned: {RAW_ARCHIVE_DIR}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="json_extract_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["json", "extract", "transform"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_and_advance_raw_files,
    )

    zip_raw_archive_task = PythonOperator(
        task_id="zip_raw_archive_task",
        python_callable=zip_and_clean_raw_archive,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_json_files,
        op_kwargs={
            "extracted_dir": EXTRACTED_DIR,
            "transformed_dir": TRANSFORMED_DIR,
            "skip_existing": False,
            "cleanup_extracted": True,
        },
    )

    extract_task >> zip_raw_archive_task >> transform_task
