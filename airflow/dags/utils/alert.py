from datetime import datetime
from pathlib import Path
from airflow.utils.email import send_email
from settings import EMAILS, INCIDENT_REPORT_FILE


def save_incident(
    dag_id: str,
    task_id: str,
    execution_date,
    exception,
):
    message = (
        f"{datetime.now():%Y-%m-%d %H:%M:%S}\t"
        f"Task {task_id} in DAG {dag_id} "
        f"with execution date: {execution_date} "
        f"failed caused by {exception}\n\n"
    )

    Path(INCIDENT_REPORT_FILE).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(INCIDENT_REPORT_FILE, "a") as f:
        f.write(message)


def send_incident_email(
    dag_id: str,
    task_id: str,
    execution_date,
    exception,
):
    send_email(
        to=EMAILS,
        subject=f"Airflow failed - {dag_id}.{task_id}",
        html_content=f"""
        DAG: {dag_id}<br>
        Task: {task_id}<br>
        Execution date: {execution_date}<br>
        Exception: {exception}<br>
        """,
    )


def task_incident_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception")

    save_incident(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        exception=exception,
    )

    send_failure_email(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        exception=exception,
    )
