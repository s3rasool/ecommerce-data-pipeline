from utils.alert import task_incident_callback
from utils.settings import EMAILS
from datetime import timedelta


DEFAULT_ARGS = {
    "owner": "Group-4",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": EMAILS,
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": task_incident_callback,
}
