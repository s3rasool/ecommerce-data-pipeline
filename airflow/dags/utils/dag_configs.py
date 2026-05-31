from datetime import timedelta

from alert import task_failure_callback
from settings import EMAILS

DEFAULT_ARGS = {
    "owner": "Group-4",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": EMAILS,
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": task_failure_callback,
}
