from datetime import datetime, timedelta, date
from pathlib import Path
from decimal import Decimal
import re
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

USERS_SCHEMA_PATH = Path("/opt/airflow/schema/avro/transactional/user.avsc")


def extract_users_incremental(**context):
    conn_id = 'postgres'

    last_id = Variable.get("last_id_users", default_var='U0')
    logging.info(f"Last ID for users was: {last_id}")

    last_id_num = int(re.sub(r'[^0-9]', '', last_id)) if last_id != 'U0' else 0

    hook = PostgresHook(postgres_conn_id=conn_id)

    sql = f"""
        SELECT * FROM raw.users
        WHERE CAST(REPLACE(user_id, 'U', '') AS INTEGER) > {last_id_num}
        ORDER BY CAST(REPLACE(user_id, 'U', '') AS INTEGER) ASC
    """

    logging.info(f"Executing query: {sql}")

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)

    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not records:
        logging.info("No new users found. Skipping.")
        context['task_instance'].xcom_push(key='users_data', value=[])
        return []
        
    result = []
    epoch_date = date(1970, 1, 1)

    for row in records:
        record_dict = dict(zip(columns, row))

        for key, value in record_dict.items():
            if key == 'signup_date' and value is not None:
                if isinstance(value, datetime):
                    record_dict[key] = (value.date() - epoch_date).days
                elif isinstance(value, date):
                    record_dict[key] = (value - epoch_date).days
                else:
                    raise TypeError(f"Unsupported type for signup_date: {type(value)} value={value}")

            elif isinstance(value, datetime):
                record_dict[key] = int(value.timestamp() * 1000)

            elif isinstance(value, Decimal):
                record_dict[key] = str(value)

        result.append(record_dict)

    last_user_id = records[-1][columns.index('user_id')]
    Variable.set("last_id_users", str(last_user_id))
    logging.info(f"Updated last_id for users: {last_user_id}")

    context['task_instance'].xcom_push(key='users_data', value=result)
    logging.info(f"Extracted {len(result)} new users")

    return result


def send_users_to_kafka(**context):
    data = context['task_instance'].xcom_pull(
        task_ids='extract_users',
        key='users_data'
    )

    if not data:
        logging.info("No new users to send")
        return

    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    with open(USERS_SCHEMA_PATH, "r", encoding="utf-8") as f:
        users_schema_str = f.read()

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=users_schema_str,
        conf={'auto.register.schemas': True}
    )

    producer_conf = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    for record in data:
        key = str(record.get('user_id', 'unknown'))
        producer.produce(
            topic='ecommerce.users.ingest',
            key=key,
            value=record
        )
        producer.poll(0)

    producer.flush()
    logging.info(f"Sent {len(data)} users to Kafka")


with DAG(
        dag_id='extract_users_to_kafka',
        default_args=default_args,
        schedule='0 * * * *',
        catchup=False,
        max_active_runs=1
) as dag:
    extract_users = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users_incremental,
    )

    send_users = PythonOperator(
        task_id='send_users_to_kafka',
        python_callable=send_users_to_kafka,
    )

    extract_users >> send_users
