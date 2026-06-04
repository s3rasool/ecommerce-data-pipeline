from datetime import datetime, timedelta
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

ORDERS_AVRO_SCHEMA = {
    "type": "record",
    "name": "orders",
    "namespace": "my_db",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "string"},
            {
                "name": "timestamp",
                "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                }
            },
        {"name": "total", "type": "float"},
        {"name": "status", "type": "string"},
        {"name": "payment_method", "type": "string"}
    ]
}


def extract_orders_incremental(**context):
    conn_id = 'AIRFLOW_CONN_POSTGRES'
    time_field = 'timestamp'
    last_run = Variable.get("orders last run was", default_var='1970-01-01 00:00:00')
    logging.info(f"Last run for orders was: {last_run}")

    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = f"""
        SELECT * FROM orders
        WHERE {time_field} > '{last_run}'
        ORDER BY {time_field} ASC
    """

    logging.info(f"Executing query: {sql}")
    records = hook.get_records(sql)

    if not records:
        logging.info(f"No new orders found")
        context['task_instance'].xcom_push(key='orders_data', value=[])
        return []

    columns = [col[0] for col in hook.get_description(sql)]

    result = []
    for row in records:
        record_dict = dict(zip(columns, row))
        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = value.isoformat()
        result.append(record_dict)

    time_index = columns.index(time_field)
    last_timestamp = records[-1][time_index]
    if isinstance(last_timestamp, datetime):
        last_timestamp = last_timestamp.isoformat()
    Variable.set("orders last run was", str(last_timestamp))
    logging.info(f"Updated last_run for orders : {last_timestamp}")

    context['task_instance'].xcom_push(key='orders_data', value=result)
    logging.info(f" Extracted {len(result)} new orders")

    return result


def send_orders_to_kafka(**context):
    data = context['task_instance'].xcom_pull(
        task_ids='extract_orders',
        key='orders_data'
    )

    if not data:
        logging.info(f"No new orders to send")
        return

    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=json.dumps(ORDERS_AVRO_SCHEMA),
        conf={'auto.register.schemas': True, 'use.latest.version': True}
    )

    producer_conf = {
        'bootstrap.servers': 'kafka:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    for record in data:
        key = str(record.get('order_id', 'unknown'))
        producer.produce(
            topic='orders',
            key=key,
            value=record
        )
        producer.poll(0)

    producer.flush()
    logging.info(f" Sent {len(data)} orders to Kafka")


with DAG(
        dag_id='extract_orders_to_kafka',
        default_args=default_args,
        schedule_interval='0 * * * *',
        catchup=False,
        max_active_runs=1
) as dag:
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_incremental,
        provide_context=True
    )

    send_orders = PythonOperator(
        task_id='send_orders_to_kafka',
        python_callable=send_orders_to_kafka,
        provide_context=True
    )

    extract_orders >> send_orders