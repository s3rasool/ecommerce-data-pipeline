from datetime import datetime, timedelta
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
    conn_id = 'postgres'

    last_id = Variable.get("last_id_orders", default_var='O0')
    logging.info(f"Last ID for orders was: {last_id}")

    last_id_num = int(re.sub(r'[^0-9]', '', last_id)) if last_id != 'O0' else 0

    hook = PostgresHook(postgres_conn_id=conn_id)

    sql = f"""
        SELECT * FROM raw.orders
        WHERE CAST(REPLACE(order_id, 'O', '') AS INTEGER) > {last_id_num}
        ORDER BY CAST(REPLACE(order_id, 'O', '') AS INTEGER) ASC
    """

    logging.info(f"Executing query: {sql}")

    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)

    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not records:
        logging.info("No new orders found. Skipping.")
        context['task_instance'].xcom_push(key='orders_data', value=[])
        return []

    result = []
    for row in records:
        record_dict = dict(zip(columns, row))
        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = int(value.timestamp() * 1000)
            elif isinstance(value, Decimal):
                record_dict[key] = str(value)
        result.append(record_dict)

    last_order_id = records[-1][columns.index('order_id')]
    Variable.set("last_id_orders", str(last_order_id))
    logging.info(f"Updated last_id for orders: {last_order_id}")

    context['task_instance'].xcom_push(key='orders_data', value=result)
    logging.info(f"Extracted {len(result)} new orders")

    return result


def send_orders_to_kafka(**context):
    data = context['task_instance'].xcom_pull(
        task_ids='extract_orders',
        key='orders_data'
    )

    if not data:
        logging.info("No new orders to send")
        return

    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=json.dumps(ORDERS_AVRO_SCHEMA),
        conf={'auto.register.schemas': True}
    )

    producer_conf = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    for record in data:
        key = str(record.get('order_id', 'unknown'))
        producer.produce(
            topic='ecommerce.orders.ingest',
            key=key,
            value=record
        )
        producer.poll(0)

    producer.flush()
    logging.info(f"Sent {len(data)} orders to Kafka")


with DAG(
    dag_id='extract_orders_to_kafka',
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_incremental,
    )

    send_orders = PythonOperator(
        task_id='send_orders_to_kafka',
        python_callable=send_orders_to_kafka,
    )

    extract_orders >> send_orders
