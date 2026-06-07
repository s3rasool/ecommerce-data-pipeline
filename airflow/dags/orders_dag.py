from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal
import re
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ORDERS_SCHEMA_PATH = Path("/opt/airflow/schema/avro/transactional/order.avsc")


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
        return {
            "data": [],
            "last_id": last_id
        }

    result = []
    for row in records:
        record_dict = dict(zip(columns, row))

        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = int(value.timestamp() * 1_000_000)
            elif isinstance(value, Decimal):
                record_dict[key] = float(value)

        result.append(record_dict)

    last_order_id = records[-1][columns.index('order_id')]
    logging.info(f"Extracted {len(result)} new orders, last_order_id={last_order_id}")

    return {
        "data": result,
        "last_id": str(last_order_id)
    }


def send_orders_to_kafka(**context):
    ti = context['task_instance']
    extracted = ti.xcom_pull(task_ids='extract_orders')

    if not extracted:
        logging.info("No extracted payload found")
        return

    data = extracted.get("data", [])
    last_id = extracted.get("last_id")

    if not data:
        logging.info("No new orders to send")
        return

    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    with open(ORDERS_SCHEMA_PATH, "r", encoding="utf-8") as f:
        orders_schema_str = f.read()

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=orders_schema_str,
        conf={'auto.register.schemas': True}
    )

    producer_conf = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }

    producer = SerializingProducer(producer_conf)

    delivery_errors = []

    def delivery_report(err, msg):
        if err is not None:
            logging.error(
                f"Message delivery failed for key={msg.key() if msg else None}: {err}"
            )
            delivery_errors.append(str(err))

    try:
        for record in data:
            logging.info(
                f"Sending order_id={record.get('order_id')} "
                f"timestamp={record.get('timestamp')} ({type(record.get('timestamp'))}) "
                f"total={record.get('total')} ({type(record.get('total'))})"
            )

            key = str(record.get('order_id', 'unknown'))

            producer.produce(
                topic='ecommerce.orders.ingest',
                key=key,
                value=record,
                on_delivery=delivery_report
            )
            producer.poll(0)

        remaining = producer.flush(timeout=60)

        if remaining > 0:
            raise Exception(f"Kafka flush incomplete, {remaining} message(s) remaining unsent")

        producer.poll(0)

        if delivery_errors:
            raise Exception(f"Kafka delivery had {len(delivery_errors)} error(s): {delivery_errors}")

        Variable.set("last_id_orders", str(last_id))
        logging.info(f"Sent {len(data)} orders to Kafka successfully")
        logging.info(f"Updated last_id_orders to {last_id}")

    except Exception as e:
        logging.exception("Failed sending orders to Kafka. Variable will NOT be updated.")
        raise


with DAG(
    dag_id='extract_orders_to_kafka',
    default_args=default_args,
    schedule='0/30 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['postgres', 'kafka', 'orders']
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
