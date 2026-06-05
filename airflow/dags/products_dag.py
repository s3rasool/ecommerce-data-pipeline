from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from decimal import Decimal
import json
import logging
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

PRODUCTS_AVRO_SCHEMA = {
    "type": "record",
    "name": "products",
    "namespace": "my_db",
    "fields": [
        {"name": "product_id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "category", "type": "string"},
        {"name": "inventory", "type": "int"},
        {"name": "popularity_score", "type": "string"}
    ]
}


def extract_products_by_id(**context):
    conn_id = 'postgres'

    last_id = Variable.get("last_id_products", default_var='P0')
    logging.info(f"Last ID for products was: {last_id}")

    last_id_num = int(re.sub(r'[^0-9]', '', last_id)) if last_id != 'P0' else 0

    hook = PostgresHook(postgres_conn_id=conn_id)

    sql = f"""
        SELECT * FROM raw.products
        WHERE CAST(REPLACE(product_id, 'P', '') AS INTEGER) > {last_id_num}
        ORDER BY CAST(REPLACE(product_id, 'P', '') AS INTEGER) ASC
    """

    logging.info(f"Executing query: {sql}")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)

    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not records:
        logging.info("No new products found. Skipping.")
        context['task_instance'].xcom_push(key='products_data', value=[])
        return []

    result = []
    for row in records:
        record_dict = dict(zip(columns, row))
        for key, value in record_dict.items():
            if isinstance(value, datetime):
                record_dict[key] = value.isoformat()
            elif isinstance(value, Decimal):
                record_dict[key] = str(value)
        result.append(record_dict)

    last_product_id = records[-1][columns.index('product_id')]
    Variable.set("last_id_products", str(last_product_id))
    logging.info(f"Updated last_id for products: {last_product_id}")

    context['task_instance'].xcom_push(key='products_data', value=result)
    logging.info(f"Extracted {len(result)} new products")

    return result


def send_products_to_kafka(**context):
    data = context['task_instance'].xcom_pull(
        task_ids='extract_products',
        key='products_data'
    )

    if not data:
        logging.info(f"No new products to send")
        return

    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=json.dumps(PRODUCTS_AVRO_SCHEMA),
        conf={'auto.register.schemas': True}
    )

    producer_conf = {
        'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    for record in data:
        key = str(record.get('product_id', 'unknown'))
        producer.produce(
            topic='products',
            key=key,
            value=record
        )
        producer.poll(0)

    producer.flush()
    logging.info(f"Sent {len(data)} products to Kafka")


with DAG(
        dag_id='extract_products_to_kafka',
        default_args=default_args,
        schedule='0 * * * *',
        catchup=False,
        max_active_runs=1
) as dag:
    extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products_by_id,
        
    )

    send_products = PythonOperator(
        task_id='send_products_to_kafka',
        python_callable=send_products_to_kafka,
        
    )

    extract_products >> send_products
