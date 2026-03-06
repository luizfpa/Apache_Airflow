from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import logging

from my_etl_utils import extract_data, transform_data, load_data


def transform_task(**kwargs):
    ti = kwargs["ti"]
    transformation_type = kwargs["transformation_type"]
    db_path = kwargs["db_path"]
    table_name = kwargs["table_name"]
    extracted_rows = ti.xcom_pull(task_ids="extract_website_data")
    if not extracted_rows:
        df = pd.DataFrame(columns=['page_url'])
    elif isinstance(extracted_rows[0], dict):
        df = pd.DataFrame(extracted_rows)
    else:
        df = pd.DataFrame(
            extracted_rows,
            columns=['page_url']
        )
    logging.info(f"Linhas extraídas: {len(df)}")
    logging.info(f"Colunas extraídas: {df.columns}")
    logging.info(f"Exemplo de dados extraídos: {df.head()}")
    return transform_data(df, transformation_type, db_path=db_path, table_name=table_name)

def load_task(ti, db_path, table):
    transformed_df = ti.xcom_pull(task_ids='transform_website_data', key='return_value')
    load_data(db_path, transformed_df, table)

default_args = {
    'owner': 'admin',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'website_log_analysis_dag',
    default_args=default_args,
    description='ETL pipeline for website log analysis',
    schedule='@daily',
    start_date=pendulum.datetime(2025, 7, 3, tz='UTC'),
    catchup=False,
) as dag:

    extract = SQLExecuteQueryOperator(
        task_id='extract_website_data',
        conn_id='website_db',
        sql="""
        SELECT page_url
        FROM visit_logs
        """,
        do_xcom_push=True,
    )

    transform = PythonOperator(
        task_id='transform_website_data',
        python_callable=transform_task,
        op_kwargs={
            'transformation_type': 'clean',
            'db_path': Variable.get('website_db_path', '/tmp/website.db'),
            'table_name': 'visit_logs'
        },
    )

    load = PythonOperator(
        task_id='load_website_data',
        python_callable=load_task,
        op_kwargs={
            'db_path': Variable.get('website_db_path', '/tmp/website.db'),
            'table': 'processed_visit_logs'
        },
    )

    extract >> transform >> load
