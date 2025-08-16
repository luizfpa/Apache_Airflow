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
    extracted_rows = ti.xcom_pull(task_ids="extract_finance_data")
    if not extracted_rows:
        df = pd.DataFrame(columns=['id','date','amount','category','description'])
    elif isinstance(extracted_rows[0], dict):
        df = pd.DataFrame(extracted_rows)
    else: 
        df = pd.DataFrame(
            extracted_rows,
            columns=['id', 'date', 'amount', 'category', 'description']
        )
    logging.info(f"Linhas extraídas: {len(df)}")
    logging.info(f"Colunas extraídas: {df.columns}")
    logging.info(f"Exemplo de dados extraídos: {df.head()}")
    return transform_data(df, transformation_type, db_path=db_path, table_name=table_name)

def load_task(ti, db_path, table):
    transformed_df = ti.xcom_pull(task_ids='transform_finance_data', key='return_value')
    load_data(db_path, transformed_df, table)

default_args = {
    'owner': 'admin',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'finance_etl_dag',
    default_args=default_args,
    description='ETL pipeline for finance data',
    schedule='@monthly',
    start_date=pendulum.datetime(2025, 7, 3, tz='UTC'),
    catchup=False,
) as dag:

    extract = SQLExecuteQueryOperator(
        task_id='extract_finance_data',
        conn_id='finance_db',
        sql="""
        SELECT * 
        FROM transactions 
        WHERE strftime('%Y-%m', date) <= '{{ macros.datetime.strftime(data_interval_start, "%Y-%m") }}'
        """,
        do_xcom_push=True,
    )

    transform = PythonOperator(
        task_id='transform_finance_data',
        python_callable=transform_task,
        op_kwargs={
            'transformation_type': 'clean',
            'db_path': Variable.get('finance_db_path'),
            'table_name': 'transactions'
        },
    )

    load = PythonOperator(
        task_id='load_finance_data',
        python_callable=load_task,
        op_kwargs={
            'db_path': Variable.get('finance_db_path'),
            'table': 'processed_transactions'
        },
    )

    extract >> transform >> load
