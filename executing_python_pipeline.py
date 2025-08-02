import airflow
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

import pandas as pd

default_args = {
    'owner': 'admin'
}

def read_csv_file():
    df = pd.read_csv('/home/luizfp22/airflow_env/airflow_3_home/datasets/insurance.csv')
    print(df)
    return df.to_json()

def remove_null_values(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()

def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges': 'mean'
    }).reset_index()
    smoker_df.to_csv(
        '/home/luizfp22/airflow_env/airflow_3_home/output/grouped_by_smoker.csv', index=False
    )

def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    region_df.to_csv(
        '/home/luizfp22/airflow_env/airflow_3_home/output/grouped_by_region.csv', index=False
    )

with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python Pipeline',
    default_args = default_args,
    start_date = pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule = '@once',
    tags = ['study','LinkedIn', 'LrnAirflow', 'Python_Pipeline','transform']
)as dag:

    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
    )

    groupby_smoker = PythonOperator(
        task_id = 'groupby_smoker',
        python_callable = groupby_smoker
    )

    groupby_region = PythonOperator(
        task_id = 'groupby_region',
        python_callable = groupby_region
    )

read_csv_file >> remove_null_values >> [groupby_smoker, groupby_region]