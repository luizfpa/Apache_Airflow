from __future__ import annotations

import airflow
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import choice

import pandas as pd
from airflow.models import Variable


default_args = {
   'owner': 'admin'
}


def read_csv_file():
    df = pd.read_csv('/home/luizfp22/airflow_env/airflow_3_home/datasets/insurance.csv')

    print(df)

    return df.to_json()


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()

    print(df)
    return df.to_json()


def filter_by_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    region_df = df[df['region'] == 'southwest']
    region_df.to_csv('/home/luizfp22/airflow_env/airflow_3_home/output/filtered_by_region.csv', index=False)


def filter_bmi_smoker_charges(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    selected_cols_df = df[['bmi', 'smoker', 'charges']]
    selected_cols_df.to_csv('/home/luizfp22/airflow_env/airflow_3_home/output/selected_cols.csv', index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv('/home/luizfp22/airflow_env/airflow_3_home/output/grouped_by_region.csv', index=False)
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv('/home/luizfp22/airflow_env/airflow_3_home/output/grouped_by_smoker.csv', index=False)


def determine_branch():
    final_output = Variable.get("final_output", default_var=None)
    
    if final_output == 'filter_by_region':
        return 'filter_by_region'
    elif final_output == 'filter_bmi_smoker_charges':
        return 'filter_bmi_smoker_charges'
    else:
        return 'groupby_region_smoker'
    
with DAG(
    dag_id = 'branching_pipeline',
    description = 'Running a Branching pipeline',
    default_args = default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule = '@once',
    tags = ['study','LinkedIn', 'LrnAirflow', 'Python_Pipeline','branching','filter']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    filter_by_region = PythonOperator(
        task_id='filter_by_region',
        python_callable=filter_by_region
    )
    
    filter_bmi_smoker_charges = PythonOperator(
        task_id='filter_bmi_smoker_charges',
        python_callable=filter_bmi_smoker_charges
    )

    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )
    
    read_csv_file >> remove_null_values >> determine_branch >> [filter_by_region, 
                                                                filter_bmi_smoker_charges, 
                                                                groupby_region_smoker]