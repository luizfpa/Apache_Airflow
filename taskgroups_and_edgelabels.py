from __future__ import annotations

import airflow
import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from io import StringIO
import pandas as pd
from airflow.sdk import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'admin'
}

DATASETS_PATH = '/home/luizfp22/airflow_env/airflow_3_home/datasets/insurance.csv'
OUTPUT_PATH = '/home/luizfp22/airflow_env/airflow_3_home/output/{0}.csv'

#Transformation Functions
def read_csv_file(ti):
    df = pd.read_csv(DATASETS_PATH)
    print(df)
    json_data = df.to_json()
    ti.xcom_push(key='my_csv', value=df.to_json())
    return json_data

def remove_null_values(ti):
    json_data = ti.xcom_pull(key="my_csv", task_ids="reading_and_processing.read_csv_file")
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    ti.xcom_push(key='my_clean_csv', value=df.to_json())
    return df.to_json()

def determine_branch():
    transform_action = Variable.get("transform_action", default=None)
    print(transform_action)

    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'groupby_region_smoker':
        return "grouping.{0}".format(transform_action)

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key="my_clean_csv", task_ids="reading_and_processing.remove_null_values")
    df = pd.read_json(StringIO(json_data))
    region_df = df[df['region'] == 'southwest']
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key="my_clean_csv", task_ids="reading_and_processing.remove_null_values")
    df = pd.read_json(StringIO(json_data))
    region_df = df[df['region'] == 'southeast']
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)

def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key="my_clean_csv", task_ids="reading_and_processing.remove_null_values")
    df = pd.read_json(StringIO(json_data))
    region_df = df[df['region'] == 'northwest']
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)

def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key="my_clean_csv", task_ids="reading_and_processing.remove_null_values")
    df = pd.read_json(StringIO(json_data))
    region_df = df[df['region'] == 'northeast']
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)

def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(key='my_clean_csv', task_ids="reading_and_processing.remove_null_values")
    df = pd.read_json(StringIO(json_data))
    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)

with DAG(
    dag_id = 'taskgroups_and_edgelabels',
    description = 'Running a branching pipeline with TaskGroups and EdgeLabels',
    default_args = default_args,
    start_date = pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule = '@once',
    tags = ['study','LinkedIn', 'LrnAirflow', 'Pipeline','taskgroup','edgelabels']
) as dag:

    with TaskGroup('reading_and_processing') as reading_and_processing:
        read_csv_file = PythonOperator(
            task_id = 'read_csv_file',
            python_callable = read_csv_file
        )
        remove_null_values = PythonOperator(
            task_id = 'remove_null_values',
            python_callable = remove_null_values
        )
        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch
    )

    with TaskGroup('filtering') as filtering:
        filter_by_southwest = PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable = filter_by_southwest
        )
        filter_by_southeast = PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable = filter_by_southeast
        )
        filter_by_northwest = PythonOperator(
            task_id = 'filter_by_northwest',
            python_callable = filter_by_northwest
        )
        filter_by_northeast = PythonOperator(
            task_id = 'filter_by_northeast',
            python_callable = filter_by_northeast
        )

    with TaskGroup('grouping') as grouping:
        groupby_region_smoker = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable = groupby_region_smoker
        )

    reading_and_processing >> Label("Preprocessed Data") >> determine_branch >> Label("Branch on Condition") >> [filtering, grouping]
