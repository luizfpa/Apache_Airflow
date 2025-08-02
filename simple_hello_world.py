from __future__ import annotations
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args ={
    'owner' : 'admin '
}

with DAG(
    dag_id='hello_world',
    description='Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule = '@daily',
    tags = ['study','Lrn Airflow', 'LinkedIn']
) as dag:

    task = BashOperator(
        task_id = 'hello_world_task',
        bash_command = 'echo Hello - created DAG using with'
    )

task