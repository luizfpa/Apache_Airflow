from datetime import datetime, timedelta

from airflow import DAG
import pendulum

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
   'owner' : 'admin'
}

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'   

def task_c():
    print("TASK C executed!")


with DAG(
    dag_id = 'cron_catchup_backfill_weeks',
    description = 'Using crons, catchup, and backfill',
    default_args = default_args,
    start_date = pendulum.datetime(2025, 7, 3, tz='UTC'),
    schedule = '0 */12 * * 6 0',
    catchup = True,
    tags = ['study','LinkedIn', 'LrnAirflow', 'Python_Pipeline','cron', 'catchup','weeks']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD


