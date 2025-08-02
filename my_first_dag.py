from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
	dag_id="my_first_dag",
	start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
	schedule=None,
	catchup=False,
	tags=["study","example", "tutorial"],
) as dag:
	task1 = BashOperator(
		task_id="print_hello",
		bash_command="echo 'Hello from my first Airflow DAG!'",
	)
	task2 = BashOperator(
		task_id="print_date",
		bash_command="date",
	)

task1 >> task2
