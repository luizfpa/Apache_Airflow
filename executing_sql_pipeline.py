from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'admin'
}

with DAG(
    dag_id='executing_sql_pipeline',
    description='Pipeline using SQL operators',
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='@once',
    tags=['study', 'LinkedIn', 'LrnAirflow', 'SQL', 'Pipeline']
) as dag:
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='my_sqlite_conn',  
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    insert_values_1 = SQLExecuteQueryOperator(
        task_id='insert_values_1',
        conn_id='my_sqlite_conn',
        sql=r"""
        INSERT INTO users (name, age, is_active)
        VALUES
            ('Alice', 30, true),
            ('Bob', 25, true),
            ('Charlie', 35, false);
        """
    )

    insert_values_2 = SQLExecuteQueryOperator(
        task_id='insert_values_2',
        conn_id='my_sqlite_conn',
        sql=r"""
        INSERT INTO users (name, age)
        VALUES
            ('David', 28),
            ('Eve', 22),
            ('Frank', 40);
        """
    )

    display_result = SQLExecuteQueryOperator(
        task_id='display_result',
        conn_id='my_sqlite_conn',
        sql=r"SELECT * FROM users;"
    )

create_table >> [insert_values_1,insert_values_2 ] >> display_result
