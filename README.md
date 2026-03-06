# Airflow DAGs Repository

This repository contains a collection of Apache Airflow DAGs and utility scripts for various ETL pipelines and study examples.

## Project Structure

### ETL Pipelines
- **`finance_etl_dag.py`**: A monthly ETL pipeline for processing finance data. It extracts transaction data from a SQLite database, cleans it, and loads it into a processed table.
- **`inventory_sync_dag.py`**: A daily ETL pipeline for synchronizing inventory data. It aggregates quantities by product name.
- **`website_log_analysis_dag.py`**: A daily ETL pipeline for analyzing website visit logs. It cleans the log data and prepares it for analysis.
- **`financeiro_pipeline.py`**: A specialized pipeline for personal finance management, processing files from multiple bank sources (PC Financial, Wealthsimple, CIBC, EQ Bank) and categorizing transactions.
- **`eq_cleaning.py`**: A DAG specifically designed to process and combine EQ Bank transaction files.

### Utility Scripts
- **`my_etl_utils.py`**: Contains core ETL functions (`extract_data`, `transform_data`, `load_data`) used across multiple DAGs. It supports various transformation types like `aggregate` and `clean`, with built-in schema validation for SQLite databases.

### Study and Example DAGs
- **`simple_hello_world.py`**: A basic "Hello World" DAG using `BashOperator`.
- **`my_first_dag.py`**: An introductory DAG demonstrating task dependencies.
- **`execute_multiple_tasks.py`**: Demonstrates a more complex task dependency structure using external bash scripts.
- **`execute_python_operators.py`**: Shows how to use the `PythonOperator` with parameters.
- **`execute_xcom.py` & `execute_xcom_pass_values.py`**: Examples of cross-task communication using Airflow XComs.
- **`executing_branching.py` & `executing_branching_filter.py`**: Demonstrate conditional workflow execution using the `BranchPythonOperator`.
- **`taskgroups_and_edgelabels.py`**: Shows how to organize DAGs using `TaskGroup` and add descriptive labels to edges between tasks.
- **`cron_catchup.py` & `cron_catchup_weeks.py`**: Examples of using cron expressions and the `catchup` feature in Airflow.
- **`backfill_weeks.py`**: Demonstrates the use of backfilling for historical data processing.

## Setup and Usage

1. **Airflow Environment**: Ensure Apache Airflow is installed and configured.
2. **Database Connections**: Configure the following connections in Airflow:
   - `finance_db`
   - `inventory_db`
   - `website_db`
   - `my_sqlite_conn`
3. **Variables**: Set the following Airflow variables to point to your SQLite database files:
   - `finance_db_path`
   - `inventory_db_path`
   - `website_db_path`
4. **Input Files**: For the `financeiro_pipeline`, place input files in the directory specified by `INPUT_FOLDER` in the script.

## Bash Scripts
The `bash_scripts/` directory contains shell scripts (`taskA.sh` through `taskG.sh`) used by the example DAGs to simulate various processing steps.
