from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to process TV viewership data daily',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task to run the PySpark script
    run_etl = BashOperator(
        task_id='run_pyspark_etl',
        bash_command='spark-submit etl_pipeline_job.py'
    )

    run_etl
