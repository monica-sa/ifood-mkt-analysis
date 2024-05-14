import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ifood_pipeline',
    default_args=default_args,
    description='A simple DAG to process iFood data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
)

def bronze_task():
    subprocess.run(["python", "/opt/airflow/dags/resources/scripts/bronze_processing.py"], check=True)

def silver_task():
    subprocess.run(["python", "/opt/airflow/dags/resources/scripts/silver_processing.py"], check=True)

def gold_task():
    subprocess.run(["python", "/opt/airflow/dags/resources/scripts/gold_processing.py"], check=True)


task1 = PythonOperator(
    task_id='bronze_task',
    python_callable=bronze_task,
    dag=dag,
)

task2 = PythonOperator(
    task_id='silver_task',
    python_callable=silver_task,
    dag=dag,
)

task3 = PythonOperator(
    task_id='gold_task',
    python_callable=gold_task,
    dag=dag,
)

task1 >> task2 >> task3
