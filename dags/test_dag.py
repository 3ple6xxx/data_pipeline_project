from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'ramikhaylov',
    'start_date': days_ago(1),
    'retries': 1,
    'catchup': False
}

dag=DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    tags=['test_dag'],
    default_args=default_args
)

hello_dag = BashOperator(
    task_id="hello_dag", #Уникальное имя таски
    bash_command='echo "Hello world. Дата запуска {{ ds }}"', #Команда bash, которую необходимо выполнить
    dag=dag
)

hello_dag