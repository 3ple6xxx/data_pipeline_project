from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd


#Пример объявления DAG
with DAG(
    dag_id="second_dag", #Обязательный параметр
    start_date=days_ago(2), #Логическая точка во времени, начиная с которой Airflow начинает планировать и запускать DAG. Чаще используют метод days_ago
    end_date=datetime(2026, 2, 28, 0), #Дата с которой Airflow перестает создавать новые запуски dag
    description='123', #Описание dag
    schedule_interval=None, #Параметр, который сообщает с какой периодичностью запускать dag. None - будет ручной запуск. Способы передачи CRON, timedelta, встроенные макросы @daily
    retries=1, #Количество повторных выполнений задачи, если выполнилась с ошибкой
    retry_delay=timedelta(minutes=10), #Задержка между повторными выполнениями
    catchup=False, #Настройка, которая отвечает за создание пропущенных запусков DAG за прошлые периоды, если начальная дата указана раньше текущего момента.
    execution_timeout=timedelta(minutes=5), #Максимальное время выполнения одной задачи
    owner="ramikhaylov", #Параметр ответственного за задачу или dag
    email="123@", #Перечень адресов, на которые могут отправляться уведомления
    tag="test dag" #Тег для фильтрации дагов в user interface
) as dag:
    hello_rn = BashOperator(
        task_id="hello_rm",
        bash_command='echo "Hello world"'
    )

hello_rm    