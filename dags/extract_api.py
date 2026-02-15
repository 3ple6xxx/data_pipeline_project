"""
## **My first real task**

Этот DAG:
- выгружает данные из API
- сохраняет их в файлы
- параллельно считает количество файлов и количество строк в этих файлах
 
Используется как учебный пример.

"""


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import json
import os
import math

DATA_DIR = "/opt/airflow/dags/data/api_data"
URL = "https://jsonplaceholder.typicode.com/posts"

def extract_and_split():

    response = requests.get(URL)
    data = response.json()
    os.makedirs(DATA_DIR, exist_ok=True)

    chunk_size = 60
    total_rows = len(data)
    total_files = math.ceil(total_rows / chunk_size)

    for i in range(total_files):
        chunk = data[i * chunk_size:(i + 1) * chunk_size]
        file_path = f"{DATA_DIR}/data_part_{i + 1}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False)

        print(f"Создан файл {file_path} с {len(chunk)} строками")

def count_rows(file_name):
    file_path = f"{DATA_DIR}/{file_name}"

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Файл {file_name}: {len(data)} строк")


with DAG(
    dag_id="extract_api",
    start_date=datetime(2026, 2, 1),
    end_date=datetime(2026, 2, 28),
    schedule_interval=None,
    catchup=False,
    tags=["test_dags"]
) as dag:

    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id="start")

    extract_api = PythonOperator(
        task_id="extract_and_split",
        python_callable=extract_and_split
    )

    count_files = BashOperator(
        task_id="count_files",
        bash_command=f"ls -l {DATA_DIR} | wc -l"
    )

    count_rows_1 = PythonOperator(
        task_id="count_rows_file_1",
        python_callable=count_rows,
        op_args=["data_part_1.json"]
    )

    count_rows_2 = PythonOperator(
        task_id="count_rows_file_2",
        python_callable=count_rows,
        op_args=["data_part_2.json"]
    )

    end = EmptyOperator(task_id='end')

    start >> extract_api
    extract_api >> [count_files, count_rows_1, count_rows_2] >> end
