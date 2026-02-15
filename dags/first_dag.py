from re import I
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas.core.interchange.from_dataframe import datetime_column_to_ndarray
import yfinance as yf
from datetime import timedelta, datetime
import pandas as pd
import logging
from sqlalchemy import create_engine


def get_data():
    """Получаем данные и сохраняем в JSON"""
    symbol = "AAPL"
    data = yf.download(symbol, start="2026-01-01", end="2026-02-01")

    #Добавляем символ в данные
    data.reset_index(inplace=True) #Изменяем текущий dataframe, а не создаем новый
    data['Symbol'] = symbol

    #Сохраняем в JSON
    data.to_json("data.json", orient="records", date_format="iso", date_unit="s")

    logging.info(f"Сохранено {len(data)} записей для {symbol} в data.json")
    return "data.json"

def load_to_postgres():
    """Загрузка данных из JSON в PostgreSQL"""
    
    # Читаем JSON
    df = pd.read_json('data.json')
    
    # Создаем чистый DataFrame
    clean_df = pd.DataFrame({
        'date': pd.to_datetime(df.iloc[:, 0]).dt.date,  # Первая колонка - дата
        'open': df.iloc[:, 4],  # Open - обычно 5-я колонка
        'high': df.iloc[:, 2],  # High - обычно 3-я колонка
        'low': df.iloc[:, 3],   # Low - обычно 4-я колонка
        'close': df.iloc[:, 1],  # Close - обычно 2-я колонка
        'adj_close': df.iloc[:, 1],  # Если нет Adj Close, используем Close
        'volume': df.iloc[:, 5],  # Volume - обычно 6-я колонка
        'symbol': df.iloc[:, 6]   # Symbol - последняя колонка
    })
    
    # Подключаемся и вставляем
    hook = PostgresHook(postgres_conn_id='source_db')
    
    for _, row in clean_df.iterrows():
        hook.run("""
            INSERT INTO stock_prices (date, open, high, low, close, adj_close, volume, symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING
        """, parameters=(
            row['date'], row['open'], row['high'], row['low'],
            row['close'], row['adj_close'], row['volume'], row['symbol']
        ))
    
    logging.info(f"Загружено {len(clean_df)} записей")


with DAG(
    dag_id = "my_first_dag",
    start_date = datetime(2026, 2, 1, 0),
    end_date = datetime(2026, 2, 28, 0),
    schedule = "@daily",
    catchup = False
) as dag:
    #Получение данных
    task_1 = PythonOperator(
        task_id = "get_date",
        python_callable=get_data,
    )

    #Загрузка в PostgreSQL
    task_2 = PythonOperator(
        task_id = "load_to_postgress",
        python_callable = load_to_postgres,
    )

task_1 >> task_2





