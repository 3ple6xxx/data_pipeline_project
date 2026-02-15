from unittest import result
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import yfinance as yf
import logging

logger = logging.getLogger(__name__)

# === Настройки констант ===
SYMBOL = "AAPL"
DAYS_TO_LOAD = 30 #сколько дней загружать при первом запуске
CONN_ID = 'source_db' #id подключения к БД в airflow
TABLE_NAME = 'stock_prices' #название таблицы в postgres

#Настройки по умолчанию для всех задач в DAG
default_args = {
    'owner': '3ple6xxx',
    'depends_on_past': False, #не зависит от прошлых запусков
    'start_date': datetime(2026, 2, 1), #с какого дня запускать
    'email_on_failure': True, #отправлять уведомление на почту при ошибке
    'email': ['3ple6xxx@gmail.com'],
    'retries': 2, #количество повторений при ошибке
    'retry_delay': timedelta(minutes=1), #через сколько повторять запуск
}



def get_last_date_from_db():
    """

    ШАГ ПЕРВЫЙ: Будем узнавать, какие данные уже есть в базе
    Нужно чтобы загружать только новые данные
    """
    logger.info("Получаем последнюю дату из базы данных...")

    #Подключаемся к PostgreSQL через специальный Hook
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)

    #Берем соединение
    with pg_hook.get_conn() as conn:
        #Создаем курсор для запросов
        with conn.cursor() as cursor:
            #SQL запрос: выберем максимальную дату для нашего тикера
            cursor.execute("""
            SELECT MAX(date)
            FROM stock_prices
            WHERE symbol = %s
            """, (SYMBOL,))

            #Получаем результат
            result = cursor.fetchone()
            last_date = result[0] if result else None

            if last_date:
                logger.info(f"Последняя дата в БД: {last_date}")
            else:
                logger.info("В БД пока нет данных")

            return last_date



def fetch_data_from_yfinance(start_date, end_date):
    """
    ШАГ ВТОРОЙ: Получаем данные из yfinance за нужный период
    """

    logger.info(f"Загружаем данные из yfinance с {start_date} по {end_date}")

    try:
        #создадим объект тикера
        ticker = yf.Ticker(SYMBOL)

        #скачиваем данные
        data = ticker.history(
            start = start_date,
            end = end_date,
            interval='1d' #дневные данные
        )

        #проверяем что данные есть
        if data.empty:
            logger.warning("Нет данных за этот период")
            return []

        #Преобразуем данные в список словарей
        records = []
        for date, row in data.iterrows():
            record = {
                'date': date.date(), #дата торгов
                'open': float(row['Open']), #цена открытия
                'high': float(row['High']), #максимум дня
                'low': float(row['Low']), #минимум дня
                'close': float(row['Close']), #цена закрытия
                'volume': int(row['Volume']), #объем торгов
                'symbol': SYMBOL, #тикер
                'load_date': datetime.now().date() #дата загрузки
            }
            records.append(record)
        
        logger.info(f"Загружено {len(records)} записей из yfinance")
        return records


    except Exception as e:
        logger.error(f"Ошибка при загрузке из yfinance: {e}")
        raise #пробрасываем ошибку дальше



def save_to_postgres(records):
    """
    ТРЕТИЙ ШАГ: Сохраняем данные в Postgres
    """

    if not records:
        logger.info("Нет данных для сохранения")
        return 0

    logger.info(f"Сохраняем {len(records)} записей в PostgreSQL...")

    #Подключаемся к БД
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            #сохраняем каждую запись
            saved_count = 0
            for record in records:
                #SQL запрос с ON CONFLICT - гарант
                #того что не будет дубликатов при повторном запуске
                cursor.execute("""
                    INSERT INTO stock_prices
                    (date, open, high, low, close, volume, symbol, load_date)
                    VALUES(%(date)s, %(open)s, %(high)s, %(low)s,
                        %(close)s, %(volume)s, %(symbol)s, %(load_date)s)
                    ON CONFLICT (date, symbol) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        load_date = EXCLUDED.load_date
                        """, record)
                saved_count += 1

            #подтверждаем транзакцию
            conn.commit()

        logger.info(f"Успешно сохранено {saved_count} записей")
        return saved_count


def main_load_function(**context):
    """
    ГЛАВНАЯ ФУНКЦИЯ: Координирует весь процесс
    """
    logger.info("="*50)
    logger.info("НАЧИНАЕМ ЗАГРУЗКУ ДАННЫХ")
    logger.info("="*50)

    #1 получаем последнюю дату из бд
    last_date = get_last_date_from_db()

    #2 определяем с какой даты загружать
    if last_date:
        #если данные уже есть, будем загружать со следующего дня
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Режим дозагрузки данных с {start_date}")
    else:
        #если данных нет, загружаем последние DAYS_TO_LOAD дней
        execution_date = context['execution_date'].date()
        start_date = (execution_date - timedelta(days=DAYS_TO_LOAD)).strftime('%Y-%m-%d')
        logger.info(f"Режим первая загрузка с {start_date}")

    #Текущая дата выполнения как конечная дата
    end_date = context['execution_date'].date().strftime('%Y-%m-%d')

    #Проверяем есть ли что загружать
    if start_date > end_date:
        logger.info("Данные уже актуальны и загрузка не требуется")
        return
    
    #3 загружаем данные из Yfinance
    records = fetch_data_from_yfinance(start_date, end_date)

    #4 сохраняем в postgresql
    saved = save_to_postgres(records)

    #5 сохраняем метрику в xcom для следующих задач
    context['ti'].xcom_push(key='records_loaded', value=saved)

    logger.info('='*50)
    logger.info("ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА")
    logger.info(f"Всего сохранено {saved} записей")
    logger.info('='*50)


def check_data_quality(**context):
    """
    ФУНКЦИЯ ПРОВЕРКИ: Проверяет, что данные загрузились корректно
    """
    logger.info("Проверяем качество загруженных данных...")
    
    # Получаем, сколько записей загрузили в прошлой задаче
    ti = context['ti']
    records_loaded = ti.xcom_pull(task_ids='load_data', key='records_loaded')
    
    if not records_loaded:
        logger.info("Новых данных не было, проверка не требуется")
        return
    
    # Подключаемся к БД
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Проверка 1: Нет ли NULL значений в важных полях
            cursor.execute("""
                SELECT COUNT(*) 
                FROM stock_prices 
                WHERE load_date = CURRENT_DATE 
                  AND (open IS NULL OR close IS NULL)
            """)
            null_count = cursor.fetchone()[0]
            
            if null_count > 0:
                raise Exception(f"ОШИБКА: Найдено {null_count} записей с NULL значениями!")
            
            # Проверка 2: Нет ли отрицательных цен
            cursor.execute("""
                SELECT COUNT(*) 
                FROM stock_prices 
                WHERE load_date = CURRENT_DATE 
                  AND (open <= 0 OR close <= 0)
            """)
            negative_count = cursor.fetchone()[0]
            
            if negative_count > 0:
                raise Exception(f"ОШИБКА: Найдено {negative_count} записей с отрицательными ценами!")
    
    logger.info("✅ Все проверки пройдены успешно!")


# === СОЗДАЕМ DAG ===
with DAG(
    # ID дага (должен быть уникальным)
    dag_id='extract_yfinance_load_postgres',
    
    # Описание (видно в интерфейсе)
    description='Загружает AAPL данные в PostgreSQL',
    
    # Настройки по умолчанию
    default_args=default_args,
    
    # Расписание: '0 3 * * *' означает "каждый день в 3 часа ночи"
    schedule_interval='0 3 * * *',
    
    # Не наверстывать пропущенные запуски
    catchup=False,
    
    # Теги для поиска в интерфейсе
    tags=['stocks', 'aapl', 'simple'],
    
    # Документация (будет видна в интерфейсе)
    doc_md=__doc__,
) as dag:

    # === ЗАДАЧА 1: Создание таблицы ===
    # Выполняется первой, если таблицы нет - создаст
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=CONN_ID,
        sql="""
        -- Создаем таблицу, если её нет
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            open NUMERIC(10,4) NOT NULL,
            high NUMERIC(10,4) NOT NULL,
            low NUMERIC(10,4) NOT NULL,
            close NUMERIC(10,4) NOT NULL,
            volume BIGINT NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Это гарантирует, что не будет дубликатов
            CONSTRAINT stock_prices_unique UNIQUE (date, symbol)
        );
        
        -- Индекс для быстрого поиска по дате и символу
        CREATE INDEX IF NOT EXISTS idx_stock_prices_lookup 
            ON stock_prices(symbol, date DESC);
        """,
    )
    
    # === ЗАДАЧА 2: Загрузка данных ===
    # Выполняет основную работу
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=main_load_function,
        provide_context=True,  # Передаем контекст (execution_date и т.д.)
    )
    
    # === ЗАДАЧА 3: Проверка данных ===
    # Проверяет, что всё хорошо
    check_data = PythonOperator(
        task_id='check_data',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # === ПОРЯДОК ВЫПОЛНЕНИЯ ===
    create_table >> load_data >> check_data


