import sys
import os
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, List, Any, Optional
import json
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable, Connection
from airflow.utils.session import provide_session

# Добавляем путь к плагинам
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../plugins'))

from extractors.sql_extractor import SQLExtractor
from extractors.mongo_extractor import MongoExtractor
#from extractors.ftp_extractor import FTPExtractor
#from extractors.api_extractor import APIExtractor

from utils.watermarks import get_watermark, set_watermark


# =============================================================================
# ОПИСАНИЕ DAG-а
# =============================================================================

default_args = {
    'owner': 'test_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 20),
    'catchup': False
}

dag = DAG(
    'extract_dag_v1',
    default_args=default_args,
    description='ETL pipeline for extracting data from 5 sources using Airflow Connections',
    schedule_interval='0 20 * * *',  # Каждый день в 20 часов
    max_active_runs=1,
    tags=['etl', 'data-extraction', 'multi-source', 'connections']
)


# =============================================================================
# ФУНКЦИИ, КОТОРЫЕ БУДУТ ВЫЗЫВАТЬСЯ ВНУТРИ ЭТАПОВ DAG-а
# =============================================================================

def extract_postgres(**context) -> Dict[str, Any]:
    """
    Извлекает данные из PostgreSQL используя Airflow Connection.
    """
    task_instance = context['ti']
    
    conn_id='source_postgres'    # ID connection из Airflow
    db_name="orders_db"
    table_name="orders"
    id_column="id"
    watermark_column="created_at"

    try:
        # Используем экстрактор с Connection ID
        extractor = SQLExtractor(
            conn_id=conn_id,
            source_name=db_name,
            table_name=table_name,
            id_column=id_column,
            watermark_column=watermark_column,
            batch_size=1000
        )
        
        # Тестируем подключение
        if not extractor.test_connection():
            raise AirflowException(f"Failed to connect to PostgreSQL using connection 'postgres_production'")
        
        # Получаем водяной знак для инкрементальной загрузки
        last_watermark = get_watermark(db_name)
        
        current_time = datetime.now()
        
				# Извлекаем данные
        data = extractor.extract(
            start_date=last_watermark,
            columns=['id', 'customer_id', 'order_date', 'amount', 'created_at'],
            where_clause="status != 'cancelled'",
            order_by='created_at'
        )
        
        # Сохраняем результаты
        task_instance.xcom_push(key='postgres_data_count', value=len(data))
        task_instance.xcom_push(key='postgres_extractor_metadata', value={
            'connection_id': conn_id,
            'table_name': table_name,
            'extractor_version': 'v1'
        })
        
        # Обновляем водяной знак если данные получены
        if data:
            set_watermark(db_name, current_time)
            
            # Получаем схему для логирования
            schema = extractor.get_table_schema()
            task_instance.xcom_push(key='postgres_schema', value=schema)
        
        # Закрываем соединение
        extractor.close()
        
        sleep(2)
        
        return {
            'status': 'success',
            'source': 'postgres',
            'data_count': len(data),
            'watermark': last_watermark.isoformat() if last_watermark else None,
            'new_watermark': current_time.isoformat() if data else None,
            'sample_data': data[:3] if data else []
        }
        
    except Exception as e:
        context['ti'].xcom_push(key='postgres_error', value=str(e))
        raise AirflowException(f"PostgreSQL extraction failed: {str(e)}")


def extract_mongo(**context) -> Dict[str, Any]:
    """
    Извлекает данные из MongoDB используя Airflow Connection.
    """
    task_instance = context['ti']
    
    conn_id='source_mongo'    # ID connection из Airflow
    db_name="payments_db"
    table_name="payments"

    try:
        # Используем экстрактор с Connection ID
        extractor = MongoExtractor(
            conn_id=conn_id,
            source_name=db_name,
            collection_name=table_name,
            batch_size=500
        )
        
        # Получаем водяной знак
        last_watermark = get_watermark(db_name)
        
        # Извлекаем данные (предполагаем, что MongoExtractor имеет метод extract)
        data = extractor.extract_incremental()
        
        # Сохраняем результаты
        task_instance.xcom_push(key='mongodb_data_count', value=len(data))
        
        # Обновляем водяной знак если данные получены
        if data:
            set_watermark(db_name)
            
            # Получаем статистику коллекции
            stats = extractor.get_collection_stats()
            task_instance.xcom_push(key='mongodb_stats', value=stats)
        
        # Закрываем соединение
        extractor.close()
        
        sleep(3)  # Чтобы избежать проблем с таймингом при быстром выполнении задач
        
        return {
            'status': 'success',
            'source': 'mongo',
            'data_count': len(data),
            'sample_data': data[:3] if data else []
        }
        
    except Exception as e:
        context['ti'].xcom_push(key='mongodb_error', value=str(e))
        raise AirflowException(f"MongoDB extraction failed: {str(e)}")


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ ДЛЯ DAG
# =============================================================================

# Начальная задача (пустая)
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag
)

# Финальная задача (пустая)
end_task = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag
)

# Задача извлечения из Postgres
extract_postgres_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_postgres,
    provide_context=True,
    dag=dag
)

extract_mongo_task = PythonOperator(
    task_id='extract_mongo',
    python_callable=extract_mongo,
    provide_context=True,
    dag=dag
)


# =============================================================================
# ОПИСАНИЕ КОНВЕЙЕРА DAG (порядок запруска задач)
# =============================================================================

start_task >> [ extract_postgres_task, extract_mongo_task ] >> end_task