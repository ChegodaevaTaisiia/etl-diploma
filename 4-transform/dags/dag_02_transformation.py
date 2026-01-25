"""
DAG 02: Трансформация данных (Data Transformation)

Демонстрирует различные типы трансформаций:
- Приведение типов данных
- Создание вычисляемых полей
- Категоризация числовых данных
- Нормализация данных
- Текстовая нормализация

Граф: start → load → [convert_types, create_features, categorize] → merge → save → end

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from transform.transformers import convert_types, create_calculated_column, categorize_numeric, normalize_column, aggregate_data

from transform.cleaners import standardize_text


# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student_etl',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 4),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag_id = 'dag_02_transformation'
dag_tags = ['lesson4', 'transformation', 'data-quality', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def load_orders_data(**context):
    """
    Загрузка данных о заказах для трансформации.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("LOADING ORDERS DATA FOR TRANSFORMATION")
    logger.info("-" * 50)

    # Извлечение параметров
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']    
    
    # Загружаем из файла
    if os.path.exists(input_path):
        df = pd.read_csv(input_path)
        logger.info(f"Загружены из файла: {input_path}")
    else:
        # Создаём тестовые данные если файла нет
        data = {
            'order_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
            'customer_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'product_id': [101, 102, 103, 104, 105, 101, 102, 103],
            'quantity': [2, 1, 3, 1, 2, 1, 5, 2],
            'price': [1299.99, 29.99, 89.99, 449.99, 1599.00, 1299.99, 29.99, 89.99],
            'amount': [2599.98, 29.99, 269.97, 449.99, 3198.00, 1299.99, 149.95, 179.98],
            'status': ['completed', 'pending', 'completed', 'completed', 'shipped', 'completed', 'pending', 'shipped'],
            'order_date': ['2024-06-01', '2024-06-02', '2024-06-03', '2024-06-04', '2024-06-05', '2024-06-10', '2024-06-12', '2024-06-15']
        }
        df = pd.DataFrame(data)
        logger.info("Загружены из тестовых данных")
    
    logger.info(f"Всего записей: {len(df)}")
    logger.info(f"Колонки: {len(df.columns)} - {list(df.columns)}")
    logger.info(f"Типы данных:\n{df.dtypes}")
    
    # Сохраняем для следующих задач
    output_path = '/opt/airflow/data/temp/orders_step1_loaded.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info("-" * 50)
    
    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'params_used': {
            'delimiter': delimiter,
            'encoding': encoding,
            'file_size_mb': os.path.getsize(input_path) / 1024 / 1024
        },
        'output_path': output_path
    }

    return result


def convert_types_task(**context):
    """
    Приведение типов данных к правильным форматам.
        - order_date: string → datetime
        - delivery_date: string → datetime
        - quantity: любой → int
        - price, amount: любой → float
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 1: CONVERTING DATA TYPES")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path=input_data['output_path']
    df = pd.read_csv(input_path)
    
    logger.info("Типы данных ДО:")
    logger.info(df.dtypes)
    
    # Конвертируем типы
    type_mapping = {
        'order_date': 'datetime',
        'delivery_date': 'datetime',
        'quantity': 'int',
        'price': 'float',
        'amount': 'float'
    }
    
    df_transformed = convert_types(df, type_mapping)
    
    logger.info("Типы данных ПОСЛЕ:")
    logger.info(df_transformed.dtypes)
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/orders_step2_types.csv'
    df_transformed.to_csv(output_path, index=False)

    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }

    return result


def create_features_task(**context):
    """
    Создание вычисляемых полей.    
    Новые поля:
        - total_check: quantity * price (проверка amount)
        - order_year: год из order_date
        - order_month: месяц из order_date
        - is_large_order: amount > 1000
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 2: CREATING CALCULATED FEATURES")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='convert_types')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path, parse_dates=['order_date'])
    
    logger.info(f"Столбцы ДО: {len(df.columns)} - {list(df.columns)}")
    
    # Создаём вычисляемые поля
    
    # 1. Проверка суммы (quantity * price)
    df = create_calculated_column(
        df, 'total_check',
        lambda df: df['quantity'] * df['price']
    )
    
    # 2. Год заказа
    df = create_calculated_column(
        df, 'order_year',
        lambda df: df['order_date'].dt.year
    )
    
    # 3. Месяц заказа
    df = create_calculated_column(
        df, 'order_month',
        lambda df: df['order_date'].dt.month
    )
    
    # 4. Флаг большого заказа
    df = create_calculated_column(
        df, 'is_large_order',
        lambda df: df['amount'] > 1000
    )
    
    logger.info(f"Столбцы ПОСЛЕ: {len(df.columns)} - {list(df.columns)}")
    logger.info(f"Пример:")
    logger.info(df[['order_id', 'amount', 'total_check', 'order_year', 'order_month', 'is_large_order']].head())
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/orders_step3_features.csv'
    df.to_csv(output_path, index=False)
    
    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }

    return result


def categorize_task(**context):
    """
    Категоризация числовых данных.
        - amount → amount_category (small/medium/large/xlarge)
        - quantity → quantity_category (single/few/many/bulk)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 3: CATEGORIZING NUMERIC DATA")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='create_features')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path, parse_dates=['order_date'])
    
    # Категоризация суммы заказа
    df = categorize_numeric(
        df, 'amount', 'amount_category',
        bins=[0, 100, 500, 1000, 10000],
        labels=['small', 'medium', 'large', 'xlarge']
    )
    
    # Категоризация количества
    df = categorize_numeric(
        df, 'quantity', 'quantity_category',
        bins=[0, 1, 3, 10, 100],
        labels=['single', 'few', 'many', 'bulk']
    )
    
    # Статистика по категориям
    logger.info("Категории столбца Amount:")
    logger.info(df['amount_category'].value_counts())
    
    logger.info("Категории столбца Quantity:")
    logger.info(df['quantity_category'].value_counts())
    
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/orders_step4_categorized.csv'
    df.to_csv(output_path, index=False)
    
    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }

    return result


def normalize_and_aggregate_task(**context):
    """
    Нормализация и агрегация данных.
        - Нормализация amount (min-max)
        - Агрегация по месяцам
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 4: NORMALIZING AND AGGREGATING")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='categorize')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path, parse_dates=['order_date'])
    
    # Нормализация amount (min-max к [0, 1])
    logger.info(f"Диапазон столбца Amount ДО нормализации: [{df['amount'].min():.2f}, {df['amount'].max():.2f}]")
    
    df = normalize_column(df, 'amount', method='minmax')
    
    logger.info(f"Диапазон столбца Amount ПОСЛЕ нормализации: [{df['amount'].min():.2f}, {df['amount'].max():.2f}]")
    
    # Агрегация по месяцам
    df_monthly = aggregate_data(
        df,
        group_by=['order_year', 'order_month'],
        aggregations={
            'order_id': 'count',
            'quantity': 'sum',
            'price': 'mean',
            'total_check': 'sum'
        }
    )
    
    logger.info("Агрегация по месяцам:")
    logger.info(df_monthly)
    
    logger.info("-" * 50)
    
    # Сохраняем оба результата
    output_path_detail = '/opt/airflow/data/output/orders_transformed.csv'
    output_path_monthly = '/opt/airflow/data/output/orders_monthly.csv'
    
    os.makedirs(os.path.dirname(output_path_detail), exist_ok=True)
    
    df.to_csv(output_path_detail, index=False)
    df_monthly.to_csv(output_path_monthly, index=False)
    
    logger.info(f"Полные данные: {output_path_detail}")
    logger.info(f"Помесячные данне: {output_path_monthly}")
    
    return {
        'detail_file': output_path_detail,
        'monthly_file': output_path_monthly,
        'total_records': len(df),
        'months': len(df_monthly)
    }


def create_summary_report(**context):
    """
    Создание итогового отчёта о трансформации.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("TRANSFORMATION SUMMARY REPORT")
    logger.info("-" * 50)
    
    result = ti.xcom_pull(task_ids='normalize_aggregate')
    
    logger.info("Трансформация данных:")
    logger.info("  1. Конвертация типов (datetime, int, float)")
    logger.info("  2. Заполнение значени полей (4 new columns)")
    logger.info("  3. Категоризация (amount & quantity)")
    logger.info("  4. Нормализация (min-max scaling)")
    logger.info("  5. Агрегация (monthly summary)")
    logger.info(f"Выходные файлы:")
    logger.info(f"  - Детальный: {result['detail_file']}")
    logger.info(f"  - Помесячный: {result['monthly_file']}")
    logger.info(f"Статистика:")
    logger.info(f"  - Всего заказов: {result['total_records']}")
    logger.info(f"  - Покрыто месяцев: {result['months']}")
    logger.info("-" * 50)
    
    return result


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='Демонстрация трансформации данных',
    schedule_interval=None,
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_orders_data,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/orders_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }

)

convert_task = PythonOperator(
    task_id='convert_types',
    python_callable=convert_types_task,
    dag=dag,
)

features_task = PythonOperator(
    task_id='create_features',
    python_callable=create_features_task,
    dag=dag,
)

categorize_task = PythonOperator(
    task_id='categorize',
    python_callable=categorize_task,
    dag=dag,
)

normalize_task = PythonOperator(
    task_id='normalize_aggregate',
    python_callable=normalize_and_aggregate_task,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='summary_report',
    python_callable=create_summary_report,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

start_task >> load_task >> convert_task >> features_task >> categorize_task >> normalize_task >> summary_task >> end_task


# =============================================================================
# ИНСТРУКЦИИ
# =============================================================================

"""
Как запустить:
--------------
1. Убедитесь что файл orders_raw.csv существует в /opt/airflow/data/input/
2. Найдите DAG dag_02_transformation в UI
3. Trigger DAG
4. Наблюдайте трансформации в логах

Что проверить:
--------------
1. Логи каждой задачи - типы данных до/после
2. Файлы в /opt/airflow/data/output/:
   - orders_transformed.csv - детальные данные
   - orders_monthly.csv - месячная агрегация

"""