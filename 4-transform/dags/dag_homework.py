"""
ДОМАШНЕЕ ЗАДАНИЕ: Комплексная обработка данных о продажах

Задача:
-------
Обработать данные о продажах (sales_raw.csv) применив очистку, трансформацию,
валидацию и расчёт метрик качества.

Входные данные: /opt/airflow/data/input/sales_raw.csv
Выходные данные: /opt/airflow/data/output/sales_clean.csv

Граф: start → load → clean → transform → validate → metrics → save → end

Оценка: 10 баллов + 3 bonus
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import logging
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

# TODO: Импортируйте необходимые функции из transform
# from transform.cleaners import ...
# from transform.validators import ...
# from transform.transformers import ...
# from transform.metrics import ...


# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student',  # TODO: Укажите ваше имя
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 4),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag_id = 'dag_homework_sales'
dag_info = 'Домашнее задание: Обработка данных о продажах'
dag_tags = ['lesson4', 'homework']


# =============================================================================
# ЗАДАНИЕ 1: ОЧИСТКА ДАННЫХ (3 балла)
# =============================================================================

def clean_sales_data(**context):
    """
    Очистка данных о продажах.
    
    TODO: Реализуйте следующие шаги (3 балла):
    
    1. Удалить дубликаты по transaction_id (1 балл)
       - Используйте функцию remove_duplicates()
       - Оставьте первое вхождение
    
    2. Заполнить пропуски (1 балл):
       - customer_name: заполнить "Guest"
       - sale_date: удалить записи с пропущенной датой
       - price: заполнить медианой
       - Используйте handle_missing_values()
    
    3. Удалить некорректные записи (1 балл):
       - Удалить записи где price < 0 или price == 0
       - Используйте filter_by_range()
    
    Returns:
        dict: Информация о результате обработки
    """
    logger = logging.getLogger(__name__)
    logger.info("-" * 50)
    logger.info("STEP 1: CLEANING SALES DATA")
    logger.info("-" * 50)
    
    # Извлечение параметров
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']
    
    # TODO: Загрузите данные из /opt/airflow/data/input/sales_raw.csv
    if os.path.exists(input_path):
        df = pd.read_csv(input_path, delimiter=delimiter, encoding=encoding)
        logger.info(f"Загружено {len(df)} записей")
    else:
        logger.error(f"Файл не найден: {input_path}")
        raise FileNotFoundError(f"Файл не найден: {input_path}")
    
    # TODO: Шаг 1 - Удаление дубликатов
    # df = remove_duplicates(df, subset=['transaction_id'], keep='first')
    
    # TODO: Шаг 2 - Обработка пропусков
    # strategy = {
    #     'customer_name': 'constant:Guest',
    #     'price': 'median'
    # }
    # df = handle_missing_values(df, strategy)
    # df = df[df['sale_date'].notna()]  # Удалить пропуски в дате
    
    # TODO: Шаг 3 - Удаление некорректных price
    # df = filter_by_range(df, 'price', min_value=0.01)
    
    logger.info(f"После очистки: {len(df)} записей")
    
    # Сохраните промежуточный результат
    output_path = '/opt/airflow/data/temp/sales_cleaned.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'output_path': output_path
    }


# =============================================================================
# ЗАДАНИЕ 2: ТРАНСФОРМАЦИЯ ДАННЫХ (3 балла)
# =============================================================================

def transform_sales_data(**context):
    """
    Трансформация данных о продажах.
    
    TODO: Реализуйте следующие шаги (3 балла):
    
    1. Нормализация текста (1 балл):
       - product_name: привести к нижнему регистру
       - customer_name: удалить пробелы в начале/конце
       - Используйте standardize_text()
    
    2. Создать вычисляемые поля (1 балл):
       - total = price * quantity
       - sale_month = месяц из sale_date (используйте pd.to_datetime)
       - Используйте create_calculated_column() или напрямую
    
    3. Категоризация (1 балл):
       - Создать поле amount_category:
         - small: total < 100
         - medium: 100 <= total < 500
         - large: total >= 500
       - Используйте categorize_numeric()
    
    Returns:
        dict: Информация о результате трансформации
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("STEP 2: TRANSFORMING SALES DATA")
    logger.info("-" * 50)
    
    # TODO: Загрузите данные из предыдущей задачи
    input_data = ti.xcom_pull(task_ids='clean_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # TODO: Шаг 1 - Нормализация текста
    # df = standardize_text(df, 'product_name', ['lowercase'])
    # df = standardize_text(df, 'customer_name', ['strip'])
    
    # TODO: Шаг 2 - Создание вычисляемых полей
    # df['total'] = df['price'] * df['quantity']
    # df['sale_date'] = pd.to_datetime(df['sale_date'])
    # df['sale_month'] = df['sale_date'].dt.month
    
    # TODO: Шаг 3 - Категоризация
    # df = categorize_numeric(
    #     df, 'total', 'amount_category',
    #     bins=[0, 100, 500, 10000],
    #     labels=['small', 'medium', 'large']
    # )
    
    logger.info(f"Трансформация завершена: {len(df)} записей")
    
    output_path = '/opt/airflow/data/temp/sales_transformed.csv'
    df.to_csv(output_path, index=False)
    
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'output_path': output_path
    }


# =============================================================================
# ЗАДАНИЕ 3: ВАЛИДАЦИЯ ДАННЫХ (2 балла)
# =============================================================================

def validate_sales_data(**context):
    """
    Валидация данных о продажах.
    
    TODO: Реализуйте следующие проверки (2 балла):
    
    1. Создать правила валидации (1 балл):
       - valid_price: price > 0
       - valid_email: email валидный (если поле существует)
       - valid_date: sale_date не в будущем
       - Используйте validate_range(), validate_email()
    
    2. Создать отчёт валидации (1 балл):
       - Используйте create_validation_report()
       - Сохраните в JSON
    
    Returns:
        dict: Отчёт валидации
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("STEP 3: VALIDATING SALES DATA")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='transform_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path, parse_dates=['sale_date'])
    
    # TODO: Создайте правила валидации
    validations = {}
    
    # TODO: valid_price - цена больше 0
    # validations['valid_price'] = validate_range(df, 'price', min_value=0.01)
    
    # TODO: valid_email - email валидный (если есть колонка)
    # if 'email' in df.columns:
    #     validations['valid_email'] = validate_email(df, 'email')
    
    # TODO: valid_date - дата не в будущем
    # today = pd.Timestamp.now()
    # validations['valid_date'] = df['sale_date'] <= today
    
    # TODO: Создайте отчёт
    # report = create_validation_report(df, validations)
    
    # TODO: Сохраните отчёт
    # report_path = '/opt/airflow/data/quality_reports/homework_validation.json'
    # os.makedirs(os.path.dirname(report_path), exist_ok=True)
    # with open(report_path, 'w') as f:
    #     json.dump(report, f, indent=2)
    
    # logger.info(f"Процент прохождения валидации: {report['overall_pass_rate']:.2%}")
    
    logger.info("-" * 50)
    
    return {}  # TODO: Замените на report


# =============================================================================
# ЗАДАНИЕ 4: МЕТРИКИ КАЧЕСТВА (2 балла)
# =============================================================================

def calculate_quality_metrics(**context):
    """
    Расчёт метрик качества данных.
    
    TODO: Реализуйте следующие расчёты (2 балла):
    
    1. Рассчитать метрики (1 балл):
       - completeness - для всех колонок
       - uniqueness - по transaction_id
       - Используйте calculate_completeness(), calculate_uniqueness()
    
    2. Рассчитать общий quality score (1 балл):
       - Средневзвешенная оценка:
         completeness * 0.4 + uniqueness * 0.6
       - Сохранить в JSON отчёт
    
    Returns:
        dict: Метрики качества
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("STEP 4: CALCULATING QUALITY METRICS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='transform_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # TODO: Рассчитайте completeness
    # completeness = calculate_completeness(df)
    # overall_completeness = sum(completeness.values()) / len(completeness)
    
    # TODO: Рассчитайте uniqueness
    # uniqueness = calculate_uniqueness(df, ['transaction_id'])
    
    # TODO: Рассчитайте quality score
    # quality_score = overall_completeness * 0.4 + uniqueness * 0.6
    
    # TODO: Создайте отчёт
    metrics_report = {
        'dataset': 'sales',
        'total_records': len(df),
        # 'completeness': completeness,
        # 'overall_completeness': overall_completeness,
        # 'uniqueness': uniqueness,
        # 'quality_score': quality_score
    }
    
    # TODO: Сохраните отчёт
    # report_path = '/opt/airflow/data/quality_reports/homework_metrics.json'
    # os.makedirs(os.path.dirname(report_path), exist_ok=True)
    # with open(report_path, 'w') as f:
    #     json.dump(metrics_report, f, indent=2)
    
    # logger.info(f"Оценка качества: {quality_score:.2%}")
    
    logger.info("-" * 50)
    
    return metrics_report


# =============================================================================
# ЗАДАНИЕ 5: СОХРАНЕНИЕ РЕЗУЛЬТАТА
# =============================================================================

def save_final_data(**context):
    """Сохранение финальных очищенных данных."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("SAVING FINAL CLEAN DATA")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='transform_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Сохраняем финальные данные
    output_path = '/opt/airflow/data/output/sales_clean.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info(f"Чистые данные сохранены: {len(df)} записей")
    logger.info(f"  Файл: {output_path}")
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'output_path': output_path,
        'clean_records': len(df)
    }


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description=dag_info,
    schedule_interval=None,
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_sales_data,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/sales_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_sales_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_sales_data,
    dag=dag,
)

metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_quality_metrics,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_final',
    python_callable=save_final_data,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

# Последовательное выполнение
start_task >> clean_task >> transform_task >> validate_task >> metrics_task >> save_task >> end_task


# =============================================================================
# ИНСТРУКЦИИ ДЛЯ СДАЧИ
# =============================================================================

"""
Критерии оценки:
----------------

Базовые задания (10 баллов):
1. Очистка данных (3 балла):
   - Удаление дубликатов: 1 балл
   - Обработка пропусков: 1 балл
   - Удаление некорректных: 1 балл

2. Трансформация (3 балла):
   - Нормализация текста: 1 балл
   - Вычисляемые поля: 1 балл
   - Категоризация: 1 балл

3. Валидация (2 балла):
   - Правила валидации: 1 балл
   - Отчёт валидации: 1 балл

4. Метрики качества (2 балла):
   - Расчёт метрик: 1 балл
   - Quality score: 1 балл

Требования к сдаче:
-------------------
1. Файл dag_homework_sales.py с выполненным кодом
2. Отчёт homework_report.md с описанием:
   - Что было сделано
   - Какие метрики получены
   - Скриншоты успешного выполнения DAG
3. Файлы результатов:
   - sales_clean.csv
   - homework_validation.json
   - homework_metrics.json

Советы:
-------
- Читайте docstrings в модулях transform - там примеры
- Смотрите на DAG 01-05 как на примеры
- Тестируйте каждую задачу отдельно
- Используйте логирование для отладки

"""
