"""
DAG 04: Метрики качества данных (Data Quality Metrics)

Демонстрирует расчёт метрик качества данных:
- Полнота (Completeness)
- Уникальность (Uniqueness)
- Валидность (Validity)
- Общая оценка качества (Quality Score)

Граф: start → load → [completeness, uniqueness, validity] → calculate_score → save_report → end

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

from transform.metrics import (
    calculate_completeness,
    calculate_uniqueness,
    calculate_validity,
    calculate_accuracy_score,
    create_quality_report,
    calculate_data_profiling
)
from transform.validators import validate_email, validate_range


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

dag_id = 'dag_04_quality_metrics'
dag_info = 'Расчёт метрик качества данных'
dag_tags = ['lesson4', 'metrics', 'data-quality', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def load_data_for_metrics(**context):
    """Загрузка данных для расчёта метрик."""
    logger = logging.getLogger(__name__)
    logger.info("-" * 50)
    logger.info("LOADING DATA FOR QUALITY METRICS")
    logger.info("-" * 50)
    
    # Извлечение параметров
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']
    
    # Загружаем customers
    if os.path.exists(input_path):
        df = pd.read_csv(input_path, delimiter=delimiter, encoding=encoding)
        logger.info(f"Загружены данные: {len(df)} записей")
    else:
        # Тестовые данные
        data = {
            'customer_id': [1, 2, 2, 3, 4, 5, 6, 7, 8],
            'email': ['alice@example.com', 'bob@example.com', 'bob@example.com', 'invalid', 'david@example.com', 'emma@example.com', 'frank@example.com', 'grace@example.com', 'henry@example.com'],
            'age': [28, 35, 35, None, 150, 29, -5, 42, 30],
            'city': ['Moscow', 'SPB', 'SPB', 'Moscow', 'Kazan', None, 'Moscow', 'Kazan', 'SPB']
        }
        df = pd.DataFrame(data)
        logger.info("Загружены тестовые данные")
    
    logger.info(f"Всего записей: {len(df)}")
    output_path = '/opt/airflow/data/temp/metrics_data.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }


def calculate_completeness_task(**context):
    """Расчёт полноты данных (% заполненности)."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("CALCULATING COMPLETENESS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Рассчитываем полноту для всех колонок
    completeness = calculate_completeness(df)
    
    logger.info("Полнота по столбцам:")
    for col, comp in completeness.items():
        status = "GOOD" if comp == 1.0 else "WARN" if comp >= 0.8 else "BAD "
        logger.info(f"  {status} {col}: {comp:.2%}")
    
    overall_completeness = sum(completeness.values()) / len(completeness)
    logger.info(f"Общая полнота: {overall_completeness:.2%}")
    logger.info("-" * 50)
    
    return {'completeness': completeness, 'overall': overall_completeness}


def calculate_uniqueness_task(**context):
    """Расчёт уникальности данных."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("CALCULATING UNIQUENESS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Уникальность по customer_id
    uniqueness_id = calculate_uniqueness(df, ['customer_id'])
    logger.info(f"Уникальность Customer ID: {uniqueness_id:.2%}")
    
    # Уникальность по email
    uniqueness_email = calculate_uniqueness(df, ['email'])
    logger.info(f"Уникальность Email: {uniqueness_email:.2%}")
    
    logger.info("-" * 50)
    
    return {'uniqueness_id': uniqueness_id, 'uniqueness_email': uniqueness_email}


def calculate_validity_task(**context):
    """Расчёт валидности данных."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("CALCULATING VALIDITY")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Создаём правила валидации
    validation_rules = {
        'valid_email': validate_email(df, 'email'),
        'valid_age': validate_range(df, 'age', 0, 120)
    }
    
    # Рассчитываем валидность
    validity = calculate_validity(df, validation_rules)
    
    logger.info("Валидность по правилам:")
    for rule, val in validity.items():
        status = "GOOD" if val >= 0.9 else "WARN" if val >= 0.7 else "BAD "
        logger.info(f"  {status} {rule}: {val:.2%}")
    
    overall_validity = sum(validity.values()) / len(validity)
    logger.info(f"Общая валидность: {overall_validity:.2%}")
    logger.info("-" * 50)
    
    return {'validity': validity, 'overall': overall_validity}


def calculate_quality_score_task(**context):
    """Расчёт общей оценки качества."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("CALCULATING OVERALL QUALITY SCORE")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Получаем результаты предыдущих задач
    completeness_result = ti.xcom_pull(task_ids='calculate_completeness')
    uniqueness_result = ti.xcom_pull(task_ids='calculate_uniqueness')
    validity_result = ti.xcom_pull(task_ids='calculate_validity')
    
    # Рассчитываем score для каждой записи
    validation_funcs = {
        'email': lambda s: validate_email(pd.DataFrame({'email': s}), 'email'),
        'age': lambda s: validate_range(pd.DataFrame({'age': s}), 'age', 0, 120)
    }
    
    df['quality_score'] = calculate_accuracy_score(
        df,
        validation_columns=['email', 'age', 'city'],
        validation_functions=validation_funcs
    )
    
    # Рассчитываем общий quality score датасета
    overall_score = (
        completeness_result['overall'] * 0.4 +
        uniqueness_result['uniqueness_id'] * 0.3 +
        validity_result['overall'] * 0.3
    )
    
    logger.info("Компоненты оценки качества:")
    logger.info(f"  Полнота: {completeness_result['overall']:.2%} (вес 40%)")
    logger.info(f"  Уникальность: {uniqueness_result['uniqueness_id']:.2%} (вес 30%)")
    logger.info(f"  Валидность: {validity_result['overall']:.2%} (вес 30%)")
    logger.info(f"  ОБЩАЯ ОЦЕНКА КАЧЕСТВА: {overall_score:.2%}")
    
    logger.info(f"Оценки качества записей:")
    logger.info(f"  Среднее: {df['quality_score'].mean():.1f}")
    logger.info(f"  Минимум: {df['quality_score'].min():.1f}")
    logger.info(f"  Максимум: {df['quality_score'].max():.1f}")
    logger.info("-" * 50)
    
    return {
        'overall_score': overall_score,
        'record_scores': {
            'mean': float(df['quality_score'].mean()),
            'min': float(df['quality_score'].min()),
            'max': float(df['quality_score'].max())
        }
    }


def save_quality_report_task(**context):
    """Сохранение полного отчёта о качестве."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    logger.info("-" * 50)
    logger.info("GENERATING QUALITY REPORT")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Получаем все результаты
    completeness_result = ti.xcom_pull(task_ids='calculate_completeness')
    uniqueness_result = ti.xcom_pull(task_ids='calculate_uniqueness')
    validity_result = ti.xcom_pull(task_ids='calculate_validity')
    quality_score_result = ti.xcom_pull(task_ids='calculate_quality_score')
    
    # Создаём полный отчёт
    report = create_quality_report(df, 'customers')
    
    # Добавляем расчётные метрики
    report['metrics'] = {
        'completeness': completeness_result,
        'uniqueness': uniqueness_result,
        'validity': validity_result,
        'quality_score': quality_score_result
    }
    
    # Сохраняем в JSON
    report_path = f'/opt/airflow/data/quality_reports/quality_metrics_{ds}.json'
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Отчет сохранен: {report_path}")
    logger.info(f"Сводка по качеству:")
    logger.info(f"  Датасет: customers")
    logger.info(f"  Всего записей: {len(df)}")
    logger.info(f"  Общая оценка качества: {quality_score_result['overall_score']:.2%}")
    logger.info("-" * 50)
    
    return report_path


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
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_for_metrics,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/customers_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }
)

completeness_task = PythonOperator(
    task_id='calculate_completeness',
    python_callable=calculate_completeness_task,
    dag=dag
)

uniqueness_task = PythonOperator(
    task_id='calculate_uniqueness',
    python_callable=calculate_uniqueness_task,
    dag=dag
)

validity_task = PythonOperator(
    task_id='calculate_validity',
    python_callable=calculate_validity_task,
    dag=dag
)

quality_score_task = PythonOperator(
    task_id='calculate_quality_score',
    python_callable=calculate_quality_score_task,
    dag=dag
)

report_task = PythonOperator(
    task_id='save_report',
    python_callable=save_quality_report_task,
    dag=dag
)

end_task = DummyOperator(task_id='end', dag=dag)

# Параллельный расчёт метрик
start_task >> load_task >> [completeness_task, uniqueness_task, validity_task]
[completeness_task, uniqueness_task, validity_task] >> quality_score_task >> report_task >> end_task

"""
Как запустить:
--------------
1. Убедитесь что файл customers_raw.csv существует
2. Trigger DAG dag_04_quality_metrics
3. Наблюдайте параллельный расчёт метрик

Что проверить:
--------------
1. Логи каждой метрики
2. JSON отчёт: /opt/airflow/data/quality_reports/quality_metrics_*.json

"""
