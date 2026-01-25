"""
DAG 03: Валидация данных (Data Validation)

Демонстрирует многоуровневую валидацию данных:
- Структурная валидация (схема, типы)
- Доменная валидация (форматы, диапазоны)
- Бизнес-валидация (правила, зависимости)

Граф: start → load → [validate_structure, validate_domain, validate_business] → merge_results → report → end

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

from transform.validators import (
    validate_schema,
    validate_email,
    validate_phone,
    validate_range,
    validate_allowed_values,
    validate_not_null,
    validate_unique,
    validate_referential_integrity,
    create_validation_report
)


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

dag_id = 'dag_03_validation'
dag_info = 'Многоуровневая валидация данных'
dag_tags = ['lesson4', 'validation', 'data-quality', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def load_validation_data(**context):
    """Загрузка данных для валидации."""
    logger = logging.getLogger(__name__)
    logger.info("-" * 50)
    logger.info("LOADING DATA FOR VALIDATION")
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
            'customer_id': [1, 2, 3, 4, 5],
            'email': ['alice@example.com', 'bob@', 'charlie@example.com', 'david@example.com', 'emma@example.com'],
            'full_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'David Lee', 'Emma Davis'],
            'age': [28, 35, None, 150, 29],
            'city': ['Moscow', 'SPB', 'Moscow', 'Kazan', None],
            'phone': ['+79151234567', 'invalid', '+79161234567', '+79163334455', '+79164445566']
        }
        df = pd.DataFrame(data)
        logger.info("Загружены тестовые данные")
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/validation_data.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    logger.info(f"Структура данных: {df.shape}")
    logger.info(f"Столбцы: {len(df.columns)} - {list(df.columns)}")
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }


def validate_structure_task(**context):
    """Шаг 1. Структурная валидация"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("LEVEL 1: STRUCTURAL VALIDATION")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Проверка схемы
    required_columns = ['customer_id', 'email', 'full_name', 'age', 'city', 'phone']
    expected_types = {
        'customer_id': 'int64',
        'age': 'float64'
    }
    
    schema_result = validate_schema(df, required_columns, expected_types)
    
    logger.info("Результаты валидации схемы:")
    logger.info(f"  Корректно: {schema_result['valid']}")
    
    if schema_result['errors']:
        logger.error(f"  Ошибки: {schema_result['errors']}")
    else:
        logger.info("  Ошибок не обнаружено")
    
    if schema_result['warnings']:
        logger.warning(f"  Предупреждения: {schema_result['warnings']}")
    else:
        logger.info("  Предупреждений нет")
    
    logger.info("-" * 50)
    
    return schema_result


def validate_domain_task(**context):
    """Шаг 2: Доменная валидация"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("LEVEL 2: DOMAIN VALIDATION")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Создаём правила валидации
    validations = {}
    
    # 1. Email format
    validations['valid_email'] = validate_email(df, 'email')
    
    # 2. Phone format
    validations['valid_phone'] = validate_phone(df, 'phone')
    
    # 3. Age range
    validations['valid_age_range'] = validate_range(df, 'age', min_value=0, max_value=120)
    
    # 4. Required fields
    validations['required_fields'] = validate_not_null(df, ['customer_id', 'email', 'full_name'])
    
    # 5. Unique customer_id
    validations['unique_id'] = validate_unique(df, ['customer_id'])
    
    # Создаём отчёт
    domain_report = create_validation_report(df, validations)
    
    logger.info("Результаты доменной валидации:")
    logger.info(f"  Всего записей: {domain_report['total_records']}")
    logger.info(f"  Корректных записей: {domain_report['valid_records']}")
    logger.info(f"  Некорректных записей: {domain_report['invalid_records']}")
    logger.info(f"  Процент прохождения: {domain_report['overall_pass_rate']:.2%}")
    logger.info("  Детализация проверок:")
    for check_name, check_result in domain_report['checks'].items():
        logger.info(f"    {check_name}: {check_result['pass_rate']:.2%} ({check_result['valid']}/{domain_report['total_records']})")
    
    logger.info("-" * 50)
    
    return domain_report


def validate_business_task(**context):
    """Шаг 3: Бизнес-валидация"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("LEVEL 3: BUSINESS VALIDATION")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Бизнес-правила
    business_validations = {}
    
    # 1. Совершеннолетие
    is_adult = (df['age'].isna()) | (df['age'] >= 18)
    business_validations['is_adult'] = is_adult
    
    adult_count = is_adult.sum()
    logger.info(f"Проверка совершеннолетия: {adult_count}/{len(df)} прошли")
    
    # 2. Допустимые города
    allowed_cities = ['Moscow', 'SPB', 'Kazan', 'Ekaterinburg', 'Novosibirsk']
    is_valid_city = df['city'].isna() | df['city'].isin(allowed_cities)
    business_validations['valid_city'] = is_valid_city
    
    city_count = is_valid_city.sum()
    logger.info(f"Корректный город: {city_count}/{len(df)} прошли")
    
    # 3. Email валиден И не NULL
    has_valid_email = validate_email(df, 'email') & df['email'].notna()
    business_validations['has_valid_email'] = has_valid_email
    
    email_count = has_valid_email.sum()
    logger.info(f"Корректный email: {email_count}/{len(df)} прошли")
    
    # Общий отчёт
    business_report = create_validation_report(df, business_validations)
    
    logger.info("Результаты бизнес-валидации:")
    logger.info(f"  Процент прохождения: {business_report['overall_pass_rate']:.2%}")
    logger.info(f"  Корректных записей: {business_report['valid_records']}/{business_report['total_records']}")
    
    logger.info("-" * 50)
    
    return business_report


def merge_validation_results(**context):
    """Шаг 4. Объединение результатов всех уровней валидации"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("MERGING VALIDATION RESULTS")
    logger.info("-" * 50)
    
    # Получаем результаты
    structure_result = ti.xcom_pull(task_ids='validate_structure')
    domain_result = ti.xcom_pull(task_ids='validate_domain')
    business_result = ti.xcom_pull(task_ids='validate_business')
    
    # Объединяем
    merged_report = {
        'validation_date': context['ds'],
        'dataset': 'customers',
        'levels': {
            'structure': {
                'valid': structure_result['valid'],
                'errors': structure_result.get('errors', []),
                'warnings': structure_result.get('warnings', [])
            },
            'domain': {
                'total_records': domain_result['total_records'],
                'valid_records': domain_result['valid_records'],
                'pass_rate': domain_result['overall_pass_rate'],
                'checks': domain_result['checks']
            },
            'business': {
                'total_records': business_result['total_records'],
                'valid_records': business_result['valid_records'],
                'pass_rate': business_result['overall_pass_rate'],
                'checks': business_result['checks']
            }
        },
        'overall_status': 'PASSED' if structure_result['valid'] and 
                         domain_result['overall_pass_rate'] > 0.8 and 
                         business_result['overall_pass_rate'] > 0.8 else 'FAILED'
    }
    
    # Рассчитываем общий quality score
    overall_score = (
        (1.0 if structure_result['valid'] else 0.0) * 0.3 +
        domain_result['overall_pass_rate'] * 0.4 +
        business_result['overall_pass_rate'] * 0.3
    )
    merged_report['overall_quality_score'] = overall_score
    
    logger.info("Сводка по уровням валидации:")
    logger.info(f"  Структура: {'Корректно' if structure_result['valid'] else 'Некорректно'}")
    logger.info(f"  Домен: {domain_result['overall_pass_rate']:.2%} прохождения")
    logger.info(f"  Бизнес: {business_result['overall_pass_rate']:.2%} прохождения")
    logger.info(f"Общая оценка качества: {overall_score:.2%}")
    logger.info(f"Общий статус: {merged_report['overall_status']}")
    logger.info("-" * 50)
    
    return merged_report


def generate_validation_report(**context):
    """Генерация финального отчёта валидации в JSON"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    
    logger.info("-" * 50)
    logger.info("GENERATING VALIDATION REPORT")
    logger.info("-" * 50)
    
    # Получаем объединённые результаты
    merged_report = ti.xcom_pull(task_ids='merge_results')
    
    # Сохраняем в JSON
    report_path = f'/opt/airflow/data/quality_reports/validation_report_{ds}.json'
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(merged_report, f, indent=2)
    
    logger.info(f"Отчет сохранен: {report_path}")
    logger.info("Сводка отчета:")
    logger.info(f"  Датасет: {merged_report['dataset']}")
    logger.info(f"  Дата валидации: {merged_report['validation_date']}")
    logger.info(f"  Общий статус: {merged_report['overall_status']}")
    logger.info(f"  Оценка качества: {merged_report['overall_quality_score']:.2%}")
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
    python_callable=load_validation_data,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/customers_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }
)

validate_structure_task = PythonOperator(
    task_id='validate_structure',
    python_callable=validate_structure_task,
    dag=dag,
)

validate_domain_task = PythonOperator(
    task_id='validate_domain',
    python_callable=validate_domain_task,
    dag=dag,
)

validate_business_task = PythonOperator(
    task_id='validate_business',
    python_callable=validate_business_task,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge_results',
    python_callable=merge_validation_results,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_validation_report,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# Параллельная валидация на трёх уровнях
start_task >> load_task
load_task >> [validate_structure_task, validate_domain_task, validate_business_task]
[validate_structure_task, validate_domain_task, validate_business_task] >> merge_task
merge_task >> report_task >> end_task


# =============================================================================
# ИНСТРУКЦИИ
# =============================================================================

"""
Как запустить:
--------------
1. Убедитесь что файл customers_raw.csv существует
2. Trigger DAG dag_03_validation
3. Наблюдайте параллельное выполнение 3 уровней валидации

Что проверить:
--------------
1. Graph View - параллельное выполнение validate_* задач
2. Логи каждого уровня валидации
3. JSON отчёт: /opt/airflow/data/quality_reports/validation_report_*.json

"""
