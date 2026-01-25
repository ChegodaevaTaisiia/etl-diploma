"""
DAG 05: Обработка ошибок и проблемных данных (Error Handling)

Демонстрирует стратегии обработки проблемных данных:
- Классификация ошибок
- Автоматическое исправление
- Карантин для ручной проверки
- Аудит изменений

Граф: start → load → classify_errors → [auto_fix, quarantine] → log_changes → save_results → end

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

from transform.validators import validate_email, validate_range
from transform.cleaners import standardize_text, clean_phone_numbers


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

dag_id = 'dag_05_error_handling'
dag_info = 'Обработка ошибок и проблемных данных'
dag_tags = ['lesson4', 'error-handling', 'data-quality', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def load_problematic_data(**context):
    """Загрузка данных с проблемами."""
    logger = logging.getLogger(__name__)
    logger.info("-" * 50)
    logger.info("LOADING PROBLEMATIC DATA")
    logger.info("-" * 50)
    
    # Извлечение параметров
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']
    
    if os.path.exists(input_path):
        df = pd.read_csv(input_path, delimiter=delimiter, encoding=encoding)
        logger.info(f"Загружены данные: {len(df)} записей")
    else:
        data = {
            'customer_id': [1, 2, 3, 4, 5, 6],
            'email': ['alice@example.com', 'BOB@EXAMPLE.COM', 'invalid_email', 'charlie@', '  dave@example.com  ', 'emma@example.com'],
            'age': [28, 35, 150, -5, None, 29],
            'phone': ['+79151234567', '89161234567', 'invalid', '+7(916)333-44-55', '  +79164445566  ', '9165556677']
        }
        df = pd.DataFrame(data)
        logger.info("Загружены тестовые данные")
    
    logger.info(f"Загружено: {len(df)} записей с потенциальными проблемами")
    output_path = '/opt/airflow/data/temp/problematic_data.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'output_path': output_path
    }


def classify_errors_task(**context):
    """Классификация ошибок на исправимые и неисправимые."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("CLASSIFYING ERRORS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Классифицируем проблемы
    df['error_type'] = ''
    df['is_fixable'] = False
    
    # 1. Email проблемы
    is_valid_email = validate_email(df, 'email')
    df.loc[~is_valid_email & df['email'].str.contains('@', na=False), 'error_type'] = 'email_format'
    df.loc[~is_valid_email & df['email'].str.contains('@', na=False), 'is_fixable'] = True
    df.loc[~is_valid_email & ~df['email'].str.contains('@', na=False), 'error_type'] = 'email_invalid'
    df.loc[~is_valid_email & ~df['email'].str.contains('@', na=False), 'is_fixable'] = False
    
    # 2. Age проблемы
    is_valid_age = validate_range(df, 'age', 0, 120)
    df.loc[~is_valid_age & df['age'].notna(), 'error_type'] = df.loc[~is_valid_age & df['age'].notna(), 'error_type'] + ',age_outlier'
    df.loc[~is_valid_age & df['age'].notna(), 'is_fixable'] = False
    
    df.loc[df['age'].isna(), 'error_type'] = df.loc[df['age'].isna(), 'error_type'] + ',age_missing'
    df.loc[df['age'].isna(), 'is_fixable'] = True
    
    # Статистика
    fixable_count = df['is_fixable'].sum()
    unfixable_count = (~df['is_fixable']).sum()
    
    logger.info(f"Классификация ошибок:")
    logger.info(f"  Исправимые: {fixable_count}")
    logger.info(f"  Требуют ручной проверки: {unfixable_count}")
    logger.info(f"Типы ошибок:")
    logger.info(df['error_type'].value_counts())
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/classified_errors.csv'
    df.to_csv(output_path, index=False)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'fixable': fixable_count,
        'unfixable': unfixable_count,
        'output_path': output_path
    }


def auto_fix_errors_task(**context):
    """Автоматическое исправление известных ошибок."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("AUTO-FIXING ERRORS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='classify_errors')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Создаём колонку для отслеживания изменений
    df['fixes_applied'] = ''
    
    # Исправляем только fixable записи
    fixable_mask = df['is_fixable'] == True
    
    logger.info(f"Обработка {fixable_mask.sum()} исправимых записей...")
    
    # 1. Исправление email (lowercase, strip)
    email_needs_fix = fixable_mask & df['error_type'].str.contains('email_format', na=False)
    if email_needs_fix.any():
        df_temp = df[email_needs_fix].copy()
        df_temp = standardize_text(df_temp, 'email', ['lowercase', 'strip'])
        df.loc[email_needs_fix, 'email'] = df_temp['email']
        df.loc[email_needs_fix, 'fixes_applied'] = df.loc[email_needs_fix, 'fixes_applied'] + 'email_cleaned,'
        logger.info(f"  Исправлено {email_needs_fix.sum()} форматов email")
    
    # 2. Заполнение пропусков в age медианой
    age_needs_fix = fixable_mask & df['error_type'].str.contains('age_missing', na=False)
    if age_needs_fix.any():
        median_age = df[df['age'].notna()]['age'].median()
        df.loc[age_needs_fix, 'age'] = median_age
        df.loc[age_needs_fix, 'fixes_applied'] = df.loc[age_needs_fix, 'fixes_applied'] + f'age_filled({median_age}),'
        logger.info(f"  Заполнено {age_needs_fix.sum()} пропусков age медианой: {median_age}")
    
    # 3. Очистка телефонов
    if 'phone' in df.columns:
        df_temp = clean_phone_numbers(df, 'phone', add_country_code='+7')
        df['phone'] = df_temp['phone']
        df['fixes_applied'] = df['fixes_applied'] + 'phone_cleaned,'
        logger.info(f"  Очищены все номера телефонов")
    
    logger.info("-" * 50)
    
    # Сохраняем исправленные
    output_path = '/opt/airflow/data/temp/auto_fixed.csv'
    df.to_csv(output_path, index=False)
    
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'output_path': output_path
    }


def quarantine_invalid_task(**context):
    """Отправка неисправимых записей в карантин."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    logger.info("-" * 50)
    logger.info("QUARANTINING INVALID RECORDS")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='auto_fix')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Записи которые не удалось исправить
    unfixable_mask = df['is_fixable'] == False
    df_quarantine = df[unfixable_mask].copy()
    
    logger.info(f"Записей отправлено в карантин: {len(df_quarantine)}")
    
    if len(df_quarantine) > 0:
        logger.info("Причины карантина:")
        logger.info(df_quarantine[['customer_id', 'error_type']].to_string())
        
        # Сохраняем в карантин
        quarantine_path = f'/opt/airflow/data/quarantine/invalid_records_{ds}.csv'
        os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)
        df_quarantine.to_csv(quarantine_path, index=False)
        logger.info(f"Сохранено в: {quarantine_path}")
    else:
        logger.info("  Нет записей требующих карантина")
        quarantine_path = None
    
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'quarantine_path': quarantine_path,
        'quarantined_count': len(df_quarantine)
    }


def log_changes_task(**context):
    """Логирование всех изменений для аудита."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    logger.info("-" * 50)
    logger.info("LOGGING CHANGES (AUDIT)")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='auto_fix')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Создаём audit log
    audit_log = []
    
    for idx, row in df.iterrows():
        if row['fixes_applied']:
            audit_log.append({
                'customer_id': int(row['customer_id']),
                'date': ds,
                'error_type': row['error_type'],
                'fixes_applied': row['fixes_applied'],
                'status': 'fixed'
            })
        elif not row['is_fixable']:
            audit_log.append({
                'customer_id': int(row['customer_id']),
                'date': ds,
                'error_type': row['error_type'],
                'fixes_applied': 'none',
                'status': 'quarantined'
            })
    
    logger.info(f"Записей в audit log: {len(audit_log)}")
    
    if audit_log:
        # Сохраняем audit log
        audit_path = f'/opt/airflow/data/quality_reports/audit_log_{ds}.json'
        os.makedirs(os.path.dirname(audit_path), exist_ok=True)
        
        with open(audit_path, 'w') as f:
            json.dump(audit_log, f, indent=2)
        
        logger.info(f"Audit log сохранен: {audit_path}")
        
        # Статистика
        fixed_count = sum(1 for entry in audit_log if entry['status'] == 'fixed')
        quarantined_count = sum(1 for entry in audit_log if entry['status'] == 'quarantined')
        
        logger.info(f"Сводка аудита:")
        logger.info(f"  Исправлено: {fixed_count}")
        logger.info(f"  В карантине: {quarantined_count}")
    
    logger.info("-" * 50)
    
    return audit_log


def save_clean_data_task(**context):
    """Сохранение очищенных данных (без карантина)."""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    logger.info("-" * 50)
    logger.info("SAVING CLEAN DATA")
    logger.info("-" * 50)
    
    input_data = ti.xcom_pull(task_ids='auto_fix')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    
    # Только исправленные записи (без карантина)
    df_clean = df[df['is_fixable'] == True].copy()
    
    # Удаляем служебные колонки
    df_clean = df_clean.drop(['error_type', 'is_fixable', 'fixes_applied'], axis=1)
    
    # Сохраняем
    output_path = '/opt/airflow/data/output/customers_error_handled.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_clean.to_csv(output_path, index=False)
    
    logger.info(f"Чистые данные сохранены: {len(df_clean)} записей")
    logger.info(f"  Файл: {output_path}")
    logger.info("-" * 50)
    
    return {
        'status': 'success',
        'output_path': output_path,
        'clean_records': len(df_clean)
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
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_problematic_data,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/customers_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }
)

classify_task = PythonOperator(
    task_id='classify_errors',
    python_callable=classify_errors_task,
    dag=dag
)

fix_task = PythonOperator(
    task_id='auto_fix',
    python_callable=auto_fix_errors_task,
    dag=dag
)

quarantine_task = PythonOperator(
    task_id='quarantine',
    python_callable=quarantine_invalid_task,
    dag=dag
)

audit_task = PythonOperator(
    task_id='log_changes',
    python_callable=log_changes_task,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_results',
    python_callable=save_clean_data_task,
    dag=dag
)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> load_task >> classify_task >> fix_task >> [quarantine_task, audit_task] >> save_task >> end_task

"""
Как запустить:
--------------
1. Убедитесь что файл customers_raw.csv существует
2. Trigger DAG dag_05_error_handling
3. Наблюдайте обработку ошибок

Что проверить:
--------------
1. Логи каждого шага
2. Файлы:
   - customers_error_handled.csv (чистые данные)
   - invalid_records_*.csv (карантин)
   - audit_log_*.json (журнал изменений)

"""
