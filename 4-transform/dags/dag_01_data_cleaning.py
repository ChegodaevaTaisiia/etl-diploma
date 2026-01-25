"""
DAG 01: Очистка данных (Data Cleaning)

Демонстрирует базовые операции очистки данных:
- Удаление дубликатов
- Обработка пропусков
- Удаление выбросов
- Стандартизация текста

Граф: start → load → remove_dups → handle_missing → remove_outliers → standardize → save → end

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import logging
import sys
import os

# Добавляем путь к plugins
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from transform.cleaners import remove_duplicates, handle_missing_values, remove_outliers, filter_by_range, standardize_text, replace_values, clean_phone_numbers


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

dag_id = 'dag_01_data_cleaning'
dag_tags = ['lesson4', 'cleaning', 'data-quality', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def load_raw_data(**context):
    """
    Загрузка "грязных" данных для очистки.
    
    В реальном проекте здесь был бы extract из БД или файла.
    Для демонстрации создаём тестовый DataFrame с проблемами.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("LOADING RAW DATA WITH QUALITY ISSUES")
    logger.info("-" * 50)
    
    # Извлечение параметров
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']

    # Проверка существования файла
    if not os.path.exists(input_path):
        error_msg = f"File not found: {input_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)   
    try:
        # Чтение CSV с переданными параметрами
        df = pd.read_csv(
            input_path,
            delimiter=delimiter,
            encoding=encoding,
            on_bad_lines='warn'
        )

        # вывод сообщений
        logger.info(f"Количество записей: {len(df)}")
        logger.info(f"Количество столбцов: {len(df.columns)} {list(df.columns)}")
        logger.info(f"Количество дубликатов: {df.duplicated(subset=['customer_id']).sum()}")
        logger.info(f"Количество пропусков: {df.isnull().sum().sum()}")
        
        # Сохранение данных для следующих задач
        output_path = '/opt/airflow/data/temp/customers_raw.csv'
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
        
    except Exception as e:
        error_result = {
            'status': 'error',
            'input_path': input_path,
            'error': str(e),
            'params': params
        }
        logger.error(f"Error processing CSV: {e}")
        raise



def remove_duplicates_task(**context):
    """
    Удаление дубликатов по customer_id.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 1: REMOVING DUPLICATES")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='load_raw_data')
    input_path=input_data['output_path']
    df = pd.read_csv(input_path)
    
    logger.info(f"Обработано записей: {len(df)}")
    
    # Удаляем дубликаты
    df_clean = remove_duplicates(df, subset=['customer_id'], keep='first')
    
    logger.info(f"Осталось записей: {len(df_clean)}")
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/customers_step1.csv'
    df_clean.to_csv(output_path, index=False)

    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'rows_after':  len(df_clean),
        'output_path': output_path
    }

    return result


def handle_missing_task(**context):
    """
    Обработка пропущенных значений.
    
    Стратегии:
    - age: заполнить медианой
    - city: заполнить "Unknown"
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 2: HANDLING MISSING VALUES")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='remove_duplicates')
    input_path = input_data['input_path']
    df = pd.read_csv(input_path)
    
    logger.info(f"Пропуски до:")
    for col in df.columns:
        missing = df[col].isnull().sum()
        if missing > 0:
            logger.info(f"  {col}: {missing}")
    
    # Обрабатываем пропуски
    strategy = {
        'age': 'median',            # Возраст: медиана
        'city': 'constant:Unknown'  # Город: "Unknown"
    }
    
    df_clean = handle_missing_values(df, strategy)
    
    logger.info(f"Пропуски после: {df_clean.isnull().sum().sum()}")
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/customers_step2.csv'
    df_clean.to_csv(output_path, index=False)

    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'rows_after':  len(df_clean),
        'output_path': output_path
    }
    
    return result

def remove_outliers_task(**context):
    """
    Удаление выбросов и некорректных значений.
    - age: должен быть в диапазоне [0, 120]
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 3: REMOVING OUTLIERS")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='handle_missing')
    input_path = input_data['input_path']    
    df = pd.read_csv(input_path)
    
    logger.info(f"Обработано записей: {len(df)}")
    logger.info(f"  Диапазон Age: [{df['age'].min()}, {df['age'].max()}]")
    
    # Фильтруем по допустимому диапазону
    df_clean = filter_by_range(df, 'age', min_value=0, max_value=120)
    
    logger.info(f"Осталось записей: {len(df_clean)}")
    logger.info(f"  Диапазон Age: [{df_clean['age'].min()}, {df_clean['age'].max()}]")
    logger.info("-" * 50)
    
    # Сохраняем
    output_path = '/opt/airflow/data/temp/customers_step3.csv'
    df_clean.to_csv(output_path, index=False)

    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'rows_after':  len(df_clean),
        'output_path': output_path
    }

    return result

def standardize_task(**context):
    """
    Стандартизация текста и значений.
    - email: lowercase, strip
    - gender: унификация (М/м/Male → male, Ж/ж/Female → female)
    - phone: очистка и нормализация
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("STEP 4: STANDARDIZING DATA")
    logger.info("-" * 50)
    
    # Загружаем данные
    input_data = ti.xcom_pull(task_ids='remove_outliers')
    input_path = input_data['input_path']    
    df = pd.read_csv(input_path)
    
    # Стандартизация email
    df_clean = standardize_text(df, 'email', ['lowercase', 'strip'])
    
    # Стандартизация gender
    gender_mapping = {
        'М': 'male', 'м': 'male', 'M': 'male', 'Male': 'male',
        'Ж': 'female', 'ж': 'female', 'F': 'female', 'Female': 'female'
    }
    df_clean = replace_values(df_clean, 'gender', gender_mapping)
    
    logger.info(f"Значения колонки Gender после стандартизации: {df_clean['gender'].unique()}")
    
    # Очистка телефонов
    df_clean = clean_phone_numbers(df_clean, 'phone', keep_only_digits=True, add_country_code='+7')
    
    logger.info("-" * 50)

    # Сохраняем финальный результат
    output_path = '/opt/airflow/data/output/customers_clean.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_clean.to_csv(output_path, index=False)

    # Собираем информацию о результате
    result = {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'rows_after':  len(df_clean),
        'output_path': output_path
    }

    return result

def save_and_report(**context):
    """
    Сохранение финального результата и создание отчёта.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("FINAL REPORT")
    logger.info("-" * 50)
    
    # Загружаем чистые данные
    input_data = ti.xcom_pull(task_ids='standardize')
    clean_path = input_data['output_path']    
    df_clean = pd.read_csv(clean_path)
    
    # Загружаем исходные данные для сравнения
    raw_data = ti.xcom_pull(task_ids='load_raw_data')
    raw_path = raw_data['input_path']
    df_raw = pd.read_csv(raw_path)
    
    logger.info(f"Всего записей: {len(df_raw)}")
    logger.info(f"Осталось записей: {len(df_clean)}")
    logger.info(f"Удалено: {len(df_raw) - len(df_clean)} ({(len(df_raw) - len(df_clean))/len(df_raw)*100:.1f}%)")
    logger.info("Оценка качества:")
    logger.info(f"  Удалено дубликатов: {df_raw.duplicated(subset=['customer_id']).sum()}")
    logger.info(f"  Заполнено недостающих значений: {df_raw.isnull().sum().sum()}")
    logger.info(f"  Стандартизация значений: email, gender, phone")
    logger.info(f"Очещенные данные сохранены в файле: {clean_path}")
    logger.info("-" * 50)
    
    return {
        'original_records': len(df_raw),
        'clean_records': len(df_clean),
        'removed_records': len(df_raw) - len(df_clean),
        'clean_file': clean_path
    }


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='Демонстрация очистки данных',
    schedule_interval=None,  # Ручной запуск
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

load_task = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    dag=dag,
    params={
        'input_path': '/opt/airflow/data/input/customers_raw.csv',
        'delimiter': ',',
        'encoding': 'utf-8'
    }
)

remove_dups_task = PythonOperator(
    task_id='remove_duplicates',
    python_callable=remove_duplicates_task,
    dag=dag,
)

handle_missing_task = PythonOperator(
    task_id='handle_missing',
    python_callable=handle_missing_task,
    dag=dag,
)

remove_outliers_task = PythonOperator(
    task_id='remove_outliers',
    python_callable=remove_outliers_task,
    dag=dag,
)

standardize_task = PythonOperator(
    task_id='standardize',
    python_callable=standardize_task,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_and_report',
    python_callable=save_and_report,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

start_task >> load_task >> remove_dups_task >> handle_missing_task >> remove_outliers_task >> standardize_task >> save_task >> end_task


# =============================================================================
# ИНСТРУКЦИИ
# =============================================================================

"""
Как запустить:
--------------
1. Убедитесь что Airflow запущен
2. Найдите DAG dag_01_data_cleaning в UI
3. Нажмите "Trigger DAG"
4. Наблюдайте выполнение в Graph View

Что проверить:
--------------
1. Логи каждой задачи - там подробная информация
2. Файлы в /opt/airflow/data/:
   - temp/customers_raw.csv - исходные данные
   - temp/customers_step*.csv - промежуточные результаты
   - output/customers_clean.csv - финальный результат

"""
