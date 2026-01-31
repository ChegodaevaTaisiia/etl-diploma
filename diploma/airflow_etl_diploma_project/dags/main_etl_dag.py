"""
Основной ETL DAG для бизнес-аналитики.
Запуск: ежедневно в 9:00. Собирает данные за предыдущий день из PostgreSQL, MongoDB, CSV, FTP (и опционально API).
Трансформирует, загружает в аналитическую БД и в DWH с SCD Type 2.
"""
from datetime import datetime, timedelta
import sys
import os
import logging

# Airflow добавляет plugins в путь; для локального запуска
if 'plugins' not in sys.path:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

LOG = logging.getLogger(__name__)

# --------------------------------------------------
# ОПРЕДЕЛЕНИЕ КОНВЕЙЕРА (DAG)
# --------------------------------------------------

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'main_etl',
    default_args=default_args,
    description='ETL pipeline для бизнес-аналитики (интернет-магазин)',
    schedule='0 9 * * *',  # Ежедневно в 9:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'analytics', 'daily'],
)


# --------------------------------------------------
# EXTRACT
# --------------------------------------------------

def extract_from_postgres(**context):
    """Извлечение заказов, клиентов и позиций заказов из PostgreSQL (инкрементально по дате)."""
    from extractors.postgres_extractor import PostgresExtractor

    execution_date = context['logical_date']
    start_d = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    end_d = execution_date.strftime('%Y-%m-%d')

    extractor = PostgresExtractor('postgres_source', 'source_db')

    orders_df = extractor.extract_incremental(
        table_name='orders',
        date_column='order_date',
        start_date=start_d,
        end_date=end_d,
    )
    if orders_df.empty:
        return {
            'orders': [], 'customers': [], 'order_items': [],
            'count_orders': 0, 'count_customers': 0, 'count_order_items': 0,
        }

    customer_ids = orders_df['customer_id'].dropna().unique().tolist()
    order_ids = orders_df['order_id'].dropna().unique().tolist()

    customers_df = pd.DataFrame()
    if customer_ids:
        customers_df = extractor.extract(
            query='SELECT * FROM customers WHERE customer_id = ANY(%(ids)s)',
            params={'ids': customer_ids},
        )
    order_items_df = pd.DataFrame()
    if order_ids:
        order_items_df = extractor.extract(
            query='SELECT * FROM order_items WHERE order_id = ANY(%(ids)s)',
            params={'ids': order_ids},
        )

    return {
        'orders': orders_df.to_dict('records'),
        'customers': customers_df.to_dict('records') if not customers_df.empty else [],
        'order_items': order_items_df.to_dict('records') if not order_items_df.empty else [],
        'count_orders': len(orders_df),
        'count_customers': len(customers_df),
        'count_order_items': len(order_items_df),
    }


def extract_from_mongo(**context):
    """Извлечение отзывов из MongoDB за период."""
    from extractors.mongo_extractor import MongoExtractor

    execution_date = context['logical_date']
    start_d = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    end_d = execution_date.strftime('%Y-%m-%d')

    extractor = MongoExtractor(conn_id='mongo_source', database='feedback_db')
    feedback_df = extractor.extract_by_date(
        'feedback',
        start_d,
        end_d,
    )
    if feedback_df.empty:
        return {'feedback': [], 'count': 0}
    return {'feedback': feedback_df.to_dict('records'), 'count': len(feedback_df)}


def extract_from_csv(**context):
    """Извлечение продуктов из CSV (последние файлы за 7 дней)."""
    from extractors.csv_extractor import CSVExtractor

    extractor = CSVExtractor(conn_id='csv_extractor', base_path='/opt/airflow/data/csv')
    latest_paths = extractor.extract_latest_files(days=7, limit=5)
    all_data = []
    for path in latest_paths:
        try:
            df = extractor.extract(path.name, encoding='utf-8', delimiter=',')
            all_data.append(df)
        except Exception as e:
            LOG.warning("Skip CSV %s: %s", path.name, e)
    combined = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    if not combined.empty:
        combined = combined.drop_duplicates(subset=['product_id'], keep='last')
    return {'products': combined.to_dict('records') if not combined.empty else [], 'count': len(combined)}


def extract_from_api(**context):
    """Опционально: извлечение из REST API. При отсутствии API возвращает пустой результат."""
    from extractors.api_extractor import APIExtractor
    try:
        extractor = APIExtractor('api_service', 'https://api.example.com')
        analytics = extractor.extract('/api/v1/analytics/daily-stats')
        return {'analytics': analytics or [], 'count': len(analytics or [])}
    except Exception as e:
        LOG.warning("API extract skipped: %s", e)
        return {'analytics': [], 'count': 0}


def extract_from_ftp(**context):
    """Извлечение логов доставки с FTP (если есть CSV)."""
    from extractors.ftp_extractor import FTPExtractor
    try:
        extractor = FTPExtractor('ftp_server')
        files = extractor.list_remote_files(directory="/", pattern="*.csv")
        if not files:
            return {'logs': [], 'count': 0, 'files_found': []}
        remote_path = f"/{files[0]}"
        logs = extractor.extract(remote_path, file_type='csv')
        return {'logs': logs or [], 'count': len(logs or []), 'files_found': files}
    except Exception as e:
        LOG.warning("FTP extract skipped: %s", e)
        return {'logs': [], 'count': 0, 'files_found': []}


def validate_data(**context):
    """Проверка наличия и объёмов извлечённых данных."""
    ti = context['ti']
    pg = ti.xcom_pull(task_ids='extract_from_postgres') or {}
    mongo = ti.xcom_pull(task_ids='extract_from_mongo') or {}
    csv_d = ti.xcom_pull(task_ids='extract_from_csv') or {}
    LOG.info(
        "Postgres: orders=%s customers=%s order_items=%s | Mongo: %s | CSV products: %s",
        pg.get('count_orders', 0), pg.get('count_customers', 0), pg.get('count_order_items', 0),
        mongo.get('count', 0), csv_d.get('count', 0),
    )
    return {'status': 'validated'}


# --------------------------------------------------
# TRANSFORM
# --------------------------------------------------

def transform_data(**context):
    """Очистка, валидация и нормализация заказов и клиентов. Продукты и отзывы проходят без строгой валидации."""
    from transformers.data_normilizer import DataNormalizer
    from validators.data_validator import DataValidator

    ti = context['ti']
    pg = ti.xcom_pull(task_ids='extract_from_postgres') or {}
    mongo = ti.xcom_pull(task_ids='extract_from_mongo') or {}
    csv_d = ti.xcom_pull(task_ids='extract_from_csv') or {}

    orders = pg.get('orders', [])
    customers = pg.get('customers', [])
    order_items = pg.get('order_items', [])
    products = csv_d.get('products', [])
    feedback = mongo.get('feedback', [])

    orders_df = pd.DataFrame(orders) if orders else pd.DataFrame()
    customers_df = pd.DataFrame(customers) if customers else pd.DataFrame()
    order_items_df = pd.DataFrame(order_items) if order_items else pd.DataFrame()
    products_df = pd.DataFrame(products) if products else pd.DataFrame()
    feedback_df = pd.DataFrame(feedback) if feedback else pd.DataFrame()

    if not orders_df.empty:
        validator = DataValidator()
        orders_df, _, _ = validator.validate_orders(orders_df)
        normalizer = DataNormalizer()
        orders_df = normalizer.transform(orders_df)
        orders_df = orders_df.drop_duplicates(subset=['order_id'], keep='last')
        if 'total_amount' in orders_df.columns:
            orders_df['total_amount'] = pd.to_numeric(orders_df['total_amount'], errors='coerce').fillna(0).round(2)
    if not customers_df.empty:
        normalizer = DataNormalizer()
        customers_df = normalizer.transform(customers_df)
        customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='last')
    if not order_items_df.empty and 'total_price' not in order_items_df.columns and 'unit_price' in order_items_df.columns and 'quantity' in order_items_df.columns:
        order_items_df['total_price'] = order_items_df['unit_price'] * order_items_df['quantity']

    return {
        'orders': orders_df.to_dict('records') if not orders_df.empty else [],
        'customers': customers_df.to_dict('records') if not customers_df.empty else [],
        'order_items': order_items_df.to_dict('records') if not order_items_df.empty else [],
        'products': products_df.to_dict('records') if not products_df.empty else [],
        'feedback': feedback_df.to_dict('records') if not feedback_df.empty else [],
    }


# --------------------------------------------------
# LOAD
# --------------------------------------------------

def load_to_analytics(**context):
    """Агрегация за день и загрузка в daily_business_analytics (UPSERT)."""
    from loaders.analytics_loader import AnalyticsLoader

    ti = context['ti']
    execution_date = context['logical_date']
    analytics_date = (execution_date - timedelta(days=1)).date()

    transformed = ti.xcom_pull(task_ids='transform_data') or {}
    orders = transformed.get('orders', [])
    feedback = transformed.get('feedback', [])

    orders_df = pd.DataFrame(orders) if orders else pd.DataFrame()
    feedback_df = pd.DataFrame(feedback) if feedback else pd.DataFrame()

    total_orders = len(orders_df)
    total_revenue = float(orders_df['total_amount'].sum()) if not orders_df.empty and 'total_amount' in orders_df.columns else 0
    avg_order_value = total_revenue / total_orders if total_orders else 0
    unique_customers = orders_df['customer_id'].nunique() if not orders_df.empty and 'customer_id' in orders_df.columns else 0

    status_col = 'status' if 'status' in orders_df.columns else None
    orders_pending = int((orders_df[status_col] == 'pending').sum()) if status_col and not orders_df.empty else 0
    orders_processing = int((orders_df[status_col] == 'processing').sum()) if status_col and not orders_df.empty else 0
    orders_shipped = int((orders_df[status_col] == 'shipped').sum()) if status_col and not orders_df.empty else 0
    orders_delivered = int((orders_df[status_col] == 'delivered').sum()) if status_col and not orders_df.empty else 0
    orders_cancelled = int((orders_df[status_col] == 'cancelled').sum()) if status_col and not orders_df.empty else 0

    rating_col = 'rating' if 'rating' in feedback_df.columns else None
    avg_customer_rating = float(feedback_df[rating_col].mean()) if rating_col and not feedback_df.empty else 0

    loader = AnalyticsLoader(conn_id='postgres_analytics')
    loader.load(analytics_date, {
        'total_orders': total_orders,
        'total_revenue': round(total_revenue, 2),
        'avg_order_value': round(avg_order_value, 2),
        'orders_pending': orders_pending,
        'orders_processing': orders_processing,
        'orders_shipped': orders_shipped,
        'orders_delivered': orders_delivered,
        'orders_cancelled': orders_cancelled,
        'unique_customers': unique_customers,
        'new_customers': 0,
        'avg_customer_rating': round(avg_customer_rating, 2),
        'total_products_sold': 0,
        'top_category': None,
        'top_product': None,
        'total_deliveries': 0,
        'avg_delivery_time_minutes': 0,
        'successful_delivery_rate': 0,
    })
    return {'status': 'loaded', 'analytics_date': str(analytics_date)}


def load_to_dwh(**context):
    """Загрузка в DWH: dim_customers (SCD Type 2), dim_products (SCD Type 2), fact_orders (customer_key на дату заказа)."""
    import traceback
    from loaders.dwh_loader import DWHLoader

    ti = context['ti']
    execution_date = context['logical_date']
    effective_date = execution_date.date() if hasattr(execution_date, 'date') else execution_date

    transformed = ti.xcom_pull(task_ids='transform_data') or {}
    orders = transformed.get('orders', [])
    customers = transformed.get('customers', [])
    order_items = transformed.get('order_items', [])
    products = transformed.get('products', [])

    orders_df = pd.DataFrame(orders) if orders else pd.DataFrame()
    customers_df = pd.DataFrame(customers) if customers else pd.DataFrame()
    order_items_df = pd.DataFrame(order_items) if order_items else pd.DataFrame()
    products_df = pd.DataFrame(products) if products else pd.DataFrame()

    try:
        loader = DWHLoader(dwh_conn_id='postgres_analytics')
        stats_c = loader.load_dim_customers(customers_df, effective_date)
        stats_p = loader.load_dim_products(products_df, effective_date)
        stats_f = loader.load_fact_orders(order_items_df, orders_df, effective_date)
        return {
            'status': 'loaded_to_dwh',
            'dim_customers': stats_c,
            'dim_products': stats_p,
            'fact_orders': stats_f,
        }
    except Exception as e:
        LOG.error("load_to_dwh failed: %s\n%s", e, traceback.format_exc())
        raise


# --------------------------------------------------
# ЗАДАЧИ И ЗАВИСИМОСТИ
# --------------------------------------------------

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

extract_postgres = PythonOperator(task_id='extract_from_postgres', python_callable=extract_from_postgres, dag=dag)
extract_mongo = PythonOperator(task_id='extract_from_mongo', python_callable=extract_from_mongo, dag=dag)
extract_csv = PythonOperator(task_id='extract_from_csv', python_callable=extract_from_csv, dag=dag)
extract_api = PythonOperator(task_id='extract_from_api', python_callable=extract_from_api, dag=dag)
extract_ftp = PythonOperator(task_id='extract_from_ftp', python_callable=extract_from_ftp, dag=dag)
validate = PythonOperator(task_id='validate_data', python_callable=validate_data, dag=dag)
transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
load_analytics = PythonOperator(task_id='load_to_analytics', python_callable=load_to_analytics, dag=dag)
load_dwh = PythonOperator(task_id='load_to_dwh', python_callable=load_to_dwh, dag=dag)

start >> [extract_postgres, extract_mongo, extract_csv, extract_api, extract_ftp]
[extract_postgres, extract_mongo, extract_csv, extract_api, extract_ftp] >> validate
validate >> transform
transform >> [load_analytics, load_dwh]
[load_analytics, load_dwh] >> end
