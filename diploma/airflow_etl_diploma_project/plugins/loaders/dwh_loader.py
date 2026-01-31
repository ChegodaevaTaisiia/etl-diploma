"""
Загрузка данных в Data Warehouse с поддержкой SCD Type 2.
Использует PostgresHook — пароли только в .env и Airflow Connections.
"""
import logging
from datetime import date, datetime
from typing import Dict, List, Any, Optional

import pandas as pd

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from scd_type2_handler import SCDType2Handler


class DWHLoader:
    """
    Загрузка в DWH: dim_customers (SCD Type 2), dim_products (SCD Type 2), fact_orders.
    При загрузке fact_orders используется customer_key на дату заказа (get_dimension_key_for_date).
    """

    def __init__(self, dwh_conn_id: str = 'postgres_analytics'):
        self.dwh_conn_id = dwh_conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self._hook = PostgresHook(postgres_conn_id=dwh_conn_id) if PostgresHook else None

    def _get_conn(self):
        if self._hook:
            return self._hook.get_conn()
        raise RuntimeError("PostgresHook not available.")

    def load_dim_customers(
        self,
        customers_df: pd.DataFrame,
        effective_date: date,
        tracked_attributes: Optional[List[str]] = None,
    ) -> dict:
        """
        Загрузка/обновление dim_customers с SCD Type 2.
        customers_df должен содержать: customer_id, first_name, last_name, email, phone, city, country.
        customer_segment можно добавить или оставить NULL.
        """
        if customers_df.empty:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}

        # Приводим к нужным колонкам (dim_customers без SCD полей)
        dim_cols = [
            'customer_id', 'first_name', 'last_name', 'email', 'phone',
            'city', 'country', 'customer_segment'
        ]
        df = customers_df.copy()
        for c in dim_cols:
            if c not in df.columns:
                df[c] = None
        df = df[dim_cols].drop_duplicates(subset=['customer_id'], keep='last')

        tracked = tracked_attributes or ['city', 'country', 'email', 'phone']
        handler = SCDType2Handler(self.dwh_conn_id, 'dim_customers', self.logger)
        return handler.process_dimension(
            natural_key='customer_id',
            new_data=df,
            effective_date=effective_date,
            tracked_attributes=tracked,
        )

    def load_dim_products(
        self,
        products_df: pd.DataFrame,
        effective_date: date,
        tracked_attributes: Optional[List[str]] = None,
    ) -> dict:
        """
        Загрузка/обновление dim_products с SCD Type 2.
        products_df: product_id, product_name, category, subcategory, price, cost (опционально).
        """
        if products_df.empty:
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}

        dim_cols = ['product_id', 'product_name', 'category', 'subcategory', 'price', 'cost']
        df = products_df.copy()
        for c in dim_cols:
            if c not in df.columns:
                df[c] = None
        df = df[dim_cols].drop_duplicates(subset=['product_id'], keep='last')

        tracked = tracked_attributes or ['product_name', 'category', 'price']
        handler = SCDType2Handler(self.dwh_conn_id, 'dim_products', self.logger)
        return handler.process_dimension(
            natural_key='product_id',
            new_data=df,
            effective_date=effective_date,
            tracked_attributes=tracked,
        )

    def load_fact_orders(
        self,
        order_items_df: pd.DataFrame,
        orders_df: pd.DataFrame,
        effective_date: date,
    ) -> dict:
        """
        Загрузка fact_orders. order_items_df: order_id, product_id, quantity, unit_price, total_price.
        orders_df: order_id, customer_id, order_date, total_amount, status, payment_method.
        Для каждой строки order_items привязываем customer_key на дату заказа (SCD Type 2).
        """
        if order_items_df.empty or orders_df.empty:
            return {'rows_inserted': 0}

        conn = self._get_conn()
        cursor = conn.cursor()
        customer_handler = SCDType2Handler(self.dwh_conn_id, 'dim_customers', self.logger)

        orders_by_id = orders_df.set_index('order_id').to_dict('index')
        inserted = 0

        # Кеш product_key по product_id (текущая версия)
        cursor.execute("""
            SELECT product_id, product_key FROM dim_products WHERE is_current = TRUE
        """)
        product_key_map = {int(r[0]): int(r[1]) for r in cursor.fetchall()}
        if not product_key_map:
            self.logger.warning("dim_products is empty — load dim_products first (e.g. run extract_from_csv and load_to_dwh with products)")

        for _, item in order_items_df.iterrows():
            order_id = int(item['order_id'])
            if order_id not in orders_by_id:
                continue
            order = orders_by_id[order_id]
            customer_id = int(order['customer_id'])
            order_date = order['order_date']
            if isinstance(order_date, str):
                try:
                    order_date = datetime.fromisoformat(order_date.replace('Z', '+00:00'))
                except ValueError:
                    order_date = pd.to_datetime(order_date)
            if hasattr(order_date, 'date'):
                order_date_val = order_date.date()
            else:
                order_date_val = pd.to_datetime(order_date).date() if hasattr(pd, 'to_datetime') else order_date

            customer_key = customer_handler.get_dimension_key_for_date(
                'customer_id', customer_id, order_date_val
            )
            if customer_key is None:
                self.logger.warning(f"No customer_key for customer_id={customer_id} on {order_date_val}, skip order_id={order_id}")
                continue

            product_id = int(item['product_id'])
            product_key = product_key_map.get(product_id)
            if product_key is None:
                self.logger.warning(f"No product_key for product_id={product_id}, skip")
                continue

            date_key = int(order_date_val.strftime('%Y%m%d'))
            if hasattr(order_date, 'hour'):
                hour, minute = order_date.hour, order_date.minute
            else:
                _dt = pd.to_datetime(order_date)
                hour, minute = _dt.hour, _dt.minute
            time_key = hour * 100 + minute

            # Проверка: date_key и time_key должны быть в dim_date / dim_time (FK)
            cursor.execute("SELECT 1 FROM dim_date WHERE date_key = %s", (date_key,))
            if not cursor.fetchone():
                self.logger.warning(f"date_key {date_key} not in dim_date, skip order_id={order_id}")
                continue
            cursor.execute("SELECT 1 FROM dim_time WHERE time_key = %s", (time_key,))
            if not cursor.fetchone():
                self.logger.warning(f"time_key {time_key} not in dim_time, skip order_id={order_id}")
                continue

            quantity = int(item.get('quantity', 0))
            unit_price = float(item.get('unit_price', 0))
            total_amount = float(item.get('total_price', 0) or item.get('total_amount', 0))
            status = str(order.get('status', ''))[:50]
            payment_method = str(order.get('payment_method', ''))[:50] if order.get('payment_method') else None

            cursor.execute("""
                INSERT INTO fact_orders (
                    order_id, customer_key, product_key, date_key, time_key,
                    quantity, unit_price, total_amount, order_status, payment_method
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, customer_key, product_key, date_key, time_key,
                  quantity, unit_price, total_amount, status, payment_method))
            inserted += 1

        conn.commit()
        cursor.close()
        conn.close()
        self.logger.info(f"Inserted {inserted} rows into fact_orders")
        return {'rows_inserted': inserted}
