"""
Загрузчик агрегированных метрик в аналитическую БД (daily_business_analytics).
Использует Airflow PostgresHook — пароли только в .env и Airflow Connections.
"""
import logging
from datetime import date
from typing import Dict, Any, Optional

import pandas as pd

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None


class AnalyticsLoader:
    """
    Загрузка агрегатов за день в таблицу daily_business_analytics.
    Стратегия: UPSERT по analytics_date.
    """

    def __init__(self, conn_id: str = 'postgres_analytics'):
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self._hook = PostgresHook(postgres_conn_id=conn_id) if PostgresHook else None

    def _get_conn(self):
        if self._hook:
            return self._hook.get_conn()
        raise RuntimeError("PostgresHook not available.")

    def load(
        self,
        analytics_date: date,
        metrics: Dict[str, Any],
    ) -> dict:
        """
        Вставка или обновление строки за analytics_date в daily_business_analytics.
        metrics: словарь с полями total_orders, total_revenue, avg_order_value, и т.д.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        defaults = {
            'total_orders': 0,
            'total_revenue': 0,
            'avg_order_value': 0,
            'orders_pending': 0,
            'orders_processing': 0,
            'orders_shipped': 0,
            'orders_delivered': 0,
            'orders_cancelled': 0,
            'unique_customers': 0,
            'new_customers': 0,
            'avg_customer_rating': 0,
            'total_products_sold': 0,
            'top_category': None,
            'top_product': None,
            'total_deliveries': 0,
            'avg_delivery_time_minutes': 0,
            'successful_delivery_rate': 0,
        }
        row = {**defaults, **metrics}
        row['analytics_date'] = analytics_date

        cols = ['analytics_date'] + [k for k in defaults]
        vals = [row.get(c) for c in cols]

        set_clause = ', '.join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'analytics_date'
        )
        placeholders = ', '.join(['%s'] * len(cols))
        cols_str = ', '.join(f'"{c}"' for c in cols)

        sql = f"""
        INSERT INTO daily_business_analytics ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (analytics_date) DO UPDATE SET {set_clause}
        """
        cursor.execute(sql, vals)
        conn.commit()
        cursor.close()
        conn.close()
        self.logger.info(f"Upserted daily_business_analytics for {analytics_date}")
        return {'rows_affected': 1}
