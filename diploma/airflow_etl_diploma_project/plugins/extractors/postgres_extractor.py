"""
Экстрактор данных из PostgreSQL.
"""
import os
import sys
import pandas as pd

from typing import Optional, List, Dict, Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class PostgresExtractor(BaseExtractor):
    """
    Экстрактор для извлечения данных из PostgreSQL.
    
    Использует Airflow PostgresHook для безопасного подключения.
    """
    
    def __init__(self, conn_id: str, database: str):
        """
        Инициализация PostgreSQL экстрактора.
        
        Args:
            conn_id: ID подключения в Airflow Connections
            database: Название базы данных
        """
        super().__init__(conn_id)
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.logger.info(f"PostgreSQL Extractor initialized with conn_id: {conn_id}")
    
    def extract(
        self,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        query: Optional[str] = None,
        where_clause: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Извлечение данных из PostgreSQL.
        Args:
            table_name: Название таблицы
            columns: Список колонок для выборки
            query: SQL запрос (если указан, table_name игнорируется)
            where_clause: WHERE условие
            params: Параметры для запроса
        Returns:
            DataFrame с данными
        """
        try:
            if query:
                # Использование пользовательского запроса
                self.logger.info(f"Executing custom query: {query[:100]}...")
                df = self.hook.get_pandas_df(query, parameters=params)
            else:
                # Построение запроса из параметров
                if not table_name:
                    raise ValueError("Either 'query' or 'table_name' must be provided")
                
                cols = ', '.join(columns) if columns else '*'
                where = f"WHERE {where_clause}" if where_clause else ''
                
                query = f"SELECT {cols} FROM {table_name} {where}"

                self.logger.info(f"Executing query: {query} with params: {params}")                
                
                df = self.hook.get_pandas_df(query, parameters=params)
            
            # ДОБАВЛЕНО: Проверка на пустой результат
            if df.empty:
                self.logger.warning(f"Query returned no data from {self.conn_id}")
            else:
                self.log_extraction_stats(df)

            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract data from PostgreSQL: {e}")
            raise
    
    def extract_incremental(
        self,
        table_name: str,
        date_column: str,
        start_date: str,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Инкрементальное извлечение данных по дате.
        Args:
            table_name: Название таблицы
            date_column: Название колонки с датой
            start_date: Начальная дата (включительно)
            end_date: Конечная дата (не включительно)
        Returns:
            DataFrame с данными за период
        """
        where = f"{date_column} >= %(start_date)s"
        params = {'start_date': start_date}
        
        if end_date:
            where += f" AND {date_column} < %(end_date)s"
            params['end_date'] = end_date
        
        return self.extract(
            table_name=table_name,
            where_clause=where,
            params=params
        )
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Получение схемы таблицы.
        Args:
            table_name: Название таблицы
        Returns:
            DataFrame со схемой таблицы
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_name = %(table_name)s
        ORDER BY ordinal_position
        """
        return self.hook.get_pandas_df(query, parameters={'table_name': table_name})
