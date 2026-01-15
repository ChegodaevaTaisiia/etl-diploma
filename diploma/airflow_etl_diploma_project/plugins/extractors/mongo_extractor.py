"""
Экстрактор данных из MongoDB.
"""
import os
import sys
import pandas as pd

from typing import Optional, Dict, Any
from datetime import datetime

from airflow.providers.mongo.hooks.mongo import MongoHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class MongoExtractor(BaseExtractor):
    """Экстрактор для извлечения данных из MongoDB."""
    
    def __init__(self, conn_id: str, database: str):
        super().__init__(conn_id)
        self.database = database
        self.hook = MongoHook(mongo_conn_id=conn_id)
        self.logger.info(f"MongoDB Extractor initialized with conn_id: {self.conn_id}, database: {database}")   

    def extract(
        self,
        collection: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
            Извлечение данных из MongoDB коллекции.
        Args:
            collection: Название коллекции
            query: Запрос для фильтрации
            projection: Проекция полей
            limit: Максимальное количество документов
        Returns:
            DataFrame с данными
        """
        client = None           
        try:
            client = self.hook.get_conn()
            db = client[self.database]
            coll = db[collection]
            
            query = query or {}
            cursor = coll.find(query, projection)
            
            if limit:
                cursor = cursor.limit(limit)
            
            data = list(cursor)

            if not data:
                self.logger.warning(f"Query returned no documents from {collection}")
                return pd.DataFrame()

            df = pd.DataFrame(data)
            
            # Конвертация ObjectId в строку
            if '_id' in df.columns:
                df['_id'] = df['_id'].astype(str)
            
            # Конвертация datetime в pandas datetime
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Проверяем, не являются ли значения datetime
                    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if isinstance(sample, datetime):
                        df[col] = pd.to_datetime(df[col])

            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from MongoDB: {e}")
            raise
        finally:
            if client is not None:
                client.close()
                self.logger.debug("MongoDB connection closed")
    
    def extract_by_date(
        self,
        collection: str,
        date_field: str,
        start_date: datetime,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Извлечение данных по дате.
        Args:
            collection: Название коллекции
            date_field: Поле с датой
            start_date: Начальная дата (включительно)
            end_date: Конечная дата (не включительно)
        Returns:
            DataFrame с данными за период
        """
        query = {date_field: {"$gte": start_date}}
        if end_date:
            query[date_field]["$lt"] = end_date
        
        self.logger.info(
            f"Date-based extract from {collection}: "
            f"{start_date} to {end_date if end_date else datetime.now().strftime('%Y-%m-%d')}"
        )

        return self.extract(collection=collection, query=query)
