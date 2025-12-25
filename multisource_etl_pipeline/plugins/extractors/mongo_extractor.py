"""
Экстрактор для MongoDB с использованием Airflow Hooks.
Поддерживает incremental load через водяные знаки.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError
from bson import ObjectId
import json

from airflow.hooks.base import BaseHook

from base_extractor import BaseExtractor


class MongoExtractor(BaseExtractor):
    """
    Экстрактор для MongoDB.
    
    Использует Airflow Connection для управления подключениями
    и поддерживает incremental load через водяные знаки.
    """
    
    def __init__(self, conn_id: str, source_name: str,
                 collection_name: str, batch_size: int = 1000):
        """
        Инициализация MongoDB экстрактора.
        
        Args:
            conn_id: ID подключения в Airflow
            source_name: Имя источника данных
            collection_name: Имя коллекции MongoDB
            batch_size: Размер батча для извлечения
        """
        super().__init__(source_name, batch_size)
        self.conn_id = conn_id
        self.collection_name = collection_name
        self._client: Optional[MongoClient] = None
        self._collection = None
        
    @property
    def client(self) -> MongoClient:
        """Ленивая инициализация MongoDB клиента."""
        if self._client is None:
            conn = BaseHook.get_connection(self.conn_id)
            
            # Строим строку подключения
            connection_string = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/"
            
            # Добавляем параметры из extra
            extra_params = json.loads(conn.extra) if conn.extra else {}
            
            self._client = MongoClient(
                connection_string,
                **extra_params
            )
            
        return self._client
    
    @property
    def collection(self):
        """Получает коллекцию MongoDB."""
        if self._collection is None:
            conn = BaseHook.get_connection(self.conn_id)
            database_name = conn.schema or 'admin'
            db = self.client[database_name]
            self._collection = db[self.collection_name]
        
        return self._collection
    
    def extract(self, start_date: Optional[datetime] = None,
                end_date: Optional[datetime] = None,
                query_filter: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Извлекает данные из MongoDB коллекции.
        
        Args:
            start_date: Начальная дата для фильтрации
            end_date: Конечная дата для фильтрации
            query_filter: Дополнительный фильтр для запроса
            
        Returns:
            Список документов MongoDB
        """
        start_time = datetime.now()
        
        try:
            # Строим базовый запрос
            mongo_filter = query_filter or {}
            
            # Добавляем фильтр по дате если указан
            if start_date:
                mongo_filter['updated_at'] = mongo_filter.get('updated_at', {})
                mongo_filter['updated_at']['$gte'] = start_date
            
            if end_date:
                mongo_filter['updated_at'] = mongo_filter.get('updated_at', {})
                mongo_filter['updated_at']['$lte'] = end_date
            
            self.logger.info(f"Extracting from MongoDB with filter: {mongo_filter}")
            
            # Используем курсор для батчевого извлечения
            cursor = self.collection.find(
                mongo_filter,
                batch_size=self.batch_size
            ).sort('_id', ASCENDING)
            
            results = []
            for doc in cursor:
                # Конвертируем ObjectId в строку
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
                
                # Конвертируем другие BSON типы если необходимо
                results.append(self._convert_bson_types(doc))
            
            end_time = datetime.now()
            self.log_extraction_metrics(len(results), start_time, end_time)
            
            return results
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB extraction failed: {e}")
            raise
    
    def _convert_bson_types(self, doc: Dict) -> Dict:
        """
        Конвертирует BSON типы в Python типы для сериализации.
        
        Args:
            doc: Документ MongoDB
            
        Returns:
            Документ с конвертированными типами
        """
        import bson
        
        converted = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                converted[key] = str(value)
            elif isinstance(value, bson.Decimal128):
                converted[key] = str(value)
            elif isinstance(value, datetime):
                converted[key] = value.isoformat()
            elif isinstance(value, dict):
                converted[key] = self._convert_bson_types(value)
            elif isinstance(value, list):
                converted[key] = [
                    self._convert_bson_types(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                converted[key] = value
        
        return converted
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Получает статистику коллекции.
        
        Returns:
            Словарь со статистикой
        """
        try:
            stats = self.collection.aggregate([
                {
                    "$group": {
                        "_id": None,
                        "count": {"$sum": 1},
                        "size_bytes": {"$sum": {"$bsonSize": "$$ROOT"}}
                    }
                }
            ])
            
            stats_list = list(stats)
            if stats_list:
                return stats_list[0]
            return {}
            
        except PyMongoError as e:
            self.logger.error(f"Failed to get collection stats: {e}")
            return {}
    
    def close(self):
        """Закрывает соединение с MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._collection = None