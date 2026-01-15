"""
Базовый класс для всех экстракторов данных.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional
import logging
import pandas as pd


class BaseExtractor(ABC):
    """
    Абстрактный базовый класс для извлечения данных.
    
    Все экстракторы должны наследоваться от этого класса
    и реализовывать метод extract().
    """
    
    def __init__(self, conn_id: str):
        """
        Инициализация экстрактора.
        
        Args:
            conn_id: ID подключения к источнику данных (Airflow Connection ID) - должно быть уникальным для каждого источника
        """
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initialized {self.__class__.__name__} for connection: {conn_id}")

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Извлечение данных из источника.
        
        Returns:
            DataFrame с извлеченными данными
        """
        pass
    
    def validate_connection(self) -> bool:
        """
        Проверка подключения к источнику данных.
        
        Returns:
            True если подключение успешно, False иначе
        """
        try:
            # Реализация в дочерних классах
            return True
        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            return False
    
    def get_metadata(self) -> dict:
        """
        Получение метаданных об источнике.
        
        Returns:
            Словарь с метаданными
        """
        return {
            'conn_id': self.conn_id,
            'extractor_type': self.__class__.__name__
        }
    
    def log_extraction_stats(self, df: pd.DataFrame):
        """
        Логирование статистики извлеченных данных.
        
        Args:
            df: DataFrame с данными
        """
        self.logger.info(f"Extracted {len(df)} records from {self.conn_id}")
        self.logger.info(f"Columns: {list(df.columns)}")
        self.logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
