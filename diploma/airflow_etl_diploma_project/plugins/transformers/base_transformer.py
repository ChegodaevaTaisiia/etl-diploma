"""
Базовый класс для трансформации данных.
"""
from abc import ABC, abstractmethod
import pandas as pd
import logging


class BaseTransformer(ABC):
    """Абстрактный базовый класс для трансформации данных."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформация данных."""
        pass
    
    def log_transform_stats(self, df_before: pd.DataFrame, df_after: pd.DataFrame):
        """Логирование статистики трансформации."""
        self.logger.info(f"Transform '{self.name}': {len(df_before)} -> {len(df_after)} records")
