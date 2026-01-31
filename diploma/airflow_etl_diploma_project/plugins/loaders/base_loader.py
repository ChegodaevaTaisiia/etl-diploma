"""
Базовый класс для загрузки данных в целевые хранилища.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional
import logging
import pandas as pd


class BaseLoader(ABC):
    """Абстрактный базовый класс для загрузки данных."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def load(self, data: Any, **kwargs) -> dict:
        """
        Загрузка данных в целевое хранилище.
        Returns:
            Словарь со статистикой загрузки (например, {'rows_inserted': N})
        """
        pass
