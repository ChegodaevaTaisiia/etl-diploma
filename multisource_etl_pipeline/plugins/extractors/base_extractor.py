"""
Базовый класс для всех экстракторов.
Определяет общий интерфейс для извлечения данных.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Абстрактный базовый класс для всех экстракторов данных.
    
    Attributes:
        source_name (str): Имя источника данных
        watermark_key (str): Ключ для хранения водяного знака
        batch_size (int): Размер батча для извлечения
    """
    
    def __init__(self, source_name: str, batch_size: int = 1000):
        """
        Инициализация базового экстрактора.
        
        Args:
            source_name: Уникальное имя источника данных
            batch_size: Размер батча для инкрементального извлечения
        """
        self.source_name = source_name
        self.batch_size = batch_size
        self.watermark_key = f"watermark_{source_name}"
        self.logger = logging.getLogger(f"{__name__}.{source_name}")
    
    @abstractmethod
    def extract(self, start_date: Optional[datetime] = None, 
                end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Абстрактный метод для извлечения данных.
        
        Args:
            start_date: Начальная дата для извлечения
            end_date: Конечная дата для извлечения
            
        Returns:
            Список словарей с данными
        """
        pass
    

    def extract_incremental(self) -> List[Dict[str, Any]]:
        """
        Инкрементальное извлечение данных с использованием водяного знака.
        
        Returns:
            Список новых или измененных записей
        """
        last_watermark = self.get_watermark()
        current_time = datetime.now()
        
        self.logger.info(f"Incremental extraction from {last_watermark} to {current_time}")
        
        try:
            # Извлекаем данные с момента последнего водяного знака
            data = self.extract(start_date=last_watermark, end_date=current_time)
            
            # Если данные успешно извлечены, обновляем водяной знак
            if data:
                self.set_watermark(current_time)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Incremental extraction failed: {e}")
            raise
    

    def get_watermark(self) -> Optional[datetime]:
        """
        Получает последний водяной знак из Airflow Variables.
        
        Returns:
            Дата последнего успешного извлечения или None
        """
        try:
            watermark_str = Variable.get(self.watermark_key, default_var=None)
            if watermark_str:
                return datetime.fromisoformat(watermark_str)
        except Exception as e:
            self.logger.warning(f"Failed to get watermark: {e}")
        return None
    
    def set_watermark(self, watermark: datetime) -> None:
        """
        Устанавливает новый водяной знак.
        
        Args:
            watermark: Дата для установки в качестве водяного знака
        """
        try:
            Variable.set(self.watermark_key, watermark.isoformat())
            self.logger.info(f"Watermark updated to {watermark}")
        except Exception as e:
            self.logger.error(f"Failed to set watermark: {e}")
    
    def log_extraction_metrics(self, data_count: int, 
                              start_time: datetime, 
                              end_time: datetime) -> None:
        """
        Логирует метрики извлечения данных.
        
        Args:
            data_count: Количество извлеченных записей
            start_time: Время начала извлечения
            end_time: Время окончания извлечения
        """
        duration = (end_time - start_time).total_seconds()
        self.logger.info(
            f"Extraction completed: {data_count} records in {duration:.2f} seconds "
            f"({data_count/duration if duration > 0 else 0:.2f} records/sec)"
        )