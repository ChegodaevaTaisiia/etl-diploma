import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import logging

# Airflow импорты
from airflow.models import Variable

# Настройка логирования
logger = logging.getLogger(__name__)

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ WATERMARKS
# =============================================================================

def get_watermark(source_name: str) -> Optional[datetime]:
    """
    Получает водяной знак (последнюю дату загрузки) для источника данных.
    
    Args:
        source_name: Название источника данных
        
    Returns:
        datetime или None если водяной знак не найден
    """
    watermark_key = f"watermark_{source_name}"
    watermark_str = Variable.get(watermark_key, default_var=None)
    
    if watermark_str:
        try:
            return datetime.fromisoformat(watermark_str)
        except (ValueError, TypeError):
            logger.warning(f"Не удалось распарсить watermark для {source_name}: {watermark_str}")
            return None
    return None


def set_watermark(source_name: str,  watermark_value: datetime = None) -> None:
    """
    Устанавливает текущую дату как водяной знак для источника данных.
    
    Args:
        source_name: Название источника данных
        watermark_value: Значение водяного знака (если None, устанавливается текущее время)
    """
    watermark_key = f"watermark_{source_name}"
    if watermark_value:
        current_time = watermark_value.isoformat()
    else:
        current_time = datetime.now().isoformat()
    
    Variable.set(watermark_key, current_time)
    logger.info(f"Watermark для {source_name} обновлен: {current_time}")


