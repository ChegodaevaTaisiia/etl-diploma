"""
Экстрактор данных из CSV файлов.
"""
import os
import sys
import pandas as pd

from typing import Optional, List

from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Экстрактор для извлечения данных из CSV файлов."""
    
    def __init__(self, conn_id: str = "csv_extractor", base_path: str = "/"):
        super().__init__(conn_id)
        self.base_path = Path(base_path)

    def extract(
        self,
        filename: str,
        encoding: str = 'utf-8',
        delimiter: str = ',',
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Извлечение данных из CSV файла."""
        try:
            filepath = f"{self.base_path} / {filename}"
            
            if not Path(filepath).exists():
                raise FileNotFoundError(f"File not found: {filepath}")
            
            self.logger.info(f"Reading CSV file: {filepath}")
            
            df = pd.read_csv(
                filepath,
                encoding=encoding,
                delimiter=delimiter,
                usecols=columns
            )
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from CSV: {e}")
            raise
    
    def list_files(self, pattern: str = "*.csv") -> List[str]:
        """Список CSV файлов в директории."""
        return [f.name for f in self.base_path.glob(pattern)]
