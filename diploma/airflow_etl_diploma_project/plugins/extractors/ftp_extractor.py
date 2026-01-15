"""
Экстрактор для извлечения данных из FTP
"""
import os
import sys
import logging
from io import StringIO
import pandas as pd

from typing import Dict, Any, List, Optional

from airflow.providers.ftp.hooks.ftp import FTPHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class FTPExtractor(BaseExtractor):
    """Класс для извлечения данных из FTP сервера"""
    
    def __init__(self, conn_id: str, 
                 config: Optional[Dict[str, Any]] = None):
        super().__init__(conn_id)
        self.hook = None
        logger.info(f"FTPExtractor initialized: {self.conn_id}")
    
    def extract(self, remote_path: str, file_type: str = 'csv', **kwargs) -> List[Dict[str, Any]]:
        """Извлечение данных из FTP файла"""
        try:
            self.hook = FTPHook(ftp_conn_id=self.conn_id)
            
            logger.info(f"Retrieving file from FTP: {remote_path}")
            file_content = self.hook.retrieve_file(remote_path)
            
            if file_type == 'csv':
                df = pd.read_csv(StringIO(file_content), **kwargs)
                result = df.to_dict('records')
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            self._update_metadata(result, 'success')
            logger.info(f"Extracted {len(result)} records from FTP")
            return result
        except Exception as e:
            self._handle_error(e)
            return []
