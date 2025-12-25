import ftplib
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import os
import tempfile
import re

from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session


class FTPExtractor:
    """
    Экстрактор для работы с FTP серверами через Airflow Connections.
    Поддерживает инкрементальную загрузку CSV файлов.
    """
    
    def __init__(self, conn_id: str, source_name: str = "ftp_source"):
        """
        Инициализация FTP экстрактора с использованием Airflow Connection.
        
        Args:
            conn_id: ID подключения в Airflow
            source_name: Имя источника данных
        """
        self.conn_id = conn_id
        self.source_name = source_name
        self._connection = None
        self._ftp_config = None
    
    @property
    def config(self) -> Dict[str, Any]:
        """Получает конфигурацию из Airflow Connection."""
        if self._ftp_config is None:
            try:
                # Получаем подключение из Airflow
                conn = BaseHook.get_connection(self.conn_id)
                
                self._ftp_config = {
                    'host': conn.host or 'localhost',
                    'username': conn.login or 'anonymous',
                    'password': conn.password or '',
                    'port': conn.port or 21,
                    'extra_params': conn.extra_dejson or {}
                }
                
                # Добавляем параметры из extra
                extra = self._ftp_config['extra_params']
                if 'passive_mode' in extra:
                    self._ftp_config['passive_mode'] = extra['passive_mode']
                if 'timeout' in extra:
                    self._ftp_config['timeout'] = int(extra['timeout'])
                if 'encoding' in extra:
                    self._ftp_config['encoding'] = extra['encoding']
                    
            except Exception as e:
                raise AirflowException(
                    f"Failed to get FTP connection '{self.conn_id}' from Airflow: {str(e)}"
                )
        
        return self._ftp_config
    
    @property
    def connection(self) -> ftplib.FTP:
        """Ленивое подключение к FTP серверу."""
        if self._connection is None:
            try:
                config = self.config
                
                self._connection = ftplib.FTP()
                
                # Таймаут подключения
                timeout = config.get('timeout', 30)
                self._connection.connect(config['host'], config['port'], timeout=timeout)
                
                # Логин
                self._connection.login(config['username'], config['password'])
                
                # Настройки
                encoding = config.get('encoding', 'utf-8')
                self._connection.encoding = encoding
                
                # Пассивный режим
                if config.get('passive_mode', True):
                    self._connection.set_pasv(True)
                
                # Проверяем соединение
                self._connection.voidcmd("NOOP")
                
                print(f"Successfully connected to FTP server {config['host']}")
                
            except Exception as e:
                raise AirflowException(
                    f"Failed to connect to FTP server {self.config['host']}: {str(e)}"
                )
        return self._connection
    
    def list_files(self, directory: str = "/", pattern: str = "*.csv") -> List[str]:
        """
        Получает список файлов на FTP сервере.
        
        Args:
            directory: Директория на FTP сервере
            pattern: Шаблон для фильтрации файлов
            
        Returns:
            Список файлов
        """
        try:
            # Переходим в указанную директорию
            if directory != "/" and directory != "":
                self.connection.cwd(directory)
            
            # Получаем список файлов
            files = self.connection.nlst()
            
            # Фильтруем по расширению
            if pattern == "*.csv":
                return [f for f in files if f.lower().endswith('.csv')]
            elif pattern == "*.xlsx":
                return [f for f in files if f.lower().endswith('.xlsx')]
            else:
                # Общий фильтр по регулярному выражению
                import fnmatch
                return [f for f in files if fnmatch.fnmatch(f.lower(), pattern.lower())]
                
        except Exception as e:
            raise AirflowException(f"Failed to list files in {directory}: {str(e)}")
    
    def download_file(self, remote_path: str, local_directory: str = None) -> str:
        """
        Скачивает файл с FTP сервера во временный файл.
        
        Args:
            remote_path: Путь к файлу на FTP сервере
            local_directory: Директория для сохранения (None = временная)
            
        Returns:
            Путь к локальному файлу
        """
        if local_directory:
            os.makedirs(local_directory, exist_ok=True)
            file_name = os.path.basename(remote_path)
            local_path = os.path.join(local_directory, file_name)
        else:
            temp_file = tempfile.NamedTemporaryFile(
                delete=False, 
                suffix=os.path.splitext(remote_path)[1]
            )
            local_path = temp_file.name
        
        try:
            # Определяем режим загрузки (бинарный или текстовый)
            file_ext = os.path.splitext(remote_path)[1].lower()
            binary_mode = file_ext in ['.csv', '.xlsx', '.xls', '.zip', '.gz']
            
            if binary_mode:
                with open(local_path, 'wb') as f:
                    self.connection.retrbinary(f'RETR {remote_path}', f.write)
            else:
                with open(local_path, 'w', encoding=self.connection.encoding) as f:
                    self.connection.retrlines(f'RETR {remote_path}', f.write)
            
            print(f"Successfully downloaded {remote_path} to {local_path}")
            return local_path
            
        except Exception as e:
            if os.path.exists(local_path):
                os.unlink(local_path)
            raise AirflowException(f"Failed to download file {remote_path}: {str(e)}")
    
    def extract_csv_data(self, file_path: str, watermark: Optional[datetime] = None,
                        timestamp_column: str = "sale_date", 
                        encoding: str = 'utf-8', delimiter: str = ',') -> List[Dict[str, Any]]:
        """
        Извлекает данные из CSV файла с фильтрацией по водяному знаку.
        
        Args:
            file_path: Путь к локальному CSV файлу
            watermark: Водяной знак для инкрементальной загрузки
            timestamp_column: Имя колонки с меткой времени
            encoding: Кодировка файла
            delimiter: Разделитель
            
        Returns:
            Список словарей с данными
        """
        try:
            # Читаем CSV файл
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
            
            # Логируем информацию о файле
            print(f"Read CSV file: {len(df)} rows, {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            
            # Фильтруем по водяному знаку если указан
            if watermark is not None:
                # Ищем колонку с временной меткой
                timestamp_col = None
                possible_names = [timestamp_column, 'timestamp', 'date', 'created_at', 'updated_at']
                
                for col_name in possible_names:
                    if col_name in df.columns:
                        timestamp_col = col_name
                        break
                
                if timestamp_col:
                    try:
                        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
                        df = df[df[timestamp_col] > watermark]
                        print(f"Filtered by watermark: {len(df)} rows remain")
                    except Exception as e:
                        print(f"Warning: Could not filter by timestamp column {timestamp_col}: {e}")
            
            # Конвертируем в список словарей
            if not df.empty:
                # Конвертируем Timestamp в строку для сериализации
                records = []
                for _, row in df.iterrows():
                    record = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            record[col] = None
                        elif isinstance(value, pd.Timestamp):
                            record[col] = value.isoformat()
                        else:
                            record[col] = value
                    records.append(record)
                
                return records
            return []
            
        except Exception as e:
            raise AirflowException(f"Failed to extract data from CSV {file_path}: {str(e)}")
    
    def close(self):
        """Закрывает соединение с FTP сервером."""
        if self._connection:
            try:
                self._connection.quit()
            except:
                try:
                    self._connection.close()
                except:
                    pass
            finally:
                self._connection = None
    
    def __enter__(self):
        """Поддержка контекстного менеджера."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие соединения."""
        self.close()
    
    def test_connection(self) -> bool:
        """Тестирует подключение к FTP серверу."""
        try:
            # Пробуем выполнить простую команду
            self.connection.voidcmd("NOOP")
            return True
        except Exception:
            return False




