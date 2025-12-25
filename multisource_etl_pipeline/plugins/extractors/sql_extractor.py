"""
Универсальный экстрактор для SQL баз данных с использованием Airflow Connections.
Поддерживает различные диалекты через SQLAlchemy.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from sqlalchemy import create_engine, text, MetaData, Table, select, inspect
from sqlalchemy.engine import Engine
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
import pandas as pd
import json

from base_extractor import BaseExtractor


class SQLExtractor(BaseExtractor):
    """
    Универсальный экстрактор для SQL баз данных с использованием Airflow Connections.
    
    Поддерживает различные диалекты SQL через SQLAlchemy:
    - PostgreSQL
    - MySQL
    - SQLite
    - Oracle
    - MS SQL
    """
    
    def __init__(self, conn_id: str, source_name: str, 
                 table_name: str, id_column: str = 'id',
                 watermark_column: str = 'created_at',
                 use_connection_params: bool = True,
                 custom_connection_string: Optional[str] = None,
                 **kwargs):
        """
        Инициализация экстрактора базы данных с использованием Airflow Connections.
        
        Args:
            conn_id: ID подключения в Airflow
            source_name: Имя источника данных
            table_name: Имя таблицы для извлечения
            id_column: Имя колонки с уникальным идентификатором
            watermark_column: Имя колонки с меткой времени
            use_connection_params: Использовать параметры из Connection (True) 
                                   или строку подключения (False)
            custom_connection_string: Кастомная строка подключения 
                                      (если use_connection_params=False)
            **kwargs: Дополнительные аргументы для BaseExtractor
        """
        super().__init__(source_name, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.id_column = id_column
        self.watermark_column = watermark_column
        self.use_connection_params = use_connection_params
        self.custom_connection_string = custom_connection_string
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
    
    def _get_connection_string(self) -> str:
        """Генерирует строку подключения из Airflow Connection."""
        if not self.use_connection_params and self.custom_connection_string:
            return self.custom_connection_string
            
        try:
            # Получаем connection из Airflow
            self._connection = BaseHook.get_connection(self.conn_id)
            
            # Определяем тип базы данных
            conn_type = self._connection.conn_type.lower()
            
            # Генерируем строку подключения в зависимости от типа
            if conn_type in ['postgres', 'postgresql']:
                return self._build_postgresql_uri()
            elif conn_type == 'mysql':
                return self._build_mysql_uri()
            elif conn_type == 'mssql':
                return self._build_mssql_uri()
            elif conn_type == 'oracle':
                return self._build_oracle_uri()
            elif conn_type == 'sqlite':
                return self._build_sqlite_uri()
            else:
                # Используем стандартную строку подключения из extra
                extra = self._connection.extra_dejson
                if 'connection_string' in extra:
                    return extra['connection_string']
                else:
                    # Пробуем собрать из стандартных параметров
                    return self._build_generic_uri()
                    
        except Exception as e:
            self.logger.error(f"Failed to get connection {self.conn_id}: {e}")
            raise
    
    def _build_postgresql_uri(self) -> str:
        """Строит URI для PostgreSQL."""
        host = self._connection.host
        port = self._connection.port or 5432
        login = self._connection.login
        password = self._connection.password
        schema = self._connection.schema or 'postgres'
        
        # Проверяем extra параметры
        extra = self._connection.extra_dejson
        sslmode = extra.get('sslmode', 'prefer')
        options = extra.get('options', '')
        
        uri = f"postgresql://{login}:{password}@{host}:{port}/{schema}"
        
        # Добавляем параметры
        params = []
        if sslmode:
            params.append(f"sslmode={sslmode}")
        if options:
            params.append(f"options={options}")
        
        if params:
            uri += "?" + "&".join(params)
            
        return uri
    
    def _build_mysql_uri(self) -> str:
        """Строит URI для MySQL."""
        host = self._connection.host
        port = self._connection.port or 3306
        login = self._connection.login
        password = self._connection.password
        schema = self._connection.schema
        
        extra = self._connection.extra_dejson
        charset = extra.get('charset', 'utf8mb4')
        ssl = extra.get('ssl', {})
        
        uri = f"mysql://{login}:{password}@{host}:{port}/{schema}"
        
        # Добавляем параметры
        params = []
        if charset:
            params.append(f"charset={charset}")
        
        ssl_params = []
        if ssl.get('ca'):
            ssl_params.append(f"ssl_ca={ssl['ca']}")
        if ssl.get('cert'):
            ssl_params.append(f"ssl_cert={ssl['cert']}")
        if ssl.get('key'):
            ssl_params.append(f"ssl_key={ssl['key']}")
        
        if ssl_params:
            params.extend(ssl_params)
        
        if params:
            uri += "?" + "&".join(params)
            
        return uri
    
    def _build_mssql_uri(self) -> str:
        """Строит URI для MS SQL Server."""
        host = self._connection.host
        port = self._connection.port or 1433
        login = self._connection.login
        password = self._connection.password
        schema = self._connection.schema
        
        extra = self._connection.extra_dejson
        driver = extra.get('driver', 'ODBC Driver 17 for SQL Server')
        
        # Используем pyodbc драйвер
        uri = f"mssql+pyodbc://{login}:{password}@{host}:{port}/{schema}"
        uri += f"?driver={driver.replace(' ', '+')}"
        
        return uri
    
    def _build_oracle_uri(self) -> str:
        """Строит URI для Oracle."""
        host = self._connection.host
        port = self._connection.port or 1521
        login = self._connection.login
        password = self._connection.password
        schema = self._connection.schema or 'XE'  # Default SID
        
        extra = self._connection.extra_dejson
        service_name = extra.get('service_name', schema)
        
        # Формат: oracle://user:pass@host:port/dbname
        uri = f"oracle://{login}:{password}@{host}:{port}/{service_name}"
        
        return uri
    
    def _build_sqlite_uri(self) -> str:
        """Строит URI для SQLite."""
        # Для SQLite host содержит путь к файлу
        path = self._connection.host
        if not path:
            raise ValueError("SQLite connection requires path to database file")
        
        uri = f"sqlite:///{path}"
        return uri
    
    def _build_generic_uri(self) -> str:
        """Строит generic URI из параметров connection."""
        conn_type = self._connection.conn_type
        host = self._connection.host
        port = self._connection.port
        login = self._connection.login
        password = self._connection.password
        schema = self._connection.schema
        
        parts = [f"{conn_type}://"]
        
        if login and password:
            parts.append(f"{login}:{password}@")
        elif login:
            parts.append(f"{login}@")
        
        parts.append(host)
        
        if port:
            parts.append(f":{port}")
        
        if schema:
            parts.append(f"/{schema}")
        
        return "".join(parts)
    
    @property
    def engine(self) -> Engine:
        """Ленивая инициализация движка SQLAlchemy."""
        if self._engine is None:
            connection_string = self._get_connection_string()
            self.logger.info(f"Using connection string: {self._mask_password(connection_string)}")
            
            self._engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False  # Включайте только для отладки
            )
        return self._engine
    
    def _mask_password(self, connection_string: str) -> str:
        """Маскирует пароль в строке подключения для логов."""
        import re
        # Маскируем пароли в формате user:password@
        return re.sub(r':([^:@]+)@', ':****@', connection_string)
    
    def extract(self, start_date: Optional[datetime] = None,
                end_date: Optional[datetime] = None,
                columns: Optional[List[str]] = None,
                where_clause: Optional[str] = None,
                order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Извлекает данные из SQL базы данных.
        
        Args:
            start_date: Начальная дата для фильтрации
            end_date: Конечная дата для фильтрации
            columns: Список колонок для извлечения (все если None)
            where_clause: Дополнительное условие WHERE в виде строки
            order_by: Поле для сортировки результатов
            
        Returns:
            Список словарей с данными
        """
        start_time = datetime.now()
        
        try:
            # Используем SQLAlchemy Core для работы с метаданными
            metadata = MetaData()
            
            # Рефлексим таблицу
            with self.engine.connect() as conn:
                inspector = inspect(self.engine)
                
                # Проверяем существование таблицы
                if self.table_name not in inspector.get_table_names():
                    raise ValueError(f"Table {self.table_name} not found in database")
                
                # Рефлексим таблицу
                table = Table(self.table_name, metadata, autoload_with=self.engine)
            
            # Строим базовый запрос
            if columns:
                # Проверяем существование колонок
                valid_columns = []
                for col in columns:
                    if col in table.columns:
                        valid_columns.append(table.c[col])
                    else:
                        self.logger.warning(f"Column {col} not found in table {self.table_name}")
                
                if not valid_columns:
                    valid_columns = [table]
                
                query = select(*valid_columns)
            else:
                query = select(table)
            
            # Добавляем фильтр по дате если указан
            if start_date and self.watermark_column in table.columns:
                query = query.where(
                    table.c[self.watermark_column] >= start_date
                )
            if end_date and self.watermark_column in table.columns:
                query = query.where(
                    table.c[self.watermark_column] <= end_date
                )
            
            # Добавляем кастомное условие WHERE
            if where_clause:
                query = query.where(text(where_clause))
            
            # Добавляем сортировку
            if order_by and order_by in table.columns:
                query = query.order_by(table.c[order_by])
            
            # Выполняем запрос с пагинацией
            results = []
            offset = 0
            
            while True:
                paginated_query = query.limit(self.batch_size).offset(offset)
                
                with self.engine.connect() as conn:
                    batch = conn.execute(paginated_query).fetchall()
                
                if not batch:
                    break
                    
                # Конвертируем в словари
                for row in batch:
                    # Преобразуем специальные типы данных
                    row_dict = {}
                    for key, value in dict(row._mapping).items():
                        if isinstance(value, (datetime, pd.Timestamp)):
                            row_dict[key] = value.isoformat()
                        else:
                            row_dict[key] = value
                    results.append(row_dict)
                
                offset += self.batch_size
                
                if len(results) % (self.batch_size * 10) == 0:
                    self.logger.info(f"Fetched {len(results)} records...")
            
            end_time = datetime.now()
            extraction_time = (end_time - start_time).total_seconds()
            
            self.logger.info(
                f"Successfully extracted {len(results)} records "
                f"from {self.table_name} in {extraction_time:.2f} seconds"
            )
            
            return results
            
        except Exception as e:
            self.logger.error(
                f"Failed to extract data from {self.table_name} "
                f"using connection {self.conn_id}: {e}"
            )
            raise
    
    def get_table_schema(self) -> Dict[str, Any]:
        """
        Получает схему таблицы.
        
        Returns:
            Словарь с информацией о колонках таблицы
        """
        try:
            with self.engine.connect() as conn:
                inspector = inspect(self.engine)
                
                # Получаем информацию о колонках
                columns_info = []
                for column in inspector.get_columns(self.table_name):
                    col_info = {
                        'name': column['name'],
                        'type': str(column['type']),
                        'nullable': column.get('nullable', True),
                        'default': column.get('default'),
                        'autoincrement': column.get('autoincrement', False)
                    }
                    
                    # Дополнительная информация о первичном ключе
                    pk_columns = inspector.get_pk_constraint(self.table_name)
                    col_info['primary_key'] = column['name'] in pk_columns.get('constrained_columns', [])
                    
                    columns_info.append(col_info)
                
                # Получаем индексы
                indexes_info = []
                for index in inspector.get_indexes(self.table_name):
                    indexes_info.append({
                        'name': index['name'],
                        'columns': index['column_names'],
                        'unique': index['unique']
                    })
                
                # Получаем внешние ключи
                foreign_keys_info = []
                for fk in inspector.get_foreign_keys(self.table_name):
                    foreign_keys_info.append({
                        'name': fk.get('name'),
                        'constrained_columns': fk['constrained_columns'],
                        'referred_table': fk['referred_table'],
                        'referred_columns': fk['referred_columns']
                    })
                
                return {
                    "table_name": self.table_name,
                    "connection_id": self.conn_id,
                    "columns": columns_info,
                    "indexes": indexes_info,
                    "foreign_keys": foreign_keys_info,
                    "row_count": self.get_row_count(),
                    "size_info": self._get_table_size_info()
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get schema for {self.table_name}: {e}")
            return {}
    
    def _get_table_size_info(self) -> Dict[str, Any]:
        """Получает информацию о размере таблицы."""
        try:
            conn_type = self._connection.conn_type.lower()
            
            with self.engine.connect() as conn:
                if conn_type in ['postgres', 'postgresql']:
                    query = text("""
                        SELECT 
                            pg_size_pretty(pg_total_relation_size(:table_name)) as total_size,
                            pg_size_pretty(pg_relation_size(:table_name)) as table_size,
                            pg_size_pretty(pg_indexes_size(:table_name)) as indexes_size
                    """)
                    result = conn.execute(query, {"table_name": self.table_name}).fetchone()
                    return dict(result._mapping) if result else {}
                    
                elif conn_type == 'mysql':
                    query = text("""
                        SELECT 
                            table_name AS "Table",
                            round(((data_length + index_length) / 1024 / 1024), 2) "Size in MB"
                        FROM information_schema.TABLES 
                        WHERE table_schema = DATABASE()
                        AND table_name = :table_name
                    """)
                    result = conn.execute(query, {"table_name": self.table_name}).fetchone()
                    return dict(result._mapping) if result else {}
            
            return {}
        except Exception:
            # Если не удалось получить размер таблицы, возвращаем пустой словарь
            return {}
    
    def get_row_count(self) -> int:
        """Получает количество строк в таблице."""
        try:
            with self.engine.connect() as conn:
                # Используем безопасный способ с параметрами
                query = text(f"SELECT COUNT(*) as count FROM {self.table_name}")
                result = conn.execute(query).fetchone()
                return result[0] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to get row count for {self.table_name}: {e}")
            return 0
    
    def test_connection(self) -> bool:
        """Тестирует подключение к базе данных."""
        try:
            with self.engine.connect() as conn:
                # Выполняем простой запрос
                result = conn.execute(text("SELECT 1")).fetchone()
                return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed for {self.conn_id}: {e}")
            return False
    
    def get_sample_data(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получает пример данных из таблицы."""
        try:
            metadata = MetaData()
            table = Table(self.table_name, metadata, autoload_with=self.engine)
            
            query = select(table).limit(limit)
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchall()
            
            return [dict(row._mapping) for row in result]
            
        except Exception as e:
            self.logger.error(f"Failed to get sample data: {e}")
            return []
    
    def execute_query(self, sql: str, parameters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Выполняет произвольный SQL запрос.
        
        Args:
            sql: SQL запрос
            parameters: Параметры для запроса
            
        Returns:
            Результаты запроса
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), parameters or {})
                
                if result.returns_rows:
                    rows = result.fetchall()
                    return [dict(row._mapping) for row in rows]
                else:
                    return []
                    
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise
    
    def close(self):
        """Закрывает соединение с базой данных."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
    
    def __enter__(self):
        """Поддержка контекстного менеджера."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Закрытие соединения при выходе из контекста."""
        self.close()