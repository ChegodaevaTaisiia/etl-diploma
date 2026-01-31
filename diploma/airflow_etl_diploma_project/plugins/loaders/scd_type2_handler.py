"""
Обработчик SCD Type 2 для измерений Data Warehouse.
Использует Airflow PostgresHook для безопасного подключения (без хардкода паролей).
"""
import pandas as pd
from datetime import date, timedelta
from typing import List, Dict, Any, Optional
import logging

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None


class SCDType2Handler:
    """
    Класс для обработки Slowly Changing Dimensions Type 2.
    
    SCD Type 2 позволяет отслеживать историю изменений в измерениях.
    При изменении атрибута создается новая версия записи.
    
    Поля для SCD Type 2:
    - effective_date: дата начала действия версии
    - expiration_date: дата окончания (9999-12-31 для активной)
    - is_current: флаг текущей версии (TRUE/FALSE)
    """
    def __init__(self, conn_id: str, table_name: str, logger: Optional[logging.Logger] = None):
        """
        Args:
            conn_id: ID подключения Airflow к БД (PostgresHook)
            table_name: Название таблицы измерения (например, 'dim_customers')
            logger: Логгер (опционально)
        """
        self.conn_id = conn_id
        self.table_name = table_name
        self.logger = logger or logging.getLogger(self.__class__.__name__)
    
        # Определяем название surrogate key из названия таблицы
        # dim_customers -> customer_key
        self.surrogate_key = table_name.replace('dim_', '') + '_key'
        
        self._hook = PostgresHook(postgres_conn_id=conn_id) if PostgresHook else None
        self.logger.info(f"SCD Type 2 Handler initialized for table: {table_name}")

    def _get_conn(self):
        """Получение соединения через PostgresHook (без хардкода паролей)."""
        if self._hook:
            return self._hook.get_conn()
        raise RuntimeError("PostgresHook not available. Install apache-airflow-providers-postgres.")

    def process_dimension(
        self,
        natural_key: str,
        new_data: pd.DataFrame,
        effective_date: date,
        tracked_attributes: List[str]
    ):
        """
        Обработка измерения с SCD Type 2.
        Для каждой записи в new_data:
        1. Проверяем, существует ли запись с таким natural_key
        2. Если нет - создаем новую версию (INSERT)
        3. Если есть - сравниваем отслеживаемые атрибуты
        4. Если атрибуты изменились - закрываем старую версию и создаем новую
        5. Если не изменились - ничего не делаем
        Args:
            natural_key: Название поля natural key (например, 'customer_id')
            new_data: DataFrame с новыми данными
            effective_date: Дата вступления изменений в силу
            tracked_attributes: Список отслеживаемых атрибутов (например, ['city', 'email'])
        Returns:
            Статистика обработки: {'inserted': N, 'updated': M, 'unchanged': K}
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        stats = {'inserted': 0, 'updated': 0, 'unchanged': 0}

        try:
            for _, row in new_data.iterrows():
                natural_key_value = row[natural_key]
                
                # Шаг 1: Получение текущей активной версии
                current_version = self._get_current_version(cursor, natural_key, natural_key_value)
                
                if current_version is None:
                    # Шаг 2: Новая запись - создаем первую версию
                    self._insert_new_version(cursor, row, effective_date, natural_key)
                    stats['inserted'] += 1
                    self.logger.info(f"✓ Inserted new record: {natural_key}={natural_key_value}")
                    
                else:
                    # Шаг 3: Существующая запись - проверяем изменения
                    if self._attributes_changed(current_version, row, tracked_attributes):
                        # Шаг 4a: Атрибуты изменились - применяем SCD Type 2
                        
                        # Закрываем текущую версию
                        self._close_current_version(
                            cursor,
                            current_version[self.surrogate_key],
                            effective_date
                        )
                        
                        # Создаем новую версию
                        self._insert_new_version(cursor, row, effective_date, natural_key)
                        
                        stats['updated'] += 1
                        self.logger.info(
                            f"✓ Applied SCD Type 2 for {natural_key}={natural_key_value}: "
                            f"closed key={current_version[self.surrogate_key]}, created new version"
                        )
                    else:
                        # Шаг 4b: Атрибуты не изменились - ничего не делаем
                        stats['unchanged'] += 1
                        self.logger.debug(f"No changes for {natural_key}={natural_key_value}")
            
            # Коммит всех изменений
            conn.commit()
            
            self.logger.info(
                f"SCD Type 2 processing completed: "
                f"inserted={stats['inserted']}, updated={stats['updated']}, unchanged={stats['unchanged']}"
            )
            
            return stats

        except Exception as e:
            # Откатываем изменения при ошибке
            conn.rollback()
            self.logger.error(f"SCD Type 2 processing failed: {e}")
            raise
            
        finally:
            cursor.close()
            conn.close()

    def _get_current_version(
        self, 
        cursor, 
        natural_key: str, 
        natural_key_value: Any
    ) -> Dict:
        """
        Получение текущей активной версии записи.
        Args:
            cursor: Курсор БД
            natural_key: Название поля natural key
            natural_key_value: Значение natural key
        Returns:
            Словарь с данными текущей версии или None если записи нет
        """
        tbl = self.table_name
        query = f"""
        SELECT * FROM {tbl}
        WHERE {natural_key} = %s AND is_current = TRUE
        LIMIT 1
        """
        
        cursor.execute(query, (natural_key_value,))
        row = cursor.fetchone()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None
    
    def _attributes_changed(
        self,
        current_version: Dict,
        new_data: pd.Series,
        tracked_attributes: List[str]
    ) -> bool:
        """
        Проверка изменения отслеживаемых атрибутов.
        Args:
            current_version: Текущая версия из БД
            new_data: Новые данные
            tracked_attributes: Список отслеживаемых атрибутов
       Returns:
            True если хотя бы один атрибут изменился
        """
        for attr in tracked_attributes:
            if attr in new_data and attr in current_version:
                old_value = str(current_version[attr]) if current_version[attr] is not None else ''
                new_value = str(new_data[attr]) if pd.notna(new_data[attr]) else ''
                
                if old_value != new_value:
                    self.logger.debug(
                        f"Attribute '{attr}' changed: '{old_value}' -> '{new_value}'"
                    )
                    return True
        return False
    
    def _close_current_version(
        self,
        cursor,
        surrogate_key: int,
        effective_date: date
    ):
        """
        Закрытие текущей версии записи.
       Устанавливает:
        - expiration_date = effective_date - 1 день
        - is_current = FALSE
        Args:
            cursor: Курсор БД
            surrogate_key: Surrogate key закрываемой версии
            effective_date: Дата вступления новой версии в силу
        """
        expiration_date = effective_date - timedelta(days=1)
        
        query = f"""
        UPDATE {self.table_name}
        SET 
            expiration_date = %s,
            is_current = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE {self.surrogate_key} = %s
        """
        
        cursor.execute(query, (expiration_date, surrogate_key))
    
    def _insert_new_version(
        self,
        cursor,
        data: pd.Series,
        effective_date: date,
        natural_key: str
    ):
        """
        Вставка новой версии записи.
        Args:
            cursor: Курсор БД
            data: Данные для вставки
            effective_date: Дата начала действия версии
            natural_key: Название поля natural key
        """
        # Список полей для вставки (исключаем surrogate_key - он автоинкрементный)
        columns = [col for col in data.index if col != self.surrogate_key]
        columns.extend(['effective_date', 'expiration_date', 'is_current'])
        
        # Значения для вставки: NaN/None -> None, иначе psycopg2 падает на numpy NaN
        raw = [data[col] if col in data.index else None for col in columns[:-3]]
        values = []
        for v in raw:
            if v is None:
                values.append(None)
            elif pd.isna(v):
                values.append(None)
            elif hasattr(v, 'item') and not isinstance(v, (str, date)):  # numpy scalar -> Python
                try:
                    values.append(int(v))
                except (TypeError, ValueError):
                    try:
                        values.append(float(v))
                    except (TypeError, ValueError):
                        values.append(v)
            else:
                values.append(v)
        values.extend([effective_date, date(9999, 12, 31), True])
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        query = f"""
        INSERT INTO {self.table_name} ({columns_str})
        VALUES ({placeholders})
        """
        
        cursor.execute(query, values)
    
    def get_dimension_key_for_date(
        self,
        natural_key: str,
        natural_key_value: Any,
        as_of_date: date
    ) -> Optional[int]:
        """
        Получение surrogate key для конкретной даты.
        Используется при загрузке фактов для привязки к правильной версии измерения.
        Возвращает surrogate_key той версии, которая была активна на указанную дату.
        Args:
            natural_key: Название поля natural key
            natural_key_value: Значение natural key
            as_of_date: Дата, на которую нужно получить версию            
        Returns:
            Surrogate key или None если версия не найдена
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        
        try:
            # Использование параметризованного запроса
            query = f"""
            SELECT {self.surrogate_key}
            FROM {self.table_name}
            WHERE {natural_key} = %s
              AND effective_date <= %s
              AND expiration_date >= %s
            LIMIT 1
            """
            
            cursor.execute(query, (natural_key_value, as_of_date, as_of_date))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            else:
                self.logger.warning(
                    f"No version found for {natural_key}={natural_key_value} on {as_of_date}"
                )
                return None
                
        finally:
            cursor.close()
            conn.close()
