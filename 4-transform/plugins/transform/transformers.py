"""
Модуль для трансформации данных (Data Transformation)

Содержит функции для изменения структуры и формата данных.

"""

import pandas as pd
from typing import List, Dict, Any, Callable
import logging

logger = logging.getLogger(__name__)


def convert_types(df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Приведение типов данных колонок.
    Args:
        df: DataFrame
        type_mapping: {колонка: новый_тип}
            Типы: 'int', 'float', 'str', 'datetime', 'bool'
    Returns:
        DataFrame с новыми типами
    Example:
        >>> df = convert_types(df, {
        ...     'age': 'int',
        ...     'price': 'float',
        ...     'order_date': 'datetime'
        ... })
    """
    df_transformed = df.copy()
    
    for column, dtype in type_mapping.items():
        if column not in df_transformed.columns:
            logger.warning(f"Column '{column}' not found")
            continue
        
        try:
            if dtype == 'datetime':
                df_transformed[column] = pd.to_datetime(df_transformed[column], errors='coerce')
            elif dtype == 'int':
                df_transformed[column] = pd.to_numeric(df_transformed[column], errors='coerce').astype('Int64')
            elif dtype == 'float':
                df_transformed[column] = pd.to_numeric(df_transformed[column], errors='coerce')
            elif dtype == 'str':
                df_transformed[column] = df_transformed[column].astype(str)
            elif dtype == 'bool':
                df_transformed[column] = df_transformed[column].astype(bool)
            
            logger.info(f"  {column}: преобразовано в {dtype}")
        
        except Exception as e:
            logger.error(f"  {column}: ошибка преобразования в {dtype}: {e}")
    
    return df_transformed


def create_calculated_column(
    df: pd.DataFrame,
    new_column: str,
    calculation: Callable
) -> pd.DataFrame:
    """
    Создание вычисляемой колонки.
    Args:
        df: DataFrame
        new_column: Имя новой колонки
        calculation: Функция для вычисления (принимает df, возвращает Series)
    Returns:
        DataFrame с новой колонкой
    Example:
        >>> # Создать колонку total = price * quantity
        >>> df = create_calculated_column(
        ...     df, 'total',
        ...     lambda df: df['price'] * df['quantity']
        ... )
    """
    df_transformed = df.copy()
    df_transformed[new_column] = calculation(df)
    
    logger.info(f"  Создана колонка: {new_column}")
    
    return df_transformed


def categorize_numeric(
    df: pd.DataFrame,
    column: str,
    new_column: str,
    bins: List[float],
    labels: List[str]
) -> pd.DataFrame:
    """
    Категоризация числовых значений (binning).
    Args:
        df: DataFrame
        column: Числовая колонка
        new_column: Имя новой категориальной колонки
        bins: Границы интервалов
        labels: Метки категорий
    Returns:
        DataFrame с категориальной колонкой
    Example:
        >>> # Возрастные группы
        >>> df = categorize_numeric(
        ...     df, 'age', 'age_group',
        ...     bins=[0, 18, 35, 60, 100],
        ...     labels=['child', 'young', 'adult', 'senior']
        ... )
    """
    df_transformed = df.copy()
    df_transformed[new_column] = pd.cut(
        df[column],
        bins=bins,
        labels=labels,
        include_lowest=True
    )
    
    logger.info(f" Категории {column} в {new_column}")
    
    return df_transformed


def normalize_column(
    df: pd.DataFrame,
    column: str,
    method: str = 'minmax'
) -> pd.DataFrame:
    """
    Нормализация числовых значений.
    Args:
        df: DataFrame
        column: Колонка для нормализации
        method: Метод нормализации
            - 'minmax': (x - min) / (max - min) → [0, 1]
            - 'zscore': (x - mean) / std → среднее=0, std=1
    Returns:
        DataFrame с нормализованной колонкой
    Example:
        >>> df = normalize_column(df, 'price', method='minmax')
    """
    df_transformed = df.copy()
    
    if method == 'minmax':
        min_val = df[column].min()
        max_val = df[column].max()
        df_transformed[column] = (df[column] - min_val) / (max_val - min_val)
        logger.info(f"  {column}: нормалищована min-max в интервал [0, 1]")
    
    elif method == 'zscore':
        mean_val = df[column].mean()
        std_val = df[column].std()
        df_transformed[column] = (df[column] - mean_val) / std_val
        logger.info(f"  {column}: нормализована z-score в (mean=0, std=1)")
    
    return df_transformed


def split_column(
    df: pd.DataFrame,
    column: str,
    separator: str,
    new_columns: List[str]
) -> pd.DataFrame:
    """
    Разделение одной колонки на несколько.
    Args:
        df: DataFrame
        column: Колонка для разделения
        separator: Разделитель
        new_columns: Имена новых колонок
    Returns:
        DataFrame с новыми колонками
    Example:
        >>> # Разделить "Smith, John" на фамилию и имя
        >>> df = split_column(
        ...     df, 'full_name', ',',
        ...     new_columns=['last_name', 'first_name']
        ... )
    """
    df_transformed = df.copy()
    
    split_data = df[column].str.split(separator, expand=True)
    
    for i, new_col in enumerate(new_columns):
        if i < len(split_data.columns):
            df_transformed[new_col] = split_data[i].str.strip()
    
    logger.info(f"  Колонка {column} разделена на {new_columns}")
    
    return df_transformed


def merge_columns(
    df: pd.DataFrame,
    columns: List[str],
    new_column: str,
    separator: str = ' '
) -> pd.DataFrame:
    """
    Объединение нескольких колонок в одну.
    Args:
        df: DataFrame
        columns: Колонки для объединения
        new_column: Имя новой колонки
        separator: Разделитель (по умолчанию пробел)
    Returns:
        DataFrame с новой объединённой колонкой
    Example:
        >>> # Объединить first_name и last_name
        >>> df = merge_columns(
        ...     df, ['first_name', 'last_name'],
        ...     'full_name', separator=' '
        ... )
    """
    df_transformed = df.copy()
    
    df_transformed[new_column] = df[columns].apply(
        lambda row: separator.join(row.values.astype(str)),
        axis=1
    )
    
    logger.info(f"  Колонки {columns} слиты в {new_column}")
    
    return df_transformed


def pivot_data(
    df: pd.DataFrame,
    index: str,
    columns: str,
    values: str,
    aggfunc: str = 'sum'
) -> pd.DataFrame:
    """
    Pivot трансформация (long → wide).
    Args:
        df: DataFrame в long формате
        index: Колонка для строк
        columns: Колонка для столбцов
        values: Колонка со значениями
        aggfunc: Функция агрегации ('sum', 'mean', 'count')
    Returns:
        DataFrame в wide формате
    Example:
        >>> # Продажи по месяцам и продуктам
        >>> df_pivot = pivot_data(
        ...     df, index='month', columns='product',
        ...     values='sales', aggfunc='sum'
        ... )
    """
    df_pivot = df.pivot_table(
        index=index,
        columns=columns,
        values=values,
        aggfunc=aggfunc,
        fill_value=0
    )
    
    logger.info(f"  Преобразована (pivot): {index} x {columns} with {values}")
    
    return df_pivot.reset_index()


def aggregate_data(
    df: pd.DataFrame,
    group_by: List[str],
    aggregations: Dict[str, Any]
) -> pd.DataFrame:
    """
    Агрегация данных с группировкой.
    Args:
        df: DataFrame
        group_by: Колонки для группировки
        aggregations: {колонка: функция} или {колонка: [функции]}
    Returns:
        Агрегированный DataFrame
    Example:
        >>> # Продажи по городам
        >>> df_agg = aggregate_data(
        ...     df, group_by=['city'],
        ...     aggregations={
        ...         'sales': 'sum',
        ...         'orders': 'count',
        ...         'price': ['mean', 'max']
        ...     }
        ... )
    """
    df_agg = df.groupby(group_by).agg(aggregations).reset_index()
    
    # Упрощаем названия колонок если одна функция
    df_agg.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                      for col in df_agg.columns.values]
    
    logger.info(f"  Агрегация по {group_by}")
    
    return df_agg
