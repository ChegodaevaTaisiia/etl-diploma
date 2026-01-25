"""
Модуль для очистки данных (Data Cleaning)

Содержит функции для обработки типичных проблем качества данных:
- Удаление дубликатов
- Обработка пропусков
- Удаление выбросов
- Исправление ошибок

Автор: Курс "ETL - Автоматизация подготовки данных"
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Удаление дубликатов из DataFrame.
    Args:
        df: Входной DataFrame
        subset: Колонки для проверки на дубликаты (None = все колонки)
        keep: Какую запись оставить ('first', 'last', False=удалить все)
    Returns:
        DataFrame без дубликатов
    Example:
        >>> df = remove_duplicates(df, subset=['email'], keep='first')
        >>> # Оставляет первое вхождение email, удаляет остальные
    """
    initial_count = len(df)
    # Удаляем дубликаты
    df_clean = df.drop_duplicates(subset=subset, keep=keep)
    duplicates_removed = initial_count - len(df_clean)
    logger.info(f"Обнаружено дубликатов: {duplicates_removed} ({duplicates_removed/initial_count*100:.1f}%)")
    
    return df_clean


def handle_missing_values(
    df: pd.DataFrame,
    strategy: Dict[str, Any]
) -> pd.DataFrame:
    """
    Обработка пропущенных значений с разными стратегиями для разных колонок.
    Args:
        df: Входной DataFrame
        strategy: Словарь {колонка: стратегия}
            Стратегии:
            - 'drop': удалить строки с пропусками
            - 'mean': заполнить средним
            - 'median': заполнить медианой
            - 'mode': заполнить модой (самое частое)
            - 'constant:VALUE': заполнить константой
            - 'ffill': forward fill (предыдущим значением)
            - 'bfill': backward fill (следующим значением)
    Returns:
        DataFrame с обработанными пропусками
    Example:
        >>> strategy = {
        ...     'age': 'median',
        ...     'city': 'constant:Unknown',
        ...     'salary': 'mean'
        ... }
        >>> df_clean = handle_missing_values(df, strategy)
    """
    df_clean = df.copy()
    for column, strat in strategy.items():
        if column not in df_clean.columns:
            logger.warning(f"Колонка '{column}' не найдена, пропуск")
            continue
        missing_count = df_clean[column].isna().sum()
        if missing_count == 0:
            logger.info(f"{column}: нет пропусков")
            continue
        logger.info(f"{column}: {missing_count} пропусков ({missing_count/len(df_clean)*100:.1f}%)")
        
        # Применяем стратегию
        if strat == 'drop':
            df_clean = df_clean.dropna(subset=[column])
            logger.info(f"  → Удалены строки с пропуском колонки {column}")
        
        elif strat == 'mean':
            mean_value = df_clean[column].mean()
            df_clean[column] = df_clean[column].fillna(mean_value)
            logger.info(f"  → Заполнены средним: {mean_value:.2f}")
        
        elif strat == 'median':
            median_value = df_clean[column].median()
            df_clean[column] = df_clean[column].fillna(median_value)
            logger.info(f"  → Заполнены медианой: {median_value}")
        
        elif strat == 'mode':
            mode_value = df_clean[column].mode()[0] if not df_clean[column].mode().empty else None
            df_clean[column] = df_clean[column].fillna(mode_value)
            logger.info(f"  → Заполнены модой (самым частым значением): {mode_value}")
        
        elif strat.startswith('constant:'):
            constant = strat.split(':', 1)[1]
            df_clean[column] = df_clean[column].fillna(constant)
            logger.info(f"  → Заполнены константой: {constant}")
        
        elif strat == 'ffill':
            df_clean[column] = df_clean[column].fillna(method='ffill')
            logger.info(f"  → предыдущее значение (forward fill)")
        
        elif strat == 'bfill':
            df_clean[column] = df_clean[column].fillna(method='bfill')
            logger.info(f"  → следующее значение (backward filled)")
        
        else:
            logger.warning(f"Неизвестная стратегия '{strat}' для {column}")
    
    return df_clean


def remove_outliers(
    df: pd.DataFrame,
    column: str,
    method: str = 'iqr',
    threshold: float = 1.5
) -> pd.DataFrame:
    """
    Удаление выбросов из числовых данных.
    Args:
        df: Входной DataFrame
        column: Колонка для проверки на выбросы
        method: Метод определения выбросов
            - 'iqr': Interquartile Range (Q1-1.5*IQR, Q3+1.5*IQR)
            - 'zscore': Z-score (|z| > threshold)
            - 'range': Указанный диапазон
        threshold: Порог для метода (1.5 для IQR, 3 для z-score)
    Returns:
        DataFrame без выбросов
    Example:
        >>> # Удалить выбросы IQR методом
        >>> df_clean = remove_outliers(df, 'age', method='iqr')
        >>> 
        >>> # Удалить выбросы Z-score методом
        >>> df_clean = remove_outliers(df, 'salary', method='zscore', threshold=3)
    """
    initial_count = len(df)
    
    if column not in df.columns:
        logger.warning(f"Колонка '{column}' не найдена")
        return df
    
    if method == 'iqr':
        # Межквартильный размах
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        
        df_clean = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        
        logger.info(f"Границы IQR: [{lower_bound:.2f}, {upper_bound:.2f}]")
    
    elif method == 'zscore':
        # Z-score (стандартные отклонения от среднего)
        mean = df[column].mean()
        std = df[column].std()
        
        df['z_score'] = np.abs((df[column] - mean) / std)
        df_clean = df[df['z_score'] <= threshold].drop('z_score', axis=1)
        
        logger.info(f"Порог Z-score: {threshold}")
    
    else:
        logger.warning(f"Неизвестный метод: '{method}'")
        return df
    
    outliers_removed = initial_count - len(df_clean)
    logger.info(f"Выбросы удалены: {outliers_removed} ({outliers_removed/initial_count*100:.1f}%)")
    
    return df_clean


def filter_by_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> pd.DataFrame:
    """
    Фильтрация данных по диапазону значений.
    Args:
        df: Входной DataFrame
        column: Колонка для фильтрации
        min_value: Минимальное допустимое значение (включительно)
        max_value: Максимальное допустимое значение (включительно)
    Returns:
        Отфильтрованный DataFrame
    Example:
        >>> # Возраст от 0 до 120
        >>> df_clean = filter_by_range(df, 'age', min_value=0, max_value=120)
        >>> 
        >>> # Цена больше 0
        >>> df_clean = filter_by_range(df, 'price', min_value=0)
    """
    initial_count = len(df)
    df_clean = df.copy()
    
    if min_value is not None:
        df_clean = df_clean[df_clean[column] >= min_value]
    
    if max_value is not None:
        df_clean = df_clean[df_clean[column] <= max_value]
    
    filtered_count = initial_count - len(df_clean)
    
    logger.info(
        f"Колонка {column}: {filtered_count} записей вне диапазона "
        f"[{min_value if min_value is not None else '-∞'}, "
        f"{max_value if max_value is not None else '+∞'}]"
    )
    
    return df_clean


def standardize_text(
    df: pd.DataFrame,
    column: str,
    operations: List[str]
) -> pd.DataFrame:
    """
    Стандартизация текстовых данных.
    Args:
        df: Входной DataFrame
        column: Текстовая колонка
        operations: Список операций:
            - 'lowercase': привести к нижнему регистру
            - 'uppercase': привести к верхнему регистру
            - 'strip': удалить пробелы в начале/конце
            - 'remove_whitespace': удалить все пробелы
            - 'remove_special': удалить спецсимволы
    Returns:
        DataFrame со стандартизированным текстом
    Example:
        >>> df_clean = standardize_text(df, 'email', ['lowercase', 'strip'])
    """
    df_clean = df.copy()
    
    for operation in operations:
        if operation == 'lowercase':
            df_clean[column] = df_clean[column].str.lower()
            logger.info(f"{column}: преобразован в lowercase")
        
        elif operation == 'uppercase':
            df_clean[column] = df_clean[column].str.upper()
            logger.info(f"{column}: преобразован в uppercase")
        
        elif operation == 'strip':
            df_clean[column] = df_clean[column].str.strip()
            logger.info(f"{column}: удалены пробелы")
        
        elif operation == 'remove_whitespace':
            df_clean[column] = df_clean[column].str.replace(r'\s+', '', regex=True)
            logger.info(f"{column}: удалены все пробелы")
        
        elif operation == 'remove_special':
            df_clean[column] = df_clean[column].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
            logger.info(f"{column}: удалены спецсимволы")
    
    return df_clean


def replace_values(
    df: pd.DataFrame,
    column: str,
    replacements: Dict[Any, Any]
) -> pd.DataFrame:
    """
    Замена значений в колонке (для стандартизации категорий).
    Args:
        df: Входной DataFrame
        column: Колонка для замены
        replacements: Словарь {старое: новое}
    Returns:
        DataFrame с заменёнными значениями
    Example:
        >>> # Стандартизация пола
        >>> replacements = {
        ...     'М': 'male', 'м': 'male', 'M': 'male',
        ...     'Ж': 'female', 'ж': 'female', 'F': 'female'
        ... }
        >>> df_clean = replace_values(df, 'gender', replacements)
    """
    df_clean = df.copy()
    
    # Считаем сколько значений будет заменено
    values_to_replace = df_clean[column].isin(replacements.keys()).sum()
    
    # Заменяем
    df_clean[column] = df_clean[column].replace(replacements)
    
    logger.info(f"{column}: {values_to_replace} значений заменено")
    logger.info(f"  Замены: {replacements}")
    
    return df_clean


def clean_phone_numbers(
    df: pd.DataFrame,
    column: str,
    keep_only_digits: bool = True,
    add_country_code: Optional[str] = None
) -> pd.DataFrame:
    """
    Очистка и стандартизация номеров телефонов.
    Args:
        df: Входной DataFrame
        column: Колонка с телефонами
        keep_only_digits: Оставить только цифры
        add_country_code: Добавить код страны если отсутствует (например '+7')
    Returns:
        DataFrame с очищенными телефонами
    Example:
        >>> # Очистить телефоны и добавить +7
        >>> df_clean = clean_phone_numbers(df, 'phone', add_country_code='+7')
        >>> # +7-915-123-45-67 → +79151234567
        >>> # 89161234567 → +79161234567
    """
    df_clean = df.copy()
    if keep_only_digits:
        # Удаляем всё кроме цифр
        df_clean[column] = df_clean[column].str.replace(r'\D', '', regex=True)
        logger.info(f"{column}: удалены символы, кроме цифр")
    
    if add_country_code:
        # Добавляем код страны если отсутствует
        # Например, 9161234567 → +79161234567
        mask = ~df_clean[column].str.startswith(add_country_code.replace('+', ''))
        df_clean.loc[mask, column] = add_country_code.replace('+', '') + df_clean.loc[mask, column]
        
        # Добавляем + в начало
        df_clean[column] = '+' + df_clean[column].str.replace('+', '')
        
        logger.info(f"{column}: добавлен код страны {add_country_code}")
    
    return df_clean
