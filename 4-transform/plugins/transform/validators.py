"""
Модуль для валидации данных (Data Validation)

Содержит функции для проверки корректности данных на разных уровнях:
- Структурная валидация
- Доменная валидация  
- Бизнес-валидация

Автор: Курс "ETL - Автоматизация подготовки данных"
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    expected_types: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Валидация структуры DataFrame (схемы).
    Args:
        df: DataFrame для валидации
        required_columns: Обязательные колонки
        expected_types: Ожидаемые типы {колонка: тип}
    Returns:
        Словарь с результатами валидации
    Example:
        >>> result = validate_schema(
        ...     df,
        ...     required_columns=['id', 'email', 'age'],
        ...     expected_types={'id': 'int64', 'age': 'int64'}
        ... )
    """
    errors = []
    warnings = []
    
    # Проверка наличия колонок
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Недостающие колонки: {missing_columns}")
        logger.error(f"Недостающие колонки: {missing_columns}")
    else:
        logger.info(f"Все колонки присутствуют")
    
    # Проверка типов данных
    if expected_types:
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    warnings.append(
                        f"  Колонка '{col}': ожидаемый тип {expected_type}, получен {actual_type}"
                    )
                    logger.warning(f"  {col}: тип не совпадает")
    
    is_valid = len(errors) == 0
    
    return {
        'valid': is_valid,
        'errors': errors,
        'warnings': warnings
    }


def validate_email(df: pd.DataFrame, column: str) -> pd.Series:
    """
    Валидация email адресов.
    Args:
        df: DataFrame с email
        column: Колонка с email
    Returns:
        Boolean Series (True = валидный email)
    Example:
        >>> df['is_valid_email'] = validate_email(df, 'email')
    """
    # Простой regex для email
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    
    is_valid = df[column].str.match(email_pattern, na=False)
    
    valid_count = is_valid.sum()
    invalid_count = (~is_valid).sum()
    
    logger.info(f"Валидация Email: верных - {valid_count}, неверных - {invalid_count}")
    
    return is_valid


def validate_phone(df: pd.DataFrame, column: str, pattern: Optional[str] = None) -> pd.Series:
    """
    Валидация телефонных номеров.
    Args:
        df: DataFrame с телефонами
        column: Колонка с телефонами
        pattern: Regex pattern (по умолчанию: российский формат)
    Returns:
        Boolean Series (True = валидный телефон)
    """
    if pattern is None:
        # Российский формат: +7XXXXXXXXXX (11 цифр)
        pattern = r'^\+7\d{10}$'
    
    is_valid = df[column].str.match(pattern, na=False)
    
    valid_count = is_valid.sum()
    logger.info(f"Валидация Phone: {valid_count}/{len(df)} верных")
    
    return is_valid


def validate_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> pd.Series:
    """
    Валидация диапазона числовых значений.
    Args:
        df: DataFrame
        column: Числовая колонка
        min_value: Минимум (включительно)
        max_value: Максимум (включительно)
    Returns:
        Boolean Series (True = в допустимом диапазоне)
    Example:
        >>> # Возраст от 0 до 120
        >>> df['valid_age'] = validate_range(df, 'age', 0, 120)
    """
    is_valid = pd.Series([True] * len(df), index=df.index)
    
    if min_value is not None:
        is_valid &= (df[column] >= min_value)
    
    if max_value is not None:
        is_valid &= (df[column] <= max_value)
    
    valid_count = is_valid.sum()
    invalid_count = (~is_valid).sum()
    
    logger.info(
        f"Валидация диапазона [{min_value}, {max_value}]: "
        f"  верных - {valid_count}, неверных - {invalid_count}"
    )
    
    return is_valid


def validate_allowed_values(
    df: pd.DataFrame,
    column: str,
    allowed_values: List[Any]
) -> pd.Series:
    """
    Валидация допустимых значений (категорий).
    Args:
        df: DataFrame
        column: Категориальная колонка
        allowed_values: Список допустимых значений
    Returns:
        Boolean Series (True = допустимое значение)
    Example:
        >>> df['valid_status'] = validate_allowed_values(
        ...     df, 'status', ['active', 'pending', 'completed']
        ... )
    """
    is_valid = df[column].isin(allowed_values)
    
    valid_count = is_valid.sum()
    invalid_count = (~is_valid).sum()
    
    # Выводим невалидные значения
    if invalid_count > 0:
        invalid_values = df.loc[~is_valid, column].unique()
        logger.warning(f"Обнаружены неверные значения: {invalid_values}")
    
    logger.info(f"Вализация разрешенных значений: верных - {valid_count}, неверных - {invalid_count}")
    
    return is_valid


def validate_not_null(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """
    Валидация отсутствия NULL значений.
    Args:
        df: DataFrame
        columns: Колонки которые не должны быть NULL
    Returns:
        Boolean Series (True = все указанные колонки не NULL)
    Example:
        >>> df['complete'] = validate_not_null(df, ['email', 'age', 'city'])
    """
    is_valid = pd.Series([True] * len(df), index=df.index)
    
    for col in columns:
        is_valid &= df[col].notna()
    
    valid_count = is_valid.sum()
    
    logger.info(f"Валидация Not null: {valid_count}/{len(df)} строк имеют все обязательные поля")
    
    return is_valid


def validate_unique(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """
    Валидация уникальности (отсутствие дубликатов).    
    Args:
        df: DataFrame
        columns: Колонки для проверки уникальности
    Returns:
        Boolean Series (True = уникальная запись)
    """
    # Помечаем дубликаты (False = дубликат)
    is_unique = ~df.duplicated(subset=columns, keep=False)
    
    unique_count = is_unique.sum()
    duplicate_count = (~is_unique).sum()
    
    logger.info(f"Валидация уникальности значений: дубликатов - {duplicate_count}")
    
    return is_unique


def validate_referential_integrity(
    df: pd.DataFrame,
    column: str,
    reference_df: pd.DataFrame,
    reference_column: str
) -> pd.Series:
    """
    Валидация ссылочной целостности (внешние ключи).
    Args:
        df: Проверяемый DataFrame
        column: Колонка с внешним ключом
        reference_df: Справочный DataFrame
        reference_column: Колонка с ключами в справочнике
    Returns:
        Boolean Series (True = ключ существует в справочнике)
    Example:
        >>> # Проверить что все customer_id существуют
        >>> df['valid_customer'] = validate_referential_integrity(
        ...     orders_df, 'customer_id',
        ...     customers_df, 'customer_id'
        ... )
    """
    valid_keys = reference_df[reference_column].unique()
    is_valid = df[column].isin(valid_keys)
    
    valid_count = is_valid.sum()
    invalid_count = (~is_valid).sum()
    
    if invalid_count > 0:
        invalid_keys = df.loc[~is_valid, column].unique()
        logger.warning(f"Неверные внешние ключи: {invalid_keys[:10]}")  # Первые 10
    
    logger.info(f"Корректные ссылки: верных - {valid_count}, неверных - {invalid_count}")
    
    return is_valid


def create_validation_report(df: pd.DataFrame, validations: Dict[str, pd.Series]) -> Dict[str, Any]:
    """
    Создание отчёта по результатам валидации.
    Args:
        df: DataFrame
        validations: Словарь {имя_проверки: Series с результатами}
    Returns:
        Словарь с отчётом о валидации
    Example:
        >>> validations = {
        ...     'valid_email': validate_email(df, 'email'),
        ...     'valid_age': validate_range(df, 'age', 0, 120),
        ...     'not_null': validate_not_null(df, ['email', 'age'])
        ... }
        >>> report = create_validation_report(df, validations)
    """
    total_records = len(df)
    
    # Общая валидность = ВСЕ проверки прошли
    overall_valid = pd.Series([True] * total_records, index=df.index)
    for validation in validations.values():
        overall_valid &= validation
    
    # Отчёт по каждой проверке
    checks = {}
    for name, validation in validations.items():
        valid_count = validation.sum()
        checks[name] = {
            'valid': int(valid_count),
            'invalid': int(total_records - valid_count),
            'pass_rate': float(valid_count / total_records)
        }
    
    # Общий результат
    overall_valid_count = overall_valid.sum()
    
    report = {
        'total_records': total_records,
        'valid_records': int(overall_valid_count),
        'invalid_records': int(total_records - overall_valid_count),
        'overall_pass_rate': float(overall_valid_count / total_records),
        'checks': checks
    }
    
    logger.info(f"Валидация завершена: верных - {overall_valid_count}/{total_records} ({report['overall_pass_rate']*100:.1f}%)")
    
    return report
