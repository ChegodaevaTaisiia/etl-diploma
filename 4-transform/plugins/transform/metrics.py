"""
Модуль для расчёта метрик качества данных (Data Quality Metrics)

Содержит функции для оценки качества данных по различным измерениям.

Автор: Курс "ETL - Автоматизация подготовки данных"
"""

import pandas as pd
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


def calculate_completeness(df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, float]:
    """
    Расчёт полноты данных (% заполненных значений).
    Args:
        df: DataFrame
        columns: Список колонок (None = все колонки)
    Returns:
        Словарь {колонка: процент_заполненности}
    Example:
        >>> completeness = calculate_completeness(df)
        >>> # {'age': 0.92, 'city': 0.88, 'email': 1.0}
    """
    if columns is None:
        columns = df.columns.tolist()
    
    completeness = {}
    total_rows = len(df)
    
    for col in columns:
        filled = df[col].notna().sum()
        completeness[col] = float(filled / total_rows) if total_rows > 0 else 0.0
    
    logger.info(f"Рассчитана полнота для {len(columns)} колонок")
    
    return completeness


def calculate_uniqueness(df: pd.DataFrame, columns: List[str]) -> float:
    """
    Расчёт уникальности данных (% уникальных записей).
    Args:
        df: DataFrame
        columns: Колонки для проверки уникальности
    Returns:
        Процент уникальных записей (0.0 - 1.0)
    Example:
        >>> uniqueness = calculate_uniqueness(df, ['email'])
        >>> # 0.95 означает 95% уникальных email
    """
    total = len(df)
    if total == 0:
        return 0.0
    
    unique = df.drop_duplicates(subset=columns).shape[0]
    uniqueness = float(unique / total)
    
    logger.info(f"Уникальность: {uniqueness:.2%} ({unique}/{total})")
    
    return uniqueness


def calculate_validity(df: pd.DataFrame, validation_rules: Dict[str, Any]) -> Dict[str, float]:
    """
    Расчёт валидности данных (% прошедших валидацию).
    Args:
        df: DataFrame
        validation_rules: Словарь {имя_правила: Series с булевыми результатами}
    Returns:
        Словарь {правило: процент_валидных}
    Example:
        >>> rules = {
        ...     'valid_email': df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
        ...     'valid_age': df['age'].between(0, 120)
        ... }
        >>> validity = calculate_validity(df, rules)
    """
    validity = {}
    total = len(df)
    
    for rule_name, is_valid in validation_rules.items():
        valid_count = is_valid.sum()
        validity[rule_name] = float(valid_count / total) if total > 0 else 0.0
    
    logger.info(f"Валидность для {len(validation_rules)} правил")
    
    return validity


def calculate_consistency(df: pd.DataFrame, column: str, expected_format: str) -> float:
    """
    Расчёт согласованности формата данных.    
    Args:
        df: DataFrame
        column: Колонка для проверки
        expected_format: Ожидаемый формат (regex pattern)
    Returns:
        Процент записей соответствующих формату
    Example:
        >>> # Проверка формата телефона
        >>> consistency = calculate_consistency(df, 'phone', r'^\+7\d{10}$')
    """
    total = len(df)
    if total == 0:
        return 0.0
    
    consistent = df[column].astype(str).str.match(expected_format, na=False).sum()
    consistency = float(consistent / total)
    
    logger.info(f"Согласованность для {column}: {consistency:.2%}")
    
    return consistency


def calculate_accuracy_score(
    df: pd.DataFrame,
    validation_columns: List[str],
    validation_functions: Dict[str, Any]
) -> pd.Series:
    """
    Расчёт точности данных для каждой записи (data quality score).
    Args:
        df: DataFrame
        validation_columns: Колонки для проверки
        validation_functions: {колонка: функция валидации}
    Returns:
        Series с оценкой качества каждой записи (0-100)
    Example:
        >>> validations = {
        ...     'email': lambda s: s.str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
        ...     'age': lambda s: s.between(0, 120)
        ... }
        >>> df['quality_score'] = calculate_accuracy_score(df, ['email', 'age'], validations)
    """
    scores = pd.DataFrame(index=df.index)
    
    # Полнота (% заполненных обязательных полей)
    completeness_score = df[validation_columns].notna().sum(axis=1) / len(validation_columns)
    scores['completeness'] = completeness_score
    
    # Валидность (% прошедших валидацию полей)
    validation_results = []
    for col, validate_func in validation_functions.items():
        if col in df.columns:
            is_valid = validate_func(df[col])
            validation_results.append(is_valid)
    
    if validation_results:
        validity_score = pd.concat(validation_results, axis=1).sum(axis=1) / len(validation_results)
        scores['validity'] = validity_score
    else:
        scores['validity'] = 1.0
    
    # Итоговая оценка (среднее)
    quality_score = scores.mean(axis=1) * 100
    
    logger.info(f"Оценка качества: mean={quality_score.mean():.1f}, min={quality_score.min():.1f}, max={quality_score.max():.1f}")
    
    return quality_score


def create_quality_report(
    df: pd.DataFrame,
    dataset_name: str,
    validation_results: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Создание полного отчёта о качестве данных.
    Args:
        df: DataFrame
        dataset_name: Имя датасета
        validation_results: Результаты валидации (опционально)
    Returns:
        Словарь с полным отчётом о качестве
    Example:
        >>> report = create_quality_report(df, 'customers', validation_results)
        >>> import json
        >>> with open('quality_report.json', 'w') as f:
        ...     json.dump(report, f, indent=2)
    """
    total_records = len(df)
    total_columns = len(df.columns)
    
    # Основная статистика
    report = {
        'dataset_name': dataset_name,
        'total_records': total_records,
        'total_columns': total_columns,
        'columns': df.columns.tolist(),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
    }
    
    # Полнота по колонкам
    completeness = calculate_completeness(df)
    report['completeness'] = completeness
    report['completeness_overall'] = float(sum(completeness.values()) / len(completeness)) if completeness else 0.0
    
    # Пропуски
    missing_counts = df.isnull().sum().to_dict()
    report['missing_values'] = {col: int(count) for col, count in missing_counts.items() if count > 0}
    
    # Дубликаты
    duplicates_count = df.duplicated().sum()
    report['duplicates'] = {
        'count': int(duplicates_count),
        'percentage': float(duplicates_count / total_records) if total_records > 0 else 0.0
    }
    
    # Результаты валидации (если есть)
    if validation_results:
        report['validation'] = validation_results
    
    # Общая оценка качества
    quality_factors = [
        report['completeness_overall'],
        1.0 - report['duplicates']['percentage']
    ]
    if validation_results and 'overall_pass_rate' in validation_results:
        quality_factors.append(validation_results['overall_pass_rate'])
    
    report['overall_quality_score'] = float(sum(quality_factors) / len(quality_factors))
    
    logger.info(f"Отчет по качеству для '{dataset_name}'")
    logger.info(f"  Обобщенный показатель: {report['overall_quality_score']:.2%}")
    
    return report


def calculate_data_profiling(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Создание профиля данных (data profiling).
    Args:
        df: DataFrame
    Returns:
        Словарь с профилем данных
    """
    profile = {}
    
    for col in df.columns:
        col_profile = {
            'dtype': str(df[col].dtype),
            'count': int(df[col].count()),
            'missing': int(df[col].isnull().sum()),
            'missing_pct': float(df[col].isnull().sum() / len(df)),
            'unique': int(df[col].nunique()),
        }
        
        # Для числовых колонок - статистика
        if pd.api.types.is_numeric_dtype(df[col]):
            col_profile.update({
                'mean': float(df[col].mean()) if df[col].notna().any() else None,
                'std': float(df[col].std()) if df[col].notna().any() else None,
                'min': float(df[col].min()) if df[col].notna().any() else None,
                'max': float(df[col].max()) if df[col].notna().any() else None,
                'median': float(df[col].median()) if df[col].notna().any() else None,
            })
        
        # Для категориальных - топ значения
        elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            value_counts = df[col].value_counts().head(5)
            col_profile['top_values'] = value_counts.to_dict()
        
        profile[col] = col_profile
    
    logger.info(f"Расчет метрик для {len(df.columns)} колонок")
    
    return profile
