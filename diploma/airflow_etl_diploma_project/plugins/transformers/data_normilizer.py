import pandas as pd

from base_transformer import BaseTransformer


class DataNormalizer(BaseTransformer):
    """Нормализация данных: строки, даты, числа. Обрабатывает только существующие колонки."""

    def __init__(self, name: str = 'data_normalizer'):
        super().__init__(name)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        # Нормализация строк (только если колонка есть)
        for col, fn in [
            ('country', lambda s: s.astype(str).str.upper().str.strip()),
            ('city', lambda s: s.astype(str).str.title().str.strip()),
            ('email', lambda s: s.astype(str).str.lower().str.strip()),
        ]:
            if col in df.columns:
                df[col] = fn(df[col])
        if 'phone' in df.columns:
            df['phone'] = df['phone'].astype(str).str.replace(r'\D', '', regex=True)
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            if hasattr(df['order_date'].dtype, 'tz') and df['order_date'].dt.tz is not None:
                df['order_date'] = df['order_date'].dt.tz_localize(None)
        if 'total_amount' in df.columns and pd.api.types.is_numeric_dtype(df['total_amount']):
            df['total_amount'] = df['total_amount'].round(2)
        return df