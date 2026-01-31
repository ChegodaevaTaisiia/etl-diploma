import pandas as pd


class DataValidator:
    """Валидация заказов: обязательные поля, типы, диапазоны, статусы."""

    def validate_orders(self, df: pd.DataFrame) -> tuple:
        """Возвращает (valid_df, invalid_df, errors). Если нет нужных колонок — возвращает df как valid."""
        if df.empty:
            return df, pd.DataFrame(), []
        errors = []
        required_fields = ['order_id', 'customer_id', 'total_amount']
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
        if errors:
            return df, pd.DataFrame(), errors

        valid_mask = pd.Series(True, index=df.index)
        if pd.api.types.is_numeric_dtype(df['total_amount']):
            valid_mask &= df['total_amount'].between(0, 1000000)
        else:
            errors.append("total_amount must be numeric")
        if 'status' in df.columns:
            valid_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
            valid_mask &= df['status'].isin(valid_statuses)
        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        return valid_df, invalid_df, errors