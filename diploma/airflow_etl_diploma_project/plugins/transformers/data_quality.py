import pandas as pd


class DataQuality:
    """Оценка качества данных. Учитывает только существующие колонки."""

    def assess_data_quality(self, df: pd.DataFrame) -> dict:
        if df.empty:
            return {'total_records': 0, 'completeness': {}, 'uniqueness': {}, 'validity': {}, 'duplicates': 0, 'null_values': {}}
        total_records = len(df)
        completeness = {field: float((df[field].notna().sum() / total_records * 100)) for field in df.columns}
        uniqueness = {}
        if 'order_id' in df.columns:
            uniqueness['order_id'] = float(df['order_id'].nunique() / total_records * 100)
        if 'customer_id' in df.columns:
            uniqueness['customer_id'] = float(df['customer_id'].nunique() / total_records * 100)
        validity = {}
        if 'email' in df.columns:
            validity['valid_emails'] = float((df['email'].astype(str).str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False).sum() / total_records * 100))
        if 'total_amount' in df.columns and pd.api.types.is_numeric_dtype(df['total_amount']):
            validity['valid_amounts'] = float(((df['total_amount'] > 0).sum() / total_records * 100))
        duplicates = 0
        if 'order_id' in df.columns:
            duplicates = int(df.duplicated(subset=['order_id']).sum())
        return {
            'total_records': total_records,
            'completeness': completeness,
            'uniqueness': uniqueness,
            'validity': validity,
            'duplicates': duplicates,
            'null_values': df.isnull().sum().to_dict(),
        }