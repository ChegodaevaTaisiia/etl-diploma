# Руководство по выполнению дипломной работы

## Что уже реализовано в проекте

### Структура проекта

- Полная структура директорий Apache Airflow
- Docker Compose конфигурация со всеми сервисами
- SQL скрипты для инициализации БД

### Extractors (извлечение данных)

- `BaseExtractor` - базовый класс
- `PostgresExtractor` - извлечение из PostgreSQL
- `MongoExtractor` - извлечение из MongoDB
- `CSVExtractor` - извлечение из CSV файлов
- `APIExtractor` - извлечение из REST API
- `FTPExtractor` - извлечение из FTP сервера

### SQL схемы

- Таблицы источников данных (PostgreSQL)
- Аналитические таблицы
- Data Warehouse со схемой "звезда" и SCD Type 2

### Основной DAG

- Базовая структура ETL pipeline
- Извлечение из всех источников
- Валидация данных
- Заглушки для трансформации и загрузки

## Что нужно реализовать студенту

### 1. Transformers (2-3 дня)

Создайте следующие классы в `plugins/transformers/`:

#### data_cleaner.py

```python
class DataCleaner:
    """Очистка данных"""
    def clean_nulls(data)
    def remove_duplicates(data)
    def fix_data_types(data)
```

#### data_validator.py

```python
class DataValidator:
    """Валидация данных"""
    def validate_schema(data, schema)
    def validate_ranges(data, rules)
    def validate_business_rules(data)
```

#### data_enricher.py

```python
class DataEnricher:
    """Обогащение данных"""
    def enrich_from_lookup(data, lookup_table)
    def calculate_derived_fields(data)
    def add_metadata(data)
```

#### data_aggregator.py

```python
class DataAggregator:
    """Агрегация данных"""
    def aggregate_daily_metrics(data)
    def calculate_kpis(data)
```

### 2. Loaders (2-3 дня)

Создайте следующие классы в `plugins/loaders/`:

#### analytics_loader.py

```python
class AnalyticsLoader:
    """Загрузка в аналитическую БД"""
    def load_daily_analytics(data)
    def update_aggregates(data)
```

#### dwh_loader.py

```python
class DWHLoader:
    """Загрузка в DWH с SCD Type 2"""
    def load_dimension_customers(data)
    def load_dimension_products(data)
    def load_fact_orders(data)
    def handle_scd_type2(dimension, data)
```

**Важно для SCD Type 2:**

- Проверять изменения атрибутов
- Закрывать старые версии (expiration_date, is_current=FALSE)
- Создавать новые версии с новым surrogate key
- Использовать функции из `create_dwh_schema.sql`

### 3. Validators (1-2 дня)

Создайте в `plugins/validators/`:

#### data_quality_validator.py

```python
class DataQualityValidator:
    """Проверки качества данных"""
    def check_completeness(data)
    def check_accuracy(data)
    def check_consistency(data)
    def log_quality_metrics(metrics)
```

### 4. Дополнительные DAG задачи (1-2 дня)

Реализуйте функции в `business_analytics_etl.py`:

- `clean_and_transform_data()` - очистка и трансформация
- `enrich_data()` - обогащение данных
- `aggregate_data()` - агрегация метрик
- `load_to_analytics()` - загрузка в аналитическую БД
- `load_to_dwh_scd2()` - загрузка в DWH с SCD Type 2
- `validate_data_quality()` - проверки качества
- `generate_dashboard_metrics()` - подготовка данных для дашборда

### 5. DAG для генерации тестовых данных (1 день)

Создайте `dags/generate_test_data.py`:

```python
def generate_customers()
def generate_orders()
def generate_feedback()
def generate_csv_products()
def generate_ftp_delivery_logs()
def generate_api_analytics()
```

### 6. Визуализация (1-2 дня)

В Grafana создайте дашборд с:

- Ежедневные метрики заказов
- География продаж
- Топ продукты и категории
- Средний рейтинг клиентов
- Метрики доставки
- KPI веб-аналитики

### 7. Документация (1 день)

Дополните README.md:

- Описание выбранной предметной области
- Архитектурная диаграмма
- Примеры использования
- Результаты тестирования
- Скриншоты дашборда

## Порядок выполнения

1. Трансформеры и валидаторы
2. Загрузчики с SCD Type 2
3. Интеграция в DAG, тестирование
4. Генерация данных, визуализация, документация

## Критерии оценки

| Критерий | Баллы | Описание |
|----------|-------|----------|
| Полнота реализации | 35 | Все источники, ETL, DWH с SCD Type 2, дашборд |
| Безопасность | 15 | .env, Airflow Connections, нет паролей в коде |
| Качество кода | 25 | Архитектура, обработка ошибок, логирование |
| Настройка Airflow | 15 | DAG, зависимости, расписание, мониторинг |
| Документация | 10 | README, инструкции, архитектура |
| **Итого** | **100** | |
