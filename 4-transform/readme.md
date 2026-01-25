# Тема 4 – Трансформация и валидация данных (Transform в ETL). Оценка качества данных

## Цель занятия

После этого занятия вы сможете:

1. **Очищать данные** - удалять дубликаты, обрабатывать пропуски, исправлять ошибки
2. **Трансформировать данные** - изменять структуру, типы, форматы
3. **Валидировать данные** - проверять корректность, полноту, согласованность
4. **Оценивать качество** - рассчитывать метрики качества данных
5. **Обрабатывать ошибки** - обнаруживать и исправлять проблемные данные
6. **Применять стратегии обработки** - выбирать правильный подход для каждой ситуации обработки невалидных данных
7. Подготовить основу для последующих шагов: **Load**.

## Необходимые знания

- Тема 1 (основы Airflow, DAG, операторы)
- Тема 2 (Hooks, Operators, Connections)
- Тема 3 (Extract)
- Базовое знание SQL
- Опыт работы с pandas

---

## Теоретическая часть

### 1. Типы трансформации данных

**Структурная трансформация:**

- Изменение формата (wide → long, pivot/unpivot - строки в столбцы и наоборот)
- Разделение/объединение колонок
- Переименование, переупорядочивание

**Типовая трансформация:**

- Приведение типов (string → int, datetime)
- Нормализация форматов (даты, числа, email, телефонный номер, текст)
- Кодирование (one-hot, label encoding) - преобразование категориальных данных в числовой формат для использования в дальнейшем в алгоритмах машинного обучения

**Вычислительная трансформация:**

- Создание вычисляемых полей
- Агрегация (группировка, суммирование)
- Оконные функции

---

### 2. Очистка данных (Data Cleaning)

**Проблемы качества данных:**

| Проблема | Пример | Решение |
|----------|--------|---------|
| **Дубликаты** | 2 записи с одинаковым email | Удаление по ключу |
| **Пропуски** | NULL в обязательном поле | Заполнение/удаление |
| **Выбросы** | Возраст = 150 | Фильтрация/замена |
| **Несогласованность** | "М", "м", "Male", "male" | Стандартизация |
| **Некорректный формат** | Дата "32.13.2025" | Валидация/исправление |

**Стратегии обработки пропусков:**

```python
# Удаление
df.dropna()

# Заполнение константой
df.fillna(0)
df.fillna("Unknown")

# Заполнение средним/медианой
df['age'].fillna(df['age'].mean())

# Прямое/обратное заполнение (для временных рядов)
df.fillna(method='ffill')  # Forward fill
df.fillna(method='bfill')  # Backward fill

# Интерполяция
df['value'].interpolate()
```

---

### 3. Фильтрация, агрегация, объединения

**Фильтрация:**

```python
# Условная фильтрация
df[df['age'] > 18]
df[(df['age'] > 18) & (df['city'] == 'Moscow')]

# Фильтрация по диапазону
df[df['date'].between('2025-01-01', '2025-12-31')]

# Фильтрация по списку
df[df['status'].isin(['active', 'pending'])]
```

**Агрегация:**

```python
# Группировка с агрегацией
df.groupby('city').agg({
    'sales': 'sum',
    'orders': 'count',
    'price': 'mean'
})

# Множественная агрегация
df.groupby('city')['sales'].agg(['sum', 'mean', 'count'])
```

**Объединения:**

```python
# Inner join (пересечение)
pd.merge(df1, df2, on='user_id', how='inner')

# Left join (все из левого)
pd.merge(df1, df2, on='user_id', how='left')

# Concatenation (вертикальное объединение)
pd.concat([df1, df2], axis=0)
```

---

### 4. Обогащение и нормализация данных

**Обогащение (Data Enrichment):**

- Добавление вычисляемых полей
- Присоединение справочников
- Геокодирование, категоризация

```python
# Вычисляемые поля
df['total'] = df['price'] * df['quantity']
df['age_group'] = pd.cut(df['age'], bins=[0, 18, 35, 60, 100])

# Присоединение справочников
df_enriched = pd.merge(df, cities_dict, on='city_id')
```

**Нормализация:**

**Min-Max нормализация:**

```python
# Значения в диапазон [0, 1]
df['normalized'] = (df['value'] - df['value'].min()) / (df['value'].max() - df['value'].min())
```

**Z-score стандартизация:**

```python
# Среднее=0, стандартное отклонение=1
df['standardized'] = (df['value'] - df['value'].mean()) / df['value'].std()
```

**Текстовая нормализация:**

```python
# Приведение к нижнему регистру, удаление пробелов
df['email'] = df['email'].str.lower().str.strip()

# Стандартизация категорий
df['gender'] = df['gender'].replace({
    'М': 'male', 'м': 'male', 'M': 'male',
    'Ж': 'female', 'ж': 'female', 'F': 'female'
})
```

---

### 5. Стратегии проверки качества данных

**Метрики качества данных:**

| Измерение | Описание | Метрика |
|-----------|----------|---------|
| **Полнота** | Наличие всех данных | % заполненных значений |
| **Уникальность** | Отсутствие дубликатов | % уникальных записей |
| **Согласованность** | Соответствие форматам | % валидных значений |
| **Точность** | Корректность значений | % прошедших валидацию |
| **Актуальность** | Свежесть данных | Дата последнего обновления |

**Уровни валидации:**

1. **Структурная валидация:**
   - Наличие обязательных колонок
   - Соответствие типов данных
   - Уникальность ключевых полей

2. **Доменная валидация:**
   - Диапазоны значений (возраст 0-120)
   - Допустимые категории (статус: active/inactive)
   - Форматы (email, телефон, дата)

3. **Бизнес-валидация:**
   - Бизнес-правила (сумма заказа > 0)
   - Согласованность связей (клиент существует)
   - Временные ограничения (дата доставки > дата заказа)

---

### 6. Обработка «проблемных» данных

**Стратегии обработки:**

| Стратегия | Когда использовать | Пример |
|-----------|-------------------|--------|
| **Удаление** | Мало данных, критичные ошибки | Удалить дубликаты |
| **Исправление** | Известно правильное значение | Исправить опечатки |
| **Заполнение** | Можно восстановить | Заполнить пропуски средним |
| **Помечание** | Нужна история изменений | Добавить флаг is_corrected |
| **Карантин** | Требует ручной проверки | Отложить в quarantine_table |

**Пример обработки:**

```python
def clean_and_validate(df):
    """Комплексная очистка и валидация данных."""
    
    # 1. Удаление дубликатов
    df = df.drop_duplicates(subset=['email'])
    
    # 2. Обработка пропусков
    df['age'] = df['age'].fillna(df['age'].median())
    df['city'] = df['city'].fillna('Unknown')
    
    # 3. Исправление ошибок
    df['email'] = df['email'].str.lower().str.strip()
    df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)
    
    # 4. Фильтрация выбросов
    df = df[df['age'].between(0, 120)]
    df = df[df['price'] > 0]
    
    # 5. Валидация
    df['is_valid_email'] = df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$')
    
    # 6. Помечание исправлений
    df['data_quality_score'] = calculate_quality_score(df)
    
    return df
```

---

## Структура проекта

```bash
lesson4_transform/
├── README.md                          # Это руководство
├── docker-compose.yml                 # Инфраструктура на docker-контейнерах
├── requirements.txt                   # Python-зависимости
├── dags/
│   ├── dag_01_data_cleaning.py        # Очистка данных
│   ├── dag_02_transformation.py       # Трансформация
│   ├── dag_03_validation.py           # Валидация
│   ├── dag_04_quality_metrics.py      # Метрики качества
│   ├── dag_05_error_handling.py       # Обработка ошибок
│   └── dag_homework.py                # Домашнее задание
├── plugins/
│   └── transform/
│       ├── cleaners.py                # Функции очистки
│       ├── validators.py              # Функции валидации
│       ├── transformers.py            # Функции трансформации
│       └── metrics.py                 # Расчёт метрик качества
└── data/
    ├── input/                         # Входные "грязные" данные
    │   ├── customers_raw.csv          # Клиенты с ошибками
    │   ├── orders_raw.csv             # Заказы с пропусками
    │   ├── products_raw.csv           # Продукты с дубликатами
    │   └── sales_raw.csv              # Продажи с пропусками и дубликаами
    ├── output/                        # Очищенные данные
    │   ├── customers_clean.csv
    │   ├── orders_clean.csv
    │   ├── products_clean.csv
    │   └── sales_clean.csv
    ├── quality_reports/               # Отчёты о качестве
    │   ├── quality_report_YYYY-MM-DD.json
    │   └── quality_dashboard.html
    └── quarantine/                    # Карантин проблемных данных
        └── invalid_records_YYYY-MM-DD.csv
```

---

## Тестовые входные данные

### 1. Клиенты (с проблемами)

Файл - [customers_raw.csv](data/input/customers_raw.csv)

**Проблемы:**

- Дубликаты по `email`
- Пропуски в `age`, `city`
- Невалидные `email`
- Несогласованность в `gender` (_М/м/Male/male_)
- Выбросы в `age` (_150_, _-5_)
- Некорректные форматы телефонов в поле `phone`

**Пример данных:**

```bash
customer_id,email,full_name,age,city,gender,phone,registration_date
1,alice@example.com,Alice Smith,28,Moscow,female,+7-915-123-45-67,2024-01-15
2,bob@example.com,Bob Johnson,35,SPB,М,89161234567,2024-02-20
2,bob@example.com,Bob Johnson,35,SPB,М,89161234567,2024-02-20  # Дубликат
3,charlie@,Charlie Brown,,Moscow,male,invalid,2024-03-10  # Невалидный email, пропуск age
4,david@example.com,David Lee,150,Kazan,м,+7(916)765-43-21,2024-04-05  # Выброс age
5,emma@example.com,Emma Davis,29,,female,+79167654321,2024-05-12  # Пропуск city
6,frank@example.com,Frank Miller,-5,Moscow,Male,123,2024-06-18  # Отрицательный age
```

**Статистика проблем:**

- Всего записей: **~100**
- Дубликаты: **~5 (5%)**
- Пропуски в age: **~10 (10%)**
- Пропуски в city: **~8 (8%)**
- Невалидные email: **~7 (7%)**
- Выбросы в age: **~6 (6%)**
- Некорректные телефоны: **~15 (15%)**

---

### 2. Заказы (с проблемами)

Файл - [orders_raw.csv](data/input/orders_raw.csv)

**Проблемы:**

- Пропуски в `amount`, `status`
- Отрицательные `amount`
- Несуществующие `customer_id` (нет в customers)
- Некорректные даты: дата доставки < дата заказа (`delivery_date < order_date`)
- Невалидные статусы

**Пример данных:**

```bash
order_id,customer_id,amount,status,order_date,delivery_date
1001,1,1500.50,completed,2024-06-01,2024-06-03
1002,2,,pending,2024-06-02,  # Пропуск amount, пропуск delivery_date
1003,999,2500.00,completed,2024-06-03,2024-06-05  # Несуществующий customer_id
1004,3,-100.00,completed,2024-06-04,2024-06-06  # Отрицательный amount
1005,4,3200.00,invalid_status,2024-06-05,2024-06-07  # Невалидный status
1006,5,1800.00,completed,2024-06-10,2024-06-08  # delivery_date < order_date
```

**Статистика проблем:**

- Всего записей: **~200**
- Пропуски в amount: **~12 (6%)**
- Пропуски в status: **~8 (4%)**
- Отрицательные amount: **~5 (2.5%)**
- Несуществующие customer_id: **~10 (5%)**
- Некорректные даты: **~7 (3.5%)**
- Невалидные статусы: **~6 (3%)**

---

### 3. Продукты (с проблемами)

Файл - [products_raw.csv](data/input/products_raw.csv)

**Проблемы:**

- Дубликаты по `product_name`
- Пропуски в `price`, `category`
- Отрицательные/нулевые `price`
- Несогласованность в `category` (`Electronics/электроника`)

**Пример данных:**

```bash
product_id,product_name,price,category,stock
101,Laptop Pro,1299.99,Electronics,15
102,Wireless Mouse,29.99,Electronics,50
102,Wireless Mouse,29.99,Electronics,50  # Дубликат
103,Coffee Maker,,Home,20  # Пропуск price
104,Headphones,-50.00,электроника,35  # Отрицательный price
105,Standing Desk,449.99,,8  # Пропуск category
```

**Статистика проблем:**

- Всего записей: **~50**
- Дубликаты: **~3 (6%)**
- Пропуски в price: **~4 (8%)**
- Пропуски в category: **~5 (10%)**
- Некорректные цены: **~4 (8%)**

---

## Ожидаемые выходные данные

### 1. Очищенные данные клиентов (customers_clean.csv)

**Изменения:**

- Дубликаты удалены
- Пропуски заполнены (`age`: _медиана_, `city`: _"Unknown"_)
- `Email` стандартизирован (_lowercase, trim_)
- `Gender` унифицирован (_male/female_)
- Выбросы удалены (в которых `age` не в `[0, 120]`)
- Телефоны нормализованы (_+79161234567_)
- Добавлен флаг `data_quality_score`

**Пример очищенных данных:**

```bash
customer_id,email,full_name,age,city,gender,phone,registration_date,data_quality_score
1,alice@example.com,Alice Smith,28,Moscow,female,+79151234567,2024-01-15,100
2,bob@example.com,Bob Johnson,35,SPB,male,+79161234567,2024-02-20,100
3,charlie@example.com,Charlie Brown,30,Moscow,male,Unknown,2024-03-10,75
5,emma@example.com,Emma Davis,29,Unknown,female,+79167654321,2024-05-12,85
```

---

### 2. Очищенные данные заказов (orders_clean.csv)

**Изменения:**

- Пропуски в `amount` заполнены средним
- Пропуски в `status = "unknown"`
- Отрицательные `amount` удалены
- Записи с несуществующими `customer_id` удалены
- Некорректные даты исправлены
- Статусы стандартизированы

**Пример очищенных данных:**

```bash
order_id,customer_id,amount,status,order_date,delivery_date,is_valid
1001,1,1500.50,completed,2024-06-01,2024-06-03,true
1002,2,2000.00,pending,2024-06-02,null,true
1005,4,3200.00,unknown,2024-06-05,2024-06-07,true
```

---

### 3. Очищенные данные о продуктах (products_clean.csv)

**Изменения:**

- Дубликаты удалены (по `product_id`)
- Пропуски в `price` заполнены медианой категории
- Пропуски в `category = "Uncategorized"`
- Пропуски в `supplier_id` удалены (опциональное поле)
- Отрицательные/нулевые `price` удалены
- `Product_name` стандартизирован (_Title Case, trim_)
- `Category` унифицирована (_Electronics, Home, Office, Sports_)
- Отрицательный `stock` заменён на 0

**Пример очищенных данных:**

```bash
product_id,product_name,price,category,stock,supplier_id
101,Laptop Pro,1299.99,Electronics,15,S001
103,Coffee Maker,89.99,Home,20,S003  # price заполнен медианой Home
105,Standing Desk,449.99,Uncategorized,8,S005  # category заполнена
107,Desk Lamp,34.99,Home,25,S007  # Стандартизирован Title Case
108,Notebook,5.99,Office,200,S008  # Пробелы удалены
112,Phone Stand,19.99,Electronics,60,S002  # category унифицирована
114,Backpack,79.99,Sports,0,S004  # stock исправлен -10 → 0
```

### 4. Трансформированные заказы (orders_transformed.csv)

**Новые колонки (добавлено 6):**

- `total_check = quantity * price` (проверка корректности)
- `order_year` = год из `order_date`
- `order_month` = месяц из `order_date`
- `is_large_order = amount > 1000` (флаг)
- `amount_category = small/medium/large/xlarge` (категория суммы)
- `quantity_category = single/few/many/bulk` (категория количества)

**Трансформированные данные:**

```bash
order_id,customer_id,quantity,price,amount,order_date,total_check,order_year,order_month,is_large_order,amount_category,quantity_category
1001,1,2,1299.99,0.324,2024-06-01,2599.98,2024,6,True,xlarge,few
1002,2,1,29.99,0.000,2024-06-02,29.99,2024,6,False,small,single
1003,3,3,89.99,0.033,2024-06-03,269.97,2024,6,False,medium,few
```

**Примечание:** `amount` нормализован min-max в диапазон [0, 1] для ML моделей

**Статистика трансформаций:**

- Типы данных преобразованы: 4 (order_date → datetime, quantity → int)
- Вычисляемых полей создано: 4 (total_check, order_year, order_month, is_large_order)
- Категоризаций выполнено: 2 (amount_category, quantity_category)
- Нормализаций выполнено: 1 (amount min-max)

---

### 5. Месячная агрегация заказов (orders_monthly.csv)

**Группировка:** order_year + order_month

**Метрики:**

- `order_id_count` - количество заказов в месяце
- `quantity_sum` - общее количество товаров
- `price_mean` - средняя цена
- `total_check_sum` - общая выручка

**Пример:**

```bash
order_year,order_month,order_id_count,quantity_sum,price_mean,total_check_sum
2024,6,8,20,614.99,12345.67
2024,7,7,15,543.21,9876.54
2024,8,5,12,478.90,7654.32
2024,9,5,10,522.45,6543.21
```

**Применение:**

- Бизнес-отчётность по месяцам
- Анализ трендов продаж
- Прогнозирование выручки
- Выявление сезонности

---

### 6. Трансформированный каталок продуктов (products_transformed.csv)

1. **Очистка:**

   ```python
   # Удаление дубликатов
   df = remove_duplicates(df, subset=['product_id'])
   
   # Удаление некорректных цен
   df = filter_by_range(df, 'price', min_value=0.01)
   
   # Исправление отрицательного stock
   df.loc[df['stock'] < 0, 'stock'] = 0
   ```

2. **Заполнение пропусков:**

   ```python
   # Price: медиана по категории
   for category in df['category'].unique():
       mask = (df['category'] == category) & (df['price'].isna())
       median_price = df[df['category'] == category]['price'].median()
       df.loc[mask, 'price'] = median_price
   
   # Category: константа
   df['category'] = df['category'].fillna('Uncategorized')
   ```

3. **Стандартизация:**

   ```python
   # Product name: Title Case + trim
   df['product_name'] = df['product_name'].str.strip().str.title()
   
   # Category: унификация
   category_mapping = {
       'electronics': 'Electronics',
       'ELECTRONICS': 'Electronics',
       'электроника': 'Electronics',
       'home': 'Home',
       'HOME': 'Home',
       'office': 'Office',
       'sports': 'Sports'
   }
   df['category'] = df['category'].replace(category_mapping)
   ```

### 7. Отчёт валидации (validation_report_YYYY-MM-DD.json)

**Уровни валидации:**

1. **Structure** - схема данных (100%)
2. **Domain** - форматы и диапазоны (60%)
3. **Business** - бизнес-правила (50%)

<details>

<summary>Пример:</summary>

```json
{
  "validation_date": "2025-12-26",
  "dataset": "customers",
  "levels": {
    "structure": {
      "valid": true,
      "errors": [],
      "warnings": ["Column 'age': expected int64, got float64"]
    },
    "domain": {
      "total_records": 20,
      "valid_records": 12,
      "pass_rate": 0.60,
      "checks": {
        "valid_email": {"valid": 13, "invalid": 7, "pass_rate": 0.65},
        "valid_phone": {"valid": 11, "invalid": 9, "pass_rate": 0.55},
        "valid_age_range": {"valid": 14, "invalid": 6, "pass_rate": 0.70},
        "required_fields": {"valid": 18, "invalid": 2, "pass_rate": 0.90},
        "unique_id": {"valid": 19, "invalid": 1, "pass_rate": 0.95}
      }
    },
    "business": {
      "total_records": 20,
      "valid_records": 10,
      "pass_rate": 0.50,
      "checks": {
        "is_adult": {"pass_rate": 0.85},
        "valid_city": {"pass_rate": 0.90},
        "has_valid_email": {"pass_rate": 0.60}
      }
    }
  },
  "overall_status": "FAILED",
  "overall_quality_score": 0.69
}
```

</details>

**Интерпретация:**

- `Structure`   - схема корректна
- `Domain`      - 60% данных валидны
- `Business`    - только 50% соответствуют правилам
- `Overall`     - 69% → требуется улучшение

---

### 8. Метрики качества (quality_metrics_YYYY-MM-DD.json)

**Метрики:**

- **Completeness** (полнота) - % заполненных значений
- **Uniqueness** (уникальность) - % недублированных записей
- **Validity** (валидность) - % прошедших валидацию
- **Quality Score** - общая оценка (0-100%)

<details>

<summary>Пример:</summary>

```json
{
  "report_date": "2025-01-24",
  "dataset_name": "customers",
  "total_records": 20,
  "metrics": {
    "completeness": {
      "customer_id": 1.00,
      "email": 0.95,
      "age": 0.85,
      "city": 0.80,
      "overall": 0.93
    },
    "uniqueness": {
      "uniqueness_id": 0.95,
      "uniqueness_email": 0.90
    },
    "validity": {
      "valid_email": 0.65,
      "valid_age": 0.70,
      "overall": 0.68
    },
    "quality_score": {
      "overall_score": 0.86,
      "record_scores": {
        "mean": 82.5,
        "min": 45.0,
        "max": 100.0
      }
    }
  },
  "overall_quality_score": 0.86
}
```

</details>

**Формула overall_quality_score:**

```python
score = completeness×0.4 + uniqueness×0.3 + validity×0.3
      = 0.93×0.4 + 0.95×0.3 + 0.68×0.3
      = 0.372 + 0.285 + 0.204
      = 0.861 (86.1%)
```

---

### 9. Журнал изменений (audit_log_YYYY-MM-DD.json)

**Отслеживает:**

- Какие записи изменены
- Какие исправления применены
- Статус обработки (fixed/quarantined)

<details>

<summary>Пример:</summary>

```json
[
  {
    "customer_id": 2,
    "date": "2025-01-24",
    "error_type": "email_format",
    "fixes_applied": "email_cleaned,phone_cleaned",
    "status": "fixed",
    "original_values": {
      "email": "BOB@EXAMPLE.COM",
      "phone": "89161234567"
    },
    "fixed_values": {
      "email": "bob@example.com",
      "phone": "+79161234567"
    }
  },
  {
    "customer_id": 3,
    "date": "2025-01-24",
    "error_type": "email_invalid,age_missing",
    "fixes_applied": "none",
    "status": "quarantined",
    "reason": "Email missing domain, cannot auto-fix"
  }
]
```

</details>

**Статистика:**

- Fixed: 12 записей (60%)
- Quarantined: 8 записей (40%)
- Всего изменений: 20

---

### 10. Карантин (invalid_records_YYYY-MM-DD.csv)

**Содержит записи требующие ручной проверки:**

```bash
customer_id,email,age,error_type,quarantine_reason,quarantine_date
3,charlie@,NULL,email_invalid,Email missing domain,2025-01-24
4,david@,150,email_invalid|age_outlier,Multiple critical errors,2025-01-24
6,frank@,-5,email_invalid|age_outlier,Invalid email AND negative age,2025-01-24
10,jack@,,email_invalid|age_missing,Cannot validate,2025-01-24
```

**Статус карантина:**

- Требуют ручной проверки: 8 записей
- Критичные ошибки (несколько проблем): 3
- Невосстановимые данные: 5

**Действия Data Steward:**

1. Проверить каждую запись вручную
2. Связаться с источником данных
3. Исправить или удалить
4. Обновить правила валидации

---

### 11. Сводный отчёт о качестве (quality_report_YYYY-MM-DD.json)

**Содержание:**

<details>

<summary>Пример:</summary>

```json
{
  "report_date": "2025-01-12",
  "datasets": {
    "customers": {
      "total_records": 100,
      "valid_records": 90,
      "removed_records": 10,
      "metrics": {
        "completeness": 0.92,
        "uniqueness": 0.95,
        "validity": 0.88,
        "consistency": 0.90
      },
      "issues": {
        "duplicates": 5,
        "missing_age": 10,
        "invalid_email": 7,
        "outliers_age": 6
      }
    },
    "orders": {
      "total_records": 200,
      "valid_records": 180,
      "removed_records": 20,
      "metrics": {
        "completeness": 0.94,
        "validity": 0.92,
        "referential_integrity": 0.95
      }
    }
  },
  "overall_quality_score": 0.89
}
```

</details>

## Запуск инфраструктуры

```bash
# 1. Запуск Docker Compose
docker compose up -d

# 2. Проверка статуса
docker compose ps

# Должны быть запущены:
# - airflow (Airflow webserver + scheduler)
# - postgres (база данных)

# 3. Открыть Airflow
# http://localhost:8080
# admin / admin
```

## Описание DAG

### DAG 01: Очистка данных (dag_01_data_cleaning.py)

#### Описание

**Цель:** Демонстрация базовых операций очистки данных

**Что делаем:** Пошагово очищаем данные о клиентах от типичных проблем качества

**Файл:** [dag_01_data_cleaning.py](dags/dag_01_data_cleaning.py)

**Демонстрирует:**

- `df.drop_duplicates()`
- `df.fillna()`, `df.dropna()`
- Фильтрация по условиям
- Логирование изменений

#### Граф выполнения

```python
start → load_raw → remove_duplicates → handle_missing → remove_outliers → standardize → save_clean → end
```

#### Особенности реализации

**Параметры DAG:**

```python
# Передаются первой задаче в конвейере DAG
params = {
    'input_path': '/opt/airflow/data/input/customers_raw.csv',
    'delimiter': ',',
    'encoding': 'utf-8'
}
```

**В задачах рализовано:**

- проверка существования входного файла
- возврат сводных данных из каждой функции
- логирование
- сохранение промежуточных результатов в папке `/opt/airflow/data/temp`
- XCom для передачи данных между задачами

---

#### Подробное описание шагов

**Шаг 1: load_raw_data** - Загрузка "грязных" данных

- **Что делаем:** Загружаем тестовый датасет с записями клиентов
- **Параметры загрузки:**

  ```python
  params = context.get('params', {})
  input_path = params['input_path']   # Путь к файлу
  delimiter = params['delimiter']     # Разделитель (,)
  encoding = params['encoding']       # Кодировка (utf-8)
  ```

- **Проблемы в данных:**
  - Дубликат: `customer_id=2` встречается дважды
  - Невалидные email: _"charlie@"_, "paula@", "invalid"_
  - Выбросы в `age`: _150, -5_
  - Пропуски: `age=None`, `city=None`
  - Несогласованность значений `gender`: _"М", "м", "Male", "male"_
  - Некорректные телефоны: _"invalid", "123"_
- **Зачем:** Показать реальные проблемы которые встречаются в данных
- **Входные данные:** CSV файл `/opt/airflow/data/input/customers_raw.csv`
- **Лог работы:**

    ```text
    Количество записей: 21
    Количество столбцов: 8 ['customer_id', 'email', 'full_name', 'age', 'city', 'gender', 'phone', 'registration_date']
    Количество дубликатов: 1
    Количество пропусков: 7
    ```

- **Возвращаемый результат:**

  ```python
  return {
      'status': 'success',
      'input_path': input_path,
      'rows_processed': len(df),
      'columns': list(df.columns),
      'output_path': output_path
  }
  ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/customers_raw.csv`

**Шаг 2: remove_duplicates** - Удаление дубликатов

- **Что делаем:** Используем функцию `remove_duplicates(df, subset=['customer_id'], keep='first')`
- **Зачем:** Оставляем только уникальных клиентов по ID
- **Получение данных через XCom:**

  ```python
  input_data = ti.xcom_pull(task_ids='load_raw_data')
  input_path = input_data['output_path']
  df = pd.read_csv(input_path)
  ```

- **Параметры:**
  - `subset=['customer_id']` - проверяем уникальность по ID
  - `keep='first'` - оставляем первое вхождение
- **Входные данные:** CSV файл `/opt/airflow/data/temp/customers_raw.csv`
- **Лог работы:**

    ```text
    Обработано записей: 21
    Обнаружено дубликатов: 1 (4.8%)
    Осталось записей: 20 
    ```

- **Возвращаемое значение:**

  ```python
  return {
      'status': 'success',
      'input_path': input_path,
      'rows_processed': len(df),
      'rows_after': len(df_clean),
      'output_path': output_path
  }
  ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/customers_step1.csv`

**Шаг 3: handle_missing** - Обработка пропущенных значений

- **Что делаем:** Применяем разные стратегии для разных колонок
- **Стратегии:**
  - `age: 'median'` - заполняем медианой (устойчиво к выбросам)
  - `city: 'constant:Unknown'` - заполняем константой
- **Зачем:** Сохранить максимум данных вместо удаления записей
- **Что получаем:** Все пропуски заполнены
- **Входные данные:** CSV файл `/opt/airflow/data/temp/customers_step1.csv`
- **Лог работы:**

    ```text
    Пропуски до:
        age: 3
        city: 4
    age: 3 пропусков (14.3%)
        → Заполнены медианой: 31.50
    city: 4 пропусков (19.0%)
        → Заполнены константой: Unknown
    Пропуски после: 0
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/customers_step2.csv`

**Шаг  4: remove_outliers** - Удаление выбросов

- **Что делаем:** Фильтруем возраст по разумному диапазону
- **Правило:** age должен быть в диапазоне [0, 120]
- **Зачем:** Удалить физически невозможные значения (150, -5)
- **Функция:** `filter_by_range(df, 'age', min_value=0, max_value=120)`
- **Входные данные:** CSV файл `/opt/airflow/data/temp/customers_step2.csv`
- **Лог работы:**

    ```text
    Обработано записей: 21
        Диапазон Age: [-5.0, 200.0]
        Колонка age: 7 записей вне диапазона [0, 120]
    Осталось записей: 14
    Диапазон Age: [25.0, 42.0]
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/customers_step3.csv`

**Шаг 5: standardize** - Стандартизация данных

- **Что делаем:** Приводим данные к единому формату
- **Операции:**
  1. **Email:** `lowercase` + `trim`: _"ALICE@EXAMPLE.COM" → "alice@example.com"_
  2. **Gender:** унификация: _"М", "м", "Male" → "male"_
  3. **Phone:** очистка + код страны: _"89161234567" → "+79161234567"_
- **Зачем:** Обеспечить консистентность для анализа и поиска
- **Функции:**
  - `standardize_text(df, 'email', ['lowercase', 'strip'])`
  - `replace_values(df, 'gender', {'М': 'male', 'м': 'male', ...})`
  - `clean_phone_numbers(df, 'phone', add_country_code='+7')`
- **Что получаем:** Все данные в едином формате
- **Входные данные:** CSV файл `/opt/airflow/data/temp/customers_step3.csv`
- **Лог работы:**

    ```text
    email: преобразован в lowercase
    email: удалены пробелы
    gender: 10 значений заменено
        Замены: {'М': 'male', 'м': 'male', 'M': 'male', 'Male': 'male', 'Ж': 'female', 'ж': 'female', 'F': 'female', 'Female': 'female'}
    Значения колонки Gender после стандартизации: ['female' 'male']
    phone: удалены символы, кроме цифр
    phone: добавлен код страны +7
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/output/customers_clean.csv`

**Шаг 6: save_and_report** - Финальный отчёт

- **Что делаем:** Сравниваем исходные и очищенные данные
- **Входные данные:** CSV файл `/opt/airflow/data/input/customers_raw.csv` и файл `/opt/airflow/data/output/customers_clean.csv`
- **Лог работы:**

    ```text
    Всего записей: 21
    Осталось записей: 21
    Удалено: 0 (0.0%)
    Оценка качества:
        Удалено дубликатов: 1
        Заполнено недостающих значений: 7
        Стандартизация значений: email, gender, phone
    Очещенные данные сохранены в файле: /opt/airflow/data/output/customers_clean.csv
    ```

- **Файл:** `/opt/airflow/data/output/customers_clean.csv`

---

#### Результат DAG 01

**Исходные данные (9 записей):**

```bash
customer_id,email,age,city,gender
1,alice@example.com,28,Moscow,female
2,bob@example.com,35,SPB,М        # Дубликат ниже
2,bob@example.com,35,SPB,М        # Дубликат
3,charlie@,,Moscow,male           # Пропуск age
4,david@example.com,150,Kazan,м   # Выброс age
5,emma@example.com,29,,female     # Пропуск city
6,frank@example.com,-5,Moscow,Male # Выброс age
```

**Очищенные данные (6 записей):**

```bash
customer_id,email,age,city,gender,phone
1,alice@example.com,28,Moscow,female,+79151234567
2,bob@example.com,35,SPB,male,+79161234567
3,charlie@example.com,30,Moscow,male,+79163334455  # age заполнен медианой
5,emma@example.com,29,Unknown,female,+79167654321  # city="Unknown"
7,grace@example.com,42,Kazan,female,+79168889900
8,henry@example.com,30,SPB,male,+79169998877
```

**Что изучили:**

- Как удалять дубликаты
- Как заполнять пропуски (разные стратегии)
- Как фильтровать выбросы
- Как стандартизировать текстовые данные
- Как логировать изменения
- Как сохранять промежуточные результаты

---

#### Упражнения

**1. Добавьте обработку ещё одной проблемы:**

- Добавьте валидацию `email` (удалите невалидные)
- Подсказка: используйте функцию `validate_email` из validators

---

### DAG 02: Трансформация данных (dag_02_transformation.py)

#### Описание

**Цель:** Демонстрация различных способов трансформаций данных

**Что делаем:** Преобразуем данные о заказах для аналитики

**Файл:** [dag_02_transformation.py](dags/dag_02_transformation.py)

#### Граф выполнения

```python
start → load → convert_types → create_features → categorize → normalize_aggregate → summary → end
```

**Операции:**

1. **type_conversion** - приведение типов (_string → datetime, string → int_)
2. **text_normalization** - нормализация текста (_lowercase, trim_)
3. **create_features** - создание вычисляемых полей
4. **merge** - объединение данных
5. **save** - сохранение результата

**Демонстрирует:**

- `pd.to_datetime()`, `astype()`
- `str.lower()`, `str.strip()`, `str.replace()`
- Создание колонок через вычисления
- `pd.merge()`, `pd.concat()`

#### Особенности реализации

**Параметры DAG:**

```python
# Передаются первой задаче в конвейере DAG
params = {
    'input_path': '/opt/airflow/data/input/orders_raw.csv',
    'delimiter': ',',
    'encoding': 'utf-8'
}
```

**В задачах рализовано:**

- проверка существования входного файла
- возврат сводных данных из каждой функции
- логирование
- сохранение промежуточных результатов в папке `/opt/airflow/data/temp`
- XCom для передачи данных между задачами
- Создание двух выходных файлов (детальный + агрегированный)

---

#### Подробное описание шагов

**Шаг 1: load_data** - Загрузка данных о заказах

- **Что делаем:** Загружаем записи заказов из `/opt/airflow/data/input/orders_raw.csv`
- **Структура данных:**

    ```csv
    order_id,customer_id,product_id,quantity,price,amount,status,order_date
    1001,1,101,2,1299.99,2599.98,completed,2024-06-01
    ```

- **Проблемы типов:**
  - `order_date` - строка, а нужен `datetime`
  - `quantity` - может быть `float`, нужен `int`
  - `price, amount` - нужны точные `float`
- **Что получаем:** CSV с исходными типами данных
- **Входные данные:** CSV файл `/opt/airflow/data/input/orders_raw.csv`
- **Лог работы:**

    ```bash
    Загружены из файла: /opt/***/data/input/orders_raw.csv
    Всего записей: 25
    Колонки: 9 - ['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'amount', 'status', 'order_date', 'delivery_date']
    Типы данных:
        order_id           int64
        customer_id        int64
        product_id         int64
        quantity           int64
        price            float64
        amount           float64
        status            object
        order_date        object  # ← Строка вместо datetime
        delivery_date     object  # ← Строка вместо datetime
        dtype:            object
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/orders_step1_loaded.csv`

**Шаг 2: convert_types** - Приведение типов данных

- **Что делаем:** Конвертируем колонки к правильным типам
- **Операции:**
  - `order_date`: _string → datetime_ (для работы с датами)
  - `quantity`: _any → int_ (целые числа)
  - `price`: _any → float_ (десятичные числа)
  - `amount`: _any → float_ (десятичные числа)
- **Зачем:**
  - `Datetime` позволяет извлекать год, месяц, день
  - `Int` экономит память и обеспечивает корректность
  - `Float` нужен для денежных расчётов
- **Функция:** `convert_types(df, {'order_date': 'datetime', 'quantity': 'int', ...})`
- **Что получаем:** Корректные типы для всех колонок
- **Входные данные:** CSV файл `/opt/airflow/data/temp/orders_step1_loaded.csv`
- **Лог работы:**

    ```text
    Типы данных ДО:
        quantity           int64
        price            float64
        amount           float64
        order_date        object
        delivery_date     object

    order_date: преобразовано в datetime
    delivery_date: преобразовано в datetime
    quantity: преобразовано в int
    price: преобразовано в float
    amount: преобразовано в float

    Типы данных ПОСЛЕ:
        quantity                  Int64
        price                   float64
        amount                  float64
        order_date       datetime64[ns]
        delivery_date    datetime64[ns]
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/orders_step2_types.csv`

**Шаг 3: create_features** - Создание вычисляемых полей

- **Что делаем:** Добавляем новые колонки для аналитики
- **Новые поля:**
  1. **total_check** = quantity * price
     - Проверка корректности поля `amount`
     - Должно совпадать с `amount`
  
  2. **order_year** = year из order_date
     - Для группировки по годам
     - Пример: 2025-06-15 → 2025
  
  3. **order_month** = month из order_date  
     - Для группировки по месяцам
     - Пример: 2025-06-15 → 6
  
  4. **is_large_order** = amount > 1000
     - Флаг крупного заказа
     - True/False для фильтрации
- **Зачем:**
  - `total_check` - валидация данных
  - `order_year`, `order_month` - временная аналитика
  - `is_large_order` - сегментация клиентов
- **Функция:** `create_calculated_column(df, 'total_check', lambda df: df['quantity'] * df['price'])`
- **Что получаем:** Датафрейм с 4 новыми колонками
- **Входные данные:** CSV файл `/opt/airflow/data/temp/orders_step2_types.csv`
- **Лог работы:**

    ```text
    Столбцы ДО: 9 - ['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'amount', 'status', 'order_date', 'delivery_date']
        Создана колонка: total_check
        Создана колонка: order_year
        Создана колонка: order_month
        Создана колонка: is_large_order
    Столбцы ПОСЛЕ: 13 - ['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'amount', 'status', 'order_date', 'delivery_date', 'total_check', 'order_year', 'order_month', 'is_large_order']
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/orders_step3_features.csv`

**Шаг 4: categorize** - Категоризация числовых данных

- **Что делаем:** Преобразуем числа в категории для анализа
- **Категоризация 1: amount → amount_category**
  - _small_: 0-100 руб
  - _medium_: 100-500 руб
  - _large_: 500-1000 руб
  - _xlarge_: >1000 руб
  - **Зачем:** Сегментация заказов по сумме
  
- **Категоризация 2: quantity → quantity_category**
  - _single_: 1 шт
  - _few_: 2-3 шт
  - _many_: 4-10 шт
  - _bulk_: >10 шт
  - **Зачем:** Анализ оптовых/розничных покупок
- **Функция:** `categorize_numeric(df, 'amount', 'amount_category', bins=[0,100,500,1000,10000], labels=[...])`
- **Что получаем:** 2 новые категориальные колонки
- **Входные данные:** CSV файл `/opt/airflow/data/temp/orders_step3_features.csv`
- **Лог работы:**

    ```text
    Категории amount в amount_category
    Категории quantity в quantity_category
    Категории столбца Amount:
        amount_category
            xlarge    10
            medium     8
            small      4
            large      1
    Категории столбца Quantity:
        quantity_category
            few       11
            single    10
            many       4
            bulk       0
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/orders_step4_categorized.csv`

**Шаг 5: normalize_aggregate** - Нормализация и агрегация

- **Что делаем:** 
  1. Нормализация amount (для ML моделей)
  2. Агрегация по месяцам (для отчётности)

- **Нормализация amount (Min-Max):**
  - Формула: `(x - min) / (max - min)`
  - Диапазон: [0, 1]
  - **Зачем:** Приведение к единой шкале для ML
  - **Пример:**
    - Было: `amount = [29.99, 2599.98, 179.98]`
    - Стало: `amount = [0.00, 1.00, 0.07]`

- **Агрегация по месяцам:**
  - Группировка: `order_year`, `order_month`
  - Метрики:
    - `order_id`: _count_ (количество заказов)
    - `quantity`: _sum_ (общее количество товаров)
    - `price`: _mean_ (средняя цена)
    - `total_check`: _sum_ (общая выручка)
  - **Зачем:** Месячные отчёты для бизнеса

- **Функции:**
  - `normalize_column(df, 'amount', method='minmax')`
  - `aggregate_data(df, group_by=['order_year', 'order_month'], aggregations={...})`

- **Что получаем:**
  - Детальные данные с нормализованным amount
  - Месячная сводка

- **Входные данные:** CSV файл `/opt/airflow/data/temp/orders_step4_categorized.csv`
- **Лог работы:**

    ```text
    Диапазон столбца Amount ДО нормализации: [29.99, 7995.00]
        amount: нормалищована min-max в интервал [0, 1]
    Диапазон столбца Amount ПОСЛЕ нормализации: [0.00, 1.00]
    Агрегация по ['order_year', 'order_month']
    Полные данные: /opt/***/data/output/orders_transformed.csv
    Помесячные данне: /opt/***/data/output/orders_monthly.csv
    ```

- **Выходные данные:** 
  - файл `/opt/airflow/data/output/orders_transformed.csv` (детальные)
  - файл `/opt/airflow/data/output/orders_monthly.csv` (агрегация)

**Шаг 6: summary_report** - Итоговый отчёт

- **Что делаем:** Выводим сводку по всем трансформациям
- **Лог работы:**

    ```text
    Трансформация данных:
        1. Конвертация типов (datetime, int, float)
        2. Заполнение значени полей (4 new columns)
        3. Категоризация (amount & quantity)
        4. Нормализация (min-max scaling)
        5. Агрегация (monthly summary)
    Выходные файлы:
        - Детальный: /opt/airflow/data/output/orders_transformed.csv
        - Помесячный: /opt/airflow/data/output/orders_monthly.csv
    Статистика:
        - Всего заказов: 25
        - Покрыто месяцев: 4    
    ```

---

#### Результат DAG 02

**Исходные данные:**

```csv
order_id,customer_id,quantity,price,amount,order_date
1001,1,2,1299.99,2599.98,2024-06-01  # order_date - строка
```

**Трансформированные данные:**

```csv
order_id,customer_id,quantity,price,amount,order_date,total_check,order_year,order_month,is_large_order,amount_category,quantity_category
1001,1,2,1299.99,0.324,2024-06-01,2599.98,2024,6,True,xlarge,few
# ↑ amount нормализован, добавлено 6 новых колонок
```

**Месячная агрегация:**

```csv
order_year,order_month,order_id_count,quantity_sum,price_mean,total_check_sum
2024,6,8,20,614.99,12345.67
2024,7,7,15,543.21,9876.54
```

**Что изучили:**

- Как приводить типы данных (datetime, int, float)
- Как создавать вычисляемые поля
- Как категоризировать числовые данные
- Как нормализовать данные (min-max)
- Как агрегировать данные (группировка)
- Как создавать сводные отчёты

---

#### Упражнения

**1. Добавьте новое вычисляемое поле:**

- `discount_percent`: _(price * quantity - amount) / (price * quantity) * 100_
- Показывает процент скидки

**2. Добавьте категоризацию по цене:**

- `bins=[0, 50, 200, 1000, 10000]`
- `labels=['budget', 'standard', 'premium', 'luxury']`

**3. Создайте агрегацию по статусам заказа:**

- Группировка по `status`
- Подсчёт количества, средней суммы, общей суммы

---

### DAG 03: Валидация данных (dag_03_validation.py)

#### Описание

**Цель:** Многоуровневая проверка корректности данных

**Что делаем:** Валидируем данные о клиентах на 3 уровнях (параллельно!)

**Файл:** [dag_03_validation.py](dags/dag_03_validation.py)

#### Граф выполнения

```python
start → load → [validate_structure, validate_domain, validate_business] → generate_report → end
```

**Операции:**

1. **validate_structure** - проверка структуры (_колонки, типы_)
2. **validate_domain** - проверка диапазонов, форматов
3. **validate_business** - проверка бизнес-правил
4. **generate_report** - создание отчёта

**Демонстрирует:**

- Проверка наличия колонок
- Валидация email, телефонов regex
- Проверка диапазонов значений
- Проверка внешних ключей

#### Особенности реализации

**Параметры DAG:**

```python
# Передаются первой задаче в конвейере DAG
params = {
    'input_path': '/opt/airflow/data/input/customers_raw.csv',
    'delimiter': ',',
    'encoding': 'utf-8'
}
```

**В задачах рализовано:**

- параллельная валидация на 3 уровнях
- возврат сводных данных из каждой функции
- логирование
- объединение результатов в merge_results
- расчёт overall_quality_score (взвешенная оценка)
- сохранение JSON отчёта с timestamp

---

#### Подробное описание шагов

**Шаг 1: load_data** - Загрузка данных для валидации

- **Что делаем:** Загружаем записи клиентов из customers_raw.csv
- **Параметры загрузки:**

    ```python
    params = context.get('params', {})
    input_path = params['input_path']
    delimiter = params['delimiter']
    encoding = params['encoding']
    ```

- **Входные данные:** CSV файл `/opt/airflow/data/input/customers_raw.csv` или сгенерированные данные

    ```python
    if os.path.exists(input_path):
        df = pd.read_csv(input_path, delimiter=delimiter, encoding=encoding)
    else:
        # Тестовые данные (если файла нет)
    ```

- **Что получаем:** Данные с известными проблемами для проверки
- **Лог работы:**

    ```text
    Загружены данные: 21 записей
    Структура данных: (21, 8)
    Столбцы: 8 - ['customer_id', 'email', 'full_name', 'age', 'city', 'gender', 'phone', 'registration_date']

     ```

- **Возвращаемый результат:**

    ```python
    return {
        'status': 'success',
        'input_path': input_path,
        'rows_processed': len(df),
        'columns': list(df.columns),
        'output_path': output_path
    }
    ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/validation_data.csv`

**Шаг 2: validate_structure** - УРОВЕНЬ 1: Структурная валидация

- **Что проверяем:** Соответствие схеме данных
- **Получение данных через XCom:**

    ```python
    input_data = ti.xcom_pull(task_ids='load_data')
    input_path = input_data['output_path']
    df = pd.read_csv(input_path)
    ```

- **Проверки:**
  1. **Наличие обязательных колонок:**
     - Required: `customer_id`, `email`, `full_name`, `age`, `city`, `phone`
     - Проверяем что все колонки присутствуют
  
  2. **Типы данных:**
     - `customer_id` должен быть `int64`
     - `age` должен быть `float64` (из-за NaN)
     - Если типы не совпадают → _warning_

- **Зачем:** Убедиться что структура данных корректна перед детальной проверкой

- **Функция:** `validate_schema(df, required_columns=[...], expected_types={...})`

- **Входные данные:** CSV файл `/opt/airflow/data/temp/validation_data.csv`
- **Лог работы:**

    ```text
    Все колонки присутствуют
    Результаты валидации схемы:
    Корректно: True
    Ошибок не обнаружено
    Предупреждений нет
    ```

- **Возвращаемый результат:** Словарь с результатами

    ```python
    {
        'valid': True,  # или False если есть ошибки
        'errors': [],   # критичные ошибки (отсутствующие колонки)
        'warnings': []  # предупреждения (несовпадение типов)
    }
    ```

**Шаг 3: validate_domain** - УРОВЕНЬ 2: Доменная валидация  

- **Что проверяем:** Форматы и диапазоны значений
- **Проверки:**
  1. **Email формат:** 
     - Regex: `^[\w\.-]+@[\w\.-]+\.\w+$`
     - Проверяем что есть @ и домен
     - Примеры: alice@example.com ✓, charlie@ ✗
  
  2. **Phone формат:**
     - Regex: `^\+7\d{10}$` (российский формат)
     - Должен начинаться с +7 и иметь 10 цифр
     - Примеры: +79161234567 ✓, invalid ✗
  
  3. **Age диапазон:**
     - Должен быть в [0, 120]
     - Примеры: 28 ✓, 150 ✗, -5 ✗
  
  4. **Required fields not NULL:**
     - customer_id, email, full_name не должны быть NULL
  
  5. **Unique customer_id:**
     - Проверяем отсутствие дубликатов

- **Зачем:** Проверить что значения соответствуют допустимым форматам

- **Функции:**
  - `validate_email(df, 'email')`
  - `validate_phone(df, 'phone')`
  - `validate_range(df, 'age', 0, 120)`
  - `validate_not_null(df, ['customer_id', 'email', 'full_name'])`
  - `validate_unique(df, ['customer_id'])`

- **Лог работы:**

    ```text
    Валидация Email: верных - 17, неверных - 4
    Валидация Phone: 5/21 верных
    Валидация диапазона [0, 120]:   верных - 14, неверных - 7
    Валидация Not null: 21/21 строк имеют все обязательные поля
    Валидация уникальности значений: дубликатов - 2
    Валидация завершена: верных - 3/21 (14.3%)
    Результаты доменной валидации:
      Всего записей: 21
      Корректных записей: 3
      Некорректных записей: 18
      Процент прохождения: 14.29%
      Детализация проверок:
        valid_email: 80.95% (17/21)
        valid_phone: 23.81% (5/21)
        valid_age_range: 66.67% (14/21)
        required_fields: 100.00% (21/21)
        unique_id: 90.48% (19/21)
    ```

- **Возвращаемый результат:** Отчёт валидации

    ```python
    {
        'total_records': 20,
        'valid_records': 12,      # Прошли ВСЕ проверки
        'invalid_records': 8,      # Не прошли хотя бы 1 проверку
        'overall_pass_rate': 0.60, # 60% валидных
        'checks': {
            'valid_email': {'valid': 13, 'invalid': 7, 'pass_rate': 0.65},
            'valid_phone': {'valid': 11, 'invalid': 9, 'pass_rate': 0.55},
            'valid_age_range': {'valid': 14, 'invalid': 6, 'pass_rate': 0.70},
            'required_fields': {'valid': 18, 'invalid': 2, 'pass_rate': 0.90},
            'unique_id': {'valid': 19, 'invalid': 1, 'pass_rate': 0.95}
        }
    }
    ```

**Шаг 4: validate_business** - УРОВЕНЬ 3: Бизнес-валидация

- **Что проверяем:** Бизнес-правила и зависимости
- **Проверки:**
  1. **Совершеннолетие:**
     - Если age заполнен → должен быть >= 18
     - age=NULL допустим (опциональное поле)
     - **Бизнес-правило:** Регистрируем только взрослых
  
  2. **Допустимые города:**
     - Список: Moscow, SPB, Kazan, Ekaterinburg, Novosibirsk
     - city=NULL допустим
     - **Бизнес-правило:** Работаем только в этих городах
  
  3. **Валидный email И не NULL:**
     - Email должен быть заполнен И валиден
     - **Бизнес-правило:** Email обязателен для связи

- **Зачем:** Проверить специфичные бизнес-требования компании

- **Логика:**

  ```python
  # Совершеннолетие
  is_adult = (df['age'].isna()) | (df['age'] >= 18)
  
  # Допустимый город
  is_valid_city = df['city'].isna() | df['city'].isin(['Moscow', 'SPB', ...])
  
  # Email заполнен И валиден
  has_valid_email = validate_email(df, 'email') & df['email'].notna()
  ```

- **Лог работы:**

    ```text
    Проверка совершеннолетия: 20/21 прошли
    Корректный город: 21/21 прошли
    Валидация Email: верных - 17, неверных - 4
      Корректный email: 17/21 прошли
    Валидация завершена: верных - 16/21 (76.2%)
    Результаты бизнес-валидации:
      Процент прохождения: 76.19%
      Корректных записей: 16/21
    ```

- **Возвращаемый результат:** Отчёт бизнес-валидации

    ```python
    {
        'total_records': 20,
        'valid_records': 10,
        'pass_rate': 0.50,
        'checks': {
            'is_adult': {'pass_rate': 0.85},
            'valid_city': {'pass_rate': 0.90},
            'has_valid_email': {'pass_rate': 0.60}
        }
    }
    ```

**Шаг 5: merge_results** - Объединение результатов всех уровней

- **Что делаем:** Собираем результаты 3 параллельных проверок

- **Лог работы:**

    ```text
    Сводка по уровням валидации:
      Структура: Корректно
      Домен: 14.29% прохождения
      Бизнес: 76.19% прохождения
    Общая оценка качества: 58.57%
    Общий статус: FAILED
    ```

- **Возвращаемый результат:** единый отчёт:

    ```python
    {
        'validation_date': '2025-01-24',
        'dataset': 'customers',
        'levels': {
            'structure': {
            'valid': True,
            'errors': [],
            'warnings': [...]
            },
            'domain': {
            'total_records': 20,
            'valid_records': 12,
            'pass_rate': 0.60,
            'checks': {...}
            },
            'business': {
            'total_records': 20,
            'valid_records': 10,
            'pass_rate': 0.50,
            'checks': {...}
            }
        },
        'overall_status': 'FAILED',  # PASSED если все уровни ОК
        'overall_quality_score': 0.70  # Взвешенная оценка
    }
    ```

- **Расчёт overall_quality_score:**

    ```python
    score = (
        (1.0 if structure.valid else 0.0) * 0.3 +  # Структура 30%
        domain.pass_rate * 0.4 +                    # Домен 40%
        business.pass_rate * 0.3                    # Бизнес 30%
    )
    # = (1.0 * 0.3) + (0.60 * 0.4) + (0.50 * 0.3) = 0.69
    ```

- **Overall status:**
  - `PASSED`: если structure valid И domain/business pass_rate > 0.8
  - `FAILED`: иначе

**Шаг 6: generate_report** - Сохранение отчёта в JSON

- **Что делаем:** Сохраняем полный отчёт для документации
- **Файл:** `/opt/airflow/data/quality_reports/validation_report_YYYY-MM-DD.json`

- **Лог работы:**

    ```text
    Отчет сохранен: /opt/airflow/data/quality_reports/validation_report_YYYY-MM-DD.json
    Сводка отчета:
    Датасет: customers
    Дата валидации: 2026-01-25
    Общий статус: FAILED
    Оценка качества: 58.57%
    ```

---

#### Результат DAG 03

**JSON Отчёт:** файл `/opt/airflow/data/quality_reports/validation_report_YYYY-MM-DD.json`

**Что изучили:**

- Как проверять структуру данных (схема, типы)
- Как валидировать форматы (email, phone, диапазоны)
- Как проверять бизнес-правила
- Как выполнять валидации параллельно
- Как объединять результаты проверок
- Как рассчитывать overall quality score
- Как создавать JSON отчёты

---

### DAG 04: Метрики качества (dag_04_quality_metrics.py)

#### Описание

**Цель:** Расчёт метрик качества данных

**Что делаем:** Рассчитываем ключевые метрики качества (параллельно)

**Файл:** [dag_04_quality_metrics.py](dags/dag_04_quality_metrics.py)

#### Граф выполнения

```python
start → load → [calc_completeness, calc_uniqueness, calc_validity] → aggregate_metrics → save_report → end
```

**Операции:**

1. **calc_completeness** - % заполненности
2. **calc_uniqueness** - % уникальности
3. **calc_validity** - % валидности
4. **aggregate_metrics** - сводная оценка качества
5. **save_report** - JSON отчёт

**Демонстрирует:**

- Расчёт метрик pandas
- Агрегация результатов
- Создание JSON отчётов
- Визуализация метрик

#### Особенности реализации

**Параметры DAG:**

```python
params = {
    'input_path': '/opt/airflow/data/input/customers_raw.csv',
    'delimiter': ',',
    'encoding': 'utf-8'
}
```

**В задачах рализовано:**

- параллельный расчёт 3 метрик (completeness, uniqueness, validity)
- возврат сводных данных из каждой функции
- логирование
- расчёт overall_quality_score (взвешенное среднее)
- расчёт quality_score для каждой записи
- сохранение JSON отчёта с timestamp

---

#### Подробное описание шагов

**Шаг 1: load_data** - Загрузка данных для анализа

- **Что делаем:** Загружаем customers_raw.csv
- **Параметры загрузки:**

  ```python
  params = context.get('params', {})
  input_path = params['input_path']
  delimiter = params['delimiter']
  encoding = params['encoding']
  ```

-- **Входные данные:** CSV файл `/opt/airflow/data/input/customers_raw.csv` или сгенерированные данные

- **Что получаем:** Данные для расчёта метрик
- **Возвращаемый результат:**

  ```python
  return {
      'status': 'success',
      'input_path': input_path,
      'rows_processed': len(df),
      'columns': list(df.columns),
      'output_path': output_path
  }
  ```

- **Лог работы:**
  
  ```text
  Загружены данные: 21 записей
  Всего записей: 21 
  ```

**Шаг 2: calculate_completeness** - Метрика полноты данных

- **Что считаем:** Процент заполненных (не NULL) значений для каждой колонки
- **Формула:** `completeness = (количество не-NULL) / (всего записей)`
- **Зачем:** Понять насколько полны данные в каждом поле

- **Расчёт:**

  ```python
  for column in df.columns:
      filled = df[column].notna().sum()
      completeness[column] = filled / total_rows
  ```

- **Функция:** `calculate_completeness(df)`

- **Возвращаемый результат:** Словарь {колонка: % заполненности}

  ```python
  {
    'customer_id': 1.00,    # 20/20 = 100% заполнено
    'email': 0.95,          # 19/20 = 95% (1 пропуск)
    'full_name': 0.90,      # 18/20 = 90% (2 пропуска)
    'age': 0.85,            # 17/20 = 85% (3 пропуска)
    'city': 0.80,           # 16/20 = 80% (4 пропуска)
    'gender': 1.00,         # 20/20 = 100%
    'phone': 1.00           # 20/20 = 100%
  }
  ```

- **Overall completeness:** Средняя по всем колонкам

  ```python
  overall = (1.00 + 0.95 + 0.90 + 0.85 + 0.80 + 1.00 + 1.00) / 7 = 0.93
  ```

- **Лог работы:**

  ```text
  Рассчитана полнота для 8 колонок
  Полнота по столбцам:
    GOOD customer_id: 100.00%
    GOOD email: 100.00%
    GOOD full_name: 100.00%
    WARN age: 85.71%
    WARN city: 80.95%
    GOOD gender: 100.00%
    GOOD phone: 100.00%
    GOOD registration_date: 100.00%
  Общая полнота: 95.83%
  ```

- **Интерпретация:**
  - _100%_ - отлично
  - _80-99%_ - хорошо, но есть пропуски
  - _<80%_ - проблема, много пропусков

**Шаг 3: calculate_uniqueness** - Метрика уникальности

- **Что считаем:** Процент уникальных (недублированных) записей
- **Формула:** `uniqueness = (уникальные записи) / (всего записей)`
- **Зачем:** Обнаружить дублирующиеся данные

- **Расчёт:**

  ```python
  # Уникальность по customer_id
  total = 20
  unique = df.drop_duplicates(subset=['customer_id']).shape[0]  # 19
  uniqueness_id = unique / total  # 19/20 = 0.95
  
  # Уникальность по email
  unique_emails = df.drop_duplicates(subset=['email']).shape[0]  # 18
  uniqueness_email = unique_emails / total  # 18/20 = 0.90
  ```

- **Функция:** `calculate_uniqueness(df, ['customer_id'])`

- **Возвращаемый результат:**

  ```python
  {
    'uniqueness_id': 0.95,     # 5% дубликатов по ID
    'uniqueness_email': 0.90   # 10% дубликатов по email
  }
  ```

- **Лог работы:**

  ```text
  Уникальность: 95.24% (20/21)
    Уникальность Customer ID: 95.24%
    Уникальность Email: 95.24%
  ```

- **Интерпретация:**
  - 100% - нет дубликатов
  - 90-99% - есть небольшие дубликаты
  - <90% - серьёзная проблема дублирования

**Шаг 4: calculate_validity** - Метрика валидности

- **Что считаем:** Процент записей прошедших валидацию
- **Формула:** `validity = (валидные записи) / (всего записей)`
- **Зачем:** Понять сколько данных корректны

- **Правила валидации:**

  ```python
  validations = {
    'valid_email': validate_email(df, 'email'),      # Regex проверка
    'valid_age': validate_range(df, 'age', 0, 120)  # Диапазон
  }
  ```

- **Расчёт:**

  ```python
  # Email валидность
  valid_emails = validate_email(df, 'email').sum()  # 13
  validity_email = valid_emails / 20  # 13/20 = 0.65
  
  # Age валидность
  valid_ages = validate_range(df, 'age', 0, 120).sum()  # 14
  validity_age = valid_ages / 20  # 14/20 = 0.70
  ```

- **Функция:** `calculate_validity(df, validation_rules)`

- **Возвращаемый результат:**

  ```python
  {
    'valid_email': 0.65,   # 65% валидных email
    'valid_age': 0.70,     # 70% валидных возрастов
    'overall': 0.675       # Среднее
  }
  ```

- **Лог работы:**

  ```text
  
  Валидация Email: верных - 17, неверных - 4
  Валидация диапазона [0, 120]:   верных - 14, неверных - 7
  Валидность для 2 правил
    WARN valid_email: 80.95%
    BAD  valid_age: 66.67%
  Общая валидность: 73.81%
  ```
  
- **Интерпретация:**
  - 90-100% - отличное качество
  - 70-89% - приемлемо, нужны улучшения
  - <70% - плохое качество, срочно исправлять

**Шаг 5: calculate_quality_score** - Общая оценка качества

- **Что делаем:** Объединяем все метрики в единую оценку
- **Формула взвешенного среднего:**

  ```python
  overall_score = (
    completeness * 0.4 +      # Полнота - 40%
    uniqueness * 0.3 +         # Уникальность - 30%
    validity * 0.3             # Валидность - 30%
  )
  ```

- **Расчёт для нашего примера:**

  ```python
  overall_score = (
    0.93 * 0.4 +    # 0.372
    0.95 * 0.3 +    # 0.285
    0.68 * 0.3      # 0.204
  ) = 0.861 = 86.1%
  ```

- **Quality Score для каждой записи:**
  - Рассчитываем индивидуальную оценку каждой строки
  - Учитываем полноту полей И прохождение валидаций
  - Диапазон: 0-100 баллов

- **Функция:** `calculate_accuracy_score(df, validation_columns, validation_functions)`

- **Возвращаемый результат:**

  ```python
  {
    'overall_score': 0.861,
    'record_scores': {
      'mean': 82.5,    # Средняя оценка записи
      'min': 45.0,     # Худшая запись
      'max': 100.0     # Идеальная запись
    }
  }
  ```

- **Лог работы:**
  
  ```text
  Валидация Email: верных - 17, неверных - 4
  Валидация диапазона [0, 120]:   верных - 14, неверных - 7
  Оценка качества: mean=81.3, min=16.7, max=100.0
  Компоненты оценки качества:
    Полнота: 95.83% (вес 40%)
    Уникальность: 95.24% (вес 30%)
    Валидность: 73.81% (вес 30%)
  ОБЩАЯ ОЦЕНКА КАЧЕСТВА: 89.05%
  Оценки качества записей:
    Среднее: 81.3
    Минимум: 16.7
    Максимум: 100.0
  ```

- **Интерпретация шкалы:**
  - 90-100% - Отличное качество
  - 80-89% - Хорошее качество (наш случай)
  - 70-79% - Удовлетворительное качество
  - <70% - Неудовлетворительное качество

**Шаг 6: save_report** - Сохранение отчёта

- **Что делаем:** Создаём полный JSON отчёт со всеми метриками
- **Файл:** `/opt/airflow/data/quality_reports/quality_metrics_YYYY-MM-DD.json`

- **Лог работы:**
  
  ```text
  Рассчитана полнота для 8 колонок
  Отчет по качеству для 'customers'
    Обобщенный показатель: 95.54%
  Отчет сохранен: /opt/airflow/data/quality_reports/quality_metrics_YYYY-MM-DD.json
  Сводка по качеству:
    Датасет: customers
    Всего записей: 21
    Общая оценка качества: 89.05%
  ```

---

#### Результат DAG 04

**Dashboard метрика**

```text
┌─────────────────────────────────────────────┐
│      QUALITY METRICS DASHBOARD              │
├─────────────────────────────────────────────┤
│                                             │
│  Completeness:  ████████████░░  92.86%      │
│  Uniqueness:    █████████████░  95.00%      │
│  Validity:      ███████░░░░░░░  67.50%      │
│                                             │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━    │
│                                             │
│  OVERALL SCORE: ███████████░░  86.10%       │
│                                             │
├─────────────────────────────────────────────┤
│  Recommendation: GOOD QUALITY               │
│  - Fix 7 invalid emails (35%)               │
│  - Fix 6 invalid ages (30%)                 │
│  - Fill 4 missing cities (20%)              │
└─────────────────────────────────────────────┘
```

**Что изучили:**

- Как рассчитывать completeness (полноту)
- Как рассчитывать uniqueness (уникальность)
- Как рассчитывать validity (валидность)
- Как объединять метрики в общий score
- Как рассчитывать quality score для записей
- Как интерпретировать метрики качества
- Как создавать JSON отчёты с метриками

---

### DAG 05: Обработка ошибок (dag_05_error_handling.py)

#### Описание

**Цель:** Стратегии обработки проблемных данных

**Что делаем:** Классифицируем ошибки и применяем разные стратегии обработки

**Файл:** [dag_05_error_handling.py](dags/dag_05_error_handling.py)

#### Граф выполнения

```python
                                   ┌─→ quarantine (неисправимые) ─┐
start → load → classify_errors ────┤                              ├─→ save_results → end
                                   └─→ auto_fix (исправимые) ─────┘
                                        ↓
                                   log_changes (audit)
```

**Операции:**

1. **classify_errors** - классификация ошибок
2. **fix_correctable** - исправление известных ошибок
3. **quarantine_invalid** - карантин для ручной проверки
4. **log_changes** - логирование изменений

**Демонстрирует:**

- Классификация ошибок
- Автоматическое исправление
- Карантин проблемных данных
- Аудит изменений

#### Особенности реализации

**Параметры DAG:**

```python
params = {
    'input_path': '/opt/airflow/data/input/customers_raw.csv',
    'delimiter': ',',
    'encoding': 'utf-8'
}
```

**В задачах рализовано:**

- Классификация ошибок (_fixable/unfixable_)
- Параллельная обработка: _quarantine + audit_
- Возврат статистики из каждой функции
- Отслеживание изменений в _audit log_
- Сохранение 3 файлов (_clean, quarantine, audit_)
- Логи на русском с детальной статистикой

---

#### Подробное описание шагов

**Шаг 1: load_data** - Загрузка проблемных данных

- **Что делаем:** Загружаем customers_raw.csv с известными проблемами
- **Параметры загрузки:**

  ```python
  params = context.get('params', {})
  input_path = params['input_path']
  delimiter = params['delimiter']
  encoding = params['encoding']
  ```

- **Типы проблем в данных:**
  - Email: _"BOB@EXAMPLE.COM"_ (uppercase)
  - Email: *"invalid_email"* (без @)
  - Email: _"charlie@"_ (нет домена)
  - Age: _150_ (выброс)
  - Age: _-5_ (невозможное значение)
  - Age: _NULL_ (пропуск)
  - Phone: _"89161234567"_ (нет кода страны)
  - Phone: _"invalid"_ (невалидный)

- **Входные данные:** CSV файл `/opt/airflow/data/input/customers_raw.csv` или сгенерированные тестовые данные
- **Возвращаемый результат:**

  ```python
  return {
      'status': 'success',
      'input_path': input_path,
      'rows_processed': len(df),
      'output_path': output_path
  }
  ```

- **Лог работы:**

  ```text
  Загружены данные: 21 записей
  Загружено: 21 записей с потенциальными проблемами
  ```

- **Выходные данные:** CSV файл `/opt/airflow/data/temp/problematic_data.csv`

**Шаг 2: classify_errors** - Классификация ошибок

- **Что делаем:** Определяем какие ошибки можем исправить автоматически
- **Классификация:**
  
  **FIXABLE (исправимые):**
  - Email с @: можно стандартизировать (lowercase, trim)
  - Age=NULL: можно заполнить медианой
  - Phone с цифрами: можно очистить и добавить код
  
  **UNFIXABLE (требуют ручной проверки):**
  - Email без @: невозможно восстановить
  - Age выброс (150, -5): неизвестно реальное значение
  - Phone "invalid": нет цифр для восстановления

- **Алгоритм:**

  ```python
  for each row:
      # Email проверка
      if email contains '@':
          error_type = 'email_format'
          is_fixable = True  # Можем почистить
      else:
          error_type = 'email_invalid'
          is_fixable = False  # Не можем восстановить
      
      # Age проверка
      if age is NULL:
          error_type += ',age_missing'
          is_fixable = True  # Можем заполнить
      elif age < 0 or age > 120:
          error_type += ',age_outlier'
          is_fixable = False  # Не знаем правильное
  ```

- **Что получаем:** Каждая запись помечена

  ```csv
  customer_id,email,age,error_type,is_fixable
  1,alice@example.com,28,,True
  2,BOB@EXAMPLE.COM,35,email_format,True
  3,charlie@,NULL,email_invalid,age_missing,False
  4,david@,150,email_invalid,age_outlier,False
  ```

- **Лог работы:**

  ```text
  Валидация Email: верных - 17, неверных - 4
  Валидация диапазона [0, 120]:   верных - 14, неверных - 7
  Классификация ошибок:
    Исправимые: 4
    Требуют ручной проверки: 17
    Типы ошибок:
      age_outlier 4
      email_format,age_missing 3
  ```

- **Файл:** `/opt/airflow/data/temp/classified_errors.csv`

**Шаг 3: auto_fix** - Автоматическое исправление

- **Что делаем:** Исправляем только FIXABLE ошибки
- **Стратегии исправления:**

  **1. Email: стандартизация формата**
  - Проблема: _"BOB@EXAMPLE.COM"_, _"  alice@example.com  "_
  - Исправление: `lowercase + trim`
  - Результат: _"bob@example.com"_, _"alice@example.com"_
  - Функция: `standardize_text(df, 'email', ['lowercase', 'strip'])`
  
  **2. Age: заполнение пропусков**
  - Проблема: `age = NULL`
  - Исправление: Заполнить медианой
  - Медиана: _30.0_ (устойчива к выбросам)
  - Результат: _NULL → 30.0_
  - Код: `df.loc[age_missing, 'age'] = median_age`
  
  **3. Phone: очистка и нормализация**
  - Проблема: _"89161234567"_, _"+7(916)333-44-55"_, _"  +79164445566  "_
  - Исправление: 
    - Удалить всё кроме цифр
    - Добавить +7 если нет
    - Убрать пробелы
  - Результат: _"+79161234567"_
  - Функция: `clean_phone_numbers(df, 'phone', add_country_code='+7')`

- **Отслеживание изменений:**

  ```python
  df['fixes_applied'] = ''
  
  if email_fixed:
      df['fixes_applied'] += 'email_cleaned,'
  
  if age_filled:
      df['fixes_applied'] += f'age_filled({median}),'
  
  if phone_cleaned:
      df['fixes_applied'] += 'phone_cleaned,'
  ```

- **Возвращаемый результат:** Исправленные данные с audit trail

  ```csv
  customer_id,email,age,phone,fixes_applied
  2,bob@example.com,35,+79161234567,"email_cleaned,phone_cleaned"
  3,charlie@example.com,30,+79163334455,"age_filled(30),phone_cleaned"
  ```

- **Лог работы:**

  ```text
  Обработка 4 исправимых записей...
    email: преобразован в lowercase
    email: удалены пробелы
  Исправлено 4 форматов email
  Заполнено 3 пропусков age медианой: 31.5
  phone: удалены символы, кроме цифр
  phone: добавлен код страны +7
  Очищены все номера телефонов
  ```

- **Файл:** `/opt/airflow/data/temp/auto_fixed.csv`

**Шаг 4: quarantine** - Карантин неисправимых данных

- **Что делаем:** Отправляем UNFIXABLE записи на ручную проверку

- **Критерии карантина:**
  - Email без @: не можем восстановить → карантин
  - Age выброс: не знаем правильное → карантин
  - Любая критичная ошибка → карантин

- **Записи в карантине:**

  ```csv
  customer_id,email,age,error_type,reason
  3,charlie@,NULL,email_invalid,Email missing domain
  4,david@,150,email_invalid,age_outlier,Email invalid AND age outlier
  6,frank@,-5,email_invalid,age_outlier,Email invalid AND negative age
  ```

- **Что делаем с карантином:**
  - Сохраняем в отдельный файл
  - Помечаем датой для отслеживания
  - Отправляем уведомление (в продакшене)
  - Требуют ручной проверки data steward'ом

- **Лог работы:**

  ```text
  Сохранено в: /opt/airflow/data/quarantine/invalid_records_YYYY-MM-DD.csv
  ```

- **Файл:** `/opt/airflow/data/quarantine/invalid_records_YYYY-MM-DD-01-4.csv`

**Шаг 5: log_changes** - Аудит всех изменений

- **Что делаем:** Создаём полный лог всех операций для прозрачности

- **Зачем:**
  - Отследить что было изменено
  - Кто/когда сделал изменения
  - Возможность отката (rollback)
  - Соответствие регуляторным требованиям (GDPR, SOX)

- **Статистика изменений:**
  - Fixed: 12 записей
  - Quarantined: 8 записей
  - Unchanged: 0 записей

- **Лог показывает:**
  ```text

  Записей в audit log: 21
  Audit log сохранен: /opt/airflow/data/quality_reports/audit_log_YYYY-MM-DD.json
  Сводка аудита:
    Исправлено: 21
    В карантине: 0
  ```

- **Файл:** `/opt/airflow/data/quality_reports/audit_log_YYYY-MM-DD.json`

**Шаг 6: save_results** - Сохранение очищенных данных

- **Что делаем:** Сохраняем только исправленные (fixable) записи
- **Фильтр:** `df[df['is_fixable'] == True]`
- **Удаляем служебные колонки:**
  - error_type
  - is_fixable
  - fixes_applied

- **Что получаем:** Чистые данные готовые к использованию

  ```csv
  customer_id,email,full_name,age,city,gender,phone
  1,alice@example.com,Alice Smith,28,Moscow,female,+79151234567
  2,bob@example.com,Bob Johnson,35,SPB,male,+79161234567
  5,emma@example.com,Emma Davis,29,Unknown,female,+79167654321
  ...
  ```

- **Лог работы:**

  ```text
  Чистые данные сохранены: 4 записей
  Файл: /opt/airflow/data/output/customers_error_handled.csv  
  ```

---

#### Результат DAG 05:

**Workflow обработки ошибок:**

```text
20 записей с проблемами
        ↓
  [Классификация]
        ↓
   ┌────┴────┐
   ↓         ↓
Fixable   Unfixable
 (12)       (8)
   ↓         ↓
[Auto-    [Quarantine]
 Fix]         ↓
   ↓      invalid_records_
   ↓      2025-01-24.csv
   ↓
12 чистых записей
   ↓
customers_error_handled.csv
```

**Файлы результатов:**

1. **customers_error_handled.csv** - 12 исправленных записей
2. **invalid_records_2025-01-24.csv** - 8 записей в карантине
3. **audit_log_2025-01-24.json** - полный лог изменений

**Статистика обработки:**

```
┌──────────────────────────────────────────┐
│     ERROR HANDLING STATISTICS            │
├──────────────────────────────────────────┤
│  Total records:        20                │
│                                          │
│  Fixable:              12  (60%)         │
│    - Email cleaned:     5                │
│    - Age filled:        5                │
│    - Phone cleaned:    12                │
│                                          │
│  Unfixable:             8  (40%)         │
│    - Invalid email:     4                │
│    - Age outliers:      4                │
│                                          │
│  Status:                                 │
│    Fixed:              12                │
│    Quarantined:        8                 │
└──────────────────────────────────────────┘
```

**Что изучили:**

- Как классифицировать ошибки (fixable/unfixable)
- Как автоматически исправлять известные проблемы
- Как отправлять данные в карантин
- Как вести audit log изменений
- Как применять разные стратегии к разным типам ошибок
- Как обеспечить прозрачность обработки данных
- Как сохранять clean data отдельно от проблемных

---

## Ход выполнения

### Блок 1: Очистка данных

1. **Теория:**
   - Типы проблем качества
   - Стратегии обработки

2. **Практика:**
   - Запуск DAG 01
   - Изучение кода очистки
   - Просмотр результатов

3. **Упражнение:**
   - Добавить обработку ещё одной проблемы

---

### Блок 2: Трансформация

1. **Теория:**
   - Типы трансформаций
   - Нормализация и обогащение

2. **Практика:**
   - Запуск DAG 02
   - Изучение трансформаций
   - Просмотр результатов

3. **Упражнение:**
   - Создать новое вычисляемое поле

---

### Блок 3: Валидация

1. **Теория:**
   - Уровни валидации
   - Правила валидации

2. **Практика:**
   - Запуск DAG 03
   - Изучение валидаторов
   - Анализ отчёта

3. **Упражнение:**
   - Добавить новое правило валидации

---

### Блок 4: Метрики качества

1. **Теория:**
   - Измерения качества
   - Метрики и KPI

2. **Практика:**
   - Запуск DAG 04
   - Расчёт метрик
   - Анализ отчёта

3. **Упражнение:**
   - Рассчитать новую метрику

---

### Блок 5: Обработка ошибок

1. **Теория:**
   - Стратегии обработки
   - Карантин и аудит

2. **Практика:**
   - Запуск DAG 05
   - Изучение стратегий
   - Просмотр логов

3. **Упражнение:**
   - Добавить новую стратегию

---

## Домашнее задание

### Задание: Комплексная обработка данных о продажах

#### Описание

Вам предоставлены данные о продажах магазина за месяц. Данные содержат ошибки, пропуски и требуют очистки, трансформации и валидации.

#### Входные данные

**Файл - "Продажи":** [sales_raw.csv](data/input/sales_raw.csv)

**Проблемы:**

- Дубликаты по `transaction_id`
- Пропуски в `customer_name`, `price`, `sale_date`, `payment_method`, `email`
- Невалидные `email` (_без @, без домена_)
- Отрицательные/нулевые `price`
- Некорректные даты (`sale_date` в будущем)
- Несогласованность в `product_name` (_UPPERCASE/lowercase/mixed_)
- Пробелы в начале/конце строк

**Пример данных:**

```bash
transaction_id,customer_name,product_name,quantity,price,sale_date,payment_method,email
T001,John Smith,laptop pro,1,1299.99,2025-01-01,credit_card,john@example.com
T002,Jane Doe,WIRELESS MOUSE,2,29.99,2025-01-02,cash,jane@example.com
T002,Jane Doe,WIRELESS MOUSE,2,29.99,2025-01-02,cash,jane@example.com  # Дубликат
T003,,Coffee Maker,1,89.99,2025-01-03,debit_card,alice@  # Пропуск name, невалидный email
T004,Bob Wilson,headphones,3,-15.00,2025-01-04,credit_card,bob@example.com  # Отрицательный price
T005,Alice Johnson,Standing Desk,1,449.99,,paypal,alice.j@example.com  # Пропуск date
T006,Charlie Brown,usb cable,5,9.99,2025-01-06,cash,  # Пропуск email
T007,  Diana Prince  ,Notebook,10,5.99,2025-01-07,credit_card,diana@example.com  # Пробелы
T008,Eve Anderson,water bottle,2,12.99,2025-01-08,debit_card,eve@  # Невалидный email
T011,,Desk Organizer,2,24.99,2025-01-12,cash,guest@example.com  # Пропуск name
T013,Isabel Garcia,BACKPACK,1,79.99,2026-02-15,credit_card,isabel@example.com  # Дата в будущем
T014,Jack Taylor,webcam,2,149.99,2025-01-14,debit_card,bad_email  # Невалидный email
T016,Leo Martinez,wireless mouse,4,29.99,2025-01-16,,leo@example.com  # Пропуск payment_method
T019,Oscar Chen,standing desk,1,-449.99,2025-01-19,credit_card,oscar@example.com  # Отрицательный price
```

**Статистика проблем:**

- Всего записей: **~20**
- Дубликаты: **1 (5%)**
- Пропуски в customer_name: **2 (10%)**
- Пропуски в sale_date: **1 (5%)**
- Пропуски в price: **0 (0%)**
- Пропуски в payment_method: **1 (5%)**
- Пропуски в email: **1 (5%)**
- Невалидные email: **4 (20%)**
- Отрицательные/нулевые price: **3 (15%)**
- Даты в будущем: **1 (5%)**
- Несогласованность product_name: **15 (75%)**
- Пробелы в полях: **2 (10%)**

#### Задачи (10 баллов)

1. **Очистка (3 балла):**
   - Удалить дубликаты по `transaction_id`
   - Заполнить пропуски в `customer_name` значением _"Guest"_
   - Удалить записи с отрицательными или нулевыми `price`
   - Исправить форматы дат (или удалить некорректные)

2. **Трансформация (3 балла):**
   - Привести `product_name` к нижнему регистру
   - Создать поле `total = price * quantity`
   - Создать поле `sale_month` из `sale_date`
   - Категоризировать `total` (_small: <100, medium: 100-500, large: 500-1000, xlarge: >1000_)

3. **Валидация (2 балла):**
   - Проверить, что `price > 0`
   - Проверить формат `email` (если оно заполнено)
   - Проверить, что `sale_date` не в будущем
   - Создать отчёт валидации в формате JSON

4. **Метрики качества (2 балла):**
   - Рассчитать `completeness` для всех полей
   - Рассчитать `uniqueness` по `transaction_id`
   - Рассчитать общий `quality_score`
   - Сохранить отчёт в JSON

5. **Bonus задания (+3 балла):**

- Создать HTML дашборд с метриками (+1 балл)
- Реализовать карантин для подозрительных записей (+1 балл)
- Добавить логирование всех изменений (+1 балл)

**Ожидаемый результат:**

- `sales_clean.csv`: ~17-18 записей (после удаления дубликатов и некорректных)
- Все текстовые поля нормализованы
- Все пропуски обработаны
- `Quality_score >= 85%`

**Критерии оценки:**

- Код работает: 5 баллов
- Все проблемы обработаны: 3 балла
- Отчёты созданы: 2 балла
- Бонусы: до 3 баллов

**Сдача:**

- Файл `dag_homework.py`
- Отчёт в формате `homework_report.md`
- Скриншоты результатов

---

## Чек-лист занятия

**До занятия:**

- `[ ]` Docker Compose запущен
- `[ ]` Airflow UI доступен
- `[ ]` Тестовые данные загружены
- `[ ]` Все DAG видны в UI

**Во время занятия:**

- `[ ]` DAG 01 запущен и успешен
- `[ ]` DAG 02 запущен и успешен
- `[ ]` DAG 03 запущен и успешен
- `[ ]` DAG 04 запущен и успешен
- `[ ]` DAG 05 запущен и успешен
- `[ ]` Отчёты созданы
- `[ ]` Упражнения выполнены

**После занятия:**

- `[ ]` Домашнее задание получено
- `[ ]` Материалы сохранены
- `[ ]` Вопросы заданы

---

## Дополнительные материалы

**Рекомендуемое чтение:**

- Pandas Documentation: Data Cleaning
- Great Expectations Documentation
- Apache Airflow Best Practices

**Полезные библиотеки:**

- `pandas` - основной инструмент
- `great_expectations` - валидация данных
- `pandera` - schema validation
- `dataprep` - EDA и очистка

**Следующая тема:**

Тема 5: Load - Загрузка данных в целевые системы

---
