-- ============================================
-- ИНИЦИАЛИЗАЦИЯ DATA WAREHOUSE (схема "Звезда")
-- с поддержкой SCD Type 2
-- ============================================

-- ============================================
-- DIMENSION: Customers (SCD Type 2)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,           -- Surrogate key
    customer_id INTEGER NOT NULL,              -- Natural key (ID из источника)
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),              -- VIP, Regular, New
    
    -- SCD Type 2 поля
    effective_date DATE NOT NULL,              -- Дата начала действия версии
    expiration_date DATE DEFAULT '9999-12-31', -- Дата окончания (9999-12-31 = активна)
    is_current BOOLEAN DEFAULT TRUE,           -- TRUE = текущая версия
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для эффективного поиска
CREATE INDEX IF NOT EXISTS idx_dim_customers_current ON dim_customers(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_customers_dates ON dim_customers(effective_date, expiration_date);
CREATE INDEX IF NOT EXISTS idx_dim_customers_natural_key ON dim_customers(customer_id);

-- ============================================
-- DIMENSION: Products (SCD Type 2)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    
    -- SCD Type 2 поля
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_products_current ON dim_products(product_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_products_dates ON dim_products(effective_date, expiration_date);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);

-- ============================================
-- DIMENSION: Date (Статическое измерение)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- ============================================
-- DIMENSION: Time (Статическое измерение)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INTEGER PRIMARY KEY,
    hour INTEGER,
    minute INTEGER,
    time_of_day VARCHAR(20),        -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN
);

-- ============================================
-- FACT TABLE: Orders
-- ============================================
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    
    -- Ссылки на измерения (SCD Type 2)
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Метрики (аддитивные)
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    total_amount DECIMAL(12, 2),
    
    -- Дегенерированные измерения
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    
    -- Служебные поля
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders(order_id);

-- ============================================
-- FACT TABLE: Feedback
-- ============================================
CREATE TABLE IF NOT EXISTS fact_feedback (
    fact_id BIGSERIAL PRIMARY KEY,
    feedback_id VARCHAR(50) NOT NULL,
    
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    order_id INTEGER,
    date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Метрики
    rating DECIMAL(3, 2),
    sentiment_score DECIMAL(5, 4),
    
    -- Атрибуты
    feedback_category VARCHAR(50),
    comment_length INTEGER,
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_feedback_customer ON fact_feedback(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_feedback_date ON fact_feedback(date_key);

-- ============================================
-- Функция для генерации измерения Date
-- ============================================
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month,
            month_name,
            quarter,
            year,
            is_weekend,
            is_holiday,
            fiscal_year,
            fiscal_quarter
        ) VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(DOW FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DAY FROM current_date)::INTEGER,
            EXTRACT(DOY FROM current_date)::INTEGER,
            EXTRACT(WEEK FROM current_date)::INTEGER,
            EXTRACT(MONTH FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Month'),
            EXTRACT(QUARTER FROM current_date)::INTEGER,
            EXTRACT(YEAR FROM current_date)::INTEGER,
            EXTRACT(DOW FROM current_date)::INTEGER IN (0, 6),
            FALSE, -- placeholder for holidays
            EXTRACT(YEAR FROM current_date)::INTEGER,
            EXTRACT(QUARTER FROM current_date)::INTEGER
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Заполнение dim_date на 5 лет вперед
SELECT populate_dim_date('2020-01-01'::DATE, '2030-12-31'::DATE);

-- ============================================
-- Функция для генерации измерения Time
-- ============================================
CREATE OR REPLACE FUNCTION populate_dim_time()
RETURNS VOID AS $$
DECLARE
    h INTEGER;
    m INTEGER;
BEGIN
    FOR h IN 0..23 LOOP
        FOR m IN 0..59 LOOP
            INSERT INTO dim_time (
                time_key,
                hour,
                minute,
                time_of_day,
                is_business_hours
            ) VALUES (
                h * 100 + m,
                h,
                m,
                CASE 
                    WHEN h BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
                    WHEN h BETWEEN 18 AND 22 THEN 'Evening'
                    ELSE 'Night'
                END,
                h BETWEEN 9 AND 18
            )
            ON CONFLICT (time_key) DO NOTHING;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Заполнение dim_time
SELECT populate_dim_time();

COMMENT ON TABLE dim_customers IS 'Измерение: Клиенты (SCD Type 2)';
COMMENT ON TABLE dim_products IS 'Измерение: Продукты (SCD Type 2)';
COMMENT ON TABLE dim_date IS 'Измерение: Дата (статическое)';
COMMENT ON TABLE dim_time IS 'Измерение: Время (статическое)';
COMMENT ON TABLE fact_orders IS 'Факты: Заказы';
COMMENT ON TABLE fact_feedback IS 'Факты: Обратная связь';
