-- ===========================================
-- CREATE DATA WAREHOUSE SCHEMA (STAR SCHEMA)
-- WITH SCD TYPE 2 IMPLEMENTATION
-- ===========================================

-- ===========================================
-- DIMENSION TABLES (with SCD Type 2)
-- ===========================================

-- Измерение: Клиенты (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,           -- Surrogate key
    customer_id INTEGER NOT NULL,              -- Natural key (ID из источника)
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),              -- 'VIP', 'Regular', 'New'
    
    -- Поля для SCD Type 2
    effective_date DATE NOT NULL,              -- Дата начала действия версии
    expiration_date DATE DEFAULT '9999-12-31', -- Дата окончания (9999-12-31 = активна)
    is_current BOOLEAN DEFAULT TRUE,           -- TRUE = текущая версия
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для эффективного поиска текущих версий
CREATE INDEX IF NOT EXISTS idx_dim_customers_natural_key ON dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_current ON dim_customers(customer_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_customers_dates ON dim_customers(effective_date, expiration_date);
CREATE INDEX IF NOT EXISTS idx_dim_customers_city ON dim_customers(city) WHERE is_current = TRUE;

-- Измерение: Продукты (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,            -- Surrogate key
    product_id INTEGER NOT NULL,               -- Natural key
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    description TEXT,
    
    -- Поля для SCD Type 2
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для products
CREATE INDEX IF NOT EXISTS idx_dim_products_natural_key ON dim_products(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_current ON dim_products(product_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_products_dates ON dim_products(effective_date, expiration_date);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category) WHERE is_current = TRUE;

-- Измерение: Дата
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,              -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,                       -- 1-7
    day_name VARCHAR(10),                      -- 'Monday', 'Tuesday', etc.
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

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- Измерение: Время
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INTEGER PRIMARY KEY,              -- Format: HHMMSS
    hour INTEGER,                              -- 0-23
    minute INTEGER,                            -- 0-59
    second INTEGER,                            -- 0-59
    time_of_day VARCHAR(20),                   -- 'Morning', 'Afternoon', 'Evening', 'Night'
    is_business_hours BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_dim_time_hour ON dim_time(hour);

-- ===========================================
-- FACT TABLES
-- ===========================================

-- Факты: Заказы
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Метрики
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    net_amount DECIMAL(12, 2) NOT NULL,
    
    -- Атрибуты
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    
    -- Метаданные
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для fact_orders
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders(order_id);

-- Факты: Отзывы клиентов
CREATE TABLE IF NOT EXISTS fact_feedback (
    fact_id BIGSERIAL PRIMARY KEY,
    feedback_id VARCHAR(100) NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    order_id INTEGER,
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Метрики
    rating DECIMAL(3, 2),
    
    -- Атрибуты
    feedback_category VARCHAR(50),
    sentiment VARCHAR(20),                     -- 'positive', 'neutral', 'negative'
    
    -- Метаданные
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для fact_feedback
CREATE INDEX IF NOT EXISTS idx_fact_feedback_customer ON fact_feedback(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_feedback_date ON fact_feedback(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_feedback_rating ON fact_feedback(rating);

-- Факты: Доставка
CREATE TABLE IF NOT EXISTS fact_deliveries (
    fact_id BIGSERIAL PRIMARY KEY,
    delivery_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Метрики
    delivery_time_minutes INTEGER,
    distance_km DECIMAL(10, 2),
    
    -- Атрибуты
    courier_id INTEGER,
    delivery_status VARCHAR(50),
    
    -- Метаданные
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для fact_deliveries
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_order ON fact_deliveries(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_date ON fact_deliveries(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_deliveries_courier ON fact_deliveries(courier_id);

-- ===========================================
-- AGGREGATE TABLES (для ускорения запросов)
-- ===========================================

-- Агрегаты: Продажи по дням и категориям
CREATE TABLE IF NOT EXISTS agg_sales_daily_category (
    date_key INTEGER REFERENCES dim_date(date_key),
    category VARCHAR(100),
    total_orders INTEGER,
    total_revenue DECIMAL(12, 2),
    total_items_sold INTEGER,
    avg_order_value DECIMAL(10, 2),
    unique_customers INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, category)
);

CREATE INDEX IF NOT EXISTS idx_agg_sales_daily_category ON agg_sales_daily_category(date_key DESC);

-- ===========================================
-- HELPER FUNCTIONS FOR SCD TYPE 2
-- ===========================================

-- Функция для получения текущей версии клиента
CREATE OR REPLACE FUNCTION get_current_customer_key(p_customer_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (
        SELECT customer_key
        FROM dim_customers
        WHERE customer_id = p_customer_id
          AND is_current = TRUE
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql;

-- Функция для получения текущей версии продукта
CREATE OR REPLACE FUNCTION get_current_product_key(p_product_id INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN (
        SELECT product_key
        FROM dim_products
        WHERE product_id = p_product_id
          AND is_current = TRUE
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql;

-- Функция для закрытия старой версии (SCD Type 2)
CREATE OR REPLACE FUNCTION close_old_version(
    p_table_name TEXT,
    p_key_column TEXT,
    p_key_value INTEGER,
    p_expiration_date DATE
)
RETURNS VOID AS $$
BEGIN
    EXECUTE format('
        UPDATE %I
        SET expiration_date = $1,
            is_current = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE %I = $2
          AND is_current = TRUE
    ', p_table_name, p_key_column)
    USING p_expiration_date, p_key_value;
END;
$$ LANGUAGE plpgsql;

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического обновления updated_at
CREATE TRIGGER update_dim_customers_updated_at BEFORE UPDATE ON dim_customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_dim_products_updated_at BEFORE UPDATE ON dim_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===========================================
-- VIEWS FOR ANALYTICS
-- ===========================================

-- Представление: Текущие клиенты
CREATE OR REPLACE VIEW v_current_customers AS
SELECT 
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    customer_segment
FROM dim_customers
WHERE is_current = TRUE;

-- Представление: Текущие продукты
CREATE OR REPLACE VIEW v_current_products AS
SELECT 
    product_key,
    product_id,
    product_name,
    category,
    price
FROM dim_products
WHERE is_current = TRUE;

-- Представление: Продажи за последние 30 дней
CREATE OR REPLACE VIEW v_sales_last_30_days AS
SELECT 
    d.full_date,
    c.city,
    p.category,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.net_amount) as total_revenue,
    SUM(f.quantity) as total_items_sold
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date, c.city, p.category;

-- ===========================================
-- POPULATE DIM_DATE AND DIM_TIME
-- ===========================================

-- Заполнение таблицы dim_date (2020-2030)
INSERT INTO dim_date (
    date_key, full_date, day_of_week, day_name, day_of_month,
    day_of_year, week_of_year, month, month_name, quarter,
    year, is_weekend, is_holiday, fiscal_year, fiscal_quarter
)
SELECT 
    TO_CHAR(date_val, 'YYYYMMDD')::INTEGER as date_key,
    date_val as full_date,
    EXTRACT(ISODOW FROM date_val)::INTEGER as day_of_week,
    TO_CHAR(date_val, 'Day') as day_name,
    EXTRACT(DAY FROM date_val)::INTEGER as day_of_month,
    EXTRACT(DOY FROM date_val)::INTEGER as day_of_year,
    EXTRACT(WEEK FROM date_val)::INTEGER as week_of_year,
    EXTRACT(MONTH FROM date_val)::INTEGER as month,
    TO_CHAR(date_val, 'Month') as month_name,
    EXTRACT(QUARTER FROM date_val)::INTEGER as quarter,
    EXTRACT(YEAR FROM date_val)::INTEGER as year,
    CASE WHEN EXTRACT(ISODOW FROM date_val) IN (6, 7) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday,
    EXTRACT(YEAR FROM date_val)::INTEGER as fiscal_year,
    EXTRACT(QUARTER FROM date_val)::INTEGER as fiscal_quarter
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) as date_val
ON CONFLICT (date_key) DO NOTHING;

-- Заполнение таблицы dim_time
INSERT INTO dim_time (time_key, hour, minute, second, time_of_day, is_business_hours)
SELECT 
    hour * 10000 + minute * 100 + second as time_key,
    hour,
    minute,
    second,
    CASE 
        WHEN hour >= 6 AND hour < 12 THEN 'Morning'
        WHEN hour >= 12 AND hour < 18 THEN 'Afternoon'
        WHEN hour >= 18 AND hour < 22 THEN 'Evening'
        ELSE 'Night'
    END as time_of_day,
    CASE WHEN hour >= 9 AND hour < 18 THEN TRUE ELSE FALSE END as is_business_hours
FROM generate_series(0, 23) as hour
CROSS JOIN generate_series(0, 59) as minute
CROSS JOIN generate_series(0, 59) as second
ON CONFLICT (time_key) DO NOTHING;

-- Комментарии
COMMENT ON TABLE dim_customers IS 'Измерение клиентов с поддержкой SCD Type 2';
COMMENT ON TABLE dim_products IS 'Измерение продуктов с поддержкой SCD Type 2';
COMMENT ON TABLE dim_date IS 'Измерение даты';
COMMENT ON TABLE dim_time IS 'Измерение времени';
COMMENT ON TABLE fact_orders IS 'Факты заказов';
COMMENT ON TABLE fact_feedback IS 'Факты отзывов клиентов';
COMMENT ON TABLE fact_deliveries IS 'Факты доставок';
