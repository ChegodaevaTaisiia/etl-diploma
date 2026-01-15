-- ============================================
-- ИНИЦИАЛИЗАЦИЯ АНАЛИТИЧЕСКОЙ БД
-- ============================================

-- Таблица ежедневной аналитики
CREATE TABLE IF NOT EXISTS daily_business_analytics (
    id SERIAL PRIMARY KEY,
    analytics_date DATE NOT NULL UNIQUE,
    
    -- Метрики заказов
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    
    -- Статусы заказов
    orders_pending INTEGER DEFAULT 0,
    orders_processing INTEGER DEFAULT 0,
    orders_shipped INTEGER DEFAULT 0,
    orders_delivered INTEGER DEFAULT 0,
    orders_cancelled INTEGER DEFAULT 0,
    
    -- Клиенты
    unique_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    avg_customer_rating DECIMAL(3, 2) DEFAULT 0,
    
    -- Продукты
    total_products_sold INTEGER DEFAULT 0,
    top_category VARCHAR(100),
    
    -- Веб-метрики
    total_page_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Доставка
    total_deliveries INTEGER DEFAULT 0,
    avg_delivery_time_minutes INTEGER DEFAULT 0,
    successful_delivery_rate DECIMAL(5, 4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс по дате
CREATE INDEX IF NOT EXISTS idx_daily_analytics_date ON daily_business_analytics(analytics_date);

-- Таблица для хранения метрик качества данных
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP NOT NULL,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    
    -- Метрики качества
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    duplicate_records INTEGER,
    null_values_count INTEGER,
    
    -- Процентные показатели
    completeness_rate DECIMAL(5, 4),
    validity_rate DECIMAL(5, 4),
    uniqueness_rate DECIMAL(5, 4),
    
    -- Детали
    error_details JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_run_date ON data_quality_metrics(run_date);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_source ON data_quality_metrics(source_name);

COMMENT ON TABLE daily_business_analytics IS 'Ежедневная бизнес-аналитика';
COMMENT ON TABLE data_quality_metrics IS 'Метрики качества данных';
