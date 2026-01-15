-- ===========================================
-- CREATE ANALYTICS DATABASE TABLES
-- ===========================================

-- Таблица ежедневной бизнес-аналитики
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
    top_product VARCHAR(255),
    
    -- Веб-метрики
    total_page_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Доставка
    total_deliveries INTEGER DEFAULT 0,
    avg_delivery_time_minutes INTEGER DEFAULT 0,
    successful_delivery_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_analytics_date ON daily_business_analytics(analytics_date DESC);

-- Таблица для хранения сырых данных из различных источников
CREATE TABLE IF NOT EXISTS raw_data_staging (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    extraction_date TIMESTAMP NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для staging
CREATE INDEX IF NOT EXISTS idx_staging_source ON raw_data_staging(source_name);
CREATE INDEX IF NOT EXISTS idx_staging_date ON raw_data_staging(extraction_date);
CREATE INDEX IF NOT EXISTS idx_staging_data ON raw_data_staging USING GIN (data);

-- Таблица логов качества данных
CREATE TABLE IF NOT EXISTS data_quality_logs (
    id SERIAL PRIMARY KEY,
    check_date TIMESTAMP NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    source_table VARCHAR(100),
    status VARCHAR(20) CHECK (status IN ('passed', 'failed', 'warning')),
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для data quality
CREATE INDEX IF NOT EXISTS idx_dq_date ON data_quality_logs(check_date DESC);
CREATE INDEX IF NOT EXISTS idx_dq_status ON data_quality_logs(status);

-- Таблица логов ETL процесса
CREATE TABLE IF NOT EXISTS etl_process_logs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для ETL logs
CREATE INDEX IF NOT EXISTS idx_etl_dag ON etl_process_logs(dag_id);
CREATE INDEX IF NOT EXISTS idx_etl_date ON etl_process_logs(execution_date DESC);
CREATE INDEX IF NOT EXISTS idx_etl_status ON etl_process_logs(status);

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггер для daily_business_analytics
CREATE TRIGGER update_analytics_updated_at BEFORE UPDATE ON daily_business_analytics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Представление для быстрого доступа к последним метрикам
CREATE OR REPLACE VIEW latest_analytics AS
SELECT * FROM daily_business_analytics
ORDER BY analytics_date DESC
LIMIT 30;

-- Представление для агрегации логов ETL
CREATE OR REPLACE VIEW etl_summary AS
SELECT 
    dag_id,
    task_id,
    DATE(execution_date) as execution_day,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    AVG(duration_seconds) as avg_duration_seconds,
    SUM(records_processed) as total_records_processed
FROM etl_process_logs
GROUP BY dag_id, task_id, DATE(execution_date)
ORDER BY execution_day DESC;

-- Комментарии
COMMENT ON TABLE daily_business_analytics IS 'Ежедневные агрегированные метрики бизнес-процессов';
COMMENT ON TABLE raw_data_staging IS 'Staging таблица для сырых данных из всех источников';
COMMENT ON TABLE data_quality_logs IS 'Логи проверок качества данных';
COMMENT ON TABLE etl_process_logs IS 'Логи выполнения ETL процессов';
