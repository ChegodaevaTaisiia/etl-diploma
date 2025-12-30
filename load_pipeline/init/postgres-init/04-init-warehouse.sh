#!/bin/bash

set -e
set -u

# Заполняем БД WAREHOUSE_POSTGRES_DATABASE
if [ -n "$WAREHOUSE_POSTGRES_DATABASE" ]; then
    echo "Creating tables in ${WAREHOUSE_POSTGRES_DATABASE}"
    psql -v ON_ERROR_STOP=1 --username "$WAREHOUSE_POSTGRES_USER" --dbname "$WAREHOUSE_POSTGRES_DATABASE" >/dev/null <<-EOSQL

        \c $WAREHOUSE_POSTGRES_DATABASE;

        -- Создание схемы для staging данных
        CREATE SCHEMA IF NOT EXISTS staging;

        -- Создание схемы для dimension таблиц
        CREATE SCHEMA IF NOT EXISTS dim;

        -- Создание схемы для fact таблиц
        CREATE SCHEMA IF NOT EXISTS fact;

        -- Создание таблицы customers с SCD Type 2
        CREATE TABLE dim.customers (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            customer_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            country VARCHAR(100),
            valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Создание индексов для оптимизации
        CREATE INDEX idx_customers_customer_id ON dim.customers(customer_id);
        CREATE INDEX idx_customers_is_current ON dim.customers(is_current);
        CREATE INDEX idx_customers_valid_from ON dim.customers(valid_from);
        CREATE INDEX idx_customers_valid_to ON dim.customers(valid_to);

        -- Создание таблицы staging для загрузки данных
        CREATE TABLE staging.customers_staging (
            customer_id INTEGER NOT NULL,
            customer_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            country VARCHAR(100),
            load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Создание партиционированной таблицы orders (fact)
        CREATE TABLE fact.orders (
            order_id BIGSERIAL,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            order_amount DECIMAL(12,2) NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            status VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, order_date)
        ) PARTITION BY RANGE (order_date);

        -- Создание партиций по месяцам для 2024 года
        CREATE TABLE fact.orders_2024_01 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

        CREATE TABLE fact.orders_2024_02 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

        CREATE TABLE fact.orders_2024_03 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

        CREATE TABLE fact.orders_2024_04 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

        CREATE TABLE fact.orders_2024_05 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');

        CREATE TABLE fact.orders_2024_06 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');

        CREATE TABLE fact.orders_2024_07 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

        CREATE TABLE fact.orders_2024_08 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

        CREATE TABLE fact.orders_2024_09 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

        CREATE TABLE fact.orders_2024_10 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

        CREATE TABLE fact.orders_2024_11 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

        CREATE TABLE fact.orders_2024_12 PARTITION OF fact.orders
            FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

        -- Создание индексов на партиционированной таблице
        CREATE INDEX idx_orders_customer_id ON fact.orders(customer_id);
        CREATE INDEX idx_orders_order_date ON fact.orders(order_date);
        CREATE INDEX idx_orders_status ON fact.orders(status);

        -- Создание таблицы для логирования производительности
        CREATE TABLE IF NOT EXISTS public.load_performance_log (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            records_loaded INTEGER NOT NULL,
            load_duration_seconds NUMERIC NOT NULL,
            records_per_second NUMERIC NOT NULL,
            load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    
EOSQL

fi