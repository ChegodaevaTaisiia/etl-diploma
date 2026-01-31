# Grafana: дашборд аналитики

## Настройка источника данных

1. Откройте Grafana: http://localhost:3000 (admin / admin).
2. **Configuration → Data sources → Add data source → PostgreSQL**.
3. Укажите:
   - **Host**: `postgres-analytics:5432` (из docker-network) или `host.docker.internal:5434` (если Grafana на хосте).
   - **Database**: `analytics_db`
   - **User / Password**: из `.env` (`POSTGRES_ANALYTICS_USER`, `POSTGRES_ANALYTICS_PASSWORD`).
4. **Save & Test**.

## Панели для дашборда

### 1. Обзор продаж (daily_business_analytics)

- **Total Revenue (30 дней)**:
```sql
SELECT SUM(total_revenue) AS total_revenue
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days';
```

- **Total Orders (30 дней)**:
```sql
SELECT SUM(total_orders) AS total_orders
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days';
```

- **Тренд выручки по дням**:
```sql
SELECT analytics_date AS time, total_revenue, total_orders, avg_order_value
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days'
ORDER BY analytics_date;
```

### 2. География и клиенты (DWH)

- **Топ-10 городов по выручке**:
```sql
SELECT c.city, COUNT(DISTINCT f.order_id) AS orders, SUM(f.total_amount) AS revenue
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.city
ORDER BY revenue DESC
LIMIT 10;
```

- **Сегменты клиентов**:
```sql
SELECT c.customer_segment, COUNT(DISTINCT c.customer_id) AS customers, SUM(f.total_amount) AS revenue
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment;
```

### 3. Качество сервиса (рейтинг из daily_business_analytics)

- **Средний рейтинг по дням**:
```sql
SELECT analytics_date AS time, avg_customer_rating
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days'
ORDER BY analytics_date;
```

## Импорт дашборда

В папке `dashboards/` лежит пример JSON дашборда. В Grafana: **Dashboards → Import → Upload JSON file** и выберите файл.
