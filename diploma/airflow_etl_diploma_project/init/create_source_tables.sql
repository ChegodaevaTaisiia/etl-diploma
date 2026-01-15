-- ===========================================
-- CREATE SOURCE DATABASE TABLES
-- ===========================================

-- Таблица клиентов
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100) DEFAULT 'Russia',
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для customers
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_active ON customers(is_active);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    payment_method VARCHAR(50) CHECK (payment_method IN ('card', 'cash', 'online', 'bank_transfer')),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для orders
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Таблица продуктов
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для products
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_active ON products(is_active);

-- Таблица элементов заказа
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price DECIMAL(10, 2) NOT NULL CHECK (total_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для order_items
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггеры для автоматического обновления updated_at
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Заполнение тестовыми данными (опционально)
-- Клиенты
INSERT INTO customers (first_name, last_name, email, phone, city, country) VALUES
('Иван', 'Иванов', 'ivan.ivanov@example.com', '+79001234567', 'Москва', 'Russia'),
('Петр', 'Петров', 'petr.petrov@example.com', '+79001234568', 'Санкт-Петербург', 'Russia'),
('Мария', 'Сидорова', 'maria.sidorova@example.com', '+79001234569', 'Новосибирск', 'Russia'),
('Анна', 'Смирнова', 'anna.smirnova@example.com', '+79001234570', 'Екатеринбург', 'Russia'),
('Дмитрий', 'Козлов', 'dmitry.kozlov@example.com', '+79001234571', 'Казань', 'Russia')
ON CONFLICT (email) DO NOTHING;

-- Продукты
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
('Ноутбук Dell XPS 13', 'Electronics', 89999.99, 50, 'Мощный ультрабук для работы'),
('iPhone 15 Pro', 'Electronics', 119999.99, 120, 'Флагманский смартфон Apple'),
('Книга "Чистый код"', 'Books', 1299.99, 200, 'Классика программирования'),
('Футболка хлопковая', 'Clothing', 899.99, 500, 'Качественная хлопковая футболка'),
('Кофеварка Philips', 'Home', 12999.99, 75, 'Автоматическая кофеварка')
ON CONFLICT DO NOTHING;

-- Комментарий
COMMENT ON TABLE customers IS 'Таблица клиентов интернет-магазина';
COMMENT ON TABLE orders IS 'Таблица заказов';
COMMENT ON TABLE products IS 'Таблица продуктов';
COMMENT ON TABLE order_items IS 'Таблица элементов заказа (детали заказа)';
