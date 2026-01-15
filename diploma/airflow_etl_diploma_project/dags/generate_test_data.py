"""
DAG для генерации тестовых данных.

Генерирует реалистичные данные для проверки работы ETL pipeline:
1. Заполнение PostgreSQL (orders, customers, order_items)
2. Заполнение MongoDB (customer_feedback)
3. Создание CSV файлов (products, delivery_logs)

Запуск: вручную через UI или ежедневно в 8:00 (перед основным ETL в 9:00)
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import random
import logging
from faker import Faker


# Инициализация Faker для генерации реалистичных данных
fake = Faker(['ru_RU', 'en_US'])
Faker.seed(42)  # Для воспроизводимости результатов


default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email': ['student@example.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@dag(
    dag_id='generate_test_data',
    default_args=default_args,
    description='Генерация тестовых данных для ETL pipeline',
    schedule='0 8 * * *',  # Ежедневно в 8:00 (за час до основного ETL)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test-data', 'generator', 'utility']
)
def generate_test_data_dag():
    """
    DAG для генерации тестовых данных во всех источниках.
    """
    
    @task()
    def generate_customers() -> dict:
        """
        Генерация клиентов в PostgreSQL.
        
        Создает 20-50 новых клиентов с реалистичными данными.
        """
        logger = logging.getLogger(__name__)
        
        try:
            hook = PostgresHook(postgres_conn_id='postgres_source')
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Генерация 20-50 клиентов
            num_customers = random.randint(20, 50)
            
            cities = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 
                     'Казань', 'Нижний Новгород', 'Челябинск', 'Самара', 'Омск', 'Ростов-на-Дону']
            countries = ['Россия', 'Беларусь', 'Казахстан']
            
            inserted = 0
            for _ in range(num_customers):
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(['example.com', 'test.ru', 'mail.com'])}"
                phone = f"+7{''.join([str(random.randint(0, 9)) for _ in range(10)])}"
                city = random.choice(cities)
                country = random.choice(countries)
                
                try:
                    cursor.execute("""
                        INSERT INTO customers (first_name, last_name, email, phone, city, country)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING
                    """, (first_name, last_name, email, phone, city, country))
                    
                    if cursor.rowcount > 0:
                        inserted += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to insert customer: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Generated {inserted} new customers (attempted {num_customers})")
            
            return {
                'generated': inserted,
                'attempted': num_customers
            }
            
        except Exception as e:
            logger.error(f"Failed to generate customers: {e}")
            raise
    
    
    @task()
    def generate_orders(customers_info: dict, **context) -> dict:
        """
        Генерация заказов в PostgreSQL.
        
        Создает 50-150 заказов за текущий день с разными статусами.
        """
        logger = logging.getLogger(__name__)
        execution_date = context['logical_date']
        
        try:
            hook = PostgresHook(postgres_conn_id='postgres_source')
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # Получение списка существующих customer_id
            cursor.execute("SELECT customer_id FROM customers")
            customer_ids = [row[0] for row in cursor.fetchall()]
            
            if not customer_ids:
                logger.warning("No customers found! Creating default customer.")
                cursor.execute("""
                    INSERT INTO customers (first_name, last_name, email, phone, city, country)
                    VALUES ('Тест', 'Тестов', 'test@example.com', '+79999999999', 'Москва', 'Россия')
                    RETURNING customer_id
                """)
                customer_ids = [cursor.fetchone()[0]]
                conn.commit()
            
            # Генерация 50-150 заказов
            num_orders = random.randint(50, 150)
            
            statuses = {
                'pending': 0.15,      # 15% - в ожидании
                'processing': 0.25,   # 25% - в обработке
                'shipped': 0.20,      # 20% - отправлен
                'delivered': 0.35,    # 35% - доставлен
                'cancelled': 0.05     # 5% - отменен
            }
            
            payment_methods = ['card', 'cash', 'online', 'transfer']
            
            inserted = 0
            for i in range(num_orders):
                customer_id = random.choice(customer_ids)
                
                # Генерация времени заказа в течение дня
                order_date = execution_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Генерация суммы заказа (более реалистичное распределение)
                if random.random() < 0.7:  # 70% - обычные заказы
                    total_amount = round(random.uniform(500, 5000), 2)
                elif random.random() < 0.9:  # 20% - крупные заказы
                    total_amount = round(random.uniform(5000, 20000), 2)
                else:  # 10% - VIP заказы
                    total_amount = round(random.uniform(20000, 100000), 2)
                
                # Выбор статуса на основе весов
                status = random.choices(
                    list(statuses.keys()),
                    weights=list(statuses.values())
                )[0]
                
                payment_method = random.choice(payment_methods)
                shipping_address = fake.address()
                
                cursor.execute("""
                    INSERT INTO orders 
                    (customer_id, order_date, total_amount, status, payment_method, shipping_address)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING order_id
                """, (customer_id, order_date, total_amount, status, payment_method, shipping_address))
                
                order_id = cursor.fetchone()[0]
                inserted += 1
                
                # Генерация order_items (1-5 позиций на заказ)
                num_items = random.randint(1, 5)
                remaining_amount = total_amount
                
                for j in range(num_items):
                    product_id = random.randint(1, 50)  # Предполагаем 50 продуктов
                    quantity = random.randint(1, 3)
                    
                    if j == num_items - 1:
                        # Последняя позиция - остаток суммы
                        item_total = round(remaining_amount, 2)
                    else:
                        # Случайная часть от оставшейся суммы
                        item_total = round(remaining_amount * random.uniform(0.2, 0.5), 2)
                    
                    unit_price = round(item_total / quantity, 2)
                    remaining_amount -= item_total
                    
                    cursor.execute("""
                        INSERT INTO order_items
                        (order_id, product_id, quantity, unit_price, total_price)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (order_id, product_id, quantity, unit_price, item_total))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Generated {inserted} orders for {execution_date.date()}")
            
            return {
                'generated': inserted,
                'date': execution_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            logger.error(f"Failed to generate orders: {e}")
            raise
    
    
    @task()
    def generate_feedback(orders_info: dict, **context) -> dict:
        """
        Генерация отзывов в MongoDB.
        
        Создает отзывы для 30-50% заказов со статусом 'delivered'.
        """
        logger = logging.getLogger(__name__)
        execution_date = context['logical_date']
        
        client = None
        try:
            # Подключение к PostgreSQL для получения доставленных заказов
            pg_hook = PostgresHook(postgres_conn_id='postgres_source')
            pg_conn = pg_hook.get_conn()
            pg_cursor = pg_conn.cursor()
            
            # Получение заказов со статусом 'delivered'
            pg_cursor.execute("""
                SELECT order_id, customer_id
                FROM orders
                WHERE status = 'delivered'
                AND order_date::date = %s
            """, (execution_date.date(),))
            
            delivered_orders = pg_cursor.fetchall()
            pg_cursor.close()
            pg_conn.close()
            
            if not delivered_orders:
                logger.warning("No delivered orders found for feedback generation")
                return {'generated': 0}
            
            # Подключение к MongoDB
            mongo_hook = MongoHook(mongo_conn_id='mongo_source')
            client = mongo_hook.get_conn()
            db = client['feedback_db']
            collection = db['feedback']
            
            # Генерация отзывов для 30-50% доставленных заказов
            num_feedback = int(len(delivered_orders) * random.uniform(0.3, 0.5))
            selected_orders = random.sample(delivered_orders, min(num_feedback, len(delivered_orders)))
            
            comments_positive = [
                'Отличный сервис!',
                'Быстрая доставка',
                'Товар соответствует описанию',
                'Все супер, рекомендую!',
                'Очень доволен покупкой',
                'Качественный товар',
                'Превосходное обслуживание'
            ]
            
            comments_neutral = [
                'Нормально',
                'Обычная доставка',
                'Товар как товар',
                'Без особых эмоций',
                'Ожидания оправдались'
            ]
            
            comments_negative = [
                'Долго ждал доставку',
                'Есть замечания к упаковке',
                'Товар с дефектом',
                'Не очень доволен',
                'Ожидал большего'
            ]
            
            categories = ['delivery', 'product', 'service', 'quality', 'price']
            
            inserted = 0
            for order_id, customer_id in selected_orders:
                # Генерация рейтинга с реалистичным распределением
                rating_dist = random.random()
                if rating_dist < 0.5:  # 50% - отличные оценки
                    rating = round(random.uniform(4.5, 5.0), 1)
                    comment = random.choice(comments_positive)
                elif rating_dist < 0.85:  # 35% - хорошие оценки
                    rating = round(random.uniform(3.5, 4.4), 1)
                    comment = random.choice(comments_positive + comments_neutral)
                else:  # 15% - негативные оценки
                    rating = round(random.uniform(1.0, 3.4), 1)
                    comment = random.choice(comments_negative + comments_neutral)
                
                # Время отзыва - через несколько часов после доставки
                feedback_date = execution_date + timedelta(
                    hours=random.randint(2, 20),
                    minutes=random.randint(0, 59)
                )
                
                feedback_doc = {
                    'customer_id': customer_id,
                    'order_id': order_id,
                    'rating': rating,
                    'comment': comment,
                    'feedback_date': feedback_date,
                    'category': random.choice(categories),
                    'verified_purchase': True
                }
                
                collection.insert_one(feedback_doc)
                inserted += 1
            
            logger.info(f"✓ Generated {inserted} feedback records for {execution_date.date()}")
            
            return {
                'generated': inserted,
                'total_delivered': len(delivered_orders)
            }
            
        except Exception as e:
            logger.error(f"Failed to generate feedback: {e}")
            raise
            
        finally:
            if client is not None:
                client.close()
    
    
    @task()
    def generate_csv_products(**context) -> dict:
        """
        Генерация CSV файла с продуктами.
        
        Создает файл products_YYYYMMDD.csv с 50 продуктами.
        """
        logger = logging.getLogger(__name__)
        execution_date = context['logical_date']
        date_str = execution_date.strftime('%Y%m%d')
        
        try:
            categories = {
                'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch'],
                'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes'],
                'Books': ['Fiction', 'Non-Fiction', 'Textbook', 'Comic', 'Magazine'],
                'Home': ['Chair', 'Table', 'Lamp', 'Pillow', 'Rug'],
                'Sports': ['Ball', 'Racket', 'Mat', 'Weights', 'Bike']
            }
            
            brands = ['Brand A', 'Brand B', 'Brand C', 'Premium', 'Budget', 'Eco']
            
            products = []
            for product_id in range(1, 51):
                category = random.choice(list(categories.keys()))
                product_type = random.choice(categories[category])
                brand = random.choice(brands)
                
                product_name = f"{brand} {product_type}"
                
                # Генерация цены в зависимости от категории
                if category == 'Electronics':
                    price = round(random.uniform(5000, 80000), 2)
                elif category == 'Clothing':
                    price = round(random.uniform(500, 8000), 2)
                elif category == 'Books':
                    price = round(random.uniform(200, 2000), 2)
                elif category == 'Home':
                    price = round(random.uniform(1000, 15000), 2)
                else:  # Sports
                    price = round(random.uniform(800, 10000), 2)
                
                stock_quantity = random.randint(0, 500)
                
                products.append({
                    'product_id': product_id,
                    'product_name': product_name,
                    'category': category,
                    'price': price,
                    'stock_quantity': stock_quantity,
                    'brand': brand,
                    'available': stock_quantity > 0
                })
            
            df = pd.DataFrame(products)
            
            # Сохранение в CSV
            output_dir = '/opt/airflow/data/csv'
            filepath = f'{output_dir}/products_{date_str}.csv'
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"✓ Generated CSV file: {filepath} with {len(df)} products")
            
            return {
                'generated': len(df),
                'filepath': filepath,
                'date': date_str
            }
            
        except Exception as e:
            logger.error(f"Failed to generate CSV products: {e}")
            raise
    
    
    @task()
    def generate_csv_delivery_logs(orders_info: dict, **context) -> dict:
        """
        Генерация CSV файла с логами доставки.
        
        Создает файл delivery_logs_YYYYMMDD.csv для заказов со статусом 'delivered' и 'shipped'.
        """
        logger = logging.getLogger(__name__)
        execution_date = context['logical_date']
        date_str = execution_date.strftime('%Y%m%d')
        
        try:
            # Подключение к PostgreSQL для получения доставленных/отправленных заказов
            hook = PostgresHook(postgres_conn_id='postgres_source')
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT order_id, order_date
                FROM orders
                WHERE status IN ('delivered', 'shipped')
                AND order_date::date = %s
                ORDER BY order_id
            """, (execution_date.date(),))
            
            orders = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not orders:
                logger.warning("No delivered/shipped orders found for delivery logs")
                return {'generated': 0}
            
            # Генерация логов доставки
            delivery_logs = []
            for delivery_id, (order_id, order_date) in enumerate(orders, start=1):
                courier_id = random.randint(1, 20)
                
                # Время забора - через 1-3 часа после заказа
                pickup_time = order_date + timedelta(hours=random.randint(1, 3))
                
                # Время доставки - через 2-8 часов после забора
                delivery_time = pickup_time + timedelta(hours=random.randint(2, 8))
                
                # Статус доставки (большинство успешны)
                status_dist = random.random()
                if status_dist < 0.85:
                    status = 'delivered'
                elif status_dist < 0.95:
                    status = 'failed'
                else:
                    status = 'returned'
                
                delivery_logs.append({
                    'delivery_id': delivery_id,
                    'order_id': order_id,
                    'courier_id': courier_id,
                    'pickup_time': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'delivery_time': delivery_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': status,
                    'delivery_duration_minutes': int((delivery_time - pickup_time).total_seconds() / 60)
                })
            
            df = pd.DataFrame(delivery_logs)
            
            # Сохранение в CSV
            output_dir = '/opt/airflow/data/ftp'
            filepath = f'{output_dir}/delivery_logs_{date_str}.csv'
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"✓ Generated delivery logs CSV: {filepath} with {len(df)} records")
            
            return {
                'generated': len(df),
                'filepath': filepath,
                'date': date_str
            }
            
        except Exception as e:
            logger.error(f"Failed to generate delivery logs: {e}")
            raise
    
    
    @task()
    def generate_summary(
        customers_info: dict,
        orders_info: dict,
        feedback_info: dict,
        products_info: dict,
        delivery_info: dict
    ) -> str:
        """
        Сводка по сгенерированным данным.
        """
        logger = logging.getLogger(__name__)
        
        summary = f"""
        ═══════════════════════════════════════════════════════════
        📊 СВОДКА ПО ГЕНЕРАЦИИ ТЕСТОВЫХ ДАННЫХ
        ═══════════════════════════════════════════════════════════
        
        ✅ PostgreSQL (postgres_source):
           • Клиенты:          {customers_info['generated']} новых
           • Заказы:           {orders_info['generated']} заказов
           • Дата заказов:     {orders_info['date']}
        
        ✅ MongoDB (feedback_db):
           • Отзывы:           {feedback_info['generated']} отзывов
           • Доставленных:     {feedback_info.get('total_delivered', 0)} заказов
        
        ✅ CSV Files:
           • Продукты:         {products_info['generated']} позиций
           • Файл:             {products_info['filepath']}
           • Логи доставки:    {delivery_info['generated']} записей
           • Файл:             {delivery_info['filepath']}
        
        ═══════════════════════════════════════════════════════════
        ✓ ГЕНЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО
        ═══════════════════════════════════════════════════════════
        
        Теперь можно запустить основной DAG 'main_etl_pipeline'
        """
        
        logger.info(summary)
        return summary
    
    
    # === ГРАФ ЗАВИСИМОСТЕЙ ===
    
    # Сначала создаем клиентов
    customers = generate_customers()
    
    # Затем заказы (нужны клиенты)
    orders = generate_orders(customers)
    
    # Параллельно генерируем:
    # - отзывы (нужны заказы)
    # - CSV продукты (независимо)
    feedback = generate_feedback(orders)
    products_csv = generate_csv_products()
    
    # Логи доставки (нужны заказы)
    delivery_logs = generate_csv_delivery_logs(orders)
    
    # Финальная сводка
    summary = generate_summary(customers, orders, feedback, products_csv, delivery_logs)


# Создание экземпляра DAG
dag_instance = generate_test_data_dag()
