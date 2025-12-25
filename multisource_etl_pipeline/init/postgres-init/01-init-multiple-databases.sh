#!/bin/bash

set -e
set -u

# Основной пользователь из переменных окружения
MAIN_USER=${POSTGRES_USER:-postgres}
MAIN_PASSWORD=${POSTGRES_PASSWORD:-}

# Проверяем, указаны ли базы данных для создания
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Creating multiple databases..."
    
    # Создаем основного пользователя, если он не postgres
    if [ "$MAIN_USER" != "postgres" ]; then
        echo "Creating main user '$MAIN_USER'"
        psql -v ON_ERROR_STOP=1 --username "postgres" >/dev/null <<-EOSQL
            CREATE USER $MAIN_USER WITH PASSWORD '$MAIN_PASSWORD';
            ALTER USER $MAIN_USER WITH SUPERUSER;
EOSQL
    fi
    
    # Создаем каждую базу данных
    # Формат переменной: user1:password1:database1,user2:password2:database2
		IFS=',' read -ra DATABASES <<< "$POSTGRES_MULTIPLE_DATABASES"
    for db in "${DATABASES[@]}"; do
			IFS=':' read -ra db_data <<< "$db"
				db_user="${db_data[0]}"
				db_pass="${db_data[1]}"
				db_name="${db_data[2]}"
				# Убираем лишние пробелы
				db_user=$(echo $db_user | xargs)
				db_pass=$(echo $db_pass | xargs)	
				db_name=$(echo $db_name | xargs)
				# Оставляем только допустимые символы
				db_user=${db_user//[^a-zA-Z0-9_]/}
				db_pass=${db_pass//[^a-zA-Z0-9_]/}
				db_name=${db_name//[^a-zA-Z0-9_]/}
				# Приводим к нижнему регистру
				db_user=${db_user,,}
				db_name=${db_name,,}
				echo "Creating database: $db_name"
				
        psql -v ON_ERROR_STOP=1 --username "$MAIN_USER" >/dev/null <<-EOSQL
            CREATE DATABASE "$db_name";
            GRANT ALL PRIVILEGES ON DATABASE "$db_name" TO $MAIN_USER;
EOSQL

        # Создаем отдельного пользователя для каждой БД
				# Проверяем, заданы ли переменные для пользователя и пароля
				if [ -n "${db_user:-}" ] && [ -n "${db_pass:-}" ]; then
						echo "Creating user '$db_user' for database '$db_name'"
            echo "Granting access to user '$db_user' for database '$db_name'"
            psql -v ON_ERROR_STOP=1 --username "$MAIN_USER" >/dev/null <<-EOSQL
                DO \$\$
                BEGIN
                    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$db_user') THEN
                        CREATE USER "$db_user" WITH PASSWORD '$db_pass';
                    END IF;
                END
                \$\$;
                GRANT CONNECT ON DATABASE "$db_name" TO "$db_user";
                GRANT USAGE ON SCHEMA public TO "$db_user";
								ALTER DATABASE "$db_name" OWNER TO "$db_user";
                GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "$db_user";
                GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "$db_user";
								ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "$db_user";
								ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES  ON SEQUENCES TO "$db_user";
EOSQL
        fi
    done
    
    echo "Multiple databases created successfully!"
fi


# Заполняем БД SOURCE_POSTGRES_DATABASE
if [ -n "$SOURCE_POSTGRES_DATABASE" ] && [ -n "$SOURCE_POSTGRES_TABLE" ]; then
    echo "Creating table ${SOURCE_POSTGRES_TABLE} in ${SOURCE_POSTGRES_DATABASE}"
    psql -v ON_ERROR_STOP=1 --username "$SOURCE_POSTGRES_USER" --dbname "$SOURCE_POSTGRES_DATABASE" >/dev/null <<-EOSQL
      \c $SOURCE_POSTGRES_DATABASE;
      DROP TABLE IF EXISTS $SOURCE_POSTGRES_TABLE;

      -- Таблица заказов
      CREATE TABLE $SOURCE_POSTGRES_TABLE (
          order_id SERIAL PRIMARY KEY,
          customer_id INTEGER NOT NULL,
          product_id INTEGER NOT NULL,
          quantity INTEGER NOT NULL,
          order_date DATE NOT NULL,
          currency VARCHAR(3),
          amount DECIMAL(10,2),
          status VARCHAR(50) DEFAULT 'completed',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );


    -- Создание индексов для ускорения incremental load
    CREATE INDEX idx_orders_created ON $SOURCE_POSTGRES_TABLE(created_at);
    CREATE INDEX idx_orders_date ON $SOURCE_POSTGRES_TABLE(order_date);

      -- Вставка тестовых данных
      INSERT INTO $SOURCE_POSTGRES_TABLE (customer_id, product_id, quantity, order_date, currency, amount) VALUES
          (101, 1, 1, '2025-12-01', 'USD', 1299.99),
          (102, 2, 2, '2025-12-01', 'USD', 1999.98),
          (103, 3, 1, '2025-12-01', 'USD', 349.99),
          (104, 4, 3, '2025-12-02', 'USD', 119.97),
          (105, 5, 5, '2025-12-02', 'USD', 99.95),    
          (200, 1, 3, '2025-12-10', 'USD', 199.99);

EOSQL

fi