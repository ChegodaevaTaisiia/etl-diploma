#!/bin/bash

set -e
set -u

# Заполняем БД SOURCE_POSTGRES_DATABASE - добавляем WATERMARK
if [ -n "$SOURCE_POSTGRES_DATABASE" ] && [ -n "$SOURCE_POSTGRES_WATERMARK_TABLE" ]; then
    echo "Creating watermark-table ${SOURCE_POSTGRES_WATERMARK_TABLE} in ${SOURCE_POSTGRES_DATABASE}"
    psql -v ON_ERROR_STOP=1 --username "$SOURCE_POSTGRES_USER" --dbname "$SOURCE_POSTGRES_DATABASE" >/dev/null <<-EOSQL
      \c $SOURCE_POSTGRES_DATABASE;
      DROP TABLE IF EXISTS $SOURCE_POSTGRES_WATERMARK_TABLE;
      
      -- Таблица для хранения водяных знаков (watermarks)
      CREATE TABLE IF NOT EXISTS $SOURCE_POSTGRES_WATERMARK_TABLE (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(100) NOT NULL UNIQUE,
        last_extracted_at TIMESTAMP NOT NULL,
        last_record_id INTEGER,
        metadata JSONB
      );
EOSQL

fi