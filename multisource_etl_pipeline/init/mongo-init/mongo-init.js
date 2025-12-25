print("Mongo init start...");

var db = connect("mongodb://localhost:27017/admin");
db.getSiblingDB("admin");
db.auth(process.env.MONGO_INITDB_ROOT_USERNAME, process.env.MONGO_INITDB_ROOT_PASSWORD);

db.createUser(
    {
        user: process.env.MONGO_USERNAME,
        pwd:  process.env.MONGO_PASSWORD,
        roles: [ { 
            role: "readWrite", 
            db: process.env.MONGO_DATABASE
        } ],
        passwordDigestor: "server",
    }
);

db = db.getSiblingDB(process.env.MONGO_DATABASE);

db.createCollection(process.env.MONGO_COLLECTION, {
        validator: {
            $jsonSchema: {
                bsonType: "object",
                required: ["payment_id", "order_id", "amount", "payment_date", "status"],
                properties: {
                    payment_id: {
                        bsonType: "string",
                        description: "Уникальный идентификатор платежа"
                    },
                    order_id: {
                        bsonType: "int",
                        description: "ID связанного заказа"
                    },
                    amount: {
                        bsonType: "decimal",
                        description: "Сумма платежа"
                    },
                    payment_date: {
                        bsonType: "date",
                        description: "Дата платежа"
                    },
                    status: {
                        bsonType: "string",
                        description: "Статус платежа"
                    },
                    created_at: {
                        bsonType: "date",
                        description: "Дата создания записи"
                    },
                    updated_at: {
                        bsonType: "date",
                        description: "Дата обновления записи"
                    }
                }
            }
        }
    });

    collection = db.getCollection(process.env.MONGO_COLLECTION);
    // Создание индексов
    collection.createIndex({ "payment_date": 1 });
    collection.createIndex({ "updated_at": 1 });
    collection.createIndex({ "order_id": 1 });


    // Вставка тестовых данных
    collection.insertMany([
        {
            payment_id: "pay_001",
            order_id: 1,
            amount: NumberDecimal("150.75"),
            payment_date: new Date("2024-01-15T10:30:00Z"),
            status: "completed",
            created_at: new Date(),
            updated_at: new Date()
        },
        {
            payment_id: "pay_002",
            order_id: 2,
            amount: NumberDecimal("89.99"),
            payment_date: new Date("2024-01-16T11:45:00Z"),
            status: "processing",
            created_at: new Date(),
            updated_at: new Date()
        },
        {
            payment_id: "pay_003",
            order_id: 3,
            amount: NumberDecimal("245.50"),
            payment_date: new Date("2024-01-17T09:15:00Z"),
            status: "completed",
            created_at: new Date(),
            updated_at: new Date()
        }
    ]);

    // Создание коллекции для водяных знаков
    db.createCollection('extraction_watermarks');
    db.extraction_watermarks.insertOne({
        source_name: process.env.MONGO_COLLECTION,
        last_extracted_at: new Date("2024-01-01T00:00:00Z"),
        last_record_id: null,
        metadata: { "collection": "payments" }
    });

print("Mongo init completed successfully!");