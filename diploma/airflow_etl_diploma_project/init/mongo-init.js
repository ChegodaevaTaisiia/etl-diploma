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

db.createCollection(process.env.MONGO_COLLECTION);


print("Mongo init completed successfully!");