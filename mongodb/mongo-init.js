// init-script.js

// Define the database name and collection name
var dbName = 'kafka_streaming';
var collectionName = 'random_names';

// Connect to the specified database
var db = db.getSiblingDB(dbName);

// Create a new collection
db.createCollection(collectionName);

// Create a new user with readWrite role for the specified database
db.createUser({
  user: 'user',
  pwd: 'password',
  roles: [{ role: 'dbOwner', db: dbName }]
});

print('Database, collection, and user created successfully.');
