// Define the database name and list of collection name
var dbName = 'kafka_streaming';
var collectionNames = ['random_beer', 'random_cannabis', 'random_vehicle', 'random_restaurant', 'random_user'];

// Connect to the specified database
var db = db.getSiblingDB(dbName);

// Create collections
collectionNames.forEach(collectionName => {
  db.createCollection(collectionName);
});

// Create a new user with dbOwner role for the specified database
db.createUser({
  user: 'user',
  pwd: 'password',
  roles: [{ role: 'dbOwner', db: dbName }]
});