bootstrap_servers = 'localhost:9092'
namespace = 'users/random_user'
topic = collection_name = namespace.split('/')[1]
print(topic, collection_name)