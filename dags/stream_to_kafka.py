import requests
from random import uniform
import json
from kafka import KafkaProducer
import asyncio
from datetime import datetime


class KafkaStreamData:
	"""
	Generates random data for specified namespaces and formats it for Kafka streaming.
	
	Args:
		namespace (str): The type of data namespace, e.g., 'beer/random_beer'.
		topic (str): The Kafka topic to which the data will be streamed.
	"""
	def __init__(self, namespace: str, topic: str):
		"""
		Generates random data for specified namespaces and formats it for Kafka streaming.
		
		Args:
			namespace (str): The type of data namespace, e.g., 'beer/random_beer'.
			topic (str): The Kafka topic to which the data will be streamed.
		"""
		valid_namespaces = ['beer/random_beer', 'cannabis/random_cannabis',
						   'vehicle/random_vehicle', 'restaurant/random_restaurant', 'users/random_user']
		if namespace not in valid_namespaces:
			raise ValueError('Invalid namespace. Must be one of: ' +
							 ', '.join(valid_namespaces))
		self.namespace = namespace
		self.url = 'https://random-data-api.com/api/'
		self.topic = topic

	def create_kafka_data(self):
		"""
		Fetches random data from the specified namespace and formats it for Kafka streaming.
		
		Returns:
			dict: Formatted data for Kafka streaming.
		"""
		response = requests.get(f'{self.url}/{self.namespace}?size=1')
		results = response.json()[0]
		# kafka_data = []
		# for result in results:
		# 	kafka_data.append(create_json_data(result))
		return self.create_json_data(results)

	def create_json_data(self, result: dict) -> dict:
		"""
		Formats the random data into JSON format for Kafka streaming.
		
		Args:
			result (dict): Raw random data fetched from the API.
		
		Returns:
			dict: Formatted JSON data for Kafka streaming.
		"""
		data = {}
		if self.namespace == 'beer/random_beer':
			data['id'] = result['id']
			data['brand'] = result['brand']
			data['name'] = result['name']
			data['style'] = result['style']
			data['hop'] = result['hop']
			data['yeast'] = result['yeast']
			data['malts'] = result['malts']
			data['alcohol'] = result['alcohol']
			data['ibu'] = result['ibu']
			data['blg'] = result['blg']

		if self.namespace == 'cannabis/random_cannabis':
			data['id'] = result['id']
			data['strain'] = result['strain']
			data['cannabinoid'] = result['cannabinoid']
			data['terpene'] = result['terpene']
			data['medical_use'] = result['medical_use']
			data['health_benefit'] = result['health_benefit']
			data['category'] = result['category']
			data['brand'] = result['brand']

		if self.namespace == 'vehicle/random_vehicle':
			data['id'] = result['id']
			data['model'] = result['make_and_model']
			data['color'] = result['color']
			data['transmission'] = result['transmission']
			data['drive_type'] = result['drive_type']
			data['fuel_type'] = result['fuel_type']
			data['car_type'] = result['car_type']
			data['car_options'] = result['car_options']
			data['specs'] = result['specs']
			data['kilometrage'] = result['kilometrage']
			data['license_plate'] = result['license_plate']

		if self.namespace == 'restaurant/random_restaurant':
			data['id'] = result['id']
			data['name'] = result['name']
			data['description'] = result['description']
			data['review'] = result['review']
			data['logo'] = result['logo']
			data['phone_number'] = result['phone_number']
			data['address'] = result['address']

		if self.namespace == 'users/random_user':
			data['id'] = result['id']
			data['full_name'] = result['first_name'] + \
				' ' + result['last_name']
			data['gender'] = result['gender']
			data['date_of_birth'] = result['date_of_birth']
			data['email'] = result['email']
			address = result['address']
			data['address'] = f"{address['street_address']} {address['street_name']}, {address['state']}, {address['city']}, {address['zip_code']}, {address['country']}"
			data['phone_number'] = result['phone_number']
			data['avatar'] = result['avatar']
			data['social_insurance_number'] = result['social_insurance_number']
			data['employment'] = result['employment']['title'] + \
				'/' + result['employment']['key_skill']
			data['credit_card'] = result['credit_card']['cc_number']
			lat = float(address['coordinates']['lat'])
			lon = float(address['coordinates']['lng'])
			data['geo'] = {'latitude': lat, 'longitude': lon}
		data['insert_timestamp'] = str(datetime.now())
		return data

	def stream_data_to_kafka(self, producer: KafkaProducer, data):
		"""
		Streams the formatted data to Kafka topic using the provided Kafka producer.
		
		Args:
			producer (KafkaProducer): Kafka producer instance.
			data (dict): Formatted data for Kafka streaming.
		"""
		producer.send(self.topic, json.dumps(data).encode(
			'unicode_escape').decode().encode('utf-8'))


async def generate_and_stream_data(producer: KafkaProducer, namespace: str, topic: str):
	"""
	Generates random data and streams it to the specified Kafka topic.
	
	Args:
		producer (KafkaProducer): Kafka producer instance.
		namespace (str): The type of data namespace, e.g., 'beer/random_beer'.
		topic (str): The Kafka topic to which the data will be streamed.
	"""
	while True:
		kafka_stream = KafkaStreamData(namespace, topic)
		kafka_data = kafka_stream.create_kafka_data()
		print(topic.upper(), kafka_data)
		kafka_stream.stream_data_to_kafka(producer, kafka_data)
		await asyncio.sleep(uniform(1, 5))


async def func1():
	await generate_and_stream_data(producer1, 'beer/random_beer', 'random_beer')


async def func2():
	await generate_and_stream_data(producer1, 'cannabis/random_cannabis', 'random_cannabis')


async def func3():
	await generate_and_stream_data(producer1, 'vehicle/random_vehicle', 'random_vehicle')


async def func4():
	await generate_and_stream_data(producer1, 'restaurant/random_restaurant', 'random_restaurant')


async def func5():
	await generate_and_stream_data(producer1, 'users/random_user', 'random_user')


async def streaming_kafka():
	tasks = []
	for func in [func1, func2, func3, func4, func5]:
		tasks.append(asyncio.create_task(func()))
	await asyncio.gather(*tasks)

def main():
	asyncio.run(streaming_kafka())
	
if __name__ == "__main__":
	producer1 = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
	producer2 = KafkaProducer(bootstrap_servers=['127.0.0.1:9093'])
	producer3 = KafkaProducer(bootstrap_servers=['127.0.0.1:9094'])
	main()
	

# kafka-server-start.bat D:\Apps\kafka_2.13-3.5.0\config\server-9093.properties
# zookeeper-server-start.bat D:\Apps\kafka_2.13-3.5.0\config\zookeeper.properties
