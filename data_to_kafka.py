from kafka import KafkaProducer
import requests
from random import randint, uniform
import time
import pprint
import json

def create_response_data(url: str='https://randomuser.me/api'):
	response = requests.get(f'{url}/?results={randint(1, 10)}')
	# response = requests.get(f'{url}/?results=1')

	results = response.json()['results']
	all_kafka_data = []
	for result in results:
		all_kafka_data.append(create_json_data(result))
	return all_kafka_data


def create_json_data(result: dict) -> dict:
	data = {}
	data['id'] = result['login']['uuid']
	data['title'] = result['name']['title']
	data['full_name'] = result['name']['first'] + ' ' + result['name']['last']
	data['gender'] = result['gender']
	data['dob'] = result['dob']['date'][:10]
	data['email'] = result['email']
	location = result['location']
	data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['state']}, {location['city']}, {location['postcode']}, {location['country']}"
	data['cell'] = result['cell']
	data['phone'] = result['phone']
	data['picture_url'] = result['picture']['large']
	lat = float(result['location']['coordinates']['latitude'])
	lon = float(result['location']['coordinates']['longitude'])
	data['geo'] = {'latitude': lat, 'longitude': lon}
	return data


def create_kafka_producer():
	return KafkaProducer(bootstrap_servers=['localhost:9092'])

def stream_data_to_kafka():
	pass


def start_streaming():
	producer = create_kafka_producer()
	while True:
		kafka_data = create_response_data()
		print(kafka_data)
		producer.send("random_names", json.dumps(kafka_data).encode('unicode_escape').decode().encode('utf-8'))
		time.sleep(uniform(1, 3))
	
start_streaming()