import requests
from random import uniform
import json
from kafka import KafkaProducer
import asyncio


def create_kafka_data(endpoint: str, url: str = 'https://random-data-api.com/api/'):
    # response = requests.get(f'{url}/?results={randint(1, 10)}')
    valid_endpoints = ['beer/random_beer', 'cannabis/random_cannabis',
                       'vehicle/random_vehicle', 'restaurant/random_restaurant', 'users/random_user']
    if endpoint not in valid_endpoints:
        raise ValueError('Invalid endpoint. Must be one of: ' +
                         ', '.join(valid_endpoints))
    response = requests.get(f'{url}/{endpoint}?size=1')
    results = response.json()[0]
    # kafka_data = []
    # for result in results:
    # 	kafka_data.append(create_json_data(result))
    return create_json_data(results, endpoint)


def create_json_data(result: dict, endpoint: str) -> dict:
    data = {}
    if endpoint == 'beer/random_beer':
        data['id'] = result['id']
        data['brand'] = result['brand']
        data['name'] = result['name']
        data['style'] = result['style']
        data['hop'] = result['hop']
        data['yeast'] = result['yeast']
        data['malts'] = result['malts']
        data['alcohol'] = result['alcohol']
        data['ibu'] = result['ibu']
        data['yeblgast'] = result['blg']

    if endpoint == 'cannabis/random_cannabis':
        data['id'] = result['id']
        data['strain'] = result['strain']
        data['cannabinoid'] = result['cannabinoid']
        data['terpene'] = result['terpene']
        data['medical_use'] = result['medical_use']
        data['health_benefit'] = result['health_benefit']
        data['category'] = result['category']
        data['brand'] = result['brand']

    if endpoint == 'vehicle/random_vehicle':
        data['id'] = result['id']
        data['model'] = result['make_and_model']
        data['color'] = result['color']
        data['transmission'] = result['transmission']
        data['drive_type'] = result['drive_type']
        data['fuel_type'] = result['fuel_type']
        data['car_type'] = result['car_type']
        data['car_options'] = result['car_options']
        data['specs'] = '. '.join(result['car_options'])
        data['kilometrage'] = result['kilometrage']
        data['license_plate'] = result['license_plate']

    if endpoint == 'restaurant/random_restaurant':
        data['id'] = result['id']
        data['name'] = result['name']
        data['description'] = result['description']
        data['review'] = result['review']
        data['logo'] = result['logo']
        data['phone_number'] = result['phone_number']
        data['address'] = result['address']

    if endpoint == 'users/random_user':
        data['id'] = result['id']
        data['full_name'] = result['first_name'] + ' ' + result['last_name']
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
    return data


def create_kafka_producer(server_address: list):
    return KafkaProducer(bootstrap_servers=server_address)


def stream_data_to_kafka(producer: KafkaProducer, data, topic: str):
    producer.send(topic, json.dumps(data).encode(
        'unicode_escape').decode().encode('utf-8'))


async def generate_and_stream_data(endpoint: str, topic: str):
    while True:
        kafka_data = create_kafka_data(endpoint=endpoint)
        print(topic.upper(), kafka_data)
        # stream_data_to_kafka(producer, kafka_data, topic)
        await asyncio.sleep(uniform(2, 5))


async def func1():
    await generate_and_stream_data('beer/random_beer', 'random_beer')


async def func2():
    await generate_and_stream_data('cannabis/random_cannabis', 'random_cannabis')


async def func3():
    await generate_and_stream_data('vehicle/random_vehicle', 'random_vehicle')


async def func4():
    await generate_and_stream_data('restaurant/random_restaurant', 'random_restaurant')


async def func5():
    await generate_and_stream_data('users/random_user', 'random_user')


async def start_streaming_to_kafka():
    while True:
        await asyncio.gather(func1(), func2(), func3(), func4(), func5())


async def main():
    tasks = []
    for func in [func1, func2, func3, func4, func5]:
        tasks.append(asyncio.create_task(func()))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    # producer = create_kafka_producer(
    #     ['localhost:9092', 'localhost:9093', 'localhost:9094'])
    asyncio.run(main())
