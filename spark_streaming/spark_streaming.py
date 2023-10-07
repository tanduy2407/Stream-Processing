from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, FloatType, ArrayType, TimestampType
from pyspark.sql import functions as f
import logging
import asyncio

logging.basicConfig(level=logging.INFO,
					format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


class SparkStreamProcessor:
	def __init__(self, bootstrap_servers: str, topic: str, namespace: str, database: str, collection: str):
		self.bootstrap_servers = bootstrap_servers
		self.topic = topic
		self.namespace = namespace
		self.database = database
		self.collection = collection
		self.spark = self.create_spark_session()

	def create_spark_session(self):
		try:
			spark = SparkSession.builder.appName('streaming') \
				.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0') \
				.getOrCreate()
			logging.info('Spark session created successfully')
		except Exception as err:
			logging.info(err)
		return spark

	def read_stream_data(self) -> DataFrame:
		try:
			df = self.spark \
				.readStream \
				.format('kafka') \
				.option('kafka.bootstrap.servers', self.bootstrap_servers) \
				.option('subscribe', self.topic) \
				.option('delimeter', ',') \
				.option('startingOffsets', 'earliest') \
				.load()
			logging.info('Initial dataframe created successfully')
		except Exception as err:
			logging.info('ERROR:', err)
		return df

	def create_dataframe(self, df: DataFrame):
		if self.namespace == 'beer/random_beer':
			schema = StructType([
				StructField('id', StringType(), False),
				StructField('brand', StringType(), False),
				StructField('name', StringType(), False),
				StructField('style', StringType(), False),
				StructField('hop', StringType(), False),
				StructField('yeast', StringType(), False),
				StructField('malts', StringType(), False),
				StructField('alcohol', StringType(), False),
				StructField('ibu', StringType(), False),
				StructField('blg', StringType(), False),
				StructField('insert_timestamp', TimestampType(), False)
			])

		if self.namespace == 'cannabis/random_cannabis':
			schema = StructType([
				StructField('id', StringType(), False),
				StructField('strain', StringType(), False),
				StructField('cannabinoid', StringType(), False),
				StructField('terpene', StringType(), False),
				StructField('medical_use', StringType(), False),
				StructField('health_benefit', StringType(), False),
				StructField('category', StringType(), False),
				StructField('brand', StringType(), False),
				StructField('insert_timestamp', TimestampType(), False)
			])

		if self.namespace == 'vehicle/random_vehicle':
			schema = StructType([
				StructField('id', StringType(), False),
				StructField('model', StringType(), False),
				StructField('color', StringType(), False),
				StructField('transmission', StringType(), False),
				StructField('drive_type', StringType(), False),
				StructField('fuel_type', StringType(), False),
				StructField('car_type', StringType(), False),
				StructField('car_options', ArrayType(
					StringType(), containsNull=False)),
				StructField('specs', ArrayType(
					StringType(), containsNull=False)),
				StructField('kilometrage', StringType(), False),
				StructField('license_plate', StringType(), False),
				StructField('insert_timestamp', TimestampType(), False)
			])

		if self.namespace == 'restaurant/random_restaurant':
			schema = StructType([
				StructField('id', StringType(), False),
				StructField('name', StringType(), False),
				StructField('description', StringType(), False),
				StructField('review', StringType(), False),
				StructField('logo', StringType(), False),
				StructField('phone_number', StringType(), False),
				StructField('address', StringType(), False),
				StructField('insert_timestamp', TimestampType(), False)
			])

		if self.namespace == 'users/random_user':
			schema = StructType([
				StructField('id', StringType(), False),
				StructField('full_name', StringType(), False),
				StructField('gender', StringType(), False),
				StructField('date_of_birth', StringType(), False),
				StructField('email', StringType(), False),
				StructField('address', StringType(), False),
				StructField('phone_number', StringType(), False),
				StructField('avatar', StringType(), False),
				StructField('social_insurance_number', StringType(), False),
				StructField('employment', StringType(), False),
				StructField('credit_card', StringType(), False),
				StructField('geo', StructType([
					StructField('latitude', FloatType(), False),
					StructField('longitude', FloatType(), False)
				]), False),
				StructField('insert_timestamp', TimestampType(), False)
			])

		df = df.selectExpr('CAST(value AS STRING)').select(
			f.from_json(f.col('value'), schema).alias('data')).select('data.*')
		df = df.withColumn('insert_timestamp', f.date_format(
			f.col('insert_timestamp'), 'yyyy-MM-dd HH:mm:ss'))
		return df

	def stream_to_mongo(self, df: DataFrame):
		logging.info('Start streaming ...')
		checkpoint = 'checkpoints_'
		query = df.writeStream.format('mongodb') \
			.option('spark.mongodb.connection.uri', f'mongodb://localhost:27017/{self.database}.{self.collection}') \
			.option('checkpointLocation', checkpoint + self.collection) \
			.outputMode('append').start()
		query.awaitTermination()

	# .option('spark.mongodb.connection.uri', f'mongodb://user:password@mongodb:27017/{database}.{collection}') \
	def process_stream(self):
		data = self.read_stream_data()
		final_data = self.create_dataframe(data)
		self.stream_to_mongo(final_data)


async def func1():
	bootstrap_servers = 'localhost:9092'
	namespace = 'vehicle/random_vehicle'
	topic = collection_name = namespace.split('/')[1]
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, database_name, collection_name)
	processor.process_stream()


async def func2():
	bootstrap_servers = 'localhost:9092'
	namespace = 'cannabis/random_cannabis'
	topic = collection_name = namespace.split('/')[1]
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, database_name, collection_name)
	processor.process_stream()


async def func3():
	bootstrap_servers = 'localhost:9092'
	namespace = 'beer/random_beer'
	topic = collection_name = namespace.split('/')[1]
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, database_name, collection_name)
	processor.process_stream()


async def func4():
	bootstrap_servers = 'localhost:9092'
	namespace = 'restaurant/random_restaurant'
	topic = collection_name = namespace.split('/')[1]
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, database_name, collection_name)
	processor.process_stream()


async def func5():
	bootstrap_servers = 'localhost:9092'
	namespace = 'users/random_user'
	topic = collection_name = namespace.split('/')[1]
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, database_name, collection_name)
	processor.process_stream()


async def streaming():
	tasks = []
	for func in [func1, func2, func3, func4, func5]:
		tasks.append(asyncio.create_task(func()))
	await asyncio.gather(*tasks)

if __name__ == "__main__":
	database_name = 'kafka_streaming'
	streaming()
