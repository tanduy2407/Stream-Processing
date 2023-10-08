from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, FloatType, ArrayType, TimestampType
from pyspark.sql import functions as f
import logging
import multiprocessing

logging.basicConfig(level=logging.INFO,
					format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


class SparkStreamProcessor:
	def __init__(self, bootstrap_servers: str, topic: str, namespace: str, database: str, collection: str):
		"""
		Initializes the SparkStreamProcessor with required parameters.

		Args:
			spark (SparkSession): SparkSession object.
			bootstrap_servers (str): Kafka bootstrap servers.
			topic (str): Kafka topic to subscribe to.
			namespace (str): Namespace for the data.
			database (str): MongoDB database name.
			collection (str): MongoDB collection name.
		"""
		self.spark = self.create_spark_session()
		self.bootstrap_servers = bootstrap_servers
		self.topic = topic
		self.namespace = namespace
		self.database = database
		self.collection = collection

	def create_spark_session(self) -> SparkSession:
		"""
		Creates and configures a SparkSession for processing streaming data.

		Returns:
			SparkSession: Configured SparkSession object.
		"""
		try:
			spark = SparkSession.builder.appName('streaming') \
				.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0') \
				.getOrCreate()
			logging.info('Spark session created successfully')
		except Exception as err:
			logging.info(err)
		return spark

	def read_stream_data(self) -> DataFrame:
		"""
		Reads streaming data from Kafka and creates an initial DataFrame.

		Returns:
			DataFrame: Initial DataFrame containing streaming data from Kafka.
		"""
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

	def create_dataframe(self, df: DataFrame) -> DataFrame:
		"""
		Parses the input DataFrame based on the namespace and creates a processed DataFrame.

		Args:
			df (DataFrame): Initial Spark DataFrame containing raw data.

		Returns:
			DataFrame: Processed DataFrame with appropriate schema and formatted timestamps.
		"""
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
		"""
		Streams the processed DataFrame to MongoDB.

		Args:
			df (DataFrame): Processed DataFrame with data to be streamed to MongoDB.
		"""
		logging.info('Start streaming ...')
		user = 'kafka_streaming'
		passwd = 'kafka_streaming'
		host = 'mongodb'
		port = 27017
		db = self.database
		collection = self.collection
		checkpoint = 'checkpoints_' + self.collection
		query = df.writeStream.format('mongodb') \
			.option('spark.mongodb.connection.uri', f'mongodb://{user}:{passwd}@{host}:{port}/{db}.{collection}') \
			.option('checkpointLocation', checkpoint) \
			.outputMode('append').start()
		query.awaitTermination()


def process_stream(bootstrap_servers, topic, namespace, db_name, collection):
	"""
	Process streaming data from Kafka and store it in MongoDB.

	Args:
		bootstrap_servers (str): Kafka bootstrap servers.
		topic (str): Kafka topic to subscribe to.
		namespace (str): Namespace for the data.
		db_name (str): MongoDB database name.
		collection (str): MongoDB collection name.
	"""
	processor = SparkStreamProcessor(
		bootstrap_servers, topic, namespace, db_name, collection)
	data = processor.read_stream_data()
	final_data = processor.create_dataframe(data)
	processor.stream_to_mongo(final_data)


if __name__ == "__main__":
	db_name = 'kafka_streaming'
	bootstrap_servers = 'localhost:9092'
	namespaces = ['beer/random_beer', 'cannabis/random_cannabis', 'vehicle/random_vehicle',
				  'restaurant/random_restaurant', 'users/random_user']

	processes = []
	for namespace in namespaces:
		topic = collection = namespace.split('/')[1]
		process = multiprocessing.Process(target=process_stream, args=(
			bootstrap_servers, topic, namespace, db_name, collection))
		process.start()
		processes.append(process)

	for process in processes:
		process.join()
