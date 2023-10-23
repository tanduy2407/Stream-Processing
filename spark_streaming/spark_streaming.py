from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t
from pyspark.sql import functions as f
import logging
import sys

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
					.config("spark.dynamicAllocation.enabled", "true") \
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
		schema = None
		if self.namespace == 'beer/random_beer':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('brand', t.StringType(), False),
				t.StructField('name', t.StringType(), False),
				t.StructField('style', t.StringType(), False),
				t.StructField('hop', t.StringType(), False),
				t.StructField('yeast', t.StringType(), False),
				t.StructField('malts', t.StringType(), False),
				t.StructField('alcohol', t.StringType(), False),
				t.StructField('ibu', t.StringType(), False),
				t.StructField('blg', t.StringType(), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
			])

		if self.namespace == 'cannabis/random_cannabis':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('strain', t.StringType(), False),
				t.StructField('cannabinoid', t.StringType(), False),
				t.StructField('terpene', t.StringType(), False),
				t.StructField('medical_use', t.StringType(), False),
				t.StructField('health_benefit', t.StringType(), False),
				t.StructField('category', t.StringType(), False),
				t.StructField('brand', t.StringType(), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
			])

		if self.namespace == 'vehicle/random_vehicle':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('model', t.StringType(), False),
				t.StructField('color', t.StringType(), False),
				t.StructField('transmission', t.StringType(), False),
				t.StructField('drive_type', t.StringType(), False),
				t.StructField('fuel_type', t.StringType(), False),
				t.StructField('car_type', t.StringType(), False),
				t.StructField('car_options', t.ArrayType(
					t.StringType(), containsNull=False)),
				t.StructField('specs', t.ArrayType(
					t.StringType(), containsNull=False)),
				t.StructField('kilometrage', t.StringType(), False),
				t.StructField('license_plate', t.StringType(), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
			])

		if self.namespace == 'restaurant/random_restaurant':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('name', t.StringType(), False),
				t.StructField('description', t.StringType(), False),
				t.StructField('review', t.StringType(), False),
				t.StructField('logo', t.StringType(), False),
				t.StructField('phone_number', t.StringType(), False),
				t.StructField('address', t.StringType(), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
			])

		if self.namespace == 'users/random_user':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('full_name', t.StringType(), False),
				t.StructField('gender', t.StringType(), False),
				t.StructField('date_of_birth', t.StringType(), False),
				t.StructField('email', t.StringType(), False),
				t.StructField('address', t.StringType(), False),
				t.StructField('phone_number', t.StringType(), False),
				t.StructField('avatar', t.StringType(), False),
				t.StructField('social_insurance_number', t.StringType(), False),
				t.StructField('employment', t.StringType(), False),
				t.StructField('credit_card', t.StringType(), False),
				t.StructField('geo', t.StructType([
					t.StructField('latitude', t.FloatType(), False),
					t.StructField('longitude', t.FloatType(), False)
				]), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
			])

		if self.namespace == 'company/random_company':
			schema = t.StructType([
				t.StructField('id', t.IntegerType(), False),
				t.StructField('business_name', t.StringType(), False),
				t.StructField('suffix', t.StringType(), False),
				t.StructField('industry', t.StringType(), False),
				t.StructField('employee_id_number', t.StringType(), False),
				t.StructField('duns_number', t.StringType(), False),
				t.StructField('logo', t.StringType(), False),
				t.StructField('type', t.StringType(), False),
				t.StructField('address', t.StringType(), False),
				t.StructField('geo', t.StructType([
					t.StructField('latitude', t.FloatType(), False),
					t.StructField('longitude', t.FloatType(), False)
				]), False),
				t.StructField('insert_timestamp', t.TimestampType(), False)
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
	if len(sys.argv) < 3:
		print('Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
     spark_streaming.py <host:port> <database> <namespace>')
		exit(-1)
	bootstrap_servers = sys.argv[1]
	db_name = sys.argv[2]
	namespace = sys.argv[3]
	topic = collection = namespace.split('/')[1]
	process_stream(bootstrap_servers, topic, namespace, db_name, collection)

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 spark_streaming.py

#spark-submit --master local[2] --jars spark-sql-kafka-0-10_2.12-3.3.0.jar,mongo-spark-connector_2.12-10.2.0.jar spark_streaming.py

#spark-submit spark_streaming.py kafka1:19092 kafka_streaming beer/random_beer
