from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql import functions as f
import logging

logging.basicConfig(level=logging.INFO,
					format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


def create_spark_session():
	try:
		spark = SparkSession.builder.appName('streaming') \
			.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0') \
			.getOrCreate()
		logging.info('Spark session created successfully')
	except Exception as err:
		logging.info(err)
	return spark


def read_stream_data(spark: SparkSession, bootstrap_server: str, topic: str) -> DataFrame:
	try:
		df = spark \
			  .readStream \
			  .format('kafka') \
			  .option('kafka.bootstrap.servers', bootstrap_server) \
			  .option('subscribe', topic) \
			  .option('delimeter', ',') \
			  .option('startingOffsets', 'earliest') \
			  .load()
		logging.info('Initial dataframe created successfully')
	except Exception as err:
		logging.info('ERROR:', err)
	return df


def create_dataframe(df: DataFrame):
	schema = StructType([
		StructField('id', StringType(), False),
		StructField('title', StringType(), False),
		StructField('full_name', StringType(), False),
		StructField('dob', StringType(), False),
		StructField('email', StringType(), False),
		StructField('gender', StringType(), False),
		StructField('address', StringType(), False),
		StructField('cell', StringType(), False),
		StructField('phone', StringType(), False),
		StructField('picture_url', StringType(), False),
		StructField('geo', StructType([
			StructField('latitude', FloatType(), False),
			StructField('longitude', FloatType(), False)
		]), False)
	])
	df = df.selectExpr('CAST(value AS STRING)').select(
		f.from_json(f.col('value'), schema).alias('data')).select('data.*')
	return df


def stream_to_mongo(df: DataFrame, database: str, collection: str):
	logging.info('Start streaming ...')
	checkpoint_location = 'checkpoints'
	query = df.writeStream.format('mongodb') \
		.option('spark.mongodb.connection.uri', f'mongodb://user:password@mongodb:27017/{database}.{collection}') \
		.option('checkpointLocation', checkpoint_location) \
		.outputMode('append').start()
	query.awaitTermination()


def streaming():
	spark = create_spark_session()
	data = read_stream_data(spark, 'kafka:19092', 'random_names')
	final_data = create_dataframe(data)
	stream_to_mongo(final_data, 'kafka_streaming', 'random_names')

streaming()