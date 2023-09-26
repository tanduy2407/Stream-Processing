from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql import functions as f

def create_spark_session():
	try:
		spark = SparkSession.builder.appName('streaming') \
			.getOrCreate()
	except Exception as err:
		print(err)
	return spark


def read_stream_data(spark: SparkSession):
	topic = "random_names"
	kafka_bootstrap_server = "localhost:9092"
	try:
		df = spark \
			.readStream \
			.format("kafka") \
			.option("kafka.bootstrap.servers", kafka_bootstrap_server) \
			.option("subscribe", topic) \
			.load()
	except Exception as err:
		print(err)
	return df


def create_dataframe(df):
	schema = StructType([
		StructField("id", StringType(), False),
		StructField("title", StringType(), False),
		StructField("full_name", StringType(), False),
		StructField("dob", StringType(), False),
		StructField("email", StringType(), False),
		StructField("gender", StringType(), False),
		StructField("address", StringType(), False),
		StructField("cell", StringType(), False),
		StructField("phone", StringType(), False),
		StructField("picture_url", StringType(), False),
		StructField("geo", StructType([
			StructField("latitude", FloatType(), False),
			StructField("longitude", FloatType(), False)
		]), False)
	])
	df = df.selectExpr("CAST(value AS STRING)").select(f.from_json(f.col("value"),schema).alias("data")).select("data.*")