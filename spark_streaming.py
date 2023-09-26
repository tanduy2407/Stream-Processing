from pyspark.sql import SparkSession


# def create_spark_session():
# 	try:
# 		spark = SparkSession.builder.appName('streaming') \
# 			.config("spark.jars", "spark-sql-kafka-0-10_2.13-3.3.2.jar") \
# 			.getOrCreate()
# 	except Exception as err:
# 		print(err)
# 	return spark


# def read_stream_data(spark: SparkSession):
# 	topic = "random_names"
# 	kafka_bootstrap_server = "localhost:9092"
# 	try:
# 		df = spark \
# 			.readStream \
# 			.format("kafka") \
# 			.option("kafka.bootstrap.servers", kafka_bootstrap_server) \
# 			.option("subscribe", topic) \
# 			.load()
# 	except Exception as err:
# 		print(err)
# 	return df


# if __name__ == "__main__":
#     spark = create_spark_session()
#     df = read_stream_data(spark)

#     # Define a simple streaming query to print data to the console
#     query = df \
#         .writeStream \
#         .outputMode("append") \
#         .format("console") \
#         .start()

#     query.awaitTermination()

spark = SparkSession.builder.appName('streaming') \
			.getOrCreate()
topic = "random_names"
kafka_bootstrap_server = "localhost:9092"
df = spark \
			.readStream \
			.format("kafka") \
			.option("kafka.bootstrap.servers", kafka_bootstrap_server) \
			.option("subscribe", topic) \
			.load()
#     spark = create_spark_session()
#     df = read_stream_data(spark)

#     # Define a simple streaming query to print data to the console
query = df \
	.writeStream \
	.outputMode("append") \
	.format("console") \
	.start()

query.awaitTermination()