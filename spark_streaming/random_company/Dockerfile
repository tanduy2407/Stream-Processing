FROM bitnami/spark:3
WORKDIR /app
COPY spark_streaming/*.jar /app
ENV NAMESPACE=beer/random_beer
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0", \
     "spark_streaming.py", "kafka_streaming", "kafka1:19092", "${NAMESPACE}"]