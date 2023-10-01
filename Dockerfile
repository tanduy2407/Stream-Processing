FROM bitnami/spark:3.3.2
WORKDIR /app
COPY ./jars ./jars
COPY spark_streaming.py spark_streaming.py
# CMD ["spark-submit", "--master", "local[2]", "spark_streaming.py"]