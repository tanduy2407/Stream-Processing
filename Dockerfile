FROM bitnami/spark:3.3.2
WORKDIR /app
COPY spark_streaming.py spark_streaming.py
# CMD ["python3", "spark_streaming.py"]