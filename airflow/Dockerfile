FROM puckel/docker-airflow:1.10.9

USER root

# Create a virtual environment
RUN python -m venv /usr/local/airflow/venv

# Activate the virtual environment
SHELL ["/bin/bash", "-c"]
RUN source /usr/local/airflow/venv/bin/activate

# Switch back to the airflow user
USER airflow

COPY requirements.txt /usr/local/airflow/requirements.txt
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt