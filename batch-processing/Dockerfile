# Dockerfile for batch processing job
# This Dockerfile sets up a Python environment with Java 17 for running the batch processing job
# It installs necessary dependencies and copies the job script into the container
#images name: dongtd6/airflow-job-scripts
FROM python:3.8-slim
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
WORKDIR /job
COPY requirements.txt .
COPY ./jars ./jars
COPY ./*.py ./
RUN pip install --no-cache-dir -r requirements.txt


#CMD ["python", "extract_job.py"]

#docker build -t dongtd6/airflow-job-scripts .
#docker push dongtd6/airflow-job-scripts:latest
