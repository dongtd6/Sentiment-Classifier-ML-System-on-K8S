# Dockerfile for batch processing job
# This Dockerfile sets up a Python environment with Java 17 for running the batch processing job
# It installs necessary dependencies and copies the job script into the container
FROM python:3.8-slim

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements and job script
WORKDIR /job
COPY requirements.txt .
COPY ./jars ./jars
COPY ./*.py ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


#CMD ["python", "extract_job.py"]

#docker build -t dongtd6/airflow-job-scripts .
#docker push dongtd6/airflow-job-scripts:latest
