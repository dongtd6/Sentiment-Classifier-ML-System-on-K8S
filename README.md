# Sentiment Classifier ML System on K8S 

## Introduction

This project develops a Text Sentiment Classifier using FastAPI for the model serving API, built with pandas, scikit-learn, and joblib.

Our MLOps approach automates the entire lifecycle:

CI/CD: We use Jenkins and Terraform for infrastructure provisioning, with Helm for deploying Docker images onto Kubernetes (K8s).

Monitoring & Observability: The system leverages ELK (Elasticsearch, Logstash, Kibana) for centralized logging, Prometheus for metrics, Grafana for visualization, and Jaeger for distributed tracing. Nginx acts as an Ingress controller, routing traffic to our services.

This ensures a robust, scalable, and fully observable AI system.

## Overall System Architecture

<div style="text-align: center;"> <img src="images/text-sentiment-classifier-2.png" style="width: 1188px; height: auto;"></div>

# Table of Contents
[Overall System Architecture](#overall-system-architecture)
1. [Text Sentiment Classifier](#text-sentiment-classifier)  
   1.1 [Introduction](#introduction)  
   1.2 [Project Structure](#project-structure)  
2. [Local](#local)  
   2.1 [Demo](#demo)  
   2.2 [Running in Docker](#running-in-docker)  
3. [Cloud](#cloud)  
   3.1 [Deploying to GCP](#deploying-to-gcp)  
   3.2 [CICD with Jenkins for GCE](#cicd-with-jenkins-for-gce)  
   3.3 [Monitoring](#monitoring)  
   3.4 [Tracing](#tracing)  
   3.5 [Logging](#logging)
4. [Data](#data)

   4.1 [Batch Processing](#batch-processing)  
   4.2 [Stream Processing](#stream-processing)  

## Project Structure
```txt
├── terraform                 - Directory for Terraform to build GKE
├── helm-charts               - Directory for Helm chart to deploy the application
├── app                       - Python script for the application
├── model                     - Directory for model files
├── tests                     - Pytest code 
├── notebooks                 - Notebook to build model
├── config                    - Config file for chart deloy
├── dockerfile                - Dockerfile for build custom image
├── dags                      - Directory for image files
├── Jenkinsfile               - Jenkins pipeline script to describe the CI/CD process
├── docker-compose.yaml       - Docker Compose configuration file
├── Dockerfile                - Dockerfile for building the image
├── requirements.txt          - Python requirements file
├── images                    - Directory for image files
└── README.md                 - This README file
```

# LOCAL
## Demo 

### Running in docker-compose

```bash
docker-compose up --build
```
The service can be accessed via http://localhost:30001/docs


# CLOUD
## Deploying to GCP


### Install Terraform
https://computingforgeeks.com/how-to-install-terraform-on-ubuntu/
```
wget https://releases.hashicorp.com/terraform/1.5.6/terraform_1.5.6_linux_amd64.zip
unzip terraform_1.5.6_linux_amd64.zip
sudo mv terraform /usr/local/bin/
terraform -version
```
<div style="text-align: center;"> <img src="images/terraform-install.png" style="width: 888px; height: auto;"></div>

### Install Google Cloud CLI
https://cloud.google.com/sdk/docs/install
```shell
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
tar -xf google-cloud-cli-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
```
<div style="text-align: center;"> <img src="images/google-cloud-cli-nstall.png" style="width: 888px; height: auto;"></div>


### Authenticate with GCP
```shell
gcloud auth application-default login
```

### Edit variables.tf with config you need

### Provision a new cluster by Terraform
<div style="text-align: center;"> <img src="images/terraform-apply.png" style="width: 888px; height: auto;"></div>

```shell
cd terraform
terraform init
terraform plan
terraform apply
cd ..
```
### Deploy NGINX-ingress
<div style="text-align: center;"> <img src="images/nginx-ingress-install.png" style="width: 888px; height: auto;"></div>
```shell
kubectl create ns nginx-system
helm upgrade --install nginx-ingress ./helm-charts/nginx-ingress -n nginx-system
```
### Update host
- Replace the External IP above in `spec/rules/host` in file `helm-charts/model-deployment/templates/nginx-ingress.yaml`
<div style="text-align: center;"> <img src="images/nginx-ingress-external-ip.png" style="width: 888px; height: auto;"></div>
```shell
kubectl get svc -n nginx-system
```
### Deploy Model
<div style="text-align: center;"> <img src="images/nginx-update-config.png" style="width: 888px; height: auto;"></div>
```shell
helm upgrade --install tsc ./helm-charts/model-deployment/ --namespace model-serving --create-namespace
```
### Get IP of nginx ingress service
<div style="text-align: center;"> <img src="images/get-ip-of-nginx-ingress-service.png" style="width: 888px; height: auto;"></div>

```bash
kubectl get svc -n nginx-system
```

The service can be accessed via `http://[INGRESS_IP_ADDRESS].nip.com/docs`
<div style="text-align: center;"> <img src="images/service-can-be-access-over-nip-com.png" style="width: 888px; height: auto;"></div>



## CICD with Jenkins

### Get Jenkins VM IP   
<div style="text-align: center;"> <img src="images/jenkins-node-ip.png" style="width: 888px; height: auto;"></div>
```bash
gcloud compute instances list --format="table(name,zone,networkInterfaces[0].accessConfigs[0].natIP:label=EXTERNAL_IP,status)"
```

### SSH to VM and get Jenkins password
<div style="text-align: center;"> <img src="images/jenkins-password-docker.png" style="width: 888px; height: auto;"></div>
```bash
ssh your-jenkins-vm-ip
sudo docker exec -it jenkins sh
cat /var/jenkins_home/secrets/initialAdminPassword
```

### Copy passsword & login with it at http://external-ip-of-your-instance:8081
<div style="text-align: center;"> <img src="images/jenkins-login.png" style="width: 888px; height: auto;"></div>
### 
<div style="text-align: center;"> <img src="images/jenkins-install-suggested-plugins.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-install-suggested-plugins-wait.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-getting-started.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-url.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-is-ready.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-welcome.png" style="width: 888px; height: auto;"></div>

### Intall plugin for Jenkins
Plugin for Jenkins: Docker, Docker Pipeline, Kubernetes plugin
<div style="text-align: center;"> <img src="images/jenkins-docker-plugin.png" style="width: 888px; height: auto;"></div>
### Config Jenkins connect to Git Hub
 https://github.com/settings/tokens
<div style="text-align: center;"> <img src="images/github-generate-new-token.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/github-generate-new-token-2.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/github-generate-new-token-3.png" style="width: 888px; height: auto;"></div>

 http://external-ip-of-your-instance:8081/manage/credentials/store/system/domain/_/newCredentials
 <div style="text-align: center;"> <img src="images/jenkins-create-github-credentials.png" style="width: 888px; height: auto;"></div>
### Config Jenkins connect to Docker Hub
 https://app.docker.com/settings/personal-access-tokens
<div style="text-align: center;"> <img src="images/docker-hub-generate-token.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-create-dockerhub-credentials.png" style="width: 888px; height: auto;"></div>
### Config Github Webhook to Jenkins
<div style="text-align: center;"> <img src="images/github-webhook-seting.png" style="width: 888px; height: auto;"></div>  
 http://external-ip-of-your-instance:8081/github-webhook/

### Install kubectl CLI on your computer
<div style="text-align: center;"> <img src="images/kubectx-kubens-install.png" style="width: 888px; height: auto;"></div>
```bash
curl -LO https://dl.k8s.io/release/v1.33.0/bin/linux/amd64/kubectl
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

sudo git clone https://github.com/ahmetb/kubectx /opt/kubectx
sudo ln -s /opt/kubectx/kubectx /usr/local/bin/kubectx
sudo ln -s /opt/kubectx/kubens /usr/local/bin/kubens
```
### Connect to GKE Cluster
<div style="text-align: center;"> <img src="images/gke-cluster-commandline-access.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/gke-cluster-commandline-access-2.png" style="width: 888px; height: auto;"></div>

### Get Kubernetes URL
kubectl cluster-info | grep 'Kubernetes control plane' | awk '{print $NF}'
### Get CA Certificate
kubectl get secret jenkins-sa-token -n jenkins -o jsonpath='{.data.ca\.crt}' 
### Get token
kubectl get secret jenkins-sa-token -n jenkins -o jsonpath='{.data.token}' | base64 -d
### Add new Cloud on Jenkins
<div style="text-align: center;"> <img src="images/jenkins-cloud-item-gke-cluster.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-cloud-item-credential.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-cloud-item-save.png" style="width: 888px; height: auto;"></div>
### Add new Item on Jenkins
<div style="text-align: center;"> <img src="images/jenkins-create-new-item.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-create-new-item-2.png" style="width: 888px; height: auto;"></div>
### Push a commit to Github
<div style="text-align: center;"> <img src="images/github-push-a-commit.png" style="width: 888px; height: auto;"></div>
### View Build Executor Status on Jenkins
<div style="text-align: center;"> <img src="images/jenkins-executor-builder-status.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-console-output.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/jenkins-pipeline-overview.png" style="width: 888px; height: auto;"></div>


## Monitoring

**Monitoring with Prometheus and Grafana**

This setup guide provides the steps to deploy Prometheus and Grafana for monitoring CPU and node metrics on Google Kubernetes Engine (GKE). Follow the steps in sequence to ensure correct deployment within the monitoring namespace using Helm charts.

#### Create Monitoring Namespace

<div style="text-align: center;"> <img src="images/monitoring-name-space.png" style="width: 888px; height: auto;"></div>

```bash
kubectl create ns monitoring
```
#### Edit host on your computer to access service by domain

<div style="text-align: center;"> <img src="images/etc-host.png" style="width: 888px; height: auto;"></div>

```bash
sudo nano /etc/hosts
```
```
34.143.169.103 api.tsc.vn
34.143.169.103 grafana.tsc.vn
34.143.169.103 prometheus.tsc.vn
34.143.169.103 jaeger.tsc.vn
34.143.169.103 kibana.tsc.vn

34.143.169.103 airflow.tsc.vn
34.143.169.103 minio.tsc.vn
34.143.169.103 postgresql.tsc.vn
34.143.169.103 trino.tsc.vn

34.143.169.103 kafka.tsc.vn
34.143.169.103 debezium.tsc.vn
34.143.169.103 flink.tsc.vn
```

34.143.169.103 is IP of nginx ingress service, get it by command below and change it in hosts by your

```bash
kubectl get svc -n nginx-system
```

#### Apply ingress configs

```bash
kubectl apply -f helm-charts/ingress
```


### Prometheus

Deploy Prometheus Operator CRDs 

<div style="text-align: center;"> <img src="images/prometheus-operator-crds.png" style="width: 888px; height: auto;"></div>

```bash
helm upgrade --install prometheus-crds ./helm-charts/prometheus-operator-crds -n monitoring
```

Deploy Prometheus  
<div style="text-align: center;"> <img src="images/prometheus.png" style="width: 888px; height: auto;"></div>

```bash
helm upgrade --install prometheus ./helm-charts/prometheus -n monitoring
```

Prometheus Service can be accessed via `http://prometheus.tsc.vn`

<div style="text-align: center;"> <img src="images/prometheus-web.png" style="width: 888px; height: auto;"></div>

### Grafana

<div style="text-align: center;"> <img src="images/grafana.png" style="width: 888px; height: auto;"></div>

```bash
helm upgrade --install grafana ./helm-charts/grafana -n monitoring
```

Grafana Service can be accessed via `http://grafana.tsc.vn`
with user admin and password is admin

<div style="text-align: center;"> <img src="images/grafana-web.png" style="width: 888px; height: auto;"></div>

## Tracing

**Tracing with Jaeger**

### Jaeger

Install Jaeger

<div style="text-align: center;"> <img src="images/jaeger-tracing-helm.png" style="width: 888px; height: auto;"></div>
```
kubectl create ns tracing
cd ./helm-charts/jaeger
helm dependency build
cd ../..
helm upgrade --install jaeger-tracing ./helm-charts/jaeger-all-in-one -n tracing
```

Jaeger query can be accessed via `http://jaeger.tsc.vn`

<div style="text-align: center;"> <img src="images/jaeger-query-web.png" style="width: 888px; height: auto;"></div>


## Loging

**Logging with ELK**

<div style="text-align: center;"> <img src="images/elk.png" style="width: 888px; height: auto;"></div>

```shell
helm repo add elastic https://helm.elastic.co
helm repo update
```

### Elasticsearch

```shell
helm upgrade --install elasticsearch elastic/elasticsearch -f ./helm-charts/elk/values-elasticsearch.yaml --version 8.5.1 -n logging
```

### Logstash
```shell
helm upgrade --install logstash elastic/logstash -f ./helm-charts/elk/values-logstash.yaml --version 8.5.1 -n logging
```

### Filebeat
```shell
helm upgrade --install filebeat elastic/filebeat -f ./helm-charts/elk/values-filebeat.yaml --version 8.5.1 -n logging
```

### Kibana

```shell
helm upgrade --install kibana elastic/kibana -f ./helm-charts/elk/values-kibana.yaml --version 8.5.1 -n logging
```

Kibana can be accessed via `http://kibana.tsc.vn` and login with password in values-elasticsearch.yaml 
or get by this command:
```bash
kubectl get secrets --namespace=logging elasticsearch-master-credentials -ojsonpath='{.data.password}' | base64 -d
```

<div style="text-align: center;"> <img src="images/kibana-password.png" style="width: 888px; height: auto;"></div>

<div style="text-align: center;"> <img src="images/kibana-web-login-elastic.png" style="width: 888px; height: auto;"></div>

<div style="text-align: center;"> <img src="images/kibana-homepage.png" style="width: 888px; height: auto;"></div>

<div style="text-align: center;"> <img src="images/kibana-log-event.png" style="width: 888px; height: auto;"></div>

<div style="text-align: center;"> <img src="images/kibana-web-stream.png" style="width: 888px; height: auto;"></div>


# DATA PROCESSING

[Request edit host](#edit-host-on-your-computer-to-access-service-by-domain)

## Batch Processing

- Apply ingress

```bash
k create namespace storage &&
kubectl apply -f ./helm-charts/ingress 
```

### Source Systems

#### Postgresql
- Install Postgresql
```bash
helm upgrade --install postgresql ./helm-charts/postgresql -f ./helm-charts/postgresql/auth-values.yaml --namespace storage --create-namespace
```
### Storage:
#### MinIO
- Install MinIO
```bash
helm upgrade --install minio-operator ./helm-charts/minio-operator --n storage 

```
```bash
helm upgrade --install minio-tenant ./helm-charts/minio-tenant  -f ./helm-charts/minio-tenant/override-values.yaml -n storage
```
- Login minio.tsc.vn with user name: minio and password: minio123

<div style="text-align: center;"> <img src="images/minio-homepage.png" style="width: 888px; height: auto;"></div>

<div style="text-align: center;"> <img src="images/minio-bronze-data.png" style="width: 888px; height: auto;"></div>

#### Trino & Hive Metastore

- Create minio secret for Hive Metastore access
```bash
kubectl create secret generic minio-credentials \
  --from-file=access-key=config/s3/access-key.properties \
  --from-file=secret-key=config/s3/secret-key.properties \
  -n storage
```
- Create database for Hive Metastore
=> Access postgresql pod with password in /helm-chart/postgresql/auth-values.yaml (pg123)
```bash
kubectl exec -it postgresql-0 -n storage -- psql -U pgadmin -d postgres
```
- Run SQL Command to create Database
```sql
CREATE DATABASE hive;
CREATE DATABASE crm_db;
```
<div style="text-align: center;"> <img src="images/dbeaver-postgresql.png" style="width: 888px; height: auto;"></div>

- Install Hive Metastore & Trino
```bash
helm upgrade --install olap ./helm-charts/olap -n storage
```
- Access trino.tsc.vn with user name is admin
<div style="text-align: center;"> <img src="images/trino-homepage.png" style="width: 888px; height: auto;"></div>

#### Initialize data

- Forward port postgresql and minio to local
```bash
kubectl port-forward svc/minio-tenant-hl 9000:9000 -n storage
kubectl port-forward svc/postgresql 5432:5432 -n storage
```
- Init data:
```bash
cd ./helm-charts/postgresql/initdata && python inputdata.py
```
<div style="text-align: center;"> <img src="images/init-data.png" style="width: 888px; height: auto;"></div>

### Pipeline Orchestration: 
#### Airflow on GKE
- Install Airflow
```bash
helm upgrade --install airflow ./helm-charts/airflow -f ./helm-charts/airflow/override-values.yaml --namespace orchestration --create-namespace
```
- Access airflow.tsc.vn with user name:admin and password: admin (in ./helm-charts/airflow/values.yaml > webserver/defaultUser/username + password)
<div style="text-align: center;"> <img src="images/airflow-homepage.png" style="width: 888px; height: auto;"></div>

#### Schedule Job Script

- Python file ".dags/airflow_dag.py" is sync with Airflow over gitSync (config in ./helm-charts/airflow/override-values.yaml)
<div style="text-align: center;"> <img src="images/airflow-dags.png" style="width: 888px; height: auto;"></div>

- Docker image dongtd6/airflow-job-scripts is use by airflow_dag.py for job script (batch-processing/Dockerfile)
<div style="text-align: center;"> <img src="images/bronze-job-py.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/silver-job-py.png" style="width: 888px; height: auto;"></div>
<div style="text-align: center;"> <img src="images/gold-job-py.png" style="width: 888px; height: auto;"></div>
- Access trino.tsc.vn (user name is admin) over Dbeaver
<div style="text-align: center;"> <img src="images/dbeaver-trino.png" style="width: 888px; height: auto;"></div>

## Stream Processing

- Create secret
```shell

kubectl create namespace infrastructure

kubectl create secret generic postgres-credentials \
  --from-file=config/postgres/postgres-credentials.properties \
  -n infrastructure &&

kubectl create secret generic minio-credentials \
  --from-file=access-key=config/s3/access-key.properties \
  --from-file=secret-key=config/s3/secret-key.properties \
  -n infrastructure &&

kubectl create secret generic minio-credentials \
  --from-file=access-key=config/s3/access-key.properties \
  --from-file=secret-key=config/s3/secret-key.properties \
  -n processor

kubectl create secret generic telegram-secrets \
  --from-literal=bot-token=<your-telegram-bot-token> \
  --from-literal=chat-id=<your-telegram-chat-id> \
  -n infrastructure
```

### Postgresql

- Postgresql Config

```SQL
CREATE PUBLICATION debezium FOR TABLE product_reviews;
ALTER ROLE pgadmin WITH REPLICATION;
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4 ;
ALTER SYSTEM SET max_wal_senders = 4;
```
- Check & test config

```sql 
SHOW wal_level; -- must be 'logical'
SELECT pg_current_wal_lsn();
SELECT * FROM pg_publication;
SELECT slot_name, active, active_pid FROM pg_replication_slots;
SELECT * FROM pg_publication_tables WHERE pubname = 'debezium';
SELECT * FROM pg_stat_replication;

INSERT INTO product_reviews (review_id, review, created_at) VALUES ('test-123', 'Test review 123', CURRENT_TIMESTAMP);
INSERT INTO product_reviews (review_id, created_at, updated_at, product_id, user_id, review, source) VALUES ('13546f11-1070-1d1b-a080-d6b901062ff9',CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,'PRD368','USR368','Sản phẩm dùng không được', 'ZALO');

```
<div style="text-align: center;"> <img src="images/postgresql-config.png" style="width: 888px; height: auto;"></div>

- Restart Postgresql
```bash
kubectl exec -it postgresql-0 -n storage -- pg_ctl restart
```

### Kafka

- Install Strimzi Kafka Operator in the `operators` namespace:
=> use custom image in ./helm-charts/strimzi-kafka-operator/values.yaml > kafkaConnect: image: dongtd6/kafka-connectors-customize:latest
=> image build by ./dockerfile/Dockerfile-kafka-conect

```bash
curl -sL https://github.com/operator-framework/operator-lifecycle-manager/releases/download/v0.20.0/install.sh | bash -s v0.20.0
kubectl apply -f ./helm-charts/strimzi-kafka-operator/strimzi-kafka-operator.yaml 
helm upgrade --install strimzi-kafka-operator ./helm-charts/strimzi-kafka-operator --namespace infrastructure
```
<div style="text-align: center;"> <img src="images/kafka-dashboard.png" style="width: 888px; height: auto;"></div>

- Create Debezium Kafka connector
```bash
kubectl apply -f ./helm-charts/strimzi-kafka-operator/postgres-connector.yaml
```

<div style="text-align: center;"> <img src="images/cdc-product-review-topic.png" style="width: 888px; height: auto;"></div>

### Flink

- Install Flink Operator
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.12.1/
kubectl apply -f ../helm-charts/flink-kubernetes-operator/cert-manager.yaml
helm install flink-kubernetes-operator ./helm-charts/flink-kubernetes-operator -n infrastructure
```
- Run jobs
```bash
kubectl apply -f ./helm-charts/flink-kubernetes-operator/flink-sentiment-job.yaml
kubectl apply -f ./helm-charts/flink-kubernetes-operator/flink-telegram-job.yaml
```
<div style="text-align: center;"> <img src="images/message-queue-topic.png" style="width: 888px; height: auto;"></div>

- Message will send to Telegram after new review received
<div style="text-align: center;"> <img src="images/telegram-alert.png" style="width: 888px; height: auto;"></div>

