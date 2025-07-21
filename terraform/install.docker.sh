#!/bin/bash

# Define the log file path
LOG_FILE="/var/log/install_docker_jenkins.log"

# Redirect all stdout and stderr to the log file
# The 'tee' command also prints to stdout/stderr, which can be captured by Cloud Logging
exec > >(tee -a ${LOG_FILE}) 2>&1
echo "--- Starting Docker and Jenkins Installation Script ---"
date

# Update the apt package index, and install the latest version of Docker Engine and containerd, or go to the next step to install a specific version
echo "Running: sudo apt update"
sudo apt-get update
echo "sudo apt update finished."

# Install Docker Engine, containerd, and Docker Compose
echo "Installing Docker Engine, containerd, and Docker Compose..."
sudo apt-get install -y docker.io
echo "Docker CE installed."

# Check if Jenkins container is already running or exists
echo "Jenkins container starting now..."
sudo docker run -d \
  --name jenkins \
  --restart unless-stopped \
  --privileged \
  --user root \
  -p 8081:8080 \
  -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /usr/bin/docker:/usr/bin/docker \
  jenkins/jenkins:lts
echo "Jenkins container started."
echo "--- Script Finished ---"
date

