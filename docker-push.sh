#!/bin/bash
# docker-push.sh: Automates building and pushing KafkaLite image tags to Docker Hub.
set -e

# Prompt for credentials if not supplied via env vars
if [ -z "$DOCKER_USERNAME" ]; then
    read -p "Enter Docker Hub Username: " DOCKER_USERNAME
fi

if [ -z "$DOCKER_PASSWORD" ]; then
    read -sp "Enter Docker Hub Password: " DOCKER_PASSWORD
    echo ""
fi

IMAGE_NAME="yashn035/kafkalite"

echo "=== Building Docker Images ==="
docker build -t ${IMAGE_NAME}:latest -t ${IMAGE_NAME}:v1.0.0 .

echo "=== Logging in to Docker Hub ==="
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

echo "=== Pushing Tag: latest ==="
docker push ${IMAGE_NAME}:latest

echo "=== Pushing Tag: v1.0.0 ==="
docker push ${IMAGE_NAME}:v1.0.0

echo "=== Docker Images successfully pushed to Docker Hub! ==="
