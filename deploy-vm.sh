#!/bin/bash
set -e

echo "🚀 Starting KafkaLite VM Deployment..."

# 1. Update the system and install dependencies
echo "📦 Installing dependencies..."
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose make git curl ufw openssl

sudo systemctl enable docker
sudo systemctl start docker

# 2. Clone the repository (if not already cloned)
REPO_URL="https://github.com/yashn035/Kafkalite.git"
CLONE_DIR="Kafkalite"

if [ ! -d "$CLONE_DIR" ]; then
    echo "📥 Cloning repository..."
    git clone "$REPO_URL"
fi

cd "$CLONE_DIR"

# 3. Generate a random JWT secret and set up .env
echo "🔐 Generating JWT Secret..."
JWT_SECRET=$(openssl rand -base64 32)
echo "JWT_SECRET=$JWT_SECRET" > .env

# 4. Configure UFW to allow ports
echo "🛡️ Configuring UFW firewall..."
sudo ufw allow 3001/tcp  # Web UI
sudo ufw allow 8086/tcp  # API Gateway
sudo ufw allow 8083/tcp  # WebSocket metrics
sudo ufw allow 9092/tcp  # Broker 0
sudo ufw allow 9093/tcp  # Broker 1
sudo ufw allow 9094/tcp  # Broker 2
sudo ufw allow 8080/tcp  # Prometheus metrics
sudo ufw allow 8081/tcp  # Health checks
sudo ufw allow ssh       # Ensure SSH isn't blocked
echo "y" | sudo ufw enable

# 5. Build and start all services
echo "🐳 Building and starting Docker containers..."
sudo docker-compose up -d --build

# 6. Wait for health checks
echo "⏳ Waiting for broker-0 health check to pass..."
max_retries=30
count=0
until curl -s -f http://localhost:8081/health > /dev/null; do
    if [ "$count" -ge "$max_retries" ]; then
        echo "❌ Health check failed after 30 seconds."
        exit 1
    fi
    count=$((count+1))
    echo "Waiting... ($count/$max_retries)"
    sleep 1
done
echo "✅ Health checks passed! All systems operational."

# 7. Print the public IP and instructions
PUBLIC_IP=$(curl -s ifconfig.me || curl -s icanhazip.com || echo "localhost")
echo ""
echo "=============================================================="
echo "🎉 KafkaLite Deployment Successful!"
echo "=============================================================="
echo "🌐 Access your Enterprise Dashboard here:"
echo "👉 http://$PUBLIC_IP:3001"
echo ""
echo "🔑 To generate an Admin JWT token, run the following command:"
echo "   sudo docker-compose exec api-gateway go run cmd/auth-cli/main.go --username admin --role admin"
echo ""
echo "   (Or if Go is installed on the host):"
echo "   go run cmd/auth-cli/main.go --username admin --role admin"
echo "=============================================================="
