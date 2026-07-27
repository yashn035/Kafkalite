#!/bin/bash
# deploy-vm.sh: Automates KafkaLite installation on a fresh Ubuntu 22.04 VM.
set -e

REPO_URL="https://github.com/yashn035/Kafkalite.git"
INSTALL_DIR="/opt/Kafkalite"

echo "=== Step 1: Installing Prerequisites ==="
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg make git ufw

# Install Docker GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add Docker Apt repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Symlink docker-compose for Makefile compatibility
sudo ln -sf /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

echo "=== Step 2: Configuring UFW Firewall ==="
sudo ufw allow 22/tcp # SSH
sudo ufw allow 9092/tcp
sudo ufw allow 9093/tcp
sudo ufw allow 9094/tcp
sudo ufw allow 8080/tcp # Prometheus metrics broker-0
sudo ufw allow 8082/tcp # Prometheus metrics broker-1
sudo ufw allow 8084/tcp # Prometheus metrics broker-2
sudo ufw allow 8081/tcp # Health checks broker-0
sudo ufw allow 8083/tcp # Health checks broker-1
sudo ufw allow 8085/tcp # Health checks broker-2
sudo ufw --force enable

echo "=== Step 3: Cloning Repository ==="
if [ -d "$INSTALL_DIR" ]; then
    echo "Directory $INSTALL_DIR exists. Pulling latest code..."
    cd "$INSTALL_DIR"
    sudo git pull origin main
else
    echo "Cloning repository into $INSTALL_DIR..."
    sudo git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

echo "=== Step 4: Compiling Container Images ==="
sudo make docker-build

echo "=== Step 5: Configuring Systemd Service ==="
sudo tee /etc/systemd/system/kafkalite.service > /dev/null <<EOF
[Unit]
Description=KafkaLite Distributed Broker Cluster
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down -v
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafkalite.service
sudo systemctl start kafkalite.service

echo "=== Step 6: Verifying Cluster Health ==="
until curl -sf http://localhost:8081/health >/dev/null && \
      curl -sf http://localhost:8083/health >/dev/null && \
      curl -sf http://localhost:8085/health >/dev/null; do
    echo -n "."
    sleep 2
done
echo " Cluster is fully operational!"

PUBLIC_IP=$(curl -s https://api.ipify.org)
echo "============================================="
echo "   KAFKALITE CLOUD DEPLOYMENT COMPLETED"
echo "============================================="
echo "Public IP Address: $PUBLIC_IP"
echo "Exposed Client Ports:"
echo "  - Broker 0: $PUBLIC_IP:9092"
echo "  - Broker 1: $PUBLIC_IP:9093"
echo "  - Broker 2: $PUBLIC_IP:9094"
echo "Exposed Admin Health Ports:"
echo "  - Broker 0: http://$PUBLIC_IP:8081/health"
echo "  - Broker 1: http://$PUBLIC_IP:8083/health"
echo "  - Broker 2: http://$PUBLIC_IP:8085/health"
echo "Exposed Metrics Ports:"
echo "  - Broker 0: http://$PUBLIC_IP:8080/metrics"
echo "============================================="
