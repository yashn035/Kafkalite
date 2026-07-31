#!/bin/bash
set -e

# Log all user_data output
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting Droplet setup for KafkaLite..."

apt-get update -y
apt-get install -y docker.io docker-compose make git curl golang

systemctl enable docker
systemctl start docker

git clone https://github.com/yashn035/Kafkalite.git /opt/kafkalite
cd /opt/kafkalite

# We will run the deploy-vm.sh from the repo to handle firewall and docker-compose
chmod +x deploy-vm.sh
./deploy-vm.sh

cat << 'EOF' > /etc/systemd/system/kafkalite.service
[Unit]
Description=KafkaLite Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/kafkalite
ExecStart=/usr/bin/docker-compose up -d
ExecStop=/usr/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

systemctl enable kafkalite
systemctl start kafkalite

echo "=============================================="
echo "GENERATING ADMIN TOKEN..."
# Generate token and save to file for terraform to fetch if needed
TOKEN=$(docker-compose exec -T api-gateway go run cmd/auth-cli/main.go --username admin --role admin)
echo "ADMIN_JWT_TOKEN=$TOKEN" > /opt/kafkalite/admin_token.txt
echo "=============================================="
echo "YOUR ADMIN TOKEN IS:"
echo "$TOKEN"
echo "=============================================="
