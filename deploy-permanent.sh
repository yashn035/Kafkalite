#!/bin/bash
set -e

echo "================================================="
echo "🌍 KafkaLite Permanent Cloud Deployment (DigitalOcean)"
echo "================================================="

if [ -z "$DIGITALOCEAN_TOKEN" ]; then
  echo "❌ Error: DIGITALOCEAN_TOKEN is not set."
  echo "Please get a token from https://cloud.digitalocean.com/account/api/tokens"
  echo "Then set it and try again:"
  echo 'export DIGITALOCEAN_TOKEN="your-token-here"'
  exit 1
fi

if [ -z "$JWT_SECRET" ]; then
  export JWT_SECRET=$(openssl rand -base64 32)
fi

cd terraform

echo "📦 Initializing Terraform..."
terraform init

echo "🚀 Applying Terraform to spin up Droplet (Ubuntu 22.04, s-2vcpu-2gb)..."
terraform apply -auto-approve

IP=$(terraform output -raw public_ip)

echo "⏳ Waiting for VM setup (Docker, Systemd, KafkaLite)..."
max_retries=60
count=0
until curl -s -f "http://$IP:3001" > /dev/null; do
    if [ "$count" -ge "$max_retries" ]; then
        echo "⚠️ Timed out waiting for UI. Check http://$IP:3001 manually later."
        break
    fi
    count=$((count+1))
    echo -n "."
    sleep 5
done

echo ""
echo "================================================="
echo "✅ PERMANENT LINK: http://$IP:3001"
echo "✅ Admin Token Command: ssh root@$IP 'cd /opt/kafkalite && go run cmd/auth-cli/main.go --username admin --role admin'"
echo "================================================="
echo "💡 Optional: Point an A record on your domain to $IP for a custom URL."
