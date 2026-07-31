#!/bin/bash
set -e

echo "================================================="
echo "🌍 KafkaLite Cloud Deployment (DigitalOcean)"
echo "================================================="

if [ -z "$DIGITALOCEAN_TOKEN" ]; then
  echo "❌ Error: DIGITALOCEAN_TOKEN is not set."
  echo "Please set your token and try again:"
  echo 'export DIGITALOCEAN_TOKEN="your-token-here"'
  exit 1
fi

if [ -z "$JWT_SECRET" ]; then
  echo "⚠️ JWT_SECRET not set. Generating a strong random secret..."
  export JWT_SECRET=$(openssl rand -base64 32)
fi

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    echo "⚙️ Terraform not found. Attempting to install Terraform..."
    # OS agnostic basic check for Linux/Mac
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
        sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
        sudo apt-get update && sudo apt-get install terraform
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew tap hashicorp/tap
        brew install hashicorp/tap/terraform
    else
        echo "❌ Please install Terraform manually from https://developer.hashicorp.com/terraform/downloads"
        exit 1
    fi
fi

cd terraform

echo "📦 Initializing Terraform..."
terraform init

echo "🚀 Applying Terraform (this will provision your Droplet)..."
terraform apply -auto-approve

echo ""
echo "================================================="
echo "✅ Terraform provisioning complete!"
echo "================================================="

IP=$(terraform output -raw public_ip)
URL=$(terraform output -raw dashboard_url)

echo "⏳ Waiting for the VM to finish setting up Docker and KafkaLite (this usually takes 3-5 minutes)..."
echo "Polling $URL..."

max_retries=60
count=0
until curl -s -f "$URL" > /dev/null; do
    if [ "$count" -ge "$max_retries" ]; then
        echo "⚠️ Timed out waiting for the UI to become available."
        echo "The installation might still be running in the background. Check $URL manually in a few minutes."
        exit 1
    fi
    count=$((count+1))
    echo -n "."
    sleep 5
done

echo ""
echo "================================================="
echo "✅ KafkaLite is live!"
echo "🌐 UI: $URL"
echo "🔑 To get your Admin Token, SSH into the box or check the user-data logs:"
echo "   ssh root@$IP 'cat /opt/kafkalite/admin_token.txt'"
echo "================================================="
