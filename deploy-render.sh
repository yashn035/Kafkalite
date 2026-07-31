#!/bin/bash
set -e

echo "================================================="
echo "☁️ KafkaLite Permanent Fallback Deployment (Render)"
echo "================================================="

if ! command -v render &> /dev/null; then
    echo "⚙️ Render CLI not found. Installing..."
    curl -sS https://render.com/install.sh | bash
fi

echo "🔑 Logging into Render..."
render login

echo "🚀 Deploying to Render using render.yaml..."
render deploy

echo ""
echo "================================================="
echo "✅ PERMANENT LINK: https://kafkalite-ui.onrender.com"
echo "================================================="
