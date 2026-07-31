$ErrorActionPreference = 'Stop'

Write-Host "================================================="
Write-Host "🌍 KafkaLite Cloudflare Tunnel (Free Public URL)"
Write-Host "================================================="

if (!(Get-Command cloudflared -ErrorAction SilentlyContinue)) {
    Write-Host "⚙️ cloudflared not found. Installing via winget..." -ForegroundColor Yellow
    winget install Cloudflare.cloudflared
    Write-Host "✅ Installation complete! Please restart your PowerShell and run this script again." -ForegroundColor Green
    exit 0
}

Write-Host "🚀 Starting Cloudflare Tunnel on port 3001..." -ForegroundColor Green
Write-Host "You will see a link ending in 'trycloudflare.com'. Share this with anyone!" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop the tunnel.`n"

cloudflared tunnel --url http://localhost:3001
