Write-Host '================================================='
Write-Host 'KafkaLite Cloud Deployment (DigitalOcean) - Windows Edition'
Write-Host '================================================='

if ([string]::IsNullOrWhiteSpace($env:DIGITALOCEAN_TOKEN)) {
    Write-Host 'Error: DIGITALOCEAN_TOKEN is not set.' -ForegroundColor Red
    Write-Host 'Please set your token and try again:'
    Write-Host '$env:DIGITALOCEAN_TOKEN="your-token-here"' -ForegroundColor Cyan
    exit 1
}

if (!(Get-Command terraform -ErrorAction SilentlyContinue)) {
    Write-Host 'Terraform not found. Please install Terraform for Windows from https://developer.hashicorp.com/terraform/downloads' -ForegroundColor Red
    exit 1
}

Set-Location terraform

Write-Host 'Initializing Terraform...' -ForegroundColor Green
terraform init

Write-Host 'Applying Terraform (this will provision your Droplet)...' -ForegroundColor Green
terraform apply -auto-approve -var="do_token=$env:DIGITALOCEAN_TOKEN"

Write-Host "`n================================================="
Write-Host 'Terraform provisioning complete!' -ForegroundColor Green
Write-Host "================================================="

$IP = terraform output -raw public_ip
$URL = terraform output -raw dashboard_url

Write-Host 'Waiting for the VM to finish setting up Docker and KafkaLite (this usually takes 3-5 minutes)...'
Write-Host "Polling $URL..."

$max_retries = 60
$count = 0
while ($count -lt $max_retries) {
    try {
        $response = Invoke-WebRequest -Uri $URL -UseBasicParsing -ErrorAction Stop
        break
    } catch {
        $count++
        Write-Host -NoNewline "."
        Start-Sleep -Seconds 5
    }
}

if ($count -ge $max_retries) {
    Write-Host "`nTimed out waiting for the UI to become available." -ForegroundColor Yellow
    Write-Host "The installation might still be running in the background. Check $URL manually in a few minutes."
    exit 1
}

Write-Host "`n================================================="
Write-Host 'KafkaLite is live!' -ForegroundColor Green
Write-Host "UI: $URL" -ForegroundColor Cyan
Write-Host 'To get your Admin Token, SSH into the box or check the user-data logs:'
Write-Host "   ssh root@$IP 'cat /opt/kafkalite/admin_token.txt'"
Write-Host "================================================="
