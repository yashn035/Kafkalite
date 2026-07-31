output "public_ip" {
  description = "The public IP address of the KafkaLite cluster Droplet"
  value       = digitalocean_droplet.kafkalite.ipv4_address
}

output "dashboard_url" {
  description = "The URL to access the Enterprise Dashboard"
  value       = "http://${digitalocean_droplet.kafkalite.ipv4_address}:3001"
}

output "admin_token_instruction" {
  description = "Instructions for getting the admin token"
  value       = "Run this command on the droplet (or check user-data logs): cd /opt/kafkalite && docker-compose exec -T api-gateway go run cmd/auth-cli/main.go --username admin --role admin"
}
