# KafkaLite Terraform Deployment

This directory contains Infrastructure-as-Code to automatically provision a live cloud instance of your KafkaLite Enterprise cluster on AWS.

## Prerequisites
1. [Terraform](https://developer.hashicorp.com/terraform/downloads) installed locally.
2. [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) installed and configured with your credentials (`aws configure`).
3. An existing EC2 KeyPair in your AWS region.

## Deployment Instructions

1. **Initialize Terraform:**
   ```bash
   terraform init
   ```

2. **Deploy the cluster:**
   ```bash
   terraform apply -var="key_name=YOUR_AWS_KEYPAIR_NAME"
   ```
   *Terraform will show a plan. Type `yes` to confirm.*

3. **Get the live URL:**
   Once applied, Terraform will output the `public_ip` and `dashboard_url`.
   Wait ~3 minutes for the VM to boot and install everything, then open the `dashboard_url` in your browser.

4. **Get your Admin Token:**
   The `user_data` script automatically generates your admin token on the server. You can view it by either checking the EC2 System Log in the AWS console, or SSHing into the box:
   ```bash
   ssh -i your-key.pem ubuntu@<public_ip>
   cat /opt/kafkalite/admin_token.txt
   ```

## Teardown
When you are done testing, you can destroy everything to stop incurring AWS charges:
```bash
terraform destroy -var="key_name=YOUR_AWS_KEYPAIR_NAME"
```
