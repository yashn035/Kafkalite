# 🚀 Getting a Permanent Live Link for Your Resume

Want to show off KafkaLite to recruiters with a link that never dies? We've provided two automated scripts to deploy the project to the cloud.

## Option 1: DigitalOcean (Recommended - Static IP, 24/7 Uptime)
This method provisions a Virtual Machine (Droplet) with a static IP and configures Systemd to automatically restart the containers if the server reboots.

### Prerequisites:
1. Sign up for a [DigitalOcean](https://www.digitalocean.com/) account (they usually offer $200 in free credits for 60 days).
2. Generate a Personal Access Token from the **API** section in their dashboard.

### Deployment Steps:
1. Open your terminal in the root of the project.
2. Export your token:
   ```bash
   export DIGITALOCEAN_TOKEN="your-secret-token-here"
   ```
3. Run the permanent deployment script:
   ```bash
   chmod +x deploy-permanent.sh
   ./deploy-permanent.sh
   ```
4. The script will automatically spin up the server, configure Docker and Systemd, and output your **Permanent Link** (e.g., `http://198.51.100.14:3001`).
5. Open `README.md` and replace `YOUR-IP-HERE` with the actual IP address generated!

---

## Option 2: Render.com (Fallback - 100% Free, Permanent URL)
If you don't want to use DigitalOcean, you can use Render.com's free tier. While the instance sleeps after 15 minutes of inactivity, the URL (e.g., `https://kafkalite.onrender.com`) is completely permanent.

### Deployment Steps:
1. Open your terminal in the root of the project.
2. Run the Render deployment script:
   ```bash
   chmod +x deploy-render.sh
   ./deploy-render.sh
   ```
3. Follow the CLI prompts to log in or sign up for Render.
4. The script will use `render.yaml` to deploy both the API Gateway and the Web UI.
5. Once complete, copy the `https://...onrender.com` link into your resume and update your `README.md`!

### 🧠 Pro-Tip: Avoid the Cold Start
Render's free tier sleeps after 15 minutes of inactivity. When a recruiter clicks your link, it might take 10-15 seconds to wake up.
To prevent this, sign up for [UptimeRobot](https://uptimerobot.com/) (Free Tier) and point it to your Render URL with a 5-minute ping interval. This keeps your application warm 24/7 for absolutely $0!
