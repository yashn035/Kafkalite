# Cloudflare Free Public Tunnel

This setup allows you to expose both your **Frontend (UI)** and **Backend (API + WebSockets)** to the entire world through a single, free Cloudflare URL (`https://your-tunnel.trycloudflare.com`).

## How it works
1. We reconfigured your local `nginx` (running on port 3001) to act as a **Reverse Proxy**.
2. It serves your web files at the root (`/`).
3. It intercepts any API calls to `/api/` and silently forwards them to your `api-gateway` on port `8082`.
4. It intercepts any WebSocket connections to `/ws/` and forwards them to `8083`.
5. Because everything runs through port 3001 now, we only need to expose port 3001 to the internet!

## Usage

1. Restart your docker containers so the new Nginx proxy config takes effect:
   ```bash
   docker-compose up -d --build
   ```

2. Run the tunnel script (Windows):
   ```powershell
   .\start-tunnel.ps1
   ```

3. Look at the output in your terminal. You will see a URL that looks like `https://something-random.trycloudflare.com`.
4. **Copy that URL and send it to anyone.** They will be able to log in, view live metrics, and send messages directly to your machine!
