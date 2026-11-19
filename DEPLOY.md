# Deploy: backend on Render, frontend on Vercel, DB on MongoDB Atlas

```
Browser ──► Vercel (React dashboard)  ──REST + WebSocket──►  Render (FastAPI + scheduler)  ──►  MongoDB Atlas
```

## 1. MongoDB Atlas (free M0)
1. Create a cluster, a database user, and under **Network Access** allow `0.0.0.0/0` (Render IPs are not fixed).
2. Copy the connection string: `mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority`

## 2. Backend on Render
1. Push this repo (Farooq_Bot_Server) to GitHub.
2. Render → **New → Blueprint** → pick the repo (uses `render.yaml`), or **New → Web Service → Docker** with repo root.
3. **Use a paid always-on plan (Starter).** On the free plan the service sleeps and the 08:30 scan / 09:30 regime jobs never run.
4. Environment variables:

| Key | Value |
|---|---|
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | dashboard login (choose a strong password) |
| `JWT_SECRET` | long random string (Blueprint generates one) |
| `MONGO_URI` | Atlas string from step 1 |
| `ALLOW_MOCK_DB` | `false` |
| `FRONTEND_URL` | your Vercel URL, e.g. `https://farooq-bot.vercel.app` (no trailing slash) |
| `CORS_ORIGINS` | same Vercel URL |
| `TZ` | `Asia/Kolkata` |

5. After deploy, open `https://<service>.onrender.com/health` → should return `{"ok":true,"mock_db":false}`.

## 3. Frontend on Vercel
1. Vercel → **Add New → Project** → same repo, **Root Directory = `.` (repo root)** (framework Vite is auto-detected).
2. Environment variable: `VITE_API_URL` = `https://<service>.onrender.com` (no trailing slash). Redeploy after changing it.
3. Open the Vercel URL and log in. `vercel.json` already handles page refreshes on routes like `/charts`.
4. Go back to Render and make sure `FRONTEND_URL` / `CORS_ORIGINS` match the Vercel URL exactly (custom domain too, if you add one).

## 4. Keep it in paper mode
Default is paper trading. Don't add Upstox/Kite keys until the client has tested. When you do:
- Upstox/Kite redirect URL = `https://<service>.onrender.com/api/broker/upstox/callback` (or `/kite/callback`), also set `UPSTOX_REDIRECT_URI`.
- Indian brokers require a **static IP** for order APIs; Render's outbound IPs are shared, so for live trading move the backend to an India VPS with a fixed IP (same Docker image).

## Notes
- Render disk is ephemeral: everything (settings, trades, broker tokens) lives in Atlas, so redeploys are safe.
- yfinance can occasionally be rate-limited from cloud IPs; if scans show data errors, switch `DATA_PROVIDER=upstox` once keys exist.
- Local dev is unchanged: `.\start.ps1`.
