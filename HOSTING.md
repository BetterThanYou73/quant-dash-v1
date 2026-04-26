# Hosting QuantDash (Phase 1)

End-to-end procedure for getting `https://your-app.example` self-reliant on
free-tier services. Tested mental model: **Render** for the API + worker,
**Cloudflare Pages** (or GitHub Pages) for the static frontend, **GitHub
Actions** as the cron driver.

## Architecture

```
[ Browser ]                  static HTML+JS+CSS         (Cloudflare Pages / GH Pages)
     |  fetch()
     v
[ FastAPI ] ── reads ──> cache/market_data.pkl          (Render Web Service, 512 MB)
                              ^
                              | pickle file
[ Worker ] ── writes ──> cache/market_data.pkl          (Render Cron Job OR Render Background Worker)
     |
     | pulls
     v
[ Yahoo Finance via yfinance ]                          (free, no key)
```

Two processes share one volume. The API never touches Yahoo at request time;
that is the worker's job. This is the property that lets us survive on free
tiers without rate-limit pain.

---

## A. Backend on Render (free tier)

1. **Push the repo** to GitHub (already done).
2. Go to **render.com → New → Web Service**.
   - Connect the repo.
   - Build command:    `pip install -r requirements.txt`
   - Start command:    `uvicorn backend.main:app --host 0.0.0.0 --port $PORT --workers 2`
   - Instance type:    `Free` (512 MB) — fine for Phase 1.
   - Add a **Disk** (1 GB free): mount path `/opt/render/project/src/cache`. This is where the worker writes the pickle.
3. **Environment variables**:
   - `PYTHON_VERSION` = `3.12.1`
   - (none required for Phase 1; add `ALLOWED_ORIGIN` later when you lock CORS down)
4. After deploy, hit `https://your-api.onrender.com/api/health` — should return 200.

### Worker as a Render Cron Job

5. Render → **New → Cron Job** (free).
   - Schedule: `0 */6 * * *` (every 6 hours; Yahoo data is slow-moving)
   - Command:  `python -m core.workers`
   - Use the **same disk** as the web service.

> Free-tier note: Render free Web Services sleep after 15 minutes of idle.
> The first request after sleep is slow (~30s cold start). For a real demo
> use a paid `$7/mo` Starter to keep it warm, or set up a 5-minute
> external pinger (UptimeRobot is free).

---

## B. Frontend on Cloudflare Pages

1. **cloudflare.com → Pages → Connect to Git → pick the repo**.
2. Build settings:
   - Framework preset: `None`
   - Build command:    *(leave blank)*
   - Output directory: `web`
3. After first deploy you get `https://your-app.pages.dev`.
4. Open `web/config.js` and set:
   ```js
   window.QD_CONFIG = { API_BASE: "https://your-api.onrender.com" };
   ```
   Commit; Pages auto-redeploys.

### GitHub Pages alternative

If you'd rather use Pages: Settings → Pages → Source = `main` branch,
folder = `/web`. Same `config.js` edit applies.

---

## C. Lock down CORS (5 min, after the URLs are known)

In `backend/main.py`:
```python
allow_origins=["https://your-app.pages.dev"],   # not "*"
```
Push, redeploy. The dev experience stays the same locally because uvicorn
on `127.0.0.1` doesn't go through CORS.

---

## D. Quick local dev recipe (recap)

```powershell
# Terminal 1 — API
.\venv\Scripts\Activate.ps1
uvicorn backend.main:app --reload

# Terminal 2 — static frontend
python -m http.server 5500 --directory web

# Terminal 3 (one-off) — fill the cache
python -m core.workers
```

Open http://localhost:5500/ — `config.js` already points at
`http://localhost:8000`.

---

## E. Costs & limits cheat-sheet

| Service              | Free tier               | What we use it for          |
| -------------------- | ----------------------- | --------------------------- |
| Render Web Service   | 512 MB, sleeps idle     | FastAPI                     |
| Render Cron Job      | Always-on schedule      | `core.workers` every 6h     |
| Render Disk          | 1 GB                    | `cache/market_data.pkl`     |
| Cloudflare Pages     | Unlimited bandwidth     | static frontend             |
| Yahoo Finance        | No key, soft rate limit | the worker (not per-user!)  |
| GitHub Actions       | 2,000 min/mo            | optional cron alternative   |

GZip middleware (already enabled in `main.py`) keeps the wire payloads
small enough that even 50 dashboard refreshes/minute fit in the free tier.

---

## F. After Phase 2 (preview)

Phase 2 adds Postgres for persistent portfolios + GitHub OAuth. Hosting
stays the same on the API side; we'll add:

- **Neon** or **Supabase** Postgres (free 0.5 GB) — connection string
  goes into `DATABASE_URL` env var on Render.
- **GitHub OAuth app** — client id/secret as env vars.
- **HttpOnly cookies** for session — same backend, same domain via Render
  custom domain so cookies are first-party.

See README.md → "Phase 2 Plan" for the schema and migration approach.
