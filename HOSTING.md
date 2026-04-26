# Hosting QuantDash on Heroku

End-to-end deploy procedure. Cost: $0 with GitHub Student Pack credits
(or ~$12/mo without). Domain: `quantdash.tech` (1 yr free via Student
Pack `.tech` perk).

## Architecture (one app, one URL)

```
                       https://quantdash.tech
                                |
                                v
                    +-----------+-----------+
                    |  Heroku web dyno      |
                    |  uvicorn FastAPI      |
                    |    /            -> StaticFiles (web/)
                    |    /api/*       -> routers
                    |    /docs        -> OpenAPI UI
                    +-----------+-----------+
                                |
                  reads pickle  |  writes pickle
                                v
                    +-----------+-----------+
                    |  Heroku Postgres mini |
                    |  market_data_cache    |
                    |    id=1 BYTEA blob    |
                    +-----------+-----------+
                                ^
                                | python -m core.workers --once --task=...
                    +-----------+-----------+
                    |  Heroku Scheduler     |
                    |  daily / hourly cron  |
                    +-----------------------+
```

Three pieces, all in one Heroku app:
- **web dyno** serves both the API (`/api/*`) and the static frontend (`/`)
- **Postgres** stores the worker's pickle blob (dyno filesystem is ephemeral)
- **Scheduler** runs the cron jobs that keep the cache fresh

The repo already contains the files Heroku needs:
- `Procfile` — declares `web` and `release` process types
- `runtime.txt` — pins Python 3.12
- `app.json` — declarative add-ons + env vars (used by review apps)
- `requirements.txt` — includes `gunicorn` and `psycopg[binary]`
- `core/data_engine.py` — auto-switches to Postgres when `DATABASE_URL` is set

---

## A. First-time deploy

### 1. Install the CLI and log in
```powershell
# (Windows) install via the official MSI from devcenter.heroku.com/articles/heroku-cli
heroku login
```

### 2. Create the app
```powershell
cd D:\Projects\quant-dash-v1
heroku create quantdash --region us
heroku stack:set heroku-22 --app quantdash
```
> If `quantdash` is taken, try `quantdash-app` or `quantdash-mj`. Doesn't
> matter — we'll mask it with the custom domain in step 5.

### 3. Add Postgres + Scheduler
```powershell
heroku addons:create heroku-postgresql:mini --app quantdash
heroku addons:create scheduler:standard --app quantdash
```
`mini` is $5/mo (covered by Student Pack credits), 10K rows / 1 GB —
ample for a single BYTEA blob.

### 4. Push the code
```powershell
git push heroku feat/web-autocomplete-and-charts:main
```
The `release` phase in `Procfile` runs
`python -m core.workers --once --task=daily` automatically — your cache is
populated **before** the new web dyno takes traffic. First deploy will
take 5–8 minutes because of the full S&P 500 fetch.

### 5. Custom domain
```powershell
heroku domains:add quantdash.tech --app quantdash
heroku domains:add www.quantdash.tech --app quantdash
heroku certs:auto:enable --app quantdash
```
Heroku prints a `dns-target` like `xyz.herokudns.com`. In your `.tech`
registrar's DNS panel:
- `CNAME  www  →  xyz.herokudns.com`
- For the apex `quantdash.tech` use either `ALIAS`/`ANAME` if your
  registrar supports it, or move DNS to Cloudflare (free) and use a
  flattened CNAME.

SSL via Let's Encrypt is automatic once DNS resolves. Hit
`https://quantdash.tech` after ~10 minutes.

### 6. Open the Scheduler dashboard and add jobs
```powershell
heroku addons:open scheduler --app quantdash
```
Add two jobs (UTC times):

| Frequency | Command | Notes |
|---|---|---|
| Daily, `22:00 UTC` | `python -m core.workers --once --task=daily` | After US close (5pm ET). Full S&P 500 EOD pull. |
| Hourly | `python -m core.workers --once --task=intraday` | ~10 macro/regime tickers. Cheap. |

That's it. The dashboard is live.

---

## B. Why this layout

### Same-origin (no CORS)

`backend/main.py` mounts the static frontend at `/` after all `/api/*`
routers. The browser fetches `/api/signals` against the same host that
served `index.html`, so there is no preflight, no CORS headers, and
cookies will Just Work for Phase 2 auth. `web/config.js` autodetects
this — no manual URL editing per environment.

### Postgres-backed cache

Heroku dyno filesystems are **ephemeral**. Anything written to `cache/`
disappears the next time the dyno restarts (at minimum every 24 h, often
more). So `core/data_engine.py` checks for `DATABASE_URL` and, when set,
reads/writes the pickle as a single BYTEA row in the
`market_data_cache` table. Locally there's no `DATABASE_URL`, so it
falls back to the file backend with no config changes.

The schema is created on first use:
```sql
CREATE TABLE market_data_cache (
    id INTEGER PRIMARY KEY,           -- always 1; this is a singleton
    payload BYTEA NOT NULL,           -- pickle of the DataFrame
    updated_utc TIMESTAMPTZ NOT NULL,
    row_count INTEGER NOT NULL,
    col_count INTEGER NOT NULL
);
```
Phase 2 will add `users`, `portfolios`, `positions`, `transactions`
alongside this — same database, same connection string.

### Refresh cadence (accurate, not arbitrary)

- **`--task=daily`** — full S&P 500 history. Run **once a day** at 22:00 UTC,
  right after the US market closes. EOD bars don't change again until
  tomorrow's close, so a second run before then is wasted Yahoo bandwidth.
- **`--task=intraday`** — macro/regime symbols (`SPY, QQQ, VIX, DXY, TLT,
  GLD, USO, HYG, LQD`). Run **hourly** for live-ish regime + macro cards.
- **`--task=quotes_warm`** — reserved for Phase 2 if we want sub-minute
  watchlist updates without hitting yfinance per-request.

If Yahoo ever rate-limits us, the mitigation order is:
  1. Drop intraday to every 2 h.
  2. Drop daily to once every other day (factors are 60-day rolling).
  3. Switch to a paid data feed (Alpaca free tier, Polygon basic, or
     Tiingo) — all have reliable Python clients and ~free Phase 1 limits.

---

## C. Day-2 operations

### Tail logs
```powershell
heroku logs --tail --app quantdash
```

### Manual cache refresh
```powershell
heroku run "python -m core.workers --once --task=daily" --app quantdash
```

### Connect to Postgres
```powershell
heroku pg:psql --app quantdash
\d market_data_cache
SELECT id, updated_utc, row_count, col_count, octet_length(payload) FROM market_data_cache;
```

### Roll back a bad deploy
```powershell
heroku releases --app quantdash
heroku rollback v42 --app quantdash
```

### Lock down CORS (after Phase 2 auth lands)
```powershell
heroku config:set ALLOWED_ORIGIN=https://quantdash.tech --app quantdash
```

---

## D. Local dev (unchanged)

```powershell
.\venv\Scripts\Activate.ps1

# Terminal 1 — API + frontend on one port
uvicorn backend.main:app --reload
# → open http://127.0.0.1:8000

# (optional) Terminal 2 — separate static server on :5500 if you want
python -m http.server 5500 --directory web

# Terminal 3 (one-off) — fill the cache
python -m core.workers --once --task=daily
```

`config.js` autodetects: on `:5500` it points to `:8000`; on `:8000` (or
in production) it uses relative URLs.

---

## E. Costs

| Item | Cost | Covered by Student Pack? |
|---|---|---|
| `quantdash.tech` domain (1 yr) | $0 | yes — `.tech` perk |
| Heroku Basic web dyno | $7/mo | yes — student credits |
| Heroku Postgres mini | $5/mo | yes — student credits |
| Heroku Scheduler | $0 | yes — free add-on |
| Let's Encrypt SSL | $0 | yes — Heroku Automated Certs |
| **Total** | **$0** | for first year |

---

## F. After Phase 2 (preview)

- GitHub OAuth via `authlib` → cookie session on the same Heroku domain
- Same Postgres database adds `users`, `portfolios`, `positions`,
  `transactions`, `snapshots` tables
- New page at `/portfolio` (separate static page, served by the same
  StaticFiles mount)
- Migration: the localStorage `qd.portfolio.v1` payload POSTs to
  `/api/portfolio/import` on first sign-in, server creates rows, frontend
  clears local copy.
