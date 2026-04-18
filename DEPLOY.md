# FITPAC Deployment Guide

This guide explains how to make the FITPAC dashboard available to other
users over the internet. The target architecture is a single always-on
container that:

  1. Runs the FITPAC pipeline on a schedule (every 10 minutes by default).
  2. Exposes `GET /api/alerts` as a read-only JSON endpoint.
  3. Serves `fitpac_dashboard.html` to any web browser.

There is no login, no user accounts, no write access — every visitor sees
the same live data. That's the "Same data (read-only)" model.

---

## 1. Run the server locally first

Before deploying, confirm the server works on your own machine.

    cd "Meme & Alt Coin"
    python3 fitpac_server.py

Then open http://localhost:8000 in a browser. You should see the
dashboard. In another terminal:

    curl -s http://localhost:8000/api/health
    curl -s http://localhost:8000/api/alerts | head -c 500

If this works, the Docker build will work identically.

Useful env vars (all optional):

| Var                   | Default | Meaning                                        |
| --------------------- | ------- | ---------------------------------------------- |
| `PORT`                | `8000`  | Listen port                                    |
| `FITPAC_REFRESH_MIN`  | `10`    | Minutes between scrape+analyze cycles (0=off)  |
| `FITPAC_SKIP_SCRAPE`  | `0`     | If `1`, refresh analyze only (no network)      |
| `FITPAC_DB`           | auto    | Path to SQLite file (use `/data/fitpac.db` in prod) |

---

## 2. Deploy to Fly.io (recommended)

Fly.io gives you a free always-on machine with a persistent volume. This
is the cheapest way to host a pipeline that must keep a fresh SQLite file.

### One-time setup

    brew install flyctl      # or: curl -L https://fly.io/install.sh | sh
    fly auth signup          # create account, add payment card (free tier)
    cd "Meme & Alt Coin"
    fly launch --no-deploy   # accepts the included fly.toml; pick a unique name

Edit `fly.toml` and change `app = "fitpac-dashboard"` to your chosen
unique name (Fly will complain if it's taken).

### Create the persistent volume

The SQLite database + `alerts.json` live in `/data`. Without a volume,
they'd be wiped on every deploy.

    fly volumes create fitpac_data --size 1 --region iad

### Deploy

    fly deploy

Fly prints a URL like `https://your-app.fly.dev`. Share that link with
anyone you want to give read-only access to the dashboard.

### Verify

    curl https://your-app.fly.dev/api/health
    # → {"status":"ok","tickers":28,"last_run":"2026-04-17T...","alerts_file_exists":true}

If `alerts_file_exists` is `false`, the first refresh cycle hasn't run
yet. Wait ~1 minute and try again.

### Tail logs

    fly logs

You'll see the `FITPAC_EMIT:` lines as scrapes and analyses run.

---

## 3. Alternative: Railway / Render / Cloud Run

The `Dockerfile` is platform-agnostic. Anywhere that runs containers
works — the only requirement is a persistent disk for `/data` if you
want `fitpac.db` to survive restarts.

### Railway

    railway init
    railway up
    railway volume mount /data

Railway auto-injects `$PORT`, which `fitpac_server.py` respects.

### Render

Create a new Web Service from the repo, select "Docker", add a disk
mounted at `/data`. Set `FITPAC_REFRESH_MIN=10`.

### Cloud Run (Google)

    gcloud run deploy fitpac --source . --region us-east1 \
      --allow-unauthenticated --memory 256Mi --port 8000

Cloud Run is stateless, so SQLite resets on each cold start. Either:
- Set `FITPAC_REFRESH_MIN` low (e.g. 5) so fresh data is rebuilt quickly,
  or
- Mount a Cloud Storage FUSE volume at `/data`.

---

## 4. Security notes

- **Read-only API.** The server exposes only `GET` routes. There is no
  write path; no remote user can modify tickers, VIPs, or alerts.
- **No authentication.** Anyone with the URL can view the dashboard.
  If you need to gate access, put Cloudflare Access or a basic-auth
  reverse proxy in front.
- **CORS is open** (`Access-Control-Allow-Origin: *`) on `/api/*` so
  other people can build their own frontends against your backend. If
  that's undesirable, tighten it in `fitpac_server.py::_send_json`.
- **Do NOT commit** `fitpac.db`, `alerts.json`, or any `.env` file.
  These are `.dockerignore`'d already.

---

## 5. Data freshness model

The background refresh loop inside `fitpac_server.py` runs
`scrape_chain_all()` + `scrape_social_all()` + `run_backend_cycle()`
every `FITPAC_REFRESH_MIN` minutes. Each cycle:

  1. Hits DexScreener / Hyperliquid / CoinGecko for fresh chain data.
  2. Pulls recent Reddit posts for each ticker.
  3. Runs FITPAC signal logic, applying bot filter, hard veto, and
     TAO rule.
  4. Writes `alerts.json` atomically.
  5. The dashboard polls `/api/alerts` every 60s and swaps in new data.

The header pill reads "Live API · 28 tickers · 14:03:22". If the API
fails, it turns red and shows the error. The pill is the source of
truth — trust it.

---

## 6. Cost estimate (Fly.io, as of 2026)

| Item                              | Monthly cost     |
| --------------------------------- | ---------------- |
| 1× shared-cpu-1x 256MB machine    | ~$1.94 (free tier covers it) |
| 1GB persistent volume             | $0.15           |
| Outbound bandwidth (<100GB)       | free             |
| **Total**                         | **effectively free under 3 machines** |

Scale up only if you need higher refresh rates or more users.
