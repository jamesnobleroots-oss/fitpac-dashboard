"""
FITPAC Web Server
=================
Zero-dependency HTTP server that exposes the FITPAC dashboard and a read-only
JSON API to remote users.

Endpoints
---------
GET /                 → fitpac_dashboard.html
GET /api/alerts       → latest alerts (JSON array, read from alerts.json)
GET /api/health       → {"status": "ok", "tickers": N, "last_run": "..."}
GET /api/meta         → watchlist + refresh interval metadata
GET /<file>           → static passthrough (css/js/svg), path-traversal safe

Background loop
---------------
Every FITPAC_REFRESH_MIN minutes (default 10), the server runs:
    fitpac_scrapers.scrape_chain_all() + scrape_social_all()
    fitpac_backend.run_backend_cycle()
…which refreshes alerts.json atomically. Set FITPAC_REFRESH_MIN=0 to disable
the loop (useful if you prefer an external cron job).

Env vars
--------
PORT                   Listen port (default 8000)
HOST                   Bind address (default 0.0.0.0)
FITPAC_REFRESH_MIN     Backend cycle interval in minutes (default 10, 0=off)
FITPAC_DB              SQLite path override (inherited from fitpac_db)
FITPAC_SKIP_SCRAPE     If "1", only run analyze (no network scrape) on refresh
"""

import json
import logging
import mimetypes
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import fitpac_db as db
import fitpac_backend as backend

logger = logging.getLogger("FITPAC_Server")

ROOT = Path(__file__).parent
ALERTS_JSON = ROOT / "alerts.json"

# Static files we are willing to serve. Anything outside this allowlist returns
# 404 so the server never accidentally leaks fitpac.db, .py sources, or .env.
STATIC_ALLOWLIST = {
    "fitpac_dashboard.html",
    "alerts.js",          # kept so users running the old static flow still work
    "favicon.ico",
}

_last_run_iso = None
_refresh_lock = threading.Lock()


def _refresh_once(skip_scrape: bool = False) -> None:
    """Run a full scrape+analyze cycle. Safe to call from background thread."""
    global _last_run_iso
    with _refresh_lock:
        try:
            if not skip_scrape:
                import fitpac_scrapers as scrapers
                logger.info("Background refresh: scraping chain + social...")
                scrapers.scrape_chain_all()
                scrapers.scrape_social_all()
            logger.info("Background refresh: running FITPAC analysis...")
            backend.run_backend_cycle()
            _last_run_iso = datetime.now(timezone.utc).isoformat()
            logger.info(f"Background refresh complete at {_last_run_iso}")
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Background refresh failed: {exc}")


def _background_loop(interval_min: float, skip_scrape: bool) -> None:
    """Sleep + refresh in a loop. Never raises — all errors are logged."""
    interval_s = interval_min * 60
    # First run shortly after boot so the dashboard has fresh data immediately.
    time.sleep(5)
    _refresh_once(skip_scrape=skip_scrape)
    while True:
        time.sleep(interval_s)
        _refresh_once(skip_scrape=skip_scrape)


class FITPACHandler(BaseHTTPRequestHandler):
    # Shorter, cleaner logs. Suppress asset noise.
    def log_message(self, fmt, *args):  # type: ignore[override]
        msg = fmt % args
        if "/api/" in msg or "HTTP/1.1\" 5" in msg or "HTTP/1.1\" 4" in msg:
            logger.info(f"{self.address_string()} {msg}")

    # ---- routing --------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path

        if path == "/" or path == "/index.html":
            return self._serve_file(ROOT / "fitpac_dashboard.html", "text/html")
        if path == "/api/alerts":
            return self._serve_alerts()
        if path == "/api/health":
            return self._serve_health()
        if path == "/api/meta":
            return self._serve_meta()

        # Static passthrough with allowlist
        rel = path.lstrip("/")
        if rel in STATIC_ALLOWLIST:
            target = ROOT / rel
            if target.is_file():
                mime, _ = mimetypes.guess_type(str(target))
                return self._serve_file(target, mime or "application/octet-stream")

        self._send_json(404, {"error": "not_found", "path": path})

    # ---- API handlers ---------------------------------------------------
    def _serve_alerts(self) -> None:
        if not ALERTS_JSON.exists():
            return self._send_json(503, {"error": "alerts_not_generated_yet"})
        try:
            data = json.loads(ALERTS_JSON.read_text())
        except json.JSONDecodeError as exc:
            return self._send_json(500, {"error": "alerts_corrupt", "detail": str(exc)})
        self._send_json(200, data, cache_seconds=15)

    def _serve_health(self) -> None:
        tickers = []
        try:
            tickers = [r["ticker"] for r in db.list_tickers()]
        except Exception:  # noqa: BLE001
            pass
        self._send_json(200, {
            "status": "ok",
            "tickers": len(tickers),
            "last_run": _last_run_iso,
            "alerts_file_exists": ALERTS_JSON.exists(),
        })

    def _serve_meta(self) -> None:
        tickers = []
        try:
            for r in db.list_tickers():
                tickers.append({
                    "ticker": r["ticker"],
                    "chain": r["chain"],
                    "display_name": r["display_name"],
                })
        except Exception:  # noqa: BLE001
            pass
        self._send_json(200, {
            "version": "4.7",
            "refresh_interval_min": float(os.environ.get("FITPAC_REFRESH_MIN", "10")),
            "tickers": tickers,
            "generated_at": _last_run_iso,
        })

    # ---- low-level helpers ---------------------------------------------
    def _serve_file(self, path: Path, content_type: str) -> None:
        try:
            payload = path.read_bytes()
        except FileNotFoundError:
            return self._send_json(404, {"error": "not_found"})
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        # No-cache on HTML so dashboard updates roll out immediately.
        self.send_header("Cache-Control", "no-cache" if content_type.startswith("text/html") else "public, max-age=60")
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, status: int, body, cache_seconds: int = 0) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")  # read-only, safe
        if cache_seconds:
            self.send_header("Cache-Control", f"public, max-age={cache_seconds}")
        else:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    refresh_min = float(os.environ.get("FITPAC_REFRESH_MIN", "10"))
    skip_scrape = os.environ.get("FITPAC_SKIP_SCRAPE", "0") == "1"

    # Ensure the DB exists (idempotent). If this is a fresh container, seeds
    # the default watchlist so /api/meta returns sensible data from boot.
    db.init_db()

    # Kick off the background refresh loop unless explicitly disabled.
    if refresh_min > 0:
        t = threading.Thread(
            target=_background_loop,
            args=(refresh_min, skip_scrape),
            daemon=True,
            name="fitpac-refresh",
        )
        t.start()
        logger.info(f"Background refresh loop started (every {refresh_min} min, skip_scrape={skip_scrape})")
    else:
        logger.info("Background refresh disabled (FITPAC_REFRESH_MIN=0).")

    server = ThreadingHTTPServer((host, port), FITPACHandler)
    logger.info(f"FITPAC server listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
        server.shutdown()


if __name__ == "__main__":
    main()
