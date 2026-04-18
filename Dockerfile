# FITPAC Alert Engine — minimal container image.
# Single-stage, no pip installs, stdlib-only Python.
FROM python:3.11-slim

# Small perf tweaks + unbuffered stdout so logs stream to Fly/Railway.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy only what the runtime needs. Keeps image tiny (~55MB with base).
COPY fitpac_db.py \
     fitpac_backend.py \
     fitpac_scrapers.py \
     fitpac_live_seed.py \
     fitpac_pipeline.py \
     fitpac_server.py \
     fitpac_dashboard.html \
     ./

# Persistent SQLite lives in /data so a mounted volume survives restarts.
# Local builds without a volume still work — fitpac_db will create the file.
RUN mkdir -p /data
ENV FITPAC_DB=/data/fitpac.db

# Fly/Railway/Cloud Run all inject $PORT. Default to 8000 for local runs.
ENV PORT=8000
EXPOSE 8000

# Refresh every 10 minutes in prod. Override with FITPAC_REFRESH_MIN at deploy time.
ENV FITPAC_REFRESH_MIN=10

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python3 -c "import urllib.request,sys; \
urllib.request.urlopen('http://127.0.0.1:' + __import__('os').environ['PORT'] + '/api/health', timeout=3); \
sys.exit(0)" || exit 1

CMD ["python3", "fitpac_server.py"]
