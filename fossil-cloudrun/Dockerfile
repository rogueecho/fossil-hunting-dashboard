# ── Fossil Hunting Dashboard — Google Cloud Run ──────────────────────────
# - Serves Flask directly (Cloud Run handles TLS termination externally)
# - No Nginx, no Certbot needed — Google manages the Let's Encrypt cert
# - PORT env var is injected by Cloud Run at runtime (default 8080)
# ─────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy application files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fossil_server.py .
COPY fossil_hunting_sites.json .
COPY index.html .

# Cloud Run injects PORT; APPLICATION_ROOT keeps the /fossil prefix
ENV APPLICATION_ROOT=/fossil
ENV PORT=8080

EXPOSE 8080

# Run as non-root for security
RUN useradd -m -u 1000 appuser && chown -R appuser /app
USER appuser

# Startup script: warm the cache in a background thread, then hand off to gunicorn
CMD ["python3", "fossil_server.py"]
