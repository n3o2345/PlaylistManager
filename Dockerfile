FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# ── tvapp2 internal settings ──────────────────────────────────────────────────
# TVAPP2_ENABLED=1    start the embedded tvapp2 daemon (default: enabled)
# TVAPP2_STREAM_QUALITY  hd | sd (passed through to tvapp2)
# TVAPP2_LOG_LEVEL       0-6 (tvapp2 verbosity, default 2)
ENV TVAPP2_ENABLED=1 \
    TVAPP2_STREAM_QUALITY=hd \
    TVAPP2_LOG_LEVEL=2 \
    TVAPP2_PORT=4124 \
    NODE_VERSION=22

WORKDIR /app

# ── System deps ───────────────────────────────────────────────────────────────
# nodejs / npm for tvapp2; git for cloning it at build time
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    redis-server \
    ca-certificates \
    git \
    && curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN playwright install-deps chromium && playwright install chromium

# ── tvapp2 ────────────────────────────────────────────────────────────────────
# Clone from GitHub and install npm deps at build time so the image is
# self-contained.  The www/ assets and index.js are all that's needed at
# runtime; node_modules stay inside /opt/tvapp2.
RUN git clone --depth=1 https://github.com/TheBinaryNinja/tvapp2.git /opt/tvapp2-src \
    && cp -r /opt/tvapp2-src/tvapp2 /opt/tvapp2 \
    && rm -rf /opt/tvapp2-src \
    && cd /opt/tvapp2 \
    && npm install --omit=dev \
    && mkdir -p /data/tvapp2

# ── PlaylistManager app ──────────────────────────────────────────────────────────
COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 5523

HEALTHCHECK --interval=15s --timeout=5s --start-period=120s --retries=5 \
    CMD curl -sf http://localhost:5523/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
