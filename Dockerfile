# ── Base image ────────────────────────────────────────────────────────────────
# nvidia/cuda runtime includes libnvidia-encode and libnvcuvid so h264_nvenc
# works without relying solely on nvidia-container-toolkit injection at startup.
# Ubuntu 24.04 (noble) ships Python 3.12 natively.
#
# CPU-only / non-NVIDIA hosts: this image still builds and runs fine; ffmpeg
# will auto-detect no GPU and fall back to libx264.
FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    # Tell Python / pip where to find the venv we create below
    PATH="/opt/venv/bin:$PATH"

# ── tvapp2 internal settings ──────────────────────────────────────────────────
# TVAPP2_ENABLED=1    start the embedded tvapp2 daemon (default: enabled)
# TVAPP2_STREAM_QUALITY  hd | sd (passed through to tvapp2)
# TVAPP2_LOG_LEVEL       0-6 (tvapp2 verbosity, default 2)
ENV TVAPP2_ENABLED=1 \
    TVAPP2_STREAM_QUALITY=hd \
    TVAPP2_LOG_LEVEL=2 \
    TVAPP2_PORT=4124 \
    NODE_VERSION=22

# ── Pluto X11 defaults (can be overridden at runtime via docker-compose env) ──
ENV PLUTO_X11_ENABLED=1 \
    PLUTO_X11_WIDTH=1280 \
    PLUTO_X11_HEIGHT=720 \
    PLUTO_X11_FPS=30 \
    PLUTO_X11_BITRATE=2500k \
    PLUTO_X11_IDLE_TIMEOUT=30 \
    PLUTO_X11_STARTUP_WAIT=12

WORKDIR /app

# ── System deps ───────────────────────────────────────────────────────────────
# python3.12 + venv; nodejs/npm for tvapp2; ffmpeg with nvenc; xvfb + pulseaudio
# for Pluto X11 screen-grab streaming.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3.12-venv \
    python3-pip \
    gcc \
    libpq-dev \
    curl \
    redis-server \
    ca-certificates \
    git \
    xvfb \
    x11-utils \
    ffmpeg \
    pulseaudio \
    pulseaudio-utils \
    && curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    # Create a venv so pip doesn't fight the system Python
    && python3.12 -m venv /opt/venv

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

# ── PlaylistManager app ───────────────────────────────────────────────────────
COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 5523

HEALTHCHECK --interval=15s --timeout=5s --start-period=120s --retries=5 \
    CMD curl -sf http://localhost:5523/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
