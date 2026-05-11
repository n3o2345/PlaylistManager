FROM node:22-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    PATH="/opt/venv/bin:$PATH"

# ── tvapp2 internal settings ──────────────────────────────────────────────────
ENV TVAPP2_ENABLED=1 \
    TVAPP2_STREAM_QUALITY=hd \
    TVAPP2_LOG_LEVEL=2 \
    TVAPP2_PORT=4124

# ── Pluto X11 defaults ────────────────────────────────────────────────────────
ENV PLUTO_X11_ENABLED=1 \
    PLUTO_X11_WIDTH=1280 \
    PLUTO_X11_HEIGHT=720 \
    PLUTO_X11_FPS=30 \
    PLUTO_X11_BITRATE=2500k \
    PLUTO_X11_IDLE_TIMEOUT=30 \
    PLUTO_X11_STARTUP_WAIT=12 \
    CHROMIUM_PATH=/usr/bin/chromium

# jellyfin-ffmpeg lives here; add to PATH so bare `ffmpeg` resolves to it
ENV PATH="/usr/lib/jellyfin-ffmpeg:$PATH"

WORKDIR /app

# ── System deps ───────────────────────────────────────────────────────────────
# Note: NO ffmpeg from apt — jellyfin-ffmpeg replaces it (NVENC/VAAPI baked in)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    gcc \
    libpq-dev \
    curl \
    wget \
    redis-server \
    ca-certificates \
    git \
    xvfb \
    x11-utils \
    xauth \
    x11-xserver-utils \
    pulseaudio \
    pulseaudio-utils \
    chromium \
  && rm -rf /var/lib/apt/lists/* \
  && python3 -m venv /opt/venv

# ── jellyfin-ffmpeg (bookworm) — NVENC + VAAPI + QSV baked in ────────────────
# Debian bookworm's stock ffmpeg is compiled WITHOUT --enable-nvenc, so
# `ffmpeg -encoders` never lists h264_nvenc even with GPU passthrough working.
# jellyfin-ffmpeg ships a static build that includes all three GPU paths.
# The NVENC runtime libs are injected at container start by
# nvidia-container-toolkit — no CUDA version is baked into the image.
#
# To update: check https://github.com/jellyfin/jellyfin-ffmpeg/releases
ARG JELLYFIN_FFMPEG_VERSION=7.0.2-9
RUN ARCH=$(dpkg --print-architecture) \
  && wget -q -O /tmp/jf-ffmpeg.deb \
     "https://github.com/jellyfin/jellyfin-ffmpeg/releases/download/v${JELLYFIN_FFMPEG_VERSION}/jellyfin-ffmpeg7_${JELLYFIN_FFMPEG_VERSION}-bookworm_${ARCH}.deb" \
  && apt-get update \
  && apt-get install -y --no-install-recommends /tmp/jf-ffmpeg.deb \
  && rm /tmp/jf-ffmpeg.deb \
  && rm -rf /var/lib/apt/lists/* \
  && ffmpeg -version | head -1

# ── Python deps ───────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# ── Playwright Chromium ───────────────────────────────────────────────────────
RUN playwright install-deps chromium && playwright install chromium

# ── tvapp2 ────────────────────────────────────────────────────────────────────
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
