# Pluto TV X11 Screen-Grab Streaming

## Why this exists

Pluto's SSAI (server-side ad insertion) stitcher rotates CDN signing tokens at
every commercial break boundary. The existing HLS proxy + TS stitcher handles
timestamp discontinuities well but clients still receive ad pods, and the token
rotation can still cause transient 403s on the first segment of each new pod.

The X11 approach sidesteps the problem entirely: a headless Chromium instance
renders the Pluto web player inside a virtual Xvfb display, and ffmpeg captures
the screen via `x11grab` and encodes it to MPEG-TS. From the downstream
player's perspective it's a seamless, continuous bitstream — no token rotation,
no manifest rewriting, no ad-break artefacts.

## Architecture

```
Per channel (lazy-started, shared across concurrent clients):

  Xvfb  :200+              virtual framebuffer, 1280×720×24
    └─ Chromium             --app=https://pluto.tv/live-tv/<slug>  (kiosk/fullscreen)
         └─ PulseAudio      null-sink virtual audio device (optional)
  ffmpeg  -f x11grab
    ├─ -f pulse (if PA available)
    ├─ GPU encoder: h264_nvenc → h264_vaapi → libx264 (auto-detected)
    └─ -f mpegts → stdout
         └─ _BroadcastPipe  fan-out to N concurrent HTTP readers
```

Sessions are reference-counted. When the last viewer disconnects the session
stays alive for `PLUTO_X11_IDLE_TIMEOUT` seconds (default 30) then tears down.

## GPU encoder auto-detection

The module probes at first use (result cached):

| Priority | Encoder       | Requirement                         |
|----------|---------------|-------------------------------------|
| 1        | `h264_nvenc`  | NVIDIA GPU + nvidia-container-toolkit |
| 2        | `h264_vaapi`  | Intel/AMD GPU + `/dev/dri/renderD128` |
| 3        | `libx264`     | Always available (CPU fallback)     |

## Deployment

### CPU-only (no GPU config needed)

```bash
docker compose up -d
```

libx264 is the fallback and always works. At 1280×720×30fps `veryfast` costs
roughly 0.5–1 core per concurrent channel.

### Intel / AMD VAAPI

```bash
docker compose -f docker-compose.yml -f docker-compose.vaapi.yml up -d
```

Requires `/dev/dri/renderD128` on the host (i915 or amdgpu kernel driver loaded).

On **TrueNAS SCALE**: add the iGPU under app → Resources → GPU Passthrough.

### NVIDIA NVENC

```bash
docker compose -f docker-compose.yml -f docker-compose.nvidia.yml up -d
```

Requires `nvidia-container-toolkit` installed and configured on the Docker host.

On **TrueNAS SCALE**: select the NVIDIA GPU under app → Resources → GPU Passthrough.

## Environment variables

| Variable                  | Default   | Description                                              |
|---------------------------|-----------|----------------------------------------------------------|
| `PLUTO_X11_ENABLED`       | `1`       | Set to `0` to disable; falls back to HLS proxy           |
| `PLUTO_X11_WIDTH`         | `1280`    | Capture width (pixels)                                   |
| `PLUTO_X11_HEIGHT`        | `720`     | Capture height (pixels)                                  |
| `PLUTO_X11_FPS`           | `30`      | Frame rate                                               |
| `PLUTO_X11_BITRATE`       | `2500k`   | Video bitrate (per active session)                       |
| `PLUTO_X11_IDLE_TIMEOUT`  | `30`      | Seconds to keep session alive after last viewer leaves   |
| `PLUTO_X11_STARTUP_WAIT`  | `8`       | Maximum seconds to wait for Chromium video readiness     |
| `FASTCHANNELS_AUTH_DIR`   | `/data/auth` | Durable auth directory on the `/data` volume          |
| `PLUTO_X11_COOKIE_PATH`   | `/data/auth/pluto_x11_cookies.json` | Stored Pluto login cookies/token        |
| `PLUTO_X11_CLIENT_ID_PATH` | `/data/auth/pluto_x11_client_id` | Stable Pluto device/client id           |
| `CHROMIUM_PATH`           | *(auto)*  | Override Chromium binary path                            |

Pluto X11 login state is stored under `/data/auth` by default, so it survives
container restarts and image rebuilds as long as the `/data` volume is kept.

## Stream URLs

The M3U generator automatically emits X11 URLs for Pluto channels when
`PLUTO_X11_ENABLED=1`:

```
http://<host>:5523/play/pluto/<channel_id>/x11.ts
```

Set `PLUTO_X11_ENABLED=0` to revert to the standard HLS proxy URLs.

### Status endpoint

```
GET http://<host>:5523/play/pluto/x11/status
```

Returns JSON showing active sessions, encoder in use, reader count, and idle time:

```json
{
  "enabled": true,
  "sessions": [
    {
      "channel_id": "5c8d3e8b5f8d5e001a2b3c4d",
      "display": ":200",
      "encoder": "h264_nvenc",
      "readers": 2,
      "idle_secs": 0.0,
      "ffmpeg_alive": true
    }
  ]
}
```

## Resource usage estimates

| Encoder       | CPU per session | GPU VRAM |
|---------------|-----------------|----------|
| `h264_nvenc`  | ~5% (1 core)    | ~50 MB   |
| `h264_vaapi`  | ~10% (1 core)   | ~30 MB   |
| `libx264`     | 50–100% (1 core)| —        |

Each session also uses ~100–200 MB RAM (Chromium + Xvfb).

With the R720's CPU headroom, libx264 handles 4–6 simultaneous Pluto channels
comfortably. VAAPI or NVENC raises that to 20+ with negligible CPU overhead.

## Audio

Audio capture requires PulseAudio (`pulseaudio` package installed in the
Dockerfile). If PulseAudio is unavailable or fails to start the stream is
video-only — Pluto's audio is silent but the video plays normally.

Audio adds `aac 192k` to the output and increases bitrate by ~200 kbps per session.

## Disabling for a specific channel

The X11 grab is per-source (`pluto`). To exempt individual channels, disable
the Pluto source's X11 mode globally with `PLUTO_X11_ENABLED=0` and the
standard HLS proxy is used for all Pluto channels.

## Files changed

| File                                  | Change                                              |
|---------------------------------------|-----------------------------------------------------|
| `app/scrapers/pluto_x11.py`           | New — X11 session manager, GPU detection, fan-out   |
| `app/routes/play.py`                  | Added `/play/pluto/<id>/x11.ts` + `/x11/status`     |
| `app/generators/m3u.py`               | Emit x11.ts URLs for Pluto when enabled             |
| `Dockerfile`                          | Added xvfb, ffmpeg, pulseaudio, pulseaudio-utils    |
| `docker-compose.yml`                  | Added shm_size + X11 env vars                       |
| `docker-compose.vaapi.yml`            | New — VAAPI GPU override                            |
| `docker-compose.nvidia.yml`           | New — NVENC GPU override                            |
