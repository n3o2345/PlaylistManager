"""
app/scrapers/pluto_x11.py

Pluto TV X11 screen-grab streamer — bypasses SSAI commercial breaks entirely.

Instead of fighting Pluto's token-rotating HLS stitcher, this module renders
the Pluto web player inside a headless Xvfb display and pipes the screen
through ffmpeg's x11grab → MPEG-TS.  Ads still play but commercial-break
token rotation never affects the stream because we're capturing pixels, not
proxying HLS segments.

Architecture
------------
  Xvfb  (:DISPLAY)
    └─ Chromium  (non-headless, --app=pluto_url)
         └─ PulseAudio null sink  (virtual audio device)
  ffmpeg  -f x11grab + -f pulse → libx264/h264_nvenc/h264_vaapi → MPEG-TS → pipe
    └─ _BroadcastPipe (fan-out to N concurrent readers)

GPU encoding
------------
Encoder preference order (auto-detected at first use):
  1. h264_nvenc   — NVIDIA, requires --gpus in docker-compose
  2. h264_vaapi   — Intel/AMD, requires /dev/dri device in compose
  3. libx264      — CPU fallback, always available

One Xvfb+Chromium+ffmpeg process group per channel slug.
Sessions are reference-counted and torn down IDLE_TIMEOUT seconds after
the last reader disconnects.

Environment variables (all optional)
-------------------------------------
PLUTO_X11_ENABLED       Set to "0" to disable this subsystem entirely.
PLUTO_X11_WIDTH         Capture width  (default 1280)
PLUTO_X11_HEIGHT        Capture height (default 720)
PLUTO_X11_FPS           Frame rate     (default 30)
PLUTO_X11_BITRATE       Video bitrate  (default 2500k)
PLUTO_X11_IDLE_TIMEOUT  Seconds to keep a session alive after last reader disconnects (default 30)
PLUTO_X11_STARTUP_WAIT  Seconds to wait for Chromium to load before grabbing (default 5)
CHROMIUM_PATH           Override path to the Chromium/Chrome binary
PULSE_SERVER            Override PulseAudio server address (default: unix socket in /tmp)
"""
from __future__ import annotations

import logging
import os
import queue
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

ENABLED         = os.environ.get("PLUTO_X11_ENABLED", "1") != "0"
DISPLAY_W       = int(os.environ.get("PLUTO_X11_WIDTH",        "1280"))
DISPLAY_H       = int(os.environ.get("PLUTO_X11_HEIGHT",       "720"))
FRAMERATE       = int(os.environ.get("PLUTO_X11_FPS",          "30"))
BITRATE         = os.environ.get("PLUTO_X11_BITRATE",          "2500k")
IDLE_TIMEOUT    = int(os.environ.get("PLUTO_X11_IDLE_TIMEOUT", "30"))
STARTUP_WAIT    = int(os.environ.get("PLUTO_X11_STARTUP_WAIT", "5"))
# Force a specific encoder; if unset, auto-detected at first use.
# Valid values: h264_nvenc | h264_vaapi | libx264
FORCE_ENCODER   = os.environ.get("PLUTO_X11_ENCODER", "").strip().lower() or None
CHUNK_SIZE      = 65536   # bytes per fan-out chunk
MAX_QUEUE_DEPTH = 64      # chunks buffered per reader before dropping

# ---------------------------------------------------------------------------
# Chromium binary discovery
# ---------------------------------------------------------------------------

def _find_chromium() -> str:
    """Return the path to a usable Chromium/Chrome binary.

    Priority (highest → lowest):
      1. CHROMIUM_PATH env var — explicit operator override
      2. System apt/package Chromium  (/usr/bin/chromium, chromium-browser, …)
         These are real browser builds without the "Chrome for Testing" brand
         that Pluto TV and other streaming sites detect and block.
      3. Playwright's bundled Chromium — last resort only.  It is branded
         "Chrome for Testing" which streaming sites use to identify and block
         automated browsers.  It should never be used for live-TV capture.
    """
    # ── 1. Explicit override ────────────────────────────────────────────────
    override = os.environ.get("CHROMIUM_PATH")
    if override and os.path.isfile(override):
        logger.info("[pluto-x11] Chromium: using CHROMIUM_PATH=%s", override)
        return override

    # ── 2. Real system Chromium (preferred — no "Chrome for Testing" brand) ─
    for candidate in (
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
    ):
        try:
            if candidate.startswith("/"):
                if os.path.isfile(candidate):
                    logger.info("[pluto-x11] Chromium: using system binary %s", candidate)
                    return candidate
            else:
                path = subprocess.check_output(["which", candidate], text=True).strip()
                if path and os.path.isfile(path):
                    logger.info("[pluto-x11] Chromium: using system binary %s", path)
                    return path
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

    # ── 3. Playwright Chromium — fallback only, with a loud warning ─────────
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            exe = p.chromium.executable_path
        if exe and os.path.isfile(exe):
            logger.warning(
                "[pluto-x11] Falling back to Playwright Chromium (%s). "
                "This binary is branded 'Chrome for Testing' and streaming "
                "sites like Pluto TV will likely block it. "
                "Install the 'chromium' apt package or set CHROMIUM_PATH.",
                exe,
            )
            return exe
    except Exception as e:
        logger.debug("[pluto-x11] Playwright chromium lookup failed: %s", e)

    raise RuntimeError(
        "No Chromium binary found. "
        "Install the 'chromium' apt package or set CHROMIUM_PATH env var."
    )


_chromium_path: str | None = None
_chromium_lock = threading.Lock()


def _get_chromium_path() -> str:
    global _chromium_path
    with _chromium_lock:
        if _chromium_path is None:
            _chromium_path = _find_chromium()
    return _chromium_path


# ---------------------------------------------------------------------------
# GPU encoder detection
# ---------------------------------------------------------------------------

_ENCODER_CACHE: str | None = None
_ENCODER_LOCK  = threading.Lock()


def _detect_encoder() -> str:
    """
    Probe ffmpeg for the best available H.264 encoder.
    Returns one of: 'h264_nvenc', 'h264_vaapi', 'libx264'.

    Result is cached only after a *successful* probe so that a transient
    failure during container start (GPU driver not yet injected by
    nvidia-container-toolkit) doesn't permanently lock us into libx264.

    Override with env var PLUTO_X11_ENCODER=h264_nvenc|h264_vaapi|libx264.
    """
    global _ENCODER_CACHE
    with _ENCODER_LOCK:
        # Explicit override always wins and is cached immediately.
        if FORCE_ENCODER:
            if _ENCODER_CACHE != FORCE_ENCODER:
                logger.info("[pluto-x11] GPU encoder forced via env: %s", FORCE_ENCODER)
                _ENCODER_CACHE = FORCE_ENCODER
            return _ENCODER_CACHE

        if _ENCODER_CACHE is not None:
            return _ENCODER_CACHE

        # Quick probe: ask ffmpeg to list encoders
        try:
            out = subprocess.check_output(
                ["ffmpeg", "-hide_banner", "-encoders"],
                stderr=subprocess.STDOUT, text=True, timeout=10,
            )
        except Exception as e:
            # Do NOT cache — allow retry on next session request.
            logger.warning("[pluto-x11] ffmpeg encoder probe failed: %s — will retry later", e)
            return "libx264"

        if "h264_nvenc" in out:
            # Verify NVENC actually works (driver present, GPU available).
            # Use a real encode via nullsrc — lavfi nullsrc → h264_nvenc → null muxer.
            try:
                subprocess.check_call(
                    [
                        "ffmpeg", "-hide_banner", "-loglevel", "error",
                        "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                        "-vframes", "4",
                        "-c:v", "h264_nvenc",
                        "-f", "null", "-",
                    ],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=15,
                )
                logger.info("[pluto-x11] GPU encoder: h264_nvenc (NVIDIA)")
                _ENCODER_CACHE = "h264_nvenc"
                return _ENCODER_CACHE
            except Exception as exc:
                # Do NOT cache — GPU may not be ready yet; retry next session.
                logger.warning(
                    "[pluto-x11] h264_nvenc probe failed (%s) — "
                    "set PLUTO_X11_ENCODER=h264_nvenc to force, or check GPU passthrough",
                    exc,
                )

        if "h264_vaapi" in out:
            # Verify VAAPI render node exists
            if os.path.exists("/dev/dri/renderD128"):
                try:
                    subprocess.check_call(
                        [
                            "ffmpeg", "-hide_banner", "-loglevel", "error",
                            "-vaapi_device", "/dev/dri/renderD128",
                            "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                            "-vframes", "4",
                            "-vf", "format=nv12,hwupload",
                            "-c:v", "h264_vaapi",
                            "-f", "null", "-",
                        ],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        timeout=15,
                    )
                    logger.info("[pluto-x11] GPU encoder: h264_vaapi (Intel/AMD)")
                    _ENCODER_CACHE = "h264_vaapi"
                    return _ENCODER_CACHE
                except Exception:
                    logger.debug("[pluto-x11] h264_vaapi probe failed — falling back to libx264")

        logger.info("[pluto-x11] GPU encoder: libx264 (CPU fallback)")
        _ENCODER_CACHE = "libx264"
        return _ENCODER_CACHE


def _build_video_encoder_args(encoder: str) -> list[str]:
    """Return ffmpeg -c:v … encoding argument list for the chosen encoder."""
    if encoder == "h264_nvenc":
        return [
            "-c:v", "h264_nvenc",
            "-preset", "p2",          # fast preset (ll = low-latency alias)
            "-tune", "ll",            # low-latency tuning
            "-b:v", BITRATE,
            "-maxrate", BITRATE,
            "-bufsize", _double_bitrate(BITRATE),
            "-g", str(FRAMERATE * 2),
            "-rc", "cbr",
        ]
    elif encoder == "h264_vaapi":
        return [
            # VAAPI requires hwupload filter; we handle this in _build_vf_chain
            "-c:v", "h264_vaapi",
            "-b:v", BITRATE,
            "-maxrate", BITRATE,
            "-bufsize", _double_bitrate(BITRATE),
            "-g", str(FRAMERATE * 2),
        ]
    else:  # libx264
        return [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-tune", "zerolatency",
            "-b:v", BITRATE,
            "-maxrate", BITRATE,
            "-bufsize", _double_bitrate(BITRATE),
            "-g", str(FRAMERATE * 2),
        ]


def _build_vf_chain(encoder: str) -> list[str]:
    """Return -vf filter chain args, or empty list if none needed."""
    if encoder == "h264_vaapi":
        return ["-vf", "format=nv12,hwupload"]
    return []


def _vaapi_device_args(encoder: str) -> list[str]:
    """Return -vaapi_device args if needed."""
    if encoder == "h264_vaapi":
        return ["-vaapi_device", "/dev/dri/renderD128"]
    return []


def _double_bitrate(bitrate: str) -> str:
    """Return bufsize = 2× bitrate, preserving the unit suffix."""
    try:
        suffix = bitrate[-1].lower()
        if suffix in ("k", "m"):
            return str(int(bitrate[:-1]) * 2) + suffix
    except (IndexError, ValueError):
        pass
    return bitrate


# ---------------------------------------------------------------------------
# PulseAudio virtual sink
# ---------------------------------------------------------------------------

def _start_pulseaudio(display_num: int) -> subprocess.Popen | None:
    """
    Start a per-display PulseAudio daemon with a null output sink.
    Returns the daemon process or None if PulseAudio is unavailable.

    Uses a config-file approach (mirrors philo.js) for reliability when running
    as root inside a container:
      - auth-anonymous=1 avoids cookie-file permission errors
      - explicit daemon.conf suppresses dbus/D-Bus noise
      - polls the socket file for up to 8 s instead of a fixed 0.5 s sleep
    """
    socket_dir = f"/tmp/pulse-x11-{display_num}"
    pa_socket   = f"{socket_dir}/native"
    pa_conf     = f"{socket_dir}/pa.conf"
    daemon_conf = f"{socket_dir}/daemon.conf"
    cookie_dir  = f"{socket_dir}/.config/pulse"
    cookie_file = f"{socket_dir}/.pulse-cookie"

    # Wipe any previous (crashed) instance so we start clean
    try:
        import shutil
        shutil.rmtree(socket_dir, ignore_errors=True)
    except Exception:
        pass
    os.makedirs(cookie_dir, exist_ok=True)

    # Pre-create zero cookie files — avoids auth-init errors when running as root
    _zero = bytes(256)
    for _cf in (cookie_file, f"{cookie_dir}/cookie"):
        try:
            with open(_cf, "wb") as _fh:
                _fh.write(_zero)
        except Exception:
            pass

    # Minimal config: null sink + native unix socket, no auth required
    try:
        with open(pa_conf, "w") as _fh:
            _fh.write(
                f"load-module module-null-sink sink_name=out\n"
                f"set-default-sink out\n"
                f"set-default-source out.monitor\n"
                f"load-module module-native-protocol-unix "
                f"auth-anonymous=1 auth-cookie-enabled=0 socket={pa_socket}\n"
            )
        with open(daemon_conf, "w") as _fh:
            _fh.write(
                "default-sample-rate = 48000\n"
                "exit-idle-time = -1\n"
                "log-level = error\n"
            )
    except Exception as e:
        logger.debug("[pluto-x11] PulseAudio config write failed: %s", e)
        return None

    pa_env = {
        **os.environ,
        "PULSE_RUNTIME_PATH":       socket_dir,
        "HOME":                     socket_dir,
        "XDG_RUNTIME_DIR":          socket_dir,
        "PULSE_COOKIE":             cookie_file,
        # Suppress dbus errors — expected in a headless container
        "DBUS_SESSION_BUS_ADDRESS": "unix:path=/nonexistent",
        "DBUS_SYSTEM_BUS_ADDRESS":  "unix:path=/nonexistent",
    }

    try:
        proc = subprocess.Popen(
            [
                "pulseaudio",
                "--daemonize=no",
                "--exit-idle-time=-1",
                "--disallow-exit",
                "-n",
                f"--file={pa_conf}",
                "--log-target=stderr",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=pa_env,
        )
    except FileNotFoundError:
        logger.debug("[pluto-x11] PulseAudio not installed — audio disabled")
        return None
    except Exception as e:
        logger.debug("[pluto-x11] PulseAudio start failed: %s", e)
        return None

    # Poll until the socket exists (up to 8 s), then verify with pactl
    ready = False
    for _ in range(80):
        time.sleep(0.1)
        if proc.poll() is not None:
            logger.debug("[pluto-x11] PulseAudio exited early — audio disabled")
            return None
        if not os.path.exists(pa_socket):
            continue
        try:
            result = subprocess.run(
                ["pactl", f"--server=unix:{pa_socket}", "list", "short", "sinks"],
                capture_output=True, text=True, timeout=1,
                env={**pa_env, "PULSE_SERVER": f"unix:{pa_socket}"},
            )
            if "out" in result.stdout:
                ready = True
                break
        except Exception:
            pass

    if not ready:
        logger.warning("[pluto-x11] PulseAudio sink not ready after 8 s — audio disabled")
        try:
            proc.kill()
        except Exception:
            pass
        return None

    logger.debug("[pluto-x11] PulseAudio started (display :%d socket=%s)", display_num, pa_socket)
    return proc


def _pulse_socket_path(display_num: int) -> str | None:
    path = f"/tmp/pulse-x11-{display_num}/native"
    return path if os.path.exists(path) else None


# ---------------------------------------------------------------------------
# Fan-out broadcast pipe
# ---------------------------------------------------------------------------

class _BroadcastPipe:
    """
    Reads raw bytes from an ffmpeg stdout pipe and fans them out to an
    arbitrary number of concurrent reader queues.

    Each reader gets its own queue.Queue so slow readers don't block fast ones.
    Overflow chunks are silently dropped on a per-reader basis.
    """

    def __init__(self, pipe):
        self._pipe     = pipe
        self._readers: list[queue.Queue] = []
        self._lock     = threading.Lock()
        self._stopped  = False
        self._thread   = threading.Thread(target=self._pump, daemon=True,
                                          name="pluto-x11-pump")
        self._thread.start()

    def add_reader(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=MAX_QUEUE_DEPTH)
        with self._lock:
            self._readers.append(q)
        return q

    def remove_reader(self, q: queue.Queue) -> None:
        with self._lock:
            try:
                self._readers.remove(q)
            except ValueError:
                pass

    def _pump(self) -> None:
        try:
            while not self._stopped:
                chunk = self._pipe.read(CHUNK_SIZE)
                if not chunk:
                    break
                with self._lock:
                    readers = list(self._readers)
                for q in readers:
                    try:
                        q.put_nowait(chunk)
                    except queue.Full:
                        # Slow reader — drop the chunk rather than block
                        pass
        except Exception as e:
            logger.warning("[pluto-x11] pump error: %s", e)
        finally:
            self._stopped = True
            # Wake all blocked readers so they can detect EOF
            with self._lock:
                for q in self._readers:
                    try:
                        q.put_nowait(None)
                    except queue.Full:
                        pass

    def is_alive(self) -> bool:
        return not self._stopped and self._thread.is_alive()

    def stop(self) -> None:
        self._stopped = True


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@dataclass
class _X11Session:
    channel_id:  str
    pluto_url:   str
    display_num: int
    encoder:     str

    xvfb_proc:    subprocess.Popen | None = field(default=None, repr=False)
    pulse_proc:   subprocess.Popen | None = field(default=None, repr=False)
    browser_proc: subprocess.Popen | None = field(default=None, repr=False)
    ffmpeg_proc:  subprocess.Popen | None = field(default=None, repr=False)
    broadcast:    _BroadcastPipe   | None = field(default=None, repr=False)

    readers:   int   = 0
    last_read: float = field(default_factory=time.monotonic)
    started:   bool  = False
    lock:      threading.Lock = field(default_factory=threading.Lock)


_sessions:     dict[str, _X11Session] = {}
_sessions_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Display number allocation — cross-process safe
# ---------------------------------------------------------------------------
# gunicorn forks N worker processes; each has its own copy of the module, so a
# simple module-level integer counter resets to 200 in every fork and workers
# collide on the same Xvfb display number.
#
# Solution: use a file-backed atomic counter in /tmp shared across all workers.
# A lockf() exclusive lock ensures no two processes draw the same number.
# ---------------------------------------------------------------------------

_DISPLAY_LOCK_PATH  = "/tmp/pluto_x11_display.lock"
_DISPLAY_COUNT_PATH = "/tmp/pluto_x11_display.count"
_DISPLAY_BASE       = 200


def _alloc_display() -> int:
    """
    Return the next available Xvfb display number, guaranteed unique across
    all gunicorn worker processes in this container.
    Uses a lock file + a plain text counter file in /tmp.
    """
    import fcntl
    with open(_DISPLAY_LOCK_PATH, "w") as lf:
        fcntl.lockf(lf, fcntl.LOCK_EX)
        try:
            try:
                with open(_DISPLAY_COUNT_PATH) as cf:
                    n = int(cf.read().strip())
            except (FileNotFoundError, ValueError):
                n = _DISPLAY_BASE
            next_n = n + 1
            with open(_DISPLAY_COUNT_PATH, "w") as cf:
                cf.write(str(next_n))
            return n
        finally:
            fcntl.lockf(lf, fcntl.LOCK_UN)


def _launch_session(channel_id: str, pluto_url: str) -> _X11Session:
    display_num = _alloc_display()
    encoder     = _detect_encoder()

    sess = _X11Session(
        channel_id  = channel_id,
        pluto_url   = pluto_url,
        display_num = display_num,
        encoder     = encoder,
    )

    # ── 1. Xvfb ────────────────────────────────────────────────────────────
    # Skip any display number whose lock file already exists — that means
    # another worker's Xvfb is already bound there.
    for _attempt in range(20):
        lock_file = f"/tmp/.X{display_num}-lock"
        if not os.path.exists(lock_file):
            break
        logger.warning(
            "[pluto-x11] display :%d already locked — skipping to next", display_num
        )
        display_num = _alloc_display()
        sess.display_num = display_num
        sess.encoder = encoder  # keep encoder unchanged
    else:
        raise RuntimeError("Could not find a free Xvfb display after 20 attempts")

    sess.xvfb_proc = subprocess.Popen(
        [
            "Xvfb", f":{display_num}",
            "-screen", "0", f"{DISPLAY_W}x{DISPLAY_H}x24",
            "-ac", "-nolisten", "tcp", "-nolisten", "unix",
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)   # let Xvfb bind before anything touches the display

    if sess.xvfb_proc.poll() is not None:
        raise RuntimeError(f"Xvfb failed to start for display :{display_num}")

    # ── 2. PulseAudio (optional — graceful fallback) ────────────────────────
    sess.pulse_proc = _start_pulseaudio(display_num)

    # ── 3. Chromium ─────────────────────────────────────────────────────────
    browser_env = {
        **os.environ,
        "DISPLAY": f":{display_num}",
        "PULSE_SERVER": (
            f"unix:{_pulse_socket_path(display_num)}"
            if sess.pulse_proc and _pulse_socket_path(display_num)
            else os.environ.get("PULSE_SERVER", "")
        ),
    }
    chromium = _get_chromium_path()

    # Each session gets its own throwaway profile directory so:
    #   1. Chromium never fights over a singleton lock with a sibling session
    #   2. Running as root doesn't cause $HOME write failures (code-0 clean exit)
    #   3. No first-run / migration dialogs from a stale previous profile
    user_data_dir = f"/tmp/pluto-x11-profile-{display_num}"
    os.makedirs(user_data_dir, exist_ok=True)

    sess.browser_proc = subprocess.Popen(
        [
            chromium,
            "--no-sandbox",
            "--disable-setuid-sandbox",
            # ── Per-session profile ───────────────────────────────────────────
            # Must be set to avoid singleton-lock collisions between sessions and
            # to prevent Chromium from exiting cleanly (code 0) when it cannot
            # write its profile to the root home directory inside the container.
            f"--user-data-dir={user_data_dir}",
            # ── GPU / rendering ───────────────────────────────────────────────
            # Use --disable-gpu (same as philo.js) rather than SwiftShader.
            # SwiftShader requires libvk_swiftshader.so / Vulkan libs that are
            # frequently absent in minimal Docker images and cause an immediate
            # crash.  x11grab captures pixels from the Xvfb framebuffer
            # regardless of the rendering backend, so software decode via the
            # CPU path is perfectly fine here.
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-gpu-sandbox",
            # ── Anti-automation detection ─────────────────────────────────────
            "--disable-blink-features=AutomationControlled",
            "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            # ── Startup / first-run suppression ──────────────────────────────
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-component-update",
            "--ash-no-nudges",
            "--password-store=basic",
            "--disable-sync",                   # no Google account sync prompt
            "--disable-background-networking",  # no update checks at startup
            "--metrics-recording-only",         # suppress crash-reporter dialogs
            "--no-pings",
            # ── General hardening ─────────────────────────────────────────────
            "--disable-dev-shm-usage",
            "--disable-extensions",
            "--disable-infobars",
            "--autoplay-policy=no-user-gesture-required",
            # NOTE: do NOT add --mute-audio here.  Pluto TV's player checks the
            # AudioContext state before starting the stream; muting at the browser
            # level can prevent it from ever transitioning out of "Loading stream..."
            f"--window-size={DISPLAY_W},{DISPLAY_H}",
            "--start-maximized",
            "--kiosk",                          # full-screen, no browser chrome
            f"--app={pluto_url}",
        ],
        env=browser_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,   # capture stderr so crashes are visible in logs
    )

    # Wait for Chromium to load the page and the stream to begin.
    # STARTUP_WAIT defaults to 12 s (Dockerfile) — increase via env if needed.
    time.sleep(STARTUP_WAIT)

    if sess.browser_proc.poll() is not None:
        err = ""
        try:
            err = sess.browser_proc.stderr.read().decode(errors="replace").strip()
        except Exception:
            pass
        raise RuntimeError(
            f"Chromium exited (code {sess.browser_proc.returncode})"
            + (f": {err[:1000]}" if err else " — check CHROMIUM_PATH and GPU/display setup")
        )

    # ── 4. ffmpeg x11grab → MPEG-TS ─────────────────────────────────────────
    ffmpeg_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        # Video input
        "-f", "x11grab",
        "-framerate", str(FRAMERATE),
        "-video_size", f"{DISPLAY_W}x{DISPLAY_H}",
        "-i", f":{display_num}.0+0,0",
    ]

    # VAAPI needs -vaapi_device before the output section
    ffmpeg_cmd += _vaapi_device_args(encoder)

    # Audio input (PulseAudio null sink) — only if pulse started
    has_audio = sess.pulse_proc is not None and _pulse_socket_path(display_num) is not None
    if has_audio:
        pulse_sock = _pulse_socket_path(display_num)
        ffmpeg_cmd += [
            "-f", "pulse",
            "-ac", "2",
            "-ar", "48000",
            "-server", f"unix:{pulse_sock}",
            "-i", "default",
        ]

    # Video filter chain (VAAPI hwupload, or nothing for NVENC/libx264)
    ffmpeg_cmd += _build_vf_chain(encoder)

    # Video encoder
    ffmpeg_cmd += _build_video_encoder_args(encoder)

    # Audio encoder (AAC if audio captured, else no audio)
    if has_audio:
        ffmpeg_cmd += ["-c:a", "aac", "-b:a", "192k", "-ar", "48000"]
    else:
        ffmpeg_cmd += ["-an"]

    # Output: MPEG-TS to stdout
    ffmpeg_cmd += ["-f", "mpegts", "pipe:1"]

    sess.ffmpeg_proc = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "DISPLAY": f":{display_num}"},
    )

    if sess.ffmpeg_proc.poll() is not None:
        raise RuntimeError(f"ffmpeg failed to start (encoder={encoder})")

    sess.broadcast = _BroadcastPipe(sess.ffmpeg_proc.stdout)
    sess.started   = True

    logger.info(
        "[pluto-x11] session started ch=%s display=:%d encoder=%s "
        "pid_xvfb=%d pid_ff=%d audio=%s",
        channel_id, display_num, encoder,
        sess.xvfb_proc.pid, sess.ffmpeg_proc.pid,
        "on" if has_audio else "off",
    )
    return sess


def _terminate_session(sess: _X11Session) -> None:
    """Kill all processes for a session, in reverse start order."""
    if sess.broadcast:
        sess.broadcast.stop()

    for proc in (sess.ffmpeg_proc, sess.browser_proc, sess.pulse_proc, sess.xvfb_proc):
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            except Exception:
                pass

    # Clean up per-session throwaway Chromium profile
    import shutil
    profile_dir = f"/tmp/pluto-x11-profile-{sess.display_num}"
    try:
        shutil.rmtree(profile_dir, ignore_errors=True)
    except Exception:
        pass

    # Clean up PulseAudio socket dir for this display
    pulse_dir = f"/tmp/pulse-x11-{sess.display_num}"
    try:
        shutil.rmtree(pulse_dir, ignore_errors=True)
    except Exception:
        pass

    logger.info("[pluto-x11] session stopped ch=%s display=:%d",
                sess.channel_id, sess.display_num)


# ---------------------------------------------------------------------------
# Background reaper
# ---------------------------------------------------------------------------

def _reaper() -> None:
    while True:
        time.sleep(5)
        to_evict: list[str] = []
        with _sessions_lock:
            for cid, sess in _sessions.items():
                idle = time.monotonic() - sess.last_read
                # Evict: no readers AND idle past timeout, OR ffmpeg died
                if sess.readers == 0 and idle > IDLE_TIMEOUT:
                    to_evict.append(cid)
                elif sess.broadcast and not sess.broadcast.is_alive():
                    logger.warning("[pluto-x11] ffmpeg pipe died for ch=%s — evicting", cid)
                    to_evict.append(cid)
        for cid in to_evict:
            with _sessions_lock:
                sess = _sessions.pop(cid, None)
            if sess:
                _terminate_session(sess)


def _cleanup_stale_pulse_dirs() -> None:
    """
    On worker startup, remove any PulseAudio socket dirs from a previous run
    that share our worker's PID namespace.  This is a best-effort cleanup;
    failures are silently ignored.
    """
    import glob, shutil
    for d in glob.glob("/tmp/pulse-x11-*"):
        try:
            shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass
    # Also reset the display counter file so a fresh container starts at :200
    # (only if no Xvfb lock files exist — i.e. we're the first worker up)
    import glob as _glob
    if not _glob.glob("/tmp/.X*-lock"):
        try:
            import os as _os
            _os.remove("/tmp/pluto_x11_display.count")
        except FileNotFoundError:
            pass


_cleanup_stale_pulse_dirs()
threading.Thread(target=_reaper, daemon=True, name="pluto-x11-reaper").start()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def stream_channel(channel_id: str, pluto_url: str) -> Iterator[bytes]:
    """
    Generator that yields raw MPEG-TS bytes from an x11grab session.

    Manages session lifecycle: starts a new session on first reader, shares
    the ffmpeg pipe across concurrent readers, and triggers idle teardown
    after the last reader disconnects.

    Usage in a Flask route::

        return Response(
            stream_with_context(stream_channel(channel_id, pluto_url)),
            mimetype='video/mp2t',
        )
    """
    if not ENABLED:
        logger.error("[pluto-x11] subsystem disabled (PLUTO_X11_ENABLED=0)")
        return

    # Acquire or create session
    with _sessions_lock:
        sess = _sessions.get(channel_id)
        if sess is None or (sess.broadcast and not sess.broadcast.is_alive()):
            if sess:
                _terminate_session(sess)
            sess = _launch_session(channel_id, pluto_url)
            _sessions[channel_id] = sess
        sess.readers += 1

    reader_q = sess.broadcast.add_reader()
    logger.debug("[pluto-x11] reader attached ch=%s readers=%d", channel_id, sess.readers)

    try:
        while True:
            try:
                chunk = reader_q.get(timeout=10)
            except queue.Empty:
                # No data in 10 s — ffmpeg may have stalled; yield empty to
                # keep the HTTP connection alive and let the reaper decide.
                continue
            if chunk is None:
                break   # EOF sentinel from _BroadcastPipe
            sess.last_read = time.monotonic()
            yield chunk
    finally:
        sess.broadcast.remove_reader(reader_q)
        with _sessions_lock:
            sess.readers = max(0, sess.readers - 1)
            sess.last_read = time.monotonic()
        logger.debug("[pluto-x11] reader detached ch=%s readers=%d", channel_id, sess.readers)


def active_sessions() -> list[dict]:
    """Return a list of dicts describing currently active sessions (for admin UI)."""
    out = []
    with _sessions_lock:
        for cid, sess in _sessions.items():
            out.append({
                "channel_id":  cid,
                "display":     f":{sess.display_num}",
                "encoder":     sess.encoder,
                "readers":     sess.readers,
                "idle_secs":   round(time.monotonic() - sess.last_read, 1),
                "ffmpeg_alive": sess.broadcast.is_alive() if sess.broadcast else False,
            })
    return out
