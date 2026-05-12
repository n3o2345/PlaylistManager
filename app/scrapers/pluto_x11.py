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
    └─ Playwright Chromium  (non-headless, launched on Xvfb display)
         └─ CSS injection   (hides UI chrome, fullscreens <video>)
         └─ CDP fullscreen  (removes browser chrome)
         └─ keepalive loop  (dismisses overlays, resumes paused video)
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
PLUTO_X11_STARTUP_WAIT  Seconds to wait for video to begin playing before grabbing (default 15)
PLUTO_X11_ENCODER       Force encoder: h264_nvenc | h264_vaapi | libx264
PULSE_SERVER            Override PulseAudio server address
"""
from __future__ import annotations

import logging
import os
import queue
import subprocess
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
STARTUP_WAIT    = int(os.environ.get("PLUTO_X11_STARTUP_WAIT", "15"))
FORCE_ENCODER   = os.environ.get("PLUTO_X11_ENCODER", "").strip().lower() or None
CHUNK_SIZE      = 65536
MAX_QUEUE_DEPTH = 64

# ---------------------------------------------------------------------------
# GPU encoder detection
# ---------------------------------------------------------------------------

_ENCODER_CACHE: str | None = None
_ENCODER_LOCK  = threading.Lock()


def _detect_encoder() -> str:
    global _ENCODER_CACHE
    with _ENCODER_LOCK:
        if FORCE_ENCODER:
            if _ENCODER_CACHE != FORCE_ENCODER:
                logger.info("[pluto-x11] GPU encoder forced via env: %s", FORCE_ENCODER)
                _ENCODER_CACHE = FORCE_ENCODER
            return _ENCODER_CACHE

        if _ENCODER_CACHE is not None:
            return _ENCODER_CACHE

        try:
            out = subprocess.check_output(
                ["ffmpeg", "-hide_banner", "-encoders"],
                stderr=subprocess.STDOUT, text=True, timeout=10,
            )
        except Exception as e:
            logger.warning("[pluto-x11] ffmpeg encoder probe failed: %s — will retry later", e)
            return "libx264"

        if "h264_nvenc" in out:
            try:
                subprocess.check_call(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error",
                     "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                     "-vframes", "4", "-c:v", "h264_nvenc", "-f", "null", "-"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
                )
                logger.info("[pluto-x11] GPU encoder: h264_nvenc (NVIDIA)")
                _ENCODER_CACHE = "h264_nvenc"
                return _ENCODER_CACHE
            except Exception as exc:
                logger.warning("[pluto-x11] h264_nvenc probe failed (%s)", exc)

        if "h264_vaapi" in out and os.path.exists("/dev/dri/renderD128"):
            try:
                subprocess.check_call(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error",
                     "-vaapi_device", "/dev/dri/renderD128",
                     "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                     "-vframes", "4", "-vf", "format=nv12,hwupload",
                     "-c:v", "h264_vaapi", "-f", "null", "-"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
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
    if encoder == "h264_nvenc":
        return ["-c:v", "h264_nvenc", "-preset", "p2", "-tune", "ll",
                "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double_bitrate(BITRATE),
                "-g", str(FRAMERATE * 2), "-rc", "cbr"]
    elif encoder == "h264_vaapi":
        return ["-c:v", "h264_vaapi",
                "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double_bitrate(BITRATE),
                "-g", str(FRAMERATE * 2)]
    else:
        return ["-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double_bitrate(BITRATE),
                "-g", str(FRAMERATE * 2)]


def _build_vf_chain(encoder: str) -> list[str]:
    return ["-vf", "format=nv12,hwupload"] if encoder == "h264_vaapi" else []


def _vaapi_device_args(encoder: str) -> list[str]:
    return ["-vaapi_device", "/dev/dri/renderD128"] if encoder == "h264_vaapi" else []


def _double_bitrate(bitrate: str) -> str:
    try:
        suffix = bitrate[-1].lower()
        if suffix in ("k", "m"):
            return str(int(bitrate[:-1]) * 2) + suffix
    except (IndexError, ValueError):
        pass
    return bitrate


# ---------------------------------------------------------------------------
# Display number allocation — cross-process safe
# ---------------------------------------------------------------------------

_DISPLAY_LOCK_PATH  = "/tmp/pluto_x11_display.lock"
_DISPLAY_COUNT_PATH = "/tmp/pluto_x11_display.count"
_DISPLAY_BASE       = 200


def _alloc_display() -> int:
    import fcntl
    with open(_DISPLAY_LOCK_PATH, "w") as lf:
        fcntl.lockf(lf, fcntl.LOCK_EX)
        try:
            try:
                with open(_DISPLAY_COUNT_PATH) as cf:
                    n = int(cf.read().strip())
            except (FileNotFoundError, ValueError):
                n = _DISPLAY_BASE
            with open(_DISPLAY_COUNT_PATH, "w") as cf:
                cf.write(str(n + 1))
            return n
        finally:
            fcntl.lockf(lf, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# Per-channel PulseAudio daemon (mirrors philo.js _createPulseSink exactly)
# ---------------------------------------------------------------------------

def _start_pulseaudio(display_num: int) -> subprocess.Popen | None:
    import shutil
    socket_dir  = f"/tmp/pulse-x11-{display_num}"
    pa_socket   = f"{socket_dir}/native"
    pa_conf     = f"{socket_dir}/pa.conf"
    daemon_conf = f"{socket_dir}/daemon.conf"
    cookie_dir  = f"{socket_dir}/.config/pulse"
    cookie_file = f"{socket_dir}/.pulse-cookie"

    shutil.rmtree(socket_dir, ignore_errors=True)
    os.makedirs(cookie_dir, exist_ok=True)

    _zero = bytes(256)
    for _cf in (cookie_file, f"{cookie_dir}/cookie"):
        try:
            open(_cf, "wb").write(_zero)
        except Exception:
            pass

    try:
        with open(pa_conf, "w") as f:
            f.write(
                f"load-module module-null-sink sink_name=out\n"
                f"set-default-sink out\n"
                f"set-default-source out.monitor\n"
                f"load-module module-native-protocol-unix "
                f"auth-anonymous=1 auth-cookie-enabled=0 socket={pa_socket}\n"
            )
        with open(daemon_conf, "w") as f:
            f.write("default-sample-rate = 48000\nexit-idle-time = -1\nlog-level = error\n")
    except Exception as e:
        logger.debug("[pluto-x11] PulseAudio config write failed: %s", e)
        return None

    pa_env = {
        **os.environ,
        "PULSE_RUNTIME_PATH":       socket_dir,
        "HOME":                     socket_dir,
        "XDG_RUNTIME_DIR":          socket_dir,
        "PULSE_COOKIE":             cookie_file,
        "DBUS_SESSION_BUS_ADDRESS": "unix:path=/nonexistent",
        "DBUS_SYSTEM_BUS_ADDRESS":  "unix:path=/nonexistent",
    }

    try:
        proc = subprocess.Popen(
            ["pulseaudio", "--daemonize=no", "--exit-idle-time=-1",
             "--disallow-exit", "-n", f"--file={pa_conf}", "--log-target=stderr"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=pa_env,
        )
    except FileNotFoundError:
        logger.debug("[pluto-x11] PulseAudio not installed — audio disabled")
        return None
    except Exception as e:
        logger.debug("[pluto-x11] PulseAudio start failed: %s", e)
        return None

    # Poll until socket exists and pactl confirms the sink is ready (up to 8 s)
    ready = False
    for _ in range(80):
        time.sleep(0.1)
        if proc.poll() is not None:
            logger.debug("[pluto-x11] PulseAudio exited early — audio disabled")
            return None
        if not os.path.exists(pa_socket):
            continue
        try:
            r = subprocess.run(
                ["pactl", f"--server=unix:{pa_socket}", "list", "short", "sinks"],
                capture_output=True, text=True, timeout=1,
                env={**pa_env, "PULSE_SERVER": f"unix:{pa_socket}"},
            )
            if "out" in r.stdout:
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
    def __init__(self, pipe):
        self._pipe    = pipe
        self._readers: list[queue.Queue] = []
        self._lock    = threading.Lock()
        self._stopped = False
        self._thread  = threading.Thread(target=self._pump, daemon=True,
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
                        pass
        except Exception as e:
            logger.warning("[pluto-x11] pump error: %s", e)
        finally:
            self._stopped = True
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
    browser_proc: object | None           = field(default=None, repr=False)  # Playwright Browser
    pw_context:   object | None           = field(default=None, repr=False)  # Playwright BrowserContext
    pw_page:      object | None           = field(default=None, repr=False)  # Playwright Page
    ffmpeg_proc:  subprocess.Popen | None = field(default=None, repr=False)
    broadcast:    _BroadcastPipe   | None = field(default=None, repr=False)
    keepalive_stop: object | None         = field(default=None, repr=False)

    readers:   int   = 0
    last_read: float = field(default_factory=time.monotonic)
    started:   bool  = False
    lock:      threading.Lock = field(default_factory=threading.Lock)


_sessions:     dict[str, _X11Session] = {}
_sessions_lock = threading.Lock()


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
    # Skip any display whose lock file already exists (another worker owns it)
    for _attempt in range(20):
        if not os.path.exists(f"/tmp/.X{display_num}-lock"):
            break
        logger.warning("[pluto-x11] display :%d already locked — skipping", display_num)
        display_num = _alloc_display()
        sess.display_num = display_num
    else:
        raise RuntimeError("Could not find a free Xvfb display after 20 attempts")

    sess.xvfb_proc = subprocess.Popen(
        ["Xvfb", f":{display_num}",
         "-screen", "0", f"{DISPLAY_W}x{DISPLAY_H}x24",
         "-ac", "-nolisten", "tcp", "-nolisten", "unix"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)

    if sess.xvfb_proc.poll() is not None:
        raise RuntimeError(f"Xvfb failed to start for display :{display_num}")

    # ── 2. PulseAudio ──────────────────────────────────────────────────────
    sess.pulse_proc = _start_pulseaudio(display_num)
    pulse_socket    = _pulse_socket_path(display_num)

    # ── 3. Playwright Chromium (non-headless on Xvfb display) ──────────────
    # Use Playwright instead of raw subprocess — it lets us interact with the
    # page after launch (inject CSS, click play, CDP fullscreen, keepalive).
    # This is exactly how philo.js works and is why philo produces video.
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "playwright is not installed — run: pip install playwright && playwright install chromium"
        )

    pw_env = {
        **os.environ,
        "DISPLAY":    f":{display_num}",
        "PULSE_SERVER": (
            f"unix:{pulse_socket}" if pulse_socket
            else os.environ.get("PULSE_SERVER", "")
        ),
        **({"PULSE_RUNTIME_PATH": os.path.dirname(pulse_socket),
            "HOME":               os.path.dirname(pulse_socket),
            "XDG_RUNTIME_DIR":    os.path.dirname(pulse_socket)}
           if pulse_socket else {}),
    }

    pw_args = [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--disable-software-rasterizer",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run", "--no-default-browser-check",
        "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
        "--disable-infobars", "--disable-notifications", "--hide-scrollbars",
        "--start-maximized", "--start-fullscreen",
        f"--window-size={DISPLAY_W},{DISPLAY_H}", "--window-position=0,0",
        "--disable-session-crashed-bubble", "--hide-crash-restore-bubble",
        "--disable-features=MediaSessionService,HardwareMediaKeyHandling",
    ]

    _pw = sync_playwright().start()
    try:
        browser = _pw.chromium.launch(
            headless=False,
            env=pw_env,
            args=pw_args,
        )
    except Exception as e:
        _pw.stop()
        raise RuntimeError(f"Playwright Chromium launch failed: {e}")

    sess.browser_proc = browser  # store so _terminate_session can close it

    # Create context with desktop UA
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        viewport={"width": DISPLAY_W, "height": DISPLAY_H},
    )
    sess.pw_context = context

    page = context.new_page()
    sess.pw_page = page

    # Navigate to Pluto live TV
    logger.info("[pluto-x11] Navigating to %s on display :%d", pluto_url, display_num)
    try:
        page.goto(pluto_url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        try:
            page.goto(pluto_url, timeout=30000)
        except Exception:
            pass

    # Wait for the page to settle
    page.wait_for_timeout(3000)
    logger.info("[pluto-x11] Landed on: %s", page.url())

    # ── 4. Inject fullscreen CSS — hides all UI chrome, fullscreens <video> ─
    # Mirrors philo.js navigateToChannel step 6 exactly.
    try:
        page.evaluate("""() => {
            if (document.getElementById('pluto-x11-fullscreen')) return;
            const s = document.createElement('style');
            s.id = 'pluto-x11-fullscreen';
            s.textContent = `
                [class*="overlay"],[class*="Overlay"],[class*="controls"],[class*="Controls"],
                [class*="nav"],[class*="Nav"],[class*="header"],[class*="Header"],
                [class*="banner"],[class*="Badge"],[class*="modal"],[class*="Modal"],
                [class*="tooltip"],[class*="Tooltip"],[class*="uiLayer"],[class*="PlayerUI"],
                [class*="stillWatching"],[class*="adOverlay"],[class*="pauseScreen"],
                [class*="endCard"],[class*="spinner"],[class*="Spinner"],
                [class*="loading"],[class*="Loading"]
                { opacity:0!important; visibility:hidden!important; pointer-events:none!important; }
                video { position:fixed!important; top:0!important; left:0!important;
                        width:100vw!important; height:100vh!important; z-index:99999!important;
                        object-fit:cover!important; background:#000!important; }
                body  { background:#000!important; overflow:hidden!important; margin:0!important; }
                *     { cursor:none!important; }
            `;
            document.head.appendChild(s);
            const v = document.querySelector('video');
            if (v) { v.muted = false; v.volume = 1.0; if (v.paused) v.play().catch(()=>{}); }
        }""")
    except Exception as e:
        logger.debug("[pluto-x11] CSS injection failed (non-fatal): %s", e)

    # ── 5. CDP fullscreen — removes browser chrome from the Xvfb frame ──────
    try:
        cdp = context.new_cdp_session(page)
        win = cdp.send("Browser.getWindowForTarget")
        cdp.send("Browser.setWindowBounds",
                 {"windowId": win["windowId"], "bounds": {"windowState": "fullscreen"}})
        cdp.detach()
        logger.debug("[pluto-x11] CDP fullscreen applied")
    except Exception:
        try:
            page.keyboard.press("F11")
        except Exception:
            pass

    # ── 6. Click play / dismiss cookie banners ───────────────────────────────
    try:
        page.evaluate("""() => {
            // Dismiss cookie/consent banners
            for (const sel of ['[id*="accept"],[class*="accept"]','button[id*="consent"]',
                                'button:not([disabled])']) {
                for (const el of document.querySelectorAll(sel)) {
                    const t = (el.textContent || '').toLowerCase();
                    if (['accept','agree','ok','got it','allow'].some(w => t.includes(w))) {
                        el.click();
                    }
                }
            }
            // Click play on any visible play button
            const playSels = [
                '[aria-label*="Play" i]','[aria-label*="Watch" i]',
                'button[class*="play" i]','button[class*="Play"]',
                '[class*="playButton"]','[class*="PlayButton"]',
                '[data-testid*="play" i]',
            ];
            for (const sel of playSels) {
                for (const el of document.querySelectorAll(sel)) {
                    if (el.offsetParent !== null) { el.click(); return; }
                }
            }
        }""")
    except Exception:
        pass

    # ── 7. Wait for video to actually start playing ──────────────────────────
    logger.info("[pluto-x11] Waiting up to %ds for video to start...", STARTUP_WAIT)
    video_started = False
    deadline = time.monotonic() + STARTUP_WAIT
    while time.monotonic() < deadline:
        try:
            result = page.evaluate("""() => {
                const v = document.querySelector('video');
                if (!v) return {found: false};
                if (v.muted) { v.muted = false; v.volume = 1.0; }
                return {
                    found:       true,
                    readyState:  v.readyState,
                    currentTime: v.currentTime,
                    paused:      v.paused,
                    src:         !!(v.src || v.currentSrc),
                };
            }""")
            if result.get("found") and result.get("readyState", 0) >= 2:
                if not result.get("paused", True) or result.get("currentTime", 0) > 0:
                    video_started = True
                    logger.info("[pluto-x11] Video playing: readyState=%d t=%.1f",
                                result["readyState"], result.get("currentTime", 0))
                    break
                # Try clicking play if paused
                if result.get("paused"):
                    try:
                        page.evaluate("() => { const v=document.querySelector('video'); if(v) v.play(); }")
                    except Exception:
                        pass
        except Exception:
            pass
        time.sleep(1)

    if not video_started:
        logger.warning("[pluto-x11] Video did not start in %ds — proceeding anyway "
                       "(may grab loading screen; check Pluto URL)", STARTUP_WAIT)

    # ── 8. Start keepalive thread ────────────────────────────────────────────
    stop_flag = threading.Event()

    def _keepalive():
        while not stop_flag.is_set():
            stop_flag.wait(30)
            if stop_flag.is_set():
                break
            try:
                result = page.evaluate("""() => {
                    // Dismiss "still watching?" overlays
                    for (const btn of document.querySelectorAll('button')) {
                        const t = (btn.textContent || '').toLowerCase();
                        if (['still watching','continue','yes','keep watching'].some(w=>t.includes(w))) {
                            btn.click();
                            return {action:'dismissed_overlay'};
                        }
                    }
                    // Resume paused video
                    const v = document.querySelector('video');
                    if (v) {
                        v.muted = false; v.volume = 1.0;
                        if (v.paused || v.ended) { v.play(); return {action:'resumed'}; }
                    }
                    // Detect session expiry / error page
                    const url = window.location.href;
                    const hasErr = /error|unavailable|not available/i.test(document.body?.innerText||'');
                    return {action: hasErr ? 'error' : 'ok', url};
                }""")
                if result.get("action") not in ("ok", None):
                    logger.info("[pluto-x11] keepalive: %s", result.get("action"))
            except Exception:
                pass

    t = threading.Thread(target=_keepalive, daemon=True, name=f"pluto-x11-keepalive-{channel_id}")
    t.start()
    sess.keepalive_stop = stop_flag

    # ── 9. ffmpeg x11grab → MPEG-TS ─────────────────────────────────────────
    has_audio = pulse_socket is not None
    ffmpeg_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-f", "x11grab",
        "-framerate", str(FRAMERATE),
        "-video_size", f"{DISPLAY_W}x{DISPLAY_H}",
        "-i", f":{display_num}.0+0,0",
    ]
    ffmpeg_cmd += _vaapi_device_args(encoder)

    if has_audio:
        ffmpeg_cmd += [
            "-f", "pulse", "-ac", "2", "-ar", "48000",
            "-server", f"unix:{pulse_socket}", "-i", "default",
        ]

    ffmpeg_cmd += _build_vf_chain(encoder)
    ffmpeg_cmd += _build_video_encoder_args(encoder)
    ffmpeg_cmd += (["-c:a", "aac", "-b:a", "192k", "-ar", "48000"] if has_audio else ["-an"])
    ffmpeg_cmd += ["-f", "mpegts", "pipe:1"]

    ffmpeg_env = {**os.environ, "DISPLAY": f":{display_num}"}
    if has_audio:
        ffmpeg_env["PULSE_SERVER"] = f"unix:{pulse_socket}"

    sess.ffmpeg_proc = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=ffmpeg_env,
    )

    time.sleep(1)
    if sess.ffmpeg_proc.poll() is not None:
        err = sess.ffmpeg_proc.stderr.read().decode(errors="replace").strip()
        raise RuntimeError(f"ffmpeg failed to start (encoder={encoder}): {err[:500]}")

    sess.broadcast = _BroadcastPipe(sess.ffmpeg_proc.stdout)
    sess.started   = True

    logger.info(
        "[pluto-x11] session started ch=%s display=:%d encoder=%s "
        "pid_xvfb=%d pid_ff=%d audio=%s video_confirmed=%s",
        channel_id, display_num, encoder,
        sess.xvfb_proc.pid, sess.ffmpeg_proc.pid,
        "on" if has_audio else "off",
        video_started,
    )
    return sess


def _terminate_session(sess: _X11Session) -> None:
    import shutil

    # Stop keepalive thread
    if sess.keepalive_stop:
        try:
            sess.keepalive_stop.set()
        except Exception:
            pass

    if sess.broadcast:
        sess.broadcast.stop()

    # Close Playwright page/context/browser
    for obj, method in [
        (sess.pw_page,    "close"),
        (sess.pw_context, "close"),
        (sess.browser_proc, "close"),  # Playwright Browser.close()
    ]:
        if obj is not None:
            try:
                getattr(obj, method)()
            except Exception:
                pass

    # Kill ffmpeg, pulse, xvfb
    for proc in (sess.ffmpeg_proc, sess.pulse_proc, sess.xvfb_proc):
        if proc and isinstance(proc, subprocess.Popen) and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            except Exception:
                pass

    # Clean up temp dirs
    for d in (f"/tmp/pulse-x11-{sess.display_num}",
              f"/tmp/pluto-x11-profile-{sess.display_num}"):
        shutil.rmtree(d, ignore_errors=True)

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


def _cleanup_on_start() -> None:
    import glob, shutil
    for d in glob.glob("/tmp/pulse-x11-*") + glob.glob("/tmp/pluto-x11-profile-*"):
        shutil.rmtree(d, ignore_errors=True)
    if not glob.glob("/tmp/.X*-lock"):
        try:
            os.remove(_DISPLAY_COUNT_PATH)
        except FileNotFoundError:
            pass


_cleanup_on_start()
threading.Thread(target=_reaper, daemon=True, name="pluto-x11-reaper").start()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def stream_channel(channel_id: str, pluto_url: str) -> Iterator[bytes]:
    if not ENABLED:
        logger.error("[pluto-x11] subsystem disabled (PLUTO_X11_ENABLED=0)")
        return

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
                continue
            if chunk is None:
                break
            sess.last_read = time.monotonic()
            yield chunk
    finally:
        sess.broadcast.remove_reader(reader_q)
        with _sessions_lock:
            sess.readers = max(0, sess.readers - 1)
            sess.last_read = time.monotonic()
        logger.debug("[pluto-x11] reader detached ch=%s readers=%d", channel_id, sess.readers)


def active_sessions() -> list[dict]:
    out = []
    with _sessions_lock:
        for cid, sess in _sessions.items():
            out.append({
                "channel_id":   cid,
                "display":      f":{sess.display_num}",
                "encoder":      sess.encoder,
                "readers":      sess.readers,
                "idle_secs":    round(time.monotonic() - sess.last_read, 1),
                "ffmpeg_alive": sess.broadcast.is_alive() if sess.broadcast else False,
            })
    return out
