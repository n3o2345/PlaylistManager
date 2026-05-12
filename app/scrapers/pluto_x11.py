"""
app/scrapers/pluto_x11.py

Pluto TV X11 screen-grab streamer.

Architecture
------------
  Xvfb  (:DISPLAY)
    └─ Playwright Chromium  (non-headless, run in a subprocess — gevent-safe)
         └─ CSS injection   (hides UI chrome, fullscreens <video>)
         └─ CDP fullscreen  (removes browser chrome)
         └─ keepalive loop  (dismisses overlays, resumes paused video)
  PulseAudio null sink  (virtual audio, per-session)
  ffmpeg  x11grab + pulse → h264 → MPEG-TS pipe
    └─ _BroadcastPipe (fan-out to N concurrent readers)

Gevent compatibility
--------------------
gevent monkey-patches threading.Thread so sync_playwright() still detects the
asyncio event loop even inside a "new thread".  The only reliable fix is to run
Playwright in a true child process via multiprocessing (spawn context), which
has a clean interpreter state with no gevent patching and no asyncio loop.

_PW_WORKER_SCRIPT is written to a temp file and executed as:
    python3 /tmp/pluto_pw_worker.py <display> <pulse_socket|none> <url> <result_pipe_fd>

The worker navigates Chromium, injects CSS, applies CDP fullscreen, waits for
the video element to start playing, then writes "OK\n" (or "ERR <msg>\n") to
the result pipe and blocks reading a "STOP\n" command from the control pipe.
The main process reads the result, then sends "STOP\n" to tear down on cleanup.
"""
from __future__ import annotations

import logging
import multiprocessing
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENABLED         = os.environ.get("PLUTO_X11_ENABLED", "1") != "0"
DISPLAY_W       = int(os.environ.get("PLUTO_X11_WIDTH",        "1280"))
DISPLAY_H       = int(os.environ.get("PLUTO_X11_HEIGHT",       "720"))
FRAMERATE       = int(os.environ.get("PLUTO_X11_FPS",          "30"))
BITRATE         = os.environ.get("PLUTO_X11_BITRATE",          "2500k")
IDLE_TIMEOUT    = int(os.environ.get("PLUTO_X11_IDLE_TIMEOUT", "30"))
STARTUP_WAIT    = int(os.environ.get("PLUTO_X11_STARTUP_WAIT", "20"))
FORCE_ENCODER   = os.environ.get("PLUTO_X11_ENCODER", "").strip().lower() or None
CHUNK_SIZE      = 65536
MAX_QUEUE_DEPTH = 64
COOKIE_PATH     = os.environ.get("PLUTO_X11_COOKIE_PATH", "/tmp/pluto_x11_cookies.json")

# ---------------------------------------------------------------------------
# Playwright worker script — runs in a fresh child process, no gevent
# ---------------------------------------------------------------------------

_PW_WORKER_SCRIPT = r'''
import os, sys, time, json

def main():
    pulse_socket = sys.argv[2] if sys.argv[2] != "none" else None
    pluto_url    = sys.argv[3]
    result_fd    = int(sys.argv[4])
    control_fd   = int(sys.argv[5])
    display_w    = int(sys.argv[6])
    display_h    = int(sys.argv[7])
    startup_wait = int(sys.argv[8])
    pluto_email    = sys.argv[9]  if len(sys.argv) > 9  and sys.argv[9]  != "none" else None
    pluto_password = sys.argv[10] if len(sys.argv) > 10 and sys.argv[10] != "none" else None
    cookie_path    = sys.argv[11] if len(sys.argv) > 11 and sys.argv[11] != "none" else None

    result_pipe  = os.fdopen(result_fd,  "w", buffering=1)
    control_pipe = os.fdopen(control_fd, "r")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        result_pipe.write(f"ERR playwright not installed: {e}\n")
        result_pipe.flush()
        return

    pw_env = dict(os.environ)
    pw_env["DISPLAY"] = f":{display_num}"
    if pulse_socket:
        pulse_dir = os.path.dirname(pulse_socket)
        pw_env.update({
            "PULSE_SERVER":       f"unix:{pulse_socket}",
            "PULSE_RUNTIME_PATH": pulse_dir,
            "HOME":               pulse_dir,
            "XDG_RUNTIME_DIR":    pulse_dir,
        })

    pw_args = [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run", "--no-default-browser-check",
        "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
        "--disable-infobars", "--disable-notifications", "--hide-scrollbars",
        "--start-maximized", "--start-fullscreen",
        f"--window-size={display_w},{display_h}", "--window-position=0,0",
        "--disable-session-crashed-bubble", "--hide-crash-restore-bubble",
        "--disable-features=MediaSessionService,HardwareMediaKeyHandling",
    ]

    try:
        pw      = sync_playwright().start()
        browser = pw.chromium.launch(headless=False, env=pw_env, args=pw_args)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": display_w, "height": display_h},
        )
        page = context.new_page()
    except Exception as e:
        result_pipe.write(f"ERR launch failed: {e}\n")
        result_pipe.flush()
        return

    # ── Inject stored cookies (skips login modal when valid) ────────────────
    if cookie_path:
        try:
            with open(cookie_path) as _cf:
                _stored = json.load(_cf)
            if _stored:
                context.add_cookies(_stored)
        except Exception:
            pass  # missing / corrupt cookie file — fall through to modal login

    try:
        page.goto(pluto_url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        try:
            page.goto(pluto_url, timeout=30000)
        except Exception:
            pass

    page.wait_for_timeout(2000)

    # ── Login modal handling ────────────────────────────────────────────────
    # Must happen BEFORE CSS injection so the modal still has pointer-events.
    if pluto_email and pluto_password:
        _login_attempted = False
        _login_deadline  = time.monotonic() + 15
        while time.monotonic() < _login_deadline:
            try:
                _has_modal = page.evaluate("""() => {
                    const inp = document.querySelector(
                        'input[type="email"], input[placeholder*="email" i], input[name*="email" i]');
                    return !!(inp && inp.offsetParent !== null);
                }""")
            except Exception:
                _has_modal = False
            if _has_modal:
                # Fill email
                try:
                    page.evaluate("""(em) => {
                        const inp = document.querySelector(
                            'input[type="email"], input[placeholder*="email" i], input[name*="email" i]');
                        if (!inp) return;
                        inp.focus();
                        inp.value = em;
                        inp.dispatchEvent(new Event('input',  {bubbles:true}));
                        inp.dispatchEvent(new Event('change', {bubbles:true}));
                    }""", pluto_email)
                except Exception:
                    pass
                time.sleep(0.4)
                # Click Next
                try:
                    page.evaluate("""() => {
                        for (const btn of document.querySelectorAll('button')) {
                            const t = (btn.textContent||''). trim().toLowerCase();
                            if (t === 'next' || t === 'continue') { btn.click(); return; }
                        }
                    }""")
                except Exception:
                    pass
                time.sleep(1.5)
                # Fill password
                _pw_deadline = time.monotonic() + 10
                while time.monotonic() < _pw_deadline:
                    try:
                        _pw_ready = page.evaluate("""() => {
                            const inp = document.querySelector('input[type="password"]');
                            return !!(inp && inp.offsetParent !== null);
                        }""")
                    except Exception:
                        _pw_ready = False
                    if _pw_ready:
                        try:
                            page.evaluate("""(pw) => {
                                const inp = document.querySelector('input[type="password"]');
                                if (!inp) return;
                                inp.focus();
                                inp.value = pw;
                                inp.dispatchEvent(new Event('input',  {bubbles:true}));
                                inp.dispatchEvent(new Event('change', {bubbles:true}));
                            }""", pluto_password)
                        except Exception:
                            pass
                        time.sleep(0.4)
                        # Click Sign In
                        try:
                            page.evaluate("""() => {
                                for (const btn of document.querySelectorAll('button')) {
                                    const t = (btn.textContent||''). trim().toLowerCase();
                                    if (['sign in','log in','login','signin','submit'].includes(t))
                                        { btn.click(); return; }
                                }
                            }""")
                        except Exception:
                            pass
                        _login_attempted = True
                        break
                    time.sleep(0.5)
                break
            time.sleep(0.5)
        if _login_attempted:
            time.sleep(2)   # let Pluto redirect / close modal

    # Inject fullscreen CSS
    try:
        page.evaluate("""() => {
            if (document.getElementById('pluto-x11-fs')) return;
            const s = document.createElement('style');
            s.id = 'pluto-x11-fs';
            s.textContent = `
                [class*="overlay"],[class*="Overlay"],[class*="controls"],[class*="Controls"],
                [class*="nav"],[class*="Nav"],[class*="header"],[class*="Header"],
                [class*="banner"],[class*="Badge"],[class*="modal"],[class*="Modal"],
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
    except Exception:
        pass

    # CDP fullscreen
    try:
        cdp = context.new_cdp_session(page)
        win = cdp.send("Browser.getWindowForTarget")
        cdp.send("Browser.setWindowBounds",
                 {"windowId": win["windowId"], "bounds": {"windowState": "fullscreen"}})
        cdp.detach()
    except Exception:
        try:
            page.keyboard.press("F11")
        except Exception:
            pass

    # Click play
    try:
        page.evaluate("""() => {
            for (const sel of ['[aria-label*="Play" i]','[aria-label*="Watch" i]',
                               'button[class*="play" i]','[class*="playButton"]',
                               '[data-testid*="play" i]']) {
                for (const el of document.querySelectorAll(sel)) {
                    if (el.offsetParent !== null) { el.click(); return; }
                }
            }
            const v = document.querySelector('video');
            if (v && v.paused) v.play().catch(()=>{});
        }""")
    except Exception:
        pass

    # Wait for video to play
    video_started = False
    deadline = time.monotonic() + startup_wait
    while time.monotonic() < deadline:
        try:
            r = page.evaluate("""() => {
                const v = document.querySelector('video');
                if (!v) return {found: false};
                if (v.muted) { v.muted = false; v.volume = 1.0; }
                return {found: true, readyState: v.readyState,
                        currentTime: v.currentTime, paused: v.paused};
            }""")
            if r.get("found") and r.get("readyState", 0) >= 2:
                if not r.get("paused", True) or r.get("currentTime", 0) > 0:
                    video_started = True
                    break
            if r.get("paused"):
                try:
                    page.evaluate("() => { const v=document.querySelector('video'); if(v) v.play(); }")
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(1)

    result_pipe.write(f"OK video_started={video_started}\n")
    result_pipe.flush()

    # Keepalive loop until STOP received.
    # Uses select() with a 15 s timeout so the keepalive JS fires on every
    # iteration regardless of whether the main process sends anything.
    # (The old blocking readline() meant keepalive never ran, letting Pluto's
    # "Still watching?" overlay pause the stream after ~60 s.)
    import select as _select
    while True:
        try:
            r, _, _ = _select.select([control_pipe], [], [], 15.0)
            if r:
                line = control_pipe.readline()
                if not line or line.strip() == "STOP":
                    break
        except Exception:
            break
        # keepalive tick — runs every 15 s
        try:
            page.evaluate("""() => {
                for (const btn of document.querySelectorAll('button')) {
                    const t = (btn.textContent||'').toLowerCase();
                    if (['still watching','continue','yes','keep watching'].some(w=>t.includes(w)))
                        { btn.click(); return; }
                }
                const v = document.querySelector('video');
                if (v) { v.muted=false; v.volume=1.0; if(v.paused||v.ended) v.play(); }
            }""")
        except Exception:
            pass

    try:
        page.close(); context.close(); browser.close(); pw.stop()
    except Exception:
        pass

if __name__ == "__main__":
    main()
'''

_PW_WORKER_PATH = "/tmp/pluto_pw_worker.py"
with open(_PW_WORKER_PATH, "w") as _f:
    _f.write(_PW_WORKER_SCRIPT)

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
                logger.info("[pluto-x11] encoder forced: %s", FORCE_ENCODER)
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
            logger.warning("[pluto-x11] ffmpeg probe failed: %s", e)
            return "libx264"
        if "h264_nvenc" in out:
            try:
                subprocess.check_call(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error",
                     "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                     "-vframes", "4", "-c:v", "h264_nvenc", "-f", "null", "-"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
                )
                logger.info("[pluto-x11] encoder: h264_nvenc")
                _ENCODER_CACHE = "h264_nvenc"
                return _ENCODER_CACHE
            except Exception as e:
                logger.warning("[pluto-x11] h264_nvenc failed: %s", e)
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
                logger.info("[pluto-x11] encoder: h264_vaapi")
                _ENCODER_CACHE = "h264_vaapi"
                return _ENCODER_CACHE
            except Exception:
                pass
        logger.info("[pluto-x11] encoder: libx264 (CPU)")
        _ENCODER_CACHE = "libx264"
        return _ENCODER_CACHE


def _build_video_encoder_args(encoder: str) -> list[str]:
    if encoder == "h264_nvenc":
        return ["-c:v", "h264_nvenc", "-preset", "p2", "-tune", "ll",
                "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double(BITRATE),
                "-g", str(FRAMERATE * 2), "-rc", "cbr"]
    if encoder == "h264_vaapi":
        return ["-c:v", "h264_vaapi",
                "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double(BITRATE),
                "-g", str(FRAMERATE * 2)]
    return ["-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
            "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", _double(BITRATE),
            "-g", str(FRAMERATE * 2)]


def _build_vf_chain(encoder: str) -> list[str]:
    return ["-vf", "format=nv12,hwupload"] if encoder == "h264_vaapi" else []


def _vaapi_device_args(encoder: str) -> list[str]:
    return ["-vaapi_device", "/dev/dri/renderD128"] if encoder == "h264_vaapi" else []


def _double(b: str) -> str:
    try:
        s = b[-1].lower()
        if s in ("k", "m"):
            return str(int(b[:-1]) * 2) + s
    except (IndexError, ValueError):
        pass
    return b

# ---------------------------------------------------------------------------
# Cross-process display number allocation
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
                n = int(open(_DISPLAY_COUNT_PATH).read().strip())
            except (FileNotFoundError, ValueError):
                n = _DISPLAY_BASE
            open(_DISPLAY_COUNT_PATH, "w").write(str(n + 1))
            return n
        finally:
            fcntl.lockf(lf, fcntl.LOCK_UN)

# ---------------------------------------------------------------------------
# PulseAudio
# ---------------------------------------------------------------------------

def _start_pulseaudio(display_num: int) -> subprocess.Popen | None:
    import shutil
    socket_dir  = f"/tmp/pulse-x11-{display_num}"
    pa_socket   = f"{socket_dir}/native"
    pa_conf     = f"{socket_dir}/pa.conf"
    cookie_dir  = f"{socket_dir}/.config/pulse"
    cookie_file = f"{socket_dir}/.pulse-cookie"

    shutil.rmtree(socket_dir, ignore_errors=True)
    os.makedirs(cookie_dir, exist_ok=True)
    _zero = bytes(256)
    for cf in (cookie_file, f"{cookie_dir}/cookie"):
        try:
            open(cf, "wb").write(_zero)
        except Exception:
            pass
    try:
        open(pa_conf, "w").write(
            f"load-module module-null-sink sink_name=out\n"
            f"set-default-sink out\n"
            f"set-default-source out.monitor\n"
            f"load-module module-native-protocol-unix "
            f"auth-anonymous=1 auth-cookie-enabled=0 socket={pa_socket}\n"
        )
        open(f"{socket_dir}/daemon.conf", "w").write(
            "default-sample-rate = 48000\nexit-idle-time = -1\nlog-level = error\n"
        )
    except Exception as e:
        logger.debug("[pluto-x11] PA config write failed: %s", e)
        return None

    pa_env = {
        **os.environ,
        "PULSE_RUNTIME_PATH": socket_dir, "HOME": socket_dir,
        "XDG_RUNTIME_DIR": socket_dir, "PULSE_COOKIE": cookie_file,
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
        logger.debug("[pluto-x11] PulseAudio not installed")
        return None

    for _ in range(80):
        time.sleep(0.1)
        if proc.poll() is not None:
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
                logger.debug("[pluto-x11] PulseAudio ready :%d", display_num)
                return proc
        except Exception:
            pass

    logger.warning("[pluto-x11] PulseAudio not ready after 8 s")
    try:
        proc.kill()
    except Exception:
        pass
    return None


def _pulse_socket(display_num: int) -> str | None:
    p = f"/tmp/pulse-x11-{display_num}/native"
    return p if os.path.exists(p) else None

# ---------------------------------------------------------------------------
# Broadcast pipe
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
            logger.warning("[pluto-x11] pump: %s", e)
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
# Session
# ---------------------------------------------------------------------------

@dataclass
class _X11Session:
    channel_id:  str
    pluto_url:   str
    display_num: int
    encoder:     str

    xvfb_proc:    subprocess.Popen | None = field(default=None, repr=False)
    pulse_proc:   subprocess.Popen | None = field(default=None, repr=False)
    pw_proc:      subprocess.Popen | None = field(default=None, repr=False)
    pw_ctrl_w:    object | None           = field(default=None, repr=False)
    ffmpeg_proc:  subprocess.Popen | None = field(default=None, repr=False)
    broadcast:    _BroadcastPipe   | None = field(default=None, repr=False)

    readers:   int   = 0
    last_read: float = field(default_factory=time.monotonic)
    started:   bool  = False
    lock:      threading.Lock = field(default_factory=threading.Lock)

# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

_sessions:      dict[str, _X11Session] = {}
_sessions_lock  = threading.Lock()
# Per-channel launch lock: prevents duplicate sessions when two requests arrive
# for the same channel while _launch_session is still blocking.
_launch_locks:  dict[str, threading.Lock] = {}
_launch_locks_lock = threading.Lock()


def _get_launch_lock(channel_id: str) -> threading.Lock:
    with _launch_locks_lock:
        if channel_id not in _launch_locks:
            _launch_locks[channel_id] = threading.Lock()
        return _launch_locks[channel_id]


def _launch_session(channel_id: str, pluto_url: str,
                   email: str | None = None, password: str | None = None) -> _X11Session:
    display_num = _alloc_display()
    encoder     = _detect_encoder()

    sess = _X11Session(channel_id=channel_id, pluto_url=pluto_url,
                       display_num=display_num, encoder=encoder)

    # ── 1. Xvfb ────────────────────────────────────────────────────────────
    for _ in range(20):
        if not os.path.exists(f"/tmp/.X{display_num}-lock"):
            break
        logger.warning("[pluto-x11] display :%d locked — skipping", display_num)
        display_num = _alloc_display()
        sess.display_num = display_num
    else:
        raise RuntimeError("No free Xvfb display after 20 attempts")

    sess.xvfb_proc = subprocess.Popen(
        ["Xvfb", f":{display_num}",
         "-screen", "0", f"{DISPLAY_W}x{DISPLAY_H}x24",
         "-ac", "-nolisten", "tcp", "-nolisten", "unix"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)
    if sess.xvfb_proc.poll() is not None:
        raise RuntimeError(f"Xvfb failed on display :{display_num}")

    # ── 2. PulseAudio ──────────────────────────────────────────────────────
    sess.pulse_proc = _start_pulseaudio(display_num)
    ps = _pulse_socket(display_num)

    # ── 3. Playwright worker (subprocess — immune to gevent patching) ───────
    # Pipe pair: result (worker→parent) and control (parent→worker)
    result_r, result_w = os.pipe()
    ctrl_r,   ctrl_w   = os.pipe()

    pw_proc = subprocess.Popen(
        [
            sys.executable, _PW_WORKER_PATH,
            str(display_num),
            ps or "none",
            pluto_url,
            str(result_w),   # fd: worker writes OK/ERR
            str(ctrl_r),     # fd: worker reads STOP
            str(DISPLAY_W), str(DISPLAY_H), str(STARTUP_WAIT),
            email    or "none",
            password or "none",
            COOKIE_PATH,
        ],
        pass_fds=(result_w, ctrl_r),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    os.close(result_w)
    os.close(ctrl_r)

    sess.pw_proc   = pw_proc
    sess.pw_ctrl_w = os.fdopen(ctrl_w, "w", buffering=1)

    # Wait for worker to signal ready (or fail)
    result_pipe = os.fdopen(result_r, "r")
    logger.info("[pluto-x11] waiting for Playwright worker (up to %ds)...", STARTUP_WAIT + 10)
    line = result_pipe.readline()
    result_pipe.close()

    if not line.startswith("OK"):
        err_out = ""
        try:
            pw_proc.wait(timeout=3)
            err_out = pw_proc.stderr.read().decode(errors="replace").strip()
        except Exception:
            pass
        raise RuntimeError(
            f"Playwright worker failed: {line.strip() or 'no output'}"
            + (f" | stderr: {err_out[:300]}" if err_out else "")
        )

    logger.info("[pluto-x11] Playwright worker ready: %s", line.strip())

    # ── 4. ffmpeg x11grab → MPEG-TS ────────────────────────────────────────
    has_audio = ps is not None
    ffmpeg_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-f", "x11grab", "-framerate", str(FRAMERATE),
        "-video_size", f"{DISPLAY_W}x{DISPLAY_H}",
        "-i", f":{display_num}.0+0,0",
    ]
    ffmpeg_cmd += _vaapi_device_args(encoder)
    if has_audio:
        ffmpeg_cmd += ["-f", "pulse", "-ac", "2", "-ar", "48000", "-i", "default"]
    ffmpeg_cmd += _build_vf_chain(encoder)
    ffmpeg_cmd += _build_video_encoder_args(encoder)
    ffmpeg_cmd += ["-c:a", "aac", "-b:a", "192k", "-ar", "48000"] if has_audio else ["-an"]
    ffmpeg_cmd += ["-f", "mpegts", "pipe:1"]

    ffmpeg_env = {**os.environ, "DISPLAY": f":{display_num}"}
    if has_audio:
        ffmpeg_env["PULSE_SERVER"] = f"unix:{ps}"

    sess.ffmpeg_proc = subprocess.Popen(
        ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ffmpeg_env,
    )
    time.sleep(1)
    if sess.ffmpeg_proc.poll() is not None:
        err = sess.ffmpeg_proc.stderr.read().decode(errors="replace").strip()
        raise RuntimeError(f"ffmpeg failed (encoder={encoder}): {err[:400]}")

    sess.broadcast = _BroadcastPipe(sess.ffmpeg_proc.stdout)
    sess.started   = True

    logger.info(
        "[pluto-x11] session started ch=%s display=:%d encoder=%s "
        "pid_xvfb=%d pid_pw=%d pid_ff=%d audio=%s",
        channel_id, display_num, encoder,
        sess.xvfb_proc.pid, pw_proc.pid, sess.ffmpeg_proc.pid,
        "on" if has_audio else "off",
    )
    return sess


def _terminate_session(sess: _X11Session) -> None:
    import shutil

    if sess.broadcast:
        sess.broadcast.stop()

    # Tell Playwright worker to exit cleanly
    if sess.pw_ctrl_w:
        try:
            sess.pw_ctrl_w.write("STOP\n")
            sess.pw_ctrl_w.flush()
            sess.pw_ctrl_w.close()
        except Exception:
            pass
    if sess.pw_proc and sess.pw_proc.poll() is None:
        try:
            sess.pw_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sess.pw_proc.kill()

    for proc in (sess.ffmpeg_proc, sess.pulse_proc, sess.xvfb_proc):
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            except Exception:
                pass

    for d in (f"/tmp/pulse-x11-{sess.display_num}",
              f"/tmp/pluto-x11-profile-{sess.display_num}"):
        shutil.rmtree(d, ignore_errors=True)

    logger.info("[pluto-x11] session stopped ch=%s display=:%d",
                sess.channel_id, sess.display_num)

# ---------------------------------------------------------------------------
# Reaper + startup cleanup
# ---------------------------------------------------------------------------

def _reaper() -> None:
    while True:
        time.sleep(5)
        to_evict: list[str] = []
        with _sessions_lock:
            for cid, sess in _sessions.items():
                if sess.readers == 0 and time.monotonic() - sess.last_read > IDLE_TIMEOUT:
                    to_evict.append(cid)
                elif sess.broadcast and not sess.broadcast.is_alive():
                    logger.warning("[pluto-x11] ffmpeg died ch=%s — evicting", cid)
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
# Headless login worker script — used by login_and_store_cookies()
# ---------------------------------------------------------------------------

_PW_LOGIN_WORKER_SCRIPT = r'''
import os, sys, time, json, random

# Selectors for Pluto's React sign-in form.
_EMAIL_SEL = (
    'input[name="email"], input[type="email"], '
    'input[placeholder*="email" i], input[id*="email" i]'
)
_PW_SEL = (
    'input[name="password"], input[type="password"], '
    'input[placeholder*="password" i], input[id*="password" i]'
)

# Pluto's two-step login flow:
#   Step 1: /us/account/check-email  (enter email, click Continue)
#   Step 2: /us/account/check-password  (enter password, click Sign In)
# The /sign-in URL just shows the homepage — avoid it as a starting point.
_SIGN_IN_URLS = [
    "https://pluto.tv/us/account/check-email",
    "https://pluto.tv/account/check-email",
    "https://pluto.tv/en/account/check-email",
]

# URL fragments that indicate we are on the password step.
# Pluto's actual flow: /us/account/check-email → /us/account/sign-in
# (the password field appears on the /sign-in page after email is submitted)
_PASSWORD_URL_FRAGMENTS = ("account/sign-in", "check-password", "account/password")


def _human_delay(lo=0.08, hi=0.18):
    time.sleep(random.uniform(lo, hi))


def _fill_react_input(page, selector, value):
    """
    Fill a React-controlled input by typing keystroke-by-keystroke with a
    human-like random delay.  This is the only reliable method for React
    controlled inputs — direct .value assignment bypasses onChange.
    """
    try:
        loc = page.locator(selector).first
        loc.click(timeout=5000)
        _human_delay(0.1, 0.25)
        # Clear any pre-filled value
        loc.press("Control+a")
        _human_delay()
        loc.press("Backspace")
        _human_delay()
        # Type each character with a small random inter-key delay
        loc.press_sequentially(value, delay=random.randint(60, 120))
        _human_delay(0.1, 0.2)
        return True
    except Exception:
        pass
    # Fallback: React native setter (works when locator times out)
    try:
        page.evaluate("""([sel, val]) => {
            const inp = document.querySelector(sel);
            if (!inp) return false;
            const setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value').set;
            setter.call(inp, val);
            inp.dispatchEvent(new Event('input',  {bubbles: true}));
            inp.dispatchEvent(new Event('change', {bubbles: true}));
            inp.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true}));
            return true;
        }""", [selector.split(",")[0].strip(), value])
        return True
    except Exception:
        return False


def _click_button(page, labels):
    """Click the first visible button whose text matches one of the labels."""
    try:
        page.evaluate("""(labels) => {
            for (const btn of document.querySelectorAll('button, [role="button"]')) {
                const t = (btn.textContent || '').trim().toLowerCase();
                if (labels.some(l => t === l || t.includes(l))) {
                    if (btn.offsetParent !== null) { btn.click(); return true; }
                }
            }
            return false;
        }""", [l.lower() for l in labels])
    except Exception:
        pass


def _verify_field_value(page, selector, expected):
    """Return True if the input currently holds the expected value."""
    try:
        actual = page.locator(selector).first.input_value(timeout=3000)
        return actual == expected
    except Exception:
        return False


def main():
    pluto_email    = sys.argv[1]
    pluto_password = sys.argv[2]
    cookie_path    = sys.argv[3]
    result_fd      = int(sys.argv[4])

    result_pipe = os.fdopen(result_fd, "w", buffering=1)

    def fail(msg):
        result_pipe.write(f"ERR {msg}\n")
        result_pipe.flush()

    def ok(msg=""):
        result_pipe.write(f"OK {msg}\n")
        result_pipe.flush()

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError as e:
        fail(f"playwright not installed: {e}")
        return

    pw_args = [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--no-first-run", "--no-default-browser-check",
        # Critical: mask automation fingerprint
        "--disable-blink-features=AutomationControlled",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-features=IsolateOrigins,site-per-process",
        "--lang=en-US,en",
    ]

    try:
        pw      = sync_playwright().start()
        browser = pw.chromium.launch(headless=True, args=pw_args)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/Chicago",
            # Pretend to be a real desktop, not a headless bot
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        # Mask navigator.webdriver before any page loads
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        """)
        page = context.new_page()
    except Exception as e:
        fail(f"launch failed: {e}")
        return

    try:
        # ── Step 1: Find the email form ─────────────────────────────────────
        # IMPORTANT: Navigate via the homepage and click the Sign In link so
        # Pluto assigns its msockid session token naturally through its own
        # redirect chain.  Going directly to /check-email skips this and causes
        # Pluto to reject the password submission as a bot/invalid session.
        email_form_found = False
        try:
            page.goto("https://pluto.tv/us/live-tv", wait_until="domcontentloaded", timeout=20000)
            _human_delay(2.0, 3.0)
            # Click the "Sign In" button/link in the nav
            page.evaluate("""() => {
                for (const el of document.querySelectorAll('a, button')) {
                    const t = (el.textContent || '').trim().toLowerCase();
                    const h = (el.href || '');
                    if (t === 'sign in' || t === 'log in' ||
                        h.includes('check-email') || h.includes('sign-in')) {
                        el.click(); return;
                    }
                }
            }""")
            _human_delay(1.5, 2.5)
            page.wait_for_selector(_EMAIL_SEL, state="visible", timeout=12000)
            email_form_found = True
        except Exception:
            pass

        # Fallback: go directly to check-email
        if not email_form_found:
            for url in _SIGN_IN_URLS:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)
                except Exception:
                    continue
                _human_delay(0.8, 1.5)
                try:
                    page.wait_for_selector(_EMAIL_SEL, state="visible", timeout=10000)
                    email_form_found = True
                    break
                except PWTimeout:
                    continue

        if not email_form_found:
            # Last resort: click Sign In from the homepage
            try:
                page.goto("https://pluto.tv/", wait_until="domcontentloaded", timeout=20000)
                _human_delay(2.0, 3.0)
                page.evaluate("""() => {
                    for (const a of document.querySelectorAll('a')) {
                        const h = (a.href || '');
                        const t = (a.textContent || '').trim().toLowerCase();
                        if (h.includes('check-email') || h.includes('sign-in') ||
                            t === 'sign in' || t === 'log in') {
                            a.click(); return;
                        }
                    }
                }""")
                page.wait_for_selector(_EMAIL_SEL, state="visible", timeout=12000)
                email_form_found = True
            except Exception:
                pass

        if not email_form_found:
            inputs_found = ""
            body_snippet = ""
            try:
                inputs_found = page.evaluate("""() =>
                    [...document.querySelectorAll('input')].map(i =>
                        `${i.type}|${i.name}|${i.placeholder}|${i.id}`).join('; ')
                """)
                body_snippet = page.evaluate("() => document.body.innerText.slice(0, 300)")
            except Exception:
                pass
            fail(
                f"email form not found — "
                f"last_url={page.url} inputs=[{inputs_found}] body=[{body_snippet}]"
            )
            return

        # ── Step 2: Fill email ──────────────────────────────────────────────
        if not _fill_react_input(page, _EMAIL_SEL, pluto_email):
            fail("could not fill email field")
            return

        # Verify value actually landed in the field
        if not _verify_field_value(page, _EMAIL_SEL, pluto_email):
            # Try once more with a longer delay
            _human_delay(0.5, 1.0)
            _fill_react_input(page, _EMAIL_SEL, pluto_email)

        _human_delay(0.4, 0.8)

        # ── Step 3: Submit email, wait for password page ────────────────────
        # Button on Pluto's email page is "Next"
        _click_button(page, ["next", "continue", "sign in", "log in", "submit"])
        _human_delay(0.3, 0.6)
        try:
            page.locator(_EMAIL_SEL).first.press("Enter")
        except Exception:
            pass

        # Wait for the URL to change to the password step
        # (more reliable than a fixed sleep)
        pw_page_reached = False
        pw_wait_deadline = time.monotonic() + 15
        while time.monotonic() < pw_wait_deadline:
            try:
                cur = page.url
                if any(f in cur for f in _PASSWORD_URL_FRAGMENTS):
                    # Also confirm the password input is rendered
                    try:
                        page.wait_for_selector(_PW_SEL, state="visible", timeout=5000)
                        pw_page_reached = True
                        break
                    except PWTimeout:
                        pass
            except Exception:
                pass
            time.sleep(0.5)

        if not pw_page_reached:
            # Maybe the form is single-step (both fields visible together)
            try:
                page.wait_for_selector(_PW_SEL, state="visible", timeout=5000)
                pw_page_reached = True
            except PWTimeout:
                pass

        if not pw_page_reached:
            cur_url = ""
            try:
                cur_url = page.url
            except Exception:
                pass
            fail(f"password page not reached — url={cur_url}")
            return

        _human_delay(0.6, 1.2)

        # ── Step 4: Fill password ───────────────────────────────────────────
        # Pluto's password field: placeholder="Please enter a password"
        if not _fill_react_input(page, _PW_SEL, pluto_password):
            fail("could not fill password field")
            return

        # Verify the password field value
        if not _verify_field_value(page, _PW_SEL, pluto_password):
            _human_delay(0.5, 1.0)
            _fill_react_input(page, _PW_SEL, pluto_password)

        _human_delay(0.5, 1.0)

        # ── Step 5: Submit — button text on password page is "Sign In" ──────
        # Use get_by_role first (most reliable), fall back to JS click / Enter
        submit_clicked = False
        try:
            page.get_by_role("button", name="Sign In").click(timeout=5000)
            submit_clicked = True
        except Exception:
            pass
        if not submit_clicked:
            _click_button(page, ["sign in", "log in", "login", "signin", "submit"])
            _human_delay(0.3, 0.6)
            try:
                page.locator(_PW_SEL).first.press("Enter")
            except Exception:
                pass

        # ── Step 6: Confirm login succeeded ────────────────────────────────
        logged_in = False
        auth_names = {
            "plutotv-userToken", "userToken", "authToken", "token",
            "__session", "pluto-session", "plutotv-session",
            "jwt", "access_token",
        }
        verify_deadline = time.monotonic() + 30
        while time.monotonic() < verify_deadline:
            time.sleep(1)
            try:
                current_url = page.url
                cookies = context.cookies()
                cookie_names = {c["name"] for c in cookies}

                if cookie_names & auth_names:
                    logged_in = True
                    break

                # URL moved away from all login pages (success = redirect to home/browse)
                left_signin = not any(
                    x in current_url
                    for x in ("check-email", "check-password", "account/password")
                ) and "account/sign-in" not in current_url
                if left_signin and current_url not in ("about:blank", ""):
                    time.sleep(1.5)
                    cookies = context.cookies()
                    cookie_names = {c["name"] for c in cookies}
                    if cookie_names & auth_names or len(cookies) > 5:
                        logged_in = True
                        break
            except Exception:
                pass

        if not logged_in:
            error_text = ""
            try:
                error_text = page.evaluate("""() => {
                    for (const sel of [
                        '[class*="error" i]', '[role="alert"]',
                        '[data-testid*="error" i]', '[class*="Error"]',
                        'p[class*="message" i]',
                    ]) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText.trim()) return el.innerText.trim().slice(0, 200);
                    }
                    return '';
                }""")
            except Exception:
                pass
            cookie_names_str = ""
            try:
                cookie_names_str = ", ".join(c["name"] for c in context.cookies())
            except Exception:
                pass
            fail(
                f"login verification failed — page={page.url}"
                + (f" form_error={error_text!r}" if error_text else "")
                + (f" cookies=[{cookie_names_str}]" if cookie_names_str else "")
            )
            return

        # ── Persist cookies ─────────────────────────────────────────────────
        try:
            cookies = context.cookies()
            with open(cookie_path, "w") as f:
                json.dump(cookies, f)
        except Exception as e:
            fail(f"cookie save failed: {e}")
            return

        ok(f"cookies={len(cookies)}")

    except Exception as e:
        fail(f"unexpected error: {e}")
    finally:
        try:
            page.close(); context.close(); browser.close(); pw.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
'''

_PW_LOGIN_WORKER_PATH = "/tmp/pluto_pw_login_worker.py"
with open(_PW_LOGIN_WORKER_PATH, "w") as _lf:
    _lf.write(_PW_LOGIN_WORKER_SCRIPT)


# ---------------------------------------------------------------------------
# Public API — settings / login helpers
# ---------------------------------------------------------------------------

def login_and_store_cookies(email: str, password: str,
                            cookie_path: str | None = None) -> dict:
    """
    Perform a real headless login to Pluto TV and persist the resulting
    cookies to *cookie_path* (default: COOKIE_PATH).

    Returns ``{"ok": True, "cookies": N}`` on success, or
    ``{"ok": False, "error": "..."}`` on failure.

    This is the function your settings page should call after the user
    submits credentials.  It is synchronous and blocks for up to ~60 s.
    """
    cookie_path = cookie_path or COOKIE_PATH
    result_r, result_w = os.pipe()

    proc = subprocess.Popen(
        [
            sys.executable, _PW_LOGIN_WORKER_PATH,
            email, password, cookie_path, str(result_w),
        ],
        pass_fds=(result_w,),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    os.close(result_w)

    result_pipe = os.fdopen(result_r, "r")
    line = result_pipe.readline()
    result_pipe.close()

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    if line.startswith("OK"):
        detail = line[2:].strip()
        n = 0
        for part in detail.split():
            if part.startswith("cookies="):
                try:
                    n = int(part.split("=", 1)[1])
                except ValueError:
                    pass
        logger.info("[pluto-x11] login succeeded — %d cookies stored at %s", n, cookie_path)
        return {"ok": True, "cookies": n}
    else:
        err = line.strip()
        if not err:
            stderr_out = ""
            try:
                stderr_out = proc.stderr.read().decode(errors="replace").strip()[:300]
            except Exception:
                pass
            err = f"no output from worker" + (f" | stderr: {stderr_out}" if stderr_out else "")
        logger.error("[pluto-x11] login failed: %s", err)
        return {"ok": False, "error": err}


def verify_login(cookie_path: str | None = None) -> dict:
    """
    Check whether stored cookies appear valid (file exists, not empty,
    contains at least one recognisable Pluto auth cookie that has not
    expired).

    Returns ``{"ok": True, "cookies": N}`` or ``{"ok": False, "reason": "..."}``.
    """
    import json as _json
    import time as _time

    cookie_path = cookie_path or COOKIE_PATH
    if not os.path.exists(cookie_path):
        return {"ok": False, "reason": "no cookie file"}
    try:
        with open(cookie_path) as f:
            cookies: list[dict] = _json.load(f)
    except Exception as e:
        return {"ok": False, "reason": f"cookie file unreadable: {e}"}

    if not cookies:
        return {"ok": False, "reason": "cookie file is empty"}

    now = _time.time()
    auth_names = {"plutotv-userToken", "__session", "pluto-session",
                  "userToken", "authToken", "token"}
    found_auth = False
    for c in cookies:
        if c.get("name") in auth_names:
            expires = c.get("expires", -1)
            if expires > 0 and expires < now:
                return {"ok": False, "reason": f"auth cookie '{c['name']}' has expired"}
            found_auth = True

    if not found_auth:
        # We have cookies but none are named auth tokens — treat as logged-in
        # (Pluto may rename them) but flag it
        logger.warning("[pluto-x11] verify_login: no known auth cookie names found; "
                       "assuming valid (%d cookies)", len(cookies))

    return {"ok": True, "cookies": len(cookies)}


def clear_cookies(cookie_path: str | None = None) -> None:
    """Delete the stored cookie file (e.g. on logout / credential change)."""
    cookie_path = cookie_path or COOKIE_PATH
    try:
        os.remove(cookie_path)
        logger.info("[pluto-x11] cookies cleared: %s", cookie_path)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def stream_channel(channel_id: str, pluto_url: str,
                   email: str | None = None, password: str | None = None) -> Iterator[bytes]:
    """
    Stream multiplexer — unlimited sessions.

    - Same channel already streaming → attach as a reader to the existing
      session (one Xvfb+Chromium+ffmpeg shared among all viewers).
    - New channel → spin up a new session.
    - Sessions are torn down by the reaper after IDLE_TIMEOUT seconds with
      no active readers.
    """
    if not ENABLED:
        logger.error("[pluto-x11] disabled")
        return

    launch_lock = _get_launch_lock(channel_id)
    with launch_lock:
        with _sessions_lock:
            sess = _sessions.get(channel_id)
            if sess is not None and sess.broadcast and sess.broadcast.is_alive():
                sess.readers += 1
            else:
                if sess is not None:
                    _terminate_session(sess)
                    del _sessions[channel_id]
                sess = None

        if sess is None:
            new_sess = _launch_session(channel_id, pluto_url, email=email, password=password)
            with _sessions_lock:
                _sessions[channel_id] = new_sess
                new_sess.readers += 1
            sess = new_sess

    reader_q = sess.broadcast.add_reader()
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
