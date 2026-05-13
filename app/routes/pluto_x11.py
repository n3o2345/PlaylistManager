"""
app/scrapers/pluto_x11.py

Pluto TV X11 screen-grab streamer — HLS-to-disk pipeline.

Mirrors the philoproxy stream.js architecture exactly:
  Xvfb (+GLX +render -noreset)
    └─ Playwright Chromium (non-headless, subprocess — gevent-safe)
         └─ CSS injection / CDP fullscreen / keepalive
  PulseAudio null sink (per-session)
  ffmpeg  x11grab + pulse → HLS segments on disk
    └─ Flask serves index.m3u8 + .ts segments directly

Why HLS-to-disk instead of MPEG-TS pipe:
  - Client gets the manifest immediately and starts playing with the first
    segment — no blocking wait for a pipe to fill
  - Faster perceived startup (same reason philo loads quickly)
  - Segments are served as static files — no streaming generator needed
  - Auto-recovery: ffmpeg restarts without disconnecting clients

Gevent safety:
  Playwright sync API is called from a subprocess (pluto_pw_worker.py) so
  gevent's monkey-patching of threading never affects it.
"""
from __future__ import annotations

import logging
import os
import queue
import shutil
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

ENABLED         = os.environ.get("PLUTO_X11_ENABLED",      "1") != "0"
DISPLAY_W       = int(os.environ.get("PLUTO_X11_WIDTH",    "1280"))
DISPLAY_H       = int(os.environ.get("PLUTO_X11_HEIGHT",   "720"))
FRAMERATE       = int(os.environ.get("PLUTO_X11_FPS",      "30"))
BITRATE         = os.environ.get("PLUTO_X11_BITRATE",      "4M")
IDLE_TIMEOUT    = int(os.environ.get("PLUTO_X11_IDLE_TIMEOUT", "300"))  # 5 min
STARTUP_WAIT    = int(os.environ.get("PLUTO_X11_STARTUP_WAIT", "20"))
FORCE_ENCODER   = os.environ.get("PLUTO_X11_ENCODER", "").strip().lower() or None
LOW_LATENCY     = os.environ.get("PLUTO_X11_LOW_LATENCY", "0") in ("1","true","yes","on")

# HLS tuning — mirrors philo stream.js getHlsTuning()
HLS_TIME      = "1" if LOW_LATENCY else "2"
HLS_LIST_SIZE = "3" if LOW_LATENCY else "5"
HLS_FLAGS     = (
    "delete_segments+append_list+omit_endlist+split_by_time+program_date_time"
    if LOW_LATENCY else
    "delete_segments+append_list+omit_endlist+split_by_time"
)

# ---------------------------------------------------------------------------
# Playwright worker script — child process, immune to gevent patching
# ---------------------------------------------------------------------------

_PW_WORKER_SCRIPT = r'''
import os, sys, time, select

def main():
    display_num  = int(sys.argv[1])
    pulse_socket = sys.argv[2] if sys.argv[2] != "none" else None
    pluto_url    = sys.argv[3]
    result_fd    = int(sys.argv[4])
    control_fd   = int(sys.argv[5])
    display_w    = int(sys.argv[6])
    display_h    = int(sys.argv[7])
    startup_wait = int(sys.argv[8])

    result_pipe  = os.fdopen(result_fd,  "w", buffering=1)
    control_pipe = os.fdopen(control_fd, "r")

    FULLSCREEN_CSS = """
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
    """

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

    # Mirrors philo.js getStreamBrowser() args exactly
    pw_args = [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--disable-software-rasterizer",
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

    try:
        page.goto(pluto_url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        try:
            page.goto(pluto_url, timeout=30000)
        except Exception:
            pass

    page.wait_for_timeout(2000)

    # Inject fullscreen CSS
    try:
        page.evaluate(f"""() => {{
            if (document.getElementById('pluto-x11-fs')) return;
            const s = document.createElement('style');
            s.id = 'pluto-x11-fs';
            s.textContent = `{FULLSCREEN_CSS}`;
            document.head.appendChild(s);
            const v = document.querySelector('video');
            if (v) {{ v.muted = false; v.volume = 1.0; if (v.paused) v.play().catch(()=>{{}}); }}
        }}""")
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

    # Keepalive loop — select() with 30s timeout so we run keepalive every cycle
    while True:
        try:
            ready, _, _ = select.select([control_pipe], [], [], 30)
            if ready:
                line = control_pipe.readline()
                if not line or line.strip() == "STOP":
                    break
        except Exception:
            break
        try:
            page.evaluate(f"""() => {{
                for (const btn of document.querySelectorAll('button')) {{
                    const t = (btn.textContent||'').toLowerCase();
                    if (['still watching','continue','yes','keep watching',
                         'resume','dismiss'].some(w=>t.includes(w)))
                        {{ btn.click(); return; }}
                }}
                const v = document.querySelector('video');
                if (v) {{ v.muted=false; v.volume=1.0; if(v.paused||v.ended) v.play().catch(()=>{{}}); }}
                if (!document.getElementById('pluto-x11-fs')) {{
                    const s = document.createElement('style');
                    s.id = 'pluto-x11-fs';
                    s.textContent = `{FULLSCREEN_CSS}`;
                    document.head.appendChild(s);
                }}
            }}""")
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


# ---------------------------------------------------------------------------
# Cross-process display allocation
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
# Session
# ---------------------------------------------------------------------------

@dataclass
class _X11Session:
    channel_id:  str
    pluto_url:   str
    display_num: int
    encoder:     str
    hls_dir:     str

    xvfb_proc:   subprocess.Popen | None = field(default=None, repr=False)
    pulse_proc:  subprocess.Popen | None = field(default=None, repr=False)
    pw_proc:     subprocess.Popen | None = field(default=None, repr=False)
    pw_ctrl_w:   object | None           = field(default=None, repr=False)
    ffmpeg_proc: subprocess.Popen | None = field(default=None, repr=False)

    clients:   int   = 0
    last_access: float = field(default_factory=time.monotonic)
    started:   bool  = False


_sessions:     dict[str, _X11Session] = {}
_sessions_lock = threading.Lock()
_launch_locks:  dict[str, threading.Lock] = {}
_launch_locks_lock = threading.Lock()


def _get_launch_lock(channel_id: str) -> threading.Lock:
    with _launch_locks_lock:
        if channel_id not in _launch_locks:
            _launch_locks[channel_id] = threading.Lock()
        return _launch_locks[channel_id]


# ---------------------------------------------------------------------------
# ffmpeg HLS builder — mirrors philo stream.js _startPhiloFfmpegX11grab()
# ---------------------------------------------------------------------------

def _build_ffmpeg_cmd(display_num: int, pulse_socket: str | None,
                      encoder: str, hls_dir: str) -> list[str]:
    manifest = os.path.join(hls_dir, "index.m3u8")
    seg_pat  = os.path.join(hls_dir, "seg%05d.ts")
    display  = f":{display_num}"

    if encoder == "h264_nvenc":
        video_args = [
            "-c:v", "h264_nvenc", "-preset", "p1", "-tune", "ll", "-rc", "cbr",
            "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", "8M",
            "-pix_fmt", "yuv420p", "-g", "30", "-keyint_min", "30",
            "-zerolatency", "1", "-fps_mode", "cfr",
        ]
    elif encoder == "h264_vaapi":
        video_args = [
            "-c:v", "h264_vaapi",
            "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", "8M",
            "-g", "30", "-fps_mode", "cfr",
        ]
    else:
        video_args = [
            "-c:v", "libx264", "-preset", "superfast", "-tune", "zerolatency",
            "-b:v", BITRATE, "-maxrate", BITRATE, "-bufsize", "8M",
            "-pix_fmt", "yuv420p", "-g", "30", "-keyint_min", "30",
            "-threads", "0", "-fps_mode", "cfr",
            "-x264-params", "nal-hrd=cbr:force-cfr=1",
        ]

    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]

    # Audio first (mirrors philo) — lets ffmpeg buffer frames before video
    if pulse_socket:
        cmd += [
            "-thread_queue_size", "4096",
            "-use_wallclock_as_timestamps", "1",
            "-f", "pulse", "-sample_rate", "48000", "-channels", "2",
            "-i", "out.monitor",
        ]

    # Video: x11grab
    cmd += [
        "-thread_queue_size", "4096",
        "-use_wallclock_as_timestamps", "1",
        "-f", "x11grab", "-video_size", f"{DISPLAY_W}x{DISPLAY_H}",
        "-framerate", str(FRAMERATE), "-i", display,
    ]

    if encoder == "h264_vaapi":
        cmd += ["-vaapi_device", "/dev/dri/renderD128", "-vf", "format=nv12,hwupload"]

    cmd += video_args

    if pulse_socket:
        cmd += [
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-af", "aresample=async=9600:min_hard_comp=0.1:first_pts=0",
            "-map", "0:a", "-map", "1:v",
        ]
    else:
        cmd += ["-an"]

    cmd += [
        "-fflags", "+genpts+discardcorrupt+igndts",
        "-max_interleave_delta", "0",
        "-f", "hls",
        "-hls_time",             HLS_TIME,
        "-hls_list_size",        HLS_LIST_SIZE,
        "-hls_flags",            HLS_FLAGS,
        "-hls_segment_filename", seg_pat,
        manifest,
    ]

    return cmd


# ---------------------------------------------------------------------------
# Session launch / terminate
# ---------------------------------------------------------------------------

def _launch_session(channel_id: str, pluto_url: str) -> _X11Session:
    display_num = _alloc_display()
    encoder     = _detect_encoder()

    hls_dir = os.path.join(tempfile.gettempdir(), f"pluto_{channel_id}")
    os.makedirs(hls_dir, exist_ok=True)
    # Clear stale segments from previous session
    for f in os.listdir(hls_dir):
        try:
            os.unlink(os.path.join(hls_dir, f))
        except Exception:
            pass

    sess = _X11Session(channel_id=channel_id, pluto_url=pluto_url,
                       display_num=display_num, encoder=encoder, hls_dir=hls_dir)

    # ── 1. Xvfb — mirrors philo _ensureXvfb() with GLX+render flags ────────
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
         "-ac", "+extension", "GLX", "+render", "-noreset"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # Verify Xvfb is up (mirrors philo's xdpyinfo poll)
    xvfb_env = {**os.environ, "DISPLAY": f":{display_num}"}
    for _ in range(40):
        time.sleep(0.25)
        if sess.xvfb_proc.poll() is not None:
            raise RuntimeError(f"Xvfb exited immediately on display :{display_num}")
        try:
            subprocess.check_call(
                ["xdpyinfo", "-display", f":{display_num}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=xvfb_env, timeout=0.5,
            )
            logger.debug("[pluto-x11] Xvfb ready :%d", display_num)
            break
        except Exception:
            pass
    else:
        logger.warning("[pluto-x11] Xvfb :%d not confirmed — continuing", display_num)

    # ── 2. PulseAudio ──────────────────────────────────────────────────────
    sess.pulse_proc = _start_pulseaudio(display_num)
    ps = _pulse_socket(display_num)

    # ── 3. Playwright worker subprocess (gevent-safe) ──────────────────────
    result_r, result_w = os.pipe()
    ctrl_r,   ctrl_w   = os.pipe()

    pw_proc = subprocess.Popen(
        [sys.executable, _PW_WORKER_PATH,
         str(display_num), ps or "none", pluto_url,
         str(result_w), str(ctrl_r),
         str(DISPLAY_W), str(DISPLAY_H), str(STARTUP_WAIT)],
        pass_fds=(result_w, ctrl_r),
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    os.close(result_w)
    os.close(ctrl_r)
    sess.pw_proc   = pw_proc
    sess.pw_ctrl_w = os.fdopen(ctrl_w, "w", buffering=1)

    logger.info("[pluto-x11] waiting for Playwright worker (up to %ds)...", STARTUP_WAIT + 10)
    result_pipe = os.fdopen(result_r, "r")
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
            + (f" | {err_out[:300]}" if err_out else "")
        )
    logger.info("[pluto-x11] Playwright worker ready: %s", line.strip())

    # ── 4. ffmpeg → HLS segments on disk ───────────────────────────────────
    ffmpeg_cmd = _build_ffmpeg_cmd(display_num, ps, encoder, hls_dir)
    ffmpeg_env = {**os.environ, "DISPLAY": f":{display_num}"}
    if ps:
        pulse_dir = os.path.dirname(ps)
        ffmpeg_env.update({
            "PULSE_SERVER":       f"unix:{ps}",
            "PULSE_RUNTIME_PATH": pulse_dir,
            "HOME":               pulse_dir,
            "XDG_RUNTIME_DIR":    pulse_dir,
            "PULSE_COOKIE":       os.path.join(pulse_dir, ".pulse-cookie"),
        })

    sess.ffmpeg_proc = subprocess.Popen(
        ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=ffmpeg_env,
    )

    # Log ffmpeg stderr in background
    def _log_ffmpeg(proc, cid):
        for line in proc.stderr:
            l = line.decode(errors="replace").strip()
            if l:
                logger.info("[pluto-x11] ffmpeg ch=%s: %s", cid, l)
    threading.Thread(target=_log_ffmpeg, args=(sess.ffmpeg_proc, channel_id),
                     daemon=True).start()

    # Start auto-recovery watcher
    threading.Thread(target=_ffmpeg_watcher, args=(channel_id,),
                     daemon=True, name=f"pluto-x11-watcher-{channel_id}").start()

    sess.started = True
    logger.info(
        "[pluto-x11] session started ch=%s display=:%d encoder=%s "
        "pid_xvfb=%d pid_pw=%d pid_ff=%d audio=%s hls=%s",
        channel_id, display_num, encoder,
        sess.xvfb_proc.pid, pw_proc.pid, sess.ffmpeg_proc.pid,
        "on" if ps else "off", hls_dir,
    )
    return sess


def _ffmpeg_watcher(channel_id: str) -> None:
    """Auto-restart ffmpeg if it dies while the session is still active."""
    while True:
        time.sleep(2)
        with _sessions_lock:
            sess = _sessions.get(channel_id)
        if sess is None:
            break
        if sess.ffmpeg_proc and sess.ffmpeg_proc.poll() is not None:
            idle = time.monotonic() - sess.last_access
            if idle < IDLE_TIMEOUT:
                logger.warning("[pluto-x11] ffmpeg died ch=%s — restarting", channel_id)
                try:
                    for f in os.listdir(sess.hls_dir):
                        os.unlink(os.path.join(sess.hls_dir, f))
                except Exception:
                    pass
                ps = _pulse_socket(sess.display_num)
                ffmpeg_cmd = _build_ffmpeg_cmd(sess.display_num, ps, sess.encoder, sess.hls_dir)
                ffmpeg_env = {**os.environ, "DISPLAY": f":{sess.display_num}"}
                if ps:
                    pulse_dir = os.path.dirname(ps)
                    ffmpeg_env.update({
                        "PULSE_SERVER": f"unix:{ps}",
                        "PULSE_RUNTIME_PATH": pulse_dir,
                        "HOME": pulse_dir,
                        "XDG_RUNTIME_DIR": pulse_dir,
                        "PULSE_COOKIE": os.path.join(pulse_dir, ".pulse-cookie"),
                    })
                sess.ffmpeg_proc = subprocess.Popen(
                    ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                    env=ffmpeg_env,
                )
                threading.Thread(target=_log_ffmpeg_stderr,
                                 args=(sess.ffmpeg_proc, channel_id), daemon=True).start()
            else:
                break


def _log_ffmpeg_stderr(proc, cid):
    for line in proc.stderr:
        l = line.decode(errors="replace").strip()
        if l:
            logger.info("[pluto-x11] ffmpeg ch=%s: %s", cid, l)


def _terminate_session(sess: _X11Session) -> None:
    # Stop Playwright worker
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

    for d in (f"/tmp/pulse-x11-{sess.display_num}",):
        shutil.rmtree(d, ignore_errors=True)

    logger.info("[pluto-x11] session stopped ch=%s display=:%d",
                sess.channel_id, sess.display_num)


# ---------------------------------------------------------------------------
# Reaper + startup cleanup
# ---------------------------------------------------------------------------

def _reaper() -> None:
    while True:
        time.sleep(15)
        to_evict: list[str] = []
        with _sessions_lock:
            for cid, sess in _sessions.items():
                if sess.clients == 0 and time.monotonic() - sess.last_access > IDLE_TIMEOUT:
                    to_evict.append(cid)
        for cid in to_evict:
            logger.info("[pluto-x11] reaping idle session ch=%s", cid)
            with _sessions_lock:
                sess = _sessions.pop(cid, None)
            if sess:
                _terminate_session(sess)


def _cleanup_on_start() -> None:
    import glob
    for d in glob.glob("/tmp/pulse-x11-*"):
        shutil.rmtree(d, ignore_errors=True)
    if not glob.glob("/tmp/.X*-lock"):
        try:
            os.remove(_DISPLAY_COUNT_PATH)
        except FileNotFoundError:
            pass


_cleanup_on_start()
threading.Thread(target=_reaper, daemon=True, name="pluto-x11-reaper").start()

# ---------------------------------------------------------------------------
# Public API — called from Flask routes
# ---------------------------------------------------------------------------

def _has_segments(hls_dir: str) -> bool:
    try:
        manifest = os.path.join(hls_dir, "index.m3u8")
        return (os.path.exists(manifest) and
                any(f.endswith(".ts") for f in os.listdir(hls_dir)))
    except Exception:
        return False


def get_or_create_session(channel_id: str, pluto_url: str) -> _X11Session:
    """
    Return existing healthy session or launch a new one.
    Blocks until the session is launched (but not until HLS segments exist —
    the route does that wait so it can stream the response immediately).
    """
    launch_lock = _get_launch_lock(channel_id)
    with launch_lock:
        with _sessions_lock:
            sess = _sessions.get(channel_id)
            if sess is not None and sess.started:
                sess.clients += 1
                sess.last_access = time.monotonic()
                return sess
            if sess is not None:
                _terminate_session(sess)
                del _sessions[channel_id]

        new_sess = _launch_session(channel_id, pluto_url)
        with _sessions_lock:
            _sessions[channel_id] = new_sess
            new_sess.clients += 1
        return new_sess


def release_client(channel_id: str) -> None:
    with _sessions_lock:
        sess = _sessions.get(channel_id)
        if sess:
            sess.clients = max(0, sess.clients - 1)
            sess.last_access = time.monotonic()


def get_hls_manifest(channel_id: str, base_url: str) -> str | None:
    """Read index.m3u8 and rewrite segment URLs to point at our serve endpoint."""
    with _sessions_lock:
        sess = _sessions.get(channel_id)
    if not sess:
        return None
    manifest_path = os.path.join(sess.hls_dir, "index.m3u8")
    try:
        text = open(manifest_path).read()
    except FileNotFoundError:
        return None
    lines = []
    for line in text.splitlines():
        t = line.strip()
        if t and not t.startswith("#"):
            lines.append(base_url + os.path.basename(t))
        else:
            lines.append(line)
    return "\n".join(lines)


def get_segment_path(channel_id: str, segment: str) -> str | None:
    with _sessions_lock:
        sess = _sessions.get(channel_id)
    if not sess:
        return None
    p = os.path.join(sess.hls_dir, os.path.basename(segment))
    return p if os.path.exists(p) else None


def wait_for_segments(channel_id: str, timeout: float = 60.0) -> bool:
    """Block until HLS segments appear or timeout. Returns True if ready."""
    with _sessions_lock:
        sess = _sessions.get(channel_id)
    if not sess:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _has_segments(sess.hls_dir):
            return True
        time.sleep(0.5)
        # keep last_access alive so reaper doesn't kill it
        with _sessions_lock:
            if channel_id in _sessions:
                _sessions[channel_id].last_access = time.monotonic()
    return False


def active_sessions() -> list[dict]:
    out = []
    with _sessions_lock:
        for cid, sess in _sessions.items():
            out.append({
                "channel_id": cid,
                "display":    f":{sess.display_num}",
                "encoder":    sess.encoder,
                "clients":    sess.clients,
                "idle_secs":  round(time.monotonic() - sess.last_access, 1),
                "hls_dir":    sess.hls_dir,
                "segments":   len([f for f in os.listdir(sess.hls_dir) if f.endswith(".ts")])
                              if os.path.exists(sess.hls_dir) else 0,
            })
    return out


# Legacy compat — kept so existing route import still works
def stream_channel(channel_id: str, pluto_url: str) -> Iterator[bytes]:
    """Deprecated MPEG-TS pipe path — kept for backward compat only."""
    logger.warning("[pluto-x11] stream_channel() pipe path called — use HLS routes instead")
    yield b""
