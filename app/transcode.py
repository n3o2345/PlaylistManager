"""
app/transcode.py

Shared hardware-transcoding helpers for FastChannels.

Any component that needs to encode video (Pluto X11 screen-grab, future
HLS re-encode route, live-TV re-stream, etc.) imports from here so that
encoder detection, preset management, and ffmpeg argument building are
defined in exactly one place.

Usage
-----
    from app.transcode import get_encoder, build_video_args, build_vf_chain, \
                               vaapi_device_args, PRESET, DISPLAY_W, DISPLAY_H, \
                               FRAMERATE, BITRATE

    encoder  = get_encoder()                      # cached, thread-safe
    cmd  = ["ffmpeg", ...,
            *vaapi_device_args(encoder),
            *build_vf_chain(encoder),
            *build_video_args(encoder),
            "-c:a", "aac", ...]

Presets
-------
Select a named preset with TRANSCODE_PRESET (env var).  Individual env vars
override individual preset fields.

    Name         W     H   FPS  Bitrate  NVENC  VAAPI-QP
    480p         854   480  30   1200k    p2     26
    720p        1280   720  30   2500k    p2     24   ← default
    720p_high   1280   720  60   4000k    p3     23
    1080p       1920  1080  30   5000k    p3     22
    1080p_high  1920  1080  60   8000k    p4     20
    4k_preview  3840  2160  30  16000k    p5     18

Environment variables
---------------------
TRANSCODE_PRESET          Named preset (default: 720p).
TRANSCODE_ENCODER         Force encoder: nvenc | vaapi | libx264 (default: auto-detect).
TRANSCODE_WIDTH           Override capture/encode width.
TRANSCODE_HEIGHT          Override capture/encode height.
TRANSCODE_FPS             Override frame rate.
TRANSCODE_BITRATE         Override video bitrate (e.g. "4000k").
TRANSCODE_NVENC_PRESET    NVENC quality preset p1–p7 (default: from named preset).
TRANSCODE_VAAPI_QP        VAAPI constant QP, 0–51 (default: from named preset).
TRANSCODE_VAAPI_DEVICE    DRM render node (default: /dev/dri/renderD128).

Legacy Pluto X11 aliases (for backwards compatibility)
------------------------------------------------------
If TRANSCODE_* vars are absent but the corresponding PLUTO_X11_* vars exist,
this module inherits their values.  This means existing docker-compose.yml /
.env files require no changes.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Preset table
# ---------------------------------------------------------------------------

PRESETS: dict[str, dict] = {
    "480p":       {"w":  854, "h":  480, "fps": 30, "bitrate":  "1200k", "nvenc": "p2", "vaapi_qp": 26},
    "720p":       {"w": 1280, "h":  720, "fps": 30, "bitrate":  "2500k", "nvenc": "p2", "vaapi_qp": 24},
    "720p_high":  {"w": 1280, "h":  720, "fps": 60, "bitrate":  "4000k", "nvenc": "p3", "vaapi_qp": 23},
    "1080p":      {"w": 1920, "h": 1080, "fps": 30, "bitrate":  "5000k", "nvenc": "p3", "vaapi_qp": 22},
    "1080p_high": {"w": 1920, "h": 1080, "fps": 60, "bitrate":  "8000k", "nvenc": "p4", "vaapi_qp": 20},
    "4k_preview": {"w": 3840, "h": 2160, "fps": 30, "bitrate": "16000k", "nvenc": "p5", "vaapi_qp": 18},
}


def _env(name: str, legacy: str | None = None, default: str = "") -> str:
    """Read env var, falling back to a legacy alias then a default."""
    v = os.environ.get(name, "").strip()
    if v:
        return v
    if legacy:
        v = os.environ.get(legacy, "").strip()
        if v:
            return v
    return default


# ---------------------------------------------------------------------------
# Resolve active configuration (preset + overrides)
# ---------------------------------------------------------------------------

PRESET_NAME: str = _env("TRANSCODE_PRESET", "PLUTO_X11_PRESET", "720p").lower()
_preset: dict   = PRESETS.get(PRESET_NAME, PRESETS["720p"])

DISPLAY_W: int  = int(_env("TRANSCODE_WIDTH",    "PLUTO_X11_WIDTH",    str(_preset["w"])))
DISPLAY_H: int  = int(_env("TRANSCODE_HEIGHT",   "PLUTO_X11_HEIGHT",   str(_preset["h"])))
FRAMERATE: int  = int(_env("TRANSCODE_FPS",      "PLUTO_X11_FPS",      str(_preset["fps"])))
BITRATE:   str  = _env("TRANSCODE_BITRATE",      "PLUTO_X11_BITRATE",  _preset["bitrate"])
NVENC_PRESET: str = _env("TRANSCODE_NVENC_PRESET", "PLUTO_X11_NVENC_PRESET", _preset["nvenc"])
VAAPI_QP: int   = int(_env("TRANSCODE_VAAPI_QP",  "PLUTO_X11_VAAPI_QP",  str(_preset["vaapi_qp"])))
VAAPI_DEVICE: str = _env("TRANSCODE_VAAPI_DEVICE", None, "/dev/dri/renderD128")

# Explicit encoder override (nvenc / vaapi / libx264).
FORCE_ENCODER: str | None = (
    _env("TRANSCODE_ENCODER", "PLUTO_X11_ENCODER", "").lower() or None
)


# ---------------------------------------------------------------------------
# Encoder detection — probed once, result cached
# ---------------------------------------------------------------------------

_ENCODER_CACHE: str | None = None
_ENCODER_LOCK                = threading.Lock()


def get_encoder() -> str:
    """
    Return the best available encoder: h264_nvenc > h264_vaapi > libx264.

    Result is cached after the first call so the ffmpeg probe only runs once
    per process.  Thread-safe.
    """
    global _ENCODER_CACHE
    with _ENCODER_LOCK:
        if FORCE_ENCODER:
            if _ENCODER_CACHE != FORCE_ENCODER:
                logger.info("[transcode] encoder forced: %s", FORCE_ENCODER)
                _ENCODER_CACHE = FORCE_ENCODER
            return _ENCODER_CACHE  # type: ignore[return-value]

        if _ENCODER_CACHE is not None:
            return _ENCODER_CACHE

        # ── probe available encoders ─────────────────────────────────────
        try:
            out = subprocess.check_output(
                ["ffmpeg", "-hide_banner", "-encoders"],
                stderr=subprocess.STDOUT, text=True, timeout=10,
            )
        except Exception as exc:
            logger.warning("[transcode] ffmpeg probe failed — falling back to libx264: %s", exc)
            _ENCODER_CACHE = "libx264"
            return _ENCODER_CACHE

        # ── NVENC ────────────────────────────────────────────────────────
        if "h264_nvenc" in out:
            try:
                subprocess.check_call(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error",
                     "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                     "-vframes", "4", "-c:v", "h264_nvenc", "-f", "null", "-"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
                )
                logger.info("[transcode] encoder: h264_nvenc (preset=%s)", NVENC_PRESET)
                _ENCODER_CACHE = "h264_nvenc"
                return _ENCODER_CACHE
            except Exception as exc:
                logger.warning("[transcode] h264_nvenc probe failed: %s", exc)

        # ── VAAPI ────────────────────────────────────────────────────────
        if "h264_vaapi" in out and os.path.exists(VAAPI_DEVICE):
            try:
                subprocess.check_call(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error",
                     "-vaapi_device", VAAPI_DEVICE,
                     "-f", "lavfi", "-i", "nullsrc=s=128x128:r=1",
                     "-vframes", "4", "-vf", "format=nv12,hwupload",
                     "-c:v", "h264_vaapi", "-f", "null", "-"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
                )
                logger.info("[transcode] encoder: h264_vaapi (qp=%d device=%s)",
                            VAAPI_QP, VAAPI_DEVICE)
                _ENCODER_CACHE = "h264_vaapi"
                return _ENCODER_CACHE
            except Exception as exc:
                logger.warning("[transcode] h264_vaapi probe failed: %s", exc)

        # ── CPU fallback ─────────────────────────────────────────────────
        logger.info("[transcode] encoder: libx264 (CPU fallback)")
        _ENCODER_CACHE = "libx264"
        return _ENCODER_CACHE


def invalidate_encoder_cache() -> None:
    """Force re-detection on the next call to get_encoder()."""
    global _ENCODER_CACHE
    with _ENCODER_LOCK:
        _ENCODER_CACHE = None


# ---------------------------------------------------------------------------
# ffmpeg argument builders
# ---------------------------------------------------------------------------

def _double_bitrate(b: str) -> str:
    """Return bitrate string doubled (e.g. '2500k' → '5000k')."""
    try:
        suffix = b[-1].lower()
        if suffix in ("k", "m"):
            return str(int(b[:-1]) * 2) + suffix
    except (IndexError, ValueError):
        pass
    return b


def build_video_args(encoder: str, *, bitrate: str | None = None,
                     nvenc_preset: str | None = None,
                     vaapi_qp: int | None = None,
                     framerate: int | None = None) -> list[str]:
    """
    Return the ffmpeg video codec arguments for the given encoder.

    All keyword args default to the module-level configured values, but can
    be overridden per-call for components that manage their own settings.

    Args:
        encoder:      One of "h264_nvenc", "h264_vaapi", "libx264".
        bitrate:      Target bitrate string, e.g. "4000k".
        nvenc_preset: NVENC quality preset p1–p7.
        vaapi_qp:     VAAPI constant-QP value (0–51).
        framerate:    Used to compute keyframe interval (-g).

    Returns:
        List of ffmpeg CLI arguments (no leading "-c:v" duplication — it's
        included in the returned list).
    """
    bv   = bitrate      or BITRATE
    np   = nvenc_preset or NVENC_PRESET
    qp   = vaapi_qp     if vaapi_qp is not None else VAAPI_QP
    fps  = framerate    or FRAMERATE
    gop  = fps * 2      # keyframe interval = 2 seconds

    if encoder == "h264_nvenc":
        return [
            "-c:v", "h264_nvenc",
            "-preset", np,
            "-tune", "ll",
            "-rc", "cbr",
            "-zerolatency", "1",
            "-b:v", bv, "-maxrate", bv, "-bufsize", _double_bitrate(bv),
            "-g", str(gop),
        ]

    if encoder == "h264_vaapi":
        # CQP mode: ignore -b:v when qp is set — quality is controlled by QP.
        # We still set -b:v as a ceiling to avoid runaway rates on complex scenes.
        return [
            "-c:v", "h264_vaapi",
            "-qp", str(qp),
            "-b:v", bv, "-maxrate", bv, "-bufsize", _double_bitrate(bv),
            "-g", str(gop),
        ]

    # libx264 CPU fallback
    return [
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-b:v", bv, "-maxrate", bv, "-bufsize", _double_bitrate(bv),
        "-g", str(gop),
    ]


def build_vf_chain(encoder: str, *, scale_w: int | None = None,
                   scale_h: int | None = None) -> list[str]:
    """
    Return the ffmpeg -vf filter chain for the given encoder.

    For VAAPI this uploads frames to GPU memory; for NVENC/libx264 this
    optionally adds a scale filter when scale_w/scale_h differ from input.

    Args:
        encoder:  Encoder name.
        scale_w:  Output width  (None = no scaling).
        scale_h:  Output height (None = no scaling).

    Returns:
        List of ffmpeg CLI arguments (empty list if no filter needed).
    """
    filters: list[str] = []

    if scale_w and scale_h:
        filters.append(f"scale={scale_w}:{scale_h}")

    if encoder == "h264_vaapi":
        filters += ["format=nv12", "hwupload"]
    elif encoder == "h264_nvenc" and (scale_w or scale_h):
        # hwupload_cuda is not needed for x11grab → nvenc on modern drivers,
        # but a format conversion helps avoid green-frame artifacts on some setups.
        filters.append("format=yuv420p")

    if not filters:
        return []
    return ["-vf", ",".join(filters)]


def vaapi_device_args(encoder: str, *, device: str | None = None) -> list[str]:
    """
    Return the -vaapi_device argument when encoder is h264_vaapi, else [].

    Must appear *before* the input (-i) in the ffmpeg command line.
    """
    if encoder == "h264_vaapi":
        return ["-vaapi_device", device or VAAPI_DEVICE]
    return []


# ---------------------------------------------------------------------------
# Convenience: full codec block for common pipelines
# ---------------------------------------------------------------------------

def codec_args(encoder: str | None = None, **kwargs) -> list[str]:
    """
    Return combined [vaapi_device_args] + [build_vf_chain] + [build_video_args].

    Convenience wrapper for callers that build the full ffmpeg command in one shot.
    vaapi_device_args must still appear before -i in the command, so callers
    that need fine-grained control should use the individual functions.

    Example (x11grab pipeline)::

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            *vaapi_device_args(enc),        # before -i
            "-f", "x11grab", "-framerate", str(FRAMERATE),
            "-video_size", f"{DISPLAY_W}x{DISPLAY_H}", "-i", display,
            *build_vf_chain(enc),           # between -i and -c:v
            *build_video_args(enc),
            "-c:a", "aac", "-b:a", "192k",
            "-f", "mpegts", "pipe:1",
        ]
    """
    enc = encoder or get_encoder()
    return (
        build_vf_chain(enc, **{k: v for k, v in kwargs.items()
                                if k in ("scale_w", "scale_h")})
        + build_video_args(enc, **{k: v for k, v in kwargs.items()
                                    if k in ("bitrate", "nvenc_preset",
                                             "vaapi_qp", "framerate")})
    )


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------

def active_config() -> dict:
    """Return the current transcoding configuration as a plain dict."""
    return {
        "preset":         PRESET_NAME,
        "encoder":        _ENCODER_CACHE or "not yet detected",
        "force_encoder":  FORCE_ENCODER,
        "width":          DISPLAY_W,
        "height":         DISPLAY_H,
        "framerate":      FRAMERATE,
        "bitrate":        BITRATE,
        "nvenc_preset":   NVENC_PRESET,
        "vaapi_qp":       VAAPI_QP,
        "vaapi_device":   VAAPI_DEVICE,
    }
