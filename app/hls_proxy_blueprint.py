"""
hls_proxy_blueprint.py
======================
Drop this file into your FastChannels app/ directory.

It adds an HLS buffering proxy directly into Flask as a blueprint —
same process, no second container. FastChannels can rewrite its output
stream URLs to go through this proxy instead of pointing clients at
upstream CDN URLs directly.

Features
--------
* Variant playlist cache with per-source TTL (short for Pluto)
* Pluto SSAI fix — EXT-X-DISCONTINUITY-aware cache eviction + 403 retry
* Segment prefetch (background threads)
* Per-source CDN header injection
* Redirect chain following
* Thread-safe (works with Flask's threaded WSGI server and gunicorn)

Registration (in your create_app() or app factory)
---------------------------------------------------
    from hls_proxy_blueprint import hls_proxy_bp, rewrite_stream_url
    app.register_blueprint(hls_proxy_bp, url_prefix="/hlsproxy")

Then wherever FastChannels builds a channel stream URL, wrap it:
    from hls_proxy_blueprint import rewrite_stream_url
    proxied = rewrite_stream_url(original_url, request)
    # proxied == "http://localhost:5000/hlsproxy/playlist?url=<encoded>"

Environment variables (all optional)
-------------------------------------
HLS_VARIANT_CACHE_TTL   Non-Pluto variant cache seconds (default 300)
HLS_PLUTO_VARIANT_TTL   Pluto variant cache seconds    (default 20)
HLS_SEGMENT_PREFETCH    Segments to prefetch ahead     (default 3)
HLS_MAX_REDIRECTS       HTTP redirect limit            (default 5)
HLS_REQUEST_TIMEOUT     Per-request timeout seconds    (default 15)
HLS_PROXY_BASE_URL      Override base URL for rewritten URLs
                        (default: auto-detected from request)
"""

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlencode, quote, unquote

import requests
from flask import Blueprint, Response, request, stream_with_context

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

VARIANT_CACHE_TTL = int(os.getenv("HLS_VARIANT_CACHE_TTL", 300))
PLUTO_VARIANT_TTL = int(os.getenv("HLS_PLUTO_VARIANT_TTL", 20))
SEGMENT_PREFETCH  = int(os.getenv("HLS_SEGMENT_PREFETCH", 3))
MAX_REDIRECTS     = int(os.getenv("HLS_MAX_REDIRECTS", 5))
REQUEST_TIMEOUT   = int(os.getenv("HLS_REQUEST_TIMEOUT", 15))
PROXY_BASE_URL    = os.getenv("HLS_PROXY_BASE_URL", "")   # e.g. "http://fastchannels:5000"

_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="hlsproxy")

# ── CDN header profiles ────────────────────────────────────────────────────────

_CDN_HEADERS: List[Tuple[str, dict]] = [
    ("pluto.tv",      {"User-Agent": "PlutoTV/5.0 (Linux; Android 9)",
                       "Origin": "https://pluto.tv", "Referer": "https://pluto.tv/"}),
    ("plutotv",       {"User-Agent": "PlutoTV/5.0 (Linux; Android 9)",
                       "Origin": "https://pluto.tv", "Referer": "https://pluto.tv/"}),
    ("jmpromo",       {"User-Agent": "PlutoTV/5.0 (Linux; Android 9)",
                       "Origin": "https://pluto.tv", "Referer": "https://pluto.tv/"}),
    ("plex.tv",       {"User-Agent": "PlexMediaPlayer/2.0",
                       "Origin": "https://app.plex.tv", "Referer": "https://app.plex.tv/"}),
    ("provider.plex", {"User-Agent": "PlexMediaPlayer/2.0",
                       "Origin": "https://app.plex.tv", "Referer": "https://app.plex.tv/"}),
    ("localnow",      {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                       "Origin": "https://www.localnow.com", "Referer": "https://www.localnow.com/"}),
    ("amagi",         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}),
    ("samsung",       {"User-Agent": "Tizen/3.0"}),
    ("tubi",          {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                       "Origin": "https://tubitv.com", "Referer": "https://tubitv.com/"}),
    ("xumo",          {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}),
    ("stirr",         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}),
    ("distrotv",      {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}),
    ("lgchannels",    {"User-Agent": "Mozilla/5.0 (SmartTV; Linux) AppleWebKit/537.36"}),
    # tvapp2 upstream CDN hosts (TheTVApp, TVPass, MoveOnJoy)
    ("thetvapp",      {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                       "Origin": "https://thetvapp.to", "Referer": "https://thetvapp.to/"}),
    ("tvpass",        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                       "Origin": "https://tvpass.org", "Referer": "https://tvpass.org/"}),
    ("moveonjoy",     {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                       "Origin": "https://moveonjoy.com", "Referer": "https://moveonjoy.com/"}),
]

PLUTO_MARKERS = ("pluto.tv", "plutotv", "stitcher.pluto", "jmpromo")

KEEP_RESP_HEADERS = {
    "content-type", "content-length", "cache-control",
    "access-control-allow-origin", "last-modified", "etag",
}


def _headers_for(url: str) -> dict:
    u = url.lower()
    for marker, hdrs in _CDN_HEADERS:
        if marker in u:
            return dict(hdrs)
    return {"User-Agent": "Mozilla/5.0"}


def _is_pluto(url: str) -> bool:
    u = url.lower()
    return any(m in u for m in PLUTO_MARKERS)


# ── Shared requests session (connection pool) ──────────────────────────────────

_http = requests.Session()
_http.max_redirects = 0   # we handle redirects manually


def _fetch_text(url: str) -> Tuple[int, str, dict, str]:
    """Fetch URL, follow redirects, return (status, body, headers, final_url)."""
    final_url = url
    for _ in range(MAX_REDIRECTS + 1):
        try:
            r = _http.get(
                url,
                headers=_headers_for(url),
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False,
                stream=False,
            )
            if r.status_code in (301, 302, 303, 307, 308):
                loc = r.headers.get("Location", "")
                url = urljoin(url, loc)
                final_url = url
                continue
            rh = {k.lower(): v for k, v in r.headers.items() if k.lower() in KEEP_RESP_HEADERS}
            return r.status_code, r.text, rh, final_url
        except requests.Timeout:
            log.warning("fetch_text timeout: %s", url[:80])
            return 504, "", {}, final_url
        except Exception as exc:
            log.warning("fetch_text error %s: %s", url[:80], exc)
            return 502, "", {}, final_url
    return 502, "", {}, final_url


def _fetch_bytes(url: str) -> Tuple[int, bytes, dict]:
    """Fetch binary URL, follow redirects, return (status, data, headers)."""
    for _ in range(MAX_REDIRECTS + 1):
        try:
            r = _http.get(
                url,
                headers=_headers_for(url),
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False,
                stream=True,
            )
            if r.status_code in (301, 302, 303, 307, 308):
                loc = r.headers.get("Location", "")
                url = urljoin(url, loc)
                continue
            data = r.content
            rh = {k.lower(): v for k, v in r.headers.items() if k.lower() in KEEP_RESP_HEADERS}
            return r.status_code, data, rh
        except requests.Timeout:
            return 504, b"", {}
        except Exception as exc:
            log.warning("fetch_bytes error %s: %s", url[:80], exc)
            return 502, b"", {}
    return 502, b"", {}


# ── Variant cache (thread-safe) ────────────────────────────────────────────────

class _VariantCache:
    def __init__(self):
        self._store: Dict[str, dict] = {}
        self._lock  = threading.Lock()

    def _ttl(self, url: str) -> int:
        return PLUTO_VARIANT_TTL if _is_pluto(url) else VARIANT_CACHE_TTL

    def get(self, url: str) -> Optional[dict]:
        with self._lock:
            e = self._store.get(url)
            if e and time.monotonic() < e["exp"]:
                return e
        return None

    def put(self, url: str, body: str, headers: dict):
        with self._lock:
            self._store[url] = {
                "body": body, "headers": headers,
                "exp": time.monotonic() + self._ttl(url),
            }

    def invalidate(self, url: str):
        with self._lock:
            self._store.pop(url, None)
            log.debug("variant cache invalidated: %.80s", url)


_vcache = _VariantCache()


# ── Discontinuity tracker (Pluto SSAI freeze fix) ─────────────────────────────

class _DiscTracker:
    """
    Tracks #EXT-X-DISCONTINUITY count per variant URL.
    A rising count means Pluto's stitcher has issued a new JWT for the
    new ad-break segment URLs — evict the cached playlist immediately so
    the next segment request gets freshly signed URLs.
    """
    def __init__(self):
        self._counts: Dict[str, int] = {}
        self._lock = threading.Lock()

    def check_and_evict(self, url: str, body: str) -> bool:
        new = body.count("#EXT-X-DISCONTINUITY")
        with self._lock:
            old = self._counts.get(url, 0)
            self._counts[url] = new
            if new > old:
                log.info("Pluto disc %d→%d, evicting cache: %.80s", old, new, url)
                return True
        return False

    def reset(self, url: str):
        with self._lock:
            self._counts.pop(url, None)


_disc = _DiscTracker()


# ── Prefetch buffer (thread-safe) ──────────────────────────────────────────────

class _PrefetchBuffer:
    def __init__(self):
        self._buf:   Dict[str, Dict[str, Tuple[bytes, dict]]] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._ml = threading.Lock()

    def _lock_for(self, vurl: str) -> threading.Lock:
        with self._ml:
            if vurl not in self._locks:
                self._locks[vurl] = threading.Lock()
            return self._locks[vurl]

    def warm(self, vurl: str, seg_urls: List[str]):
        """Submit prefetch jobs to the thread pool."""
        lk = self._lock_for(vurl)
        with lk:
            buf = self._buf.setdefault(vurl, {})
            for u in [u for u in list(buf) if u not in seg_urls]:
                del buf[u]
            to_fetch = [u for u in seg_urls[:SEGMENT_PREFETCH] if u not in buf]

        def _fetch_one(url):
            try:
                status, data, rh = _fetch_bytes(url)
                if status == 200:
                    with lk:
                        self._buf[vurl][url] = (data, rh)
            except Exception as exc:
                log.debug("prefetch err %.60s: %s", url, exc)

        for url in to_fetch:
            _executor.submit(_fetch_one, url)

    def pop(self, vurl: str, seg_url: str) -> Optional[Tuple[bytes, dict]]:
        lk = self._lock_for(vurl)
        with lk:
            return self._buf.get(vurl, {}).pop(seg_url, None)


_prefetch = _PrefetchBuffer()


# ── Variant fetch with cache + disc check ─────────────────────────────────────

def _get_variant(url: str) -> Tuple[int, str, dict, str]:
    entry = _vcache.get(url)
    if entry:
        return 200, entry["body"], entry["headers"], url

    status, body, hdrs, final_url = _fetch_text(url)
    if status != 200:
        return status, body, hdrs, final_url

    _vcache.put(url, body, hdrs)
    if final_url != url:
        _vcache.put(final_url, body, hdrs)

    if _is_pluto(url):
        if _disc.check_and_evict(url, body):
            _vcache.invalidate(url)
            _vcache.invalidate(final_url)

    return status, body, hdrs, final_url


# ── Pluto 403 retry ────────────────────────────────────────────────────────────

def _pluto_fetch_segment(seg_url: str, variant_url: str) -> Tuple[int, bytes, dict]:
    status, data, hdrs = _fetch_bytes(seg_url)
    if status not in (403, 410):
        return status, data, hdrs

    log.warning("Pluto seg 403 — refreshing variant: %.80s", variant_url)

    for attempt in range(2):
        _vcache.invalidate(variant_url)
        _disc.reset(variant_url)
        time.sleep(0.4)

        vs, vbody, vhdrs, _ = _fetch_text(variant_url)
        if vs != 200:
            log.warning("Pluto variant re-fetch %d (attempt %d)", vs, attempt + 1)
            continue

        _vcache.put(variant_url, vbody, vhdrs)
        new_seg = _first_seg_url(variant_url, vbody)
        if not new_seg:
            continue

        status, data, hdrs = _fetch_bytes(new_seg)
        if status == 200:
            log.info("Pluto 403 retry ok (attempt %d)", attempt + 1)
            return status, data, hdrs

    log.error("Pluto seg failed after retries: %.80s", seg_url)
    return status, data, hdrs


def _first_seg_url(variant_url: str, body: str) -> str:
    base = variant_url.rsplit("/", 1)[0]
    for line in body.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line if line.startswith("http") else f"{base}/{line}"
    return ""


# ── URL rewriting helpers ──────────────────────────────────────────────────────

def _base_url() -> str:
    """Return the proxy base URL (scheme://host:port) for rewriting."""
    if PROXY_BASE_URL:
        return PROXY_BASE_URL.rstrip("/")
    # Auto-detect from current Flask request
    try:
        host = request.host          # includes port if non-standard
        scheme = request.scheme
        return f"{scheme}://{host}"
    except RuntimeError:
        return "http://localhost:5000"


def rewrite_stream_url(upstream_url: str, req=None) -> str:
    """
    Convert an upstream HLS URL into a proxied FastChannels URL.
    Call this wherever FastChannels builds channel stream URLs.

    Example:
        from hls_proxy_blueprint import rewrite_stream_url
        stream_url = rewrite_stream_url(pluto_stream_url)
    """
    base = PROXY_BASE_URL.rstrip("/") if PROXY_BASE_URL else (
        f"{req.scheme}://{req.host}" if req else "http://localhost:5000"
    )
    return f"{base}/hlsproxy/playlist?url={quote(upstream_url, safe='')}"


def _proxy_url(url: str) -> str:
    return f"{_base_url()}/hlsproxy/playlist?url={quote(url, safe='')}"


def _seg_url(seg: str, variant: str) -> str:
    q = urlencode({"url": seg, "variant": variant})
    return f"{_base_url()}/hlsproxy/segment?{q}"


def _abs(u: str, base: str) -> str:
    return u if u.startswith("http") else urljoin(base.rstrip("/") + "/", u.lstrip("/"))


# ── M3U8 rewriting ─────────────────────────────────────────────────────────────

def _rewrite_m3u8(body: str, base_url: str, variant_url: str) -> str:
    def rewrite_uri(m):
        return m.group(0).replace(m.group(1), _proxy_url(_abs(m.group(1), base_url)))

    out = []
    lines = body.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.strip()

        if s.startswith("#EXT-X-KEY") or s.startswith("#EXT-X-MAP"):
            line = re.sub(r'URI="([^"]+)"', rewrite_uri, line)
            out.append(line)

        elif s.startswith("#EXT-X-STREAM-INF") or (
                s.startswith("#EXT-X-MEDIA") and 'URI="' in s):
            out.append(line)
            i += 1
            if i < len(lines):
                nxt = lines[i].strip()
                if nxt and not nxt.startswith("#"):
                    out.append(_proxy_url(_abs(nxt, base_url)))
                    i += 1
                    continue

        elif s and not s.startswith("#"):
            out.append(_seg_url(_abs(s, base_url), variant_url))

        else:
            out.append(line)

        i += 1
    return "\n".join(out)


def _trigger_prefetch(variant_url: str, final_url: str, body: str):
    base = final_url.rsplit("/", 1)[0]
    segs = [
        (_abs(l.strip(), base))
        for l in body.splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    if segs:
        _prefetch.warm(variant_url, segs)


# ── Blueprint routes ───────────────────────────────────────────────────────────

hls_proxy_bp = Blueprint("hls_proxy", __name__)


@hls_proxy_bp.route("/playlist")
def proxy_playlist():
    """
    GET /hlsproxy/playlist?url=<encoded_upstream_url>
    Serves master or variant M3U8 playlists with all URIs rewritten
    to route through this proxy.
    """
    url = request.args.get("url", "")
    if not url:
        return Response("Missing url", status=400)

    status, body, hdrs, final_url = _get_variant(url)
    if status != 200:
        return Response(status=status)

    base = final_url.rsplit("/", 1)[0]
    rewritten = _rewrite_m3u8(body, base, final_url)

    # Kick off prefetch for variant playlists (not masters)
    if "#EXT-X-STREAM-INF" not in body:
        _executor.submit(_trigger_prefetch, url, final_url, body)

    return Response(
        rewritten,
        content_type="application/vnd.apple.mpegurl",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Access-Control-Allow-Origin": "*",
        },
    )


@hls_proxy_bp.route("/segment")
def proxy_segment():
    """
    GET /hlsproxy/segment?url=<seg_url>&variant=<variant_url>
    Serves TS/fMP4 segments. Checks prefetch buffer first.
    Pluto: 403 triggers variant refresh + retry.
    """
    seg_url     = request.args.get("url", "")
    variant_url = request.args.get("variant", "")
    if not seg_url:
        return Response("Missing url", status=400)

    # Check prefetch buffer
    if variant_url:
        hit = _prefetch.pop(variant_url, seg_url)
        if hit:
            data, hdrs = hit
            log.debug("prefetch hit: %.60s", seg_url)
            _executor.submit(_trigger_prefetch_from_variant, variant_url)
            return Response(data, headers={**hdrs, "Access-Control-Allow-Origin": "*"})

    # Fetch — Pluto gets 403-retry path
    if _is_pluto(seg_url) and variant_url:
        status, data, hdrs = _pluto_fetch_segment(seg_url, variant_url)
    else:
        status, data, hdrs = _fetch_bytes(seg_url)

    if status != 200:
        return Response(status=status)

    if variant_url:
        _executor.submit(_trigger_prefetch_from_variant, variant_url)

    return Response(data, headers={**hdrs, "Access-Control-Allow-Origin": "*"})


def _trigger_prefetch_from_variant(variant_url: str):
    try:
        status, body, _, final_url = _get_variant(variant_url)
        if status == 200:
            _trigger_prefetch(variant_url, final_url, body)
    except Exception as exc:
        log.debug("prefetch trigger err: %s", exc)


@hls_proxy_bp.route("/health")
def proxy_health():
    return Response("ok")


# ── Integration snippet (printed on import in dev mode) ────────────────────────

_REGISTRATION_HINT = """
┌─ hls_proxy_blueprint: Registration ───────────────────────────────────────┐
│                                                                            │
│  In your Flask app factory / create_app():                                 │
│                                                                            │
│    from hls_proxy_blueprint import hls_proxy_bp, rewrite_stream_url       │
│    app.register_blueprint(hls_proxy_bp, url_prefix="/hlsproxy")           │
│                                                                            │
│  Where you build channel stream URLs (e.g. in routes/channels.py or       │
│  wherever the M3U playlist is generated):                                  │
│                                                                            │
│    stream_url = rewrite_stream_url(upstream_url)                          │
│    # → http://fastchannels:5000/hlsproxy/playlist?url=<encoded>           │
│                                                                            │
│  Set HLS_PROXY_BASE_URL env var if FastChannels runs behind a             │
│  reverse proxy or on a non-standard port, e.g.:                           │
│    HLS_PROXY_BASE_URL=http://fastchannels:5000                            │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
"""

if os.getenv("FLASK_ENV") == "development" or os.getenv("FLASK_DEBUG"):
    print(_REGISTRATION_HINT)
