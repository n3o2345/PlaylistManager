"""
HDHomeRun scraper for PlaylistManager.

Discovers HDHomeRun network tuners on the LAN (or a configured IP list),
reads their device/discover XML, fetches the lineup, and produces live-TV
ChannelData entries.

Config (set via admin UI):
  - hosts          comma-separated IPs/hostnames, e.g. "192.168.1.10,192.168.1.11"
                   Leave blank to attempt LAN auto-discovery via the Silicondust API.
  - use_discovery  toggle: try https://api.hdhomerun.com/discover (default on)
  - quality        hts | full | internet720 | internet480 | internet360 | internet240
                   (passed as ?ClientVersion= quality param, default "hts")
  - tuner_count    max tuner streams to use per device (default: all)

Stream URL format:  hdhomerun://<host>/<guide_number>
resolve() rewrites to the direct HTTP tuner URL.
"""
from __future__ import annotations

import logging
import threading
from typing import Optional
from urllib.parse import quote, unquote

import requests

from .base import (
    BaseScraper,
    ChannelData,
    ConfigField,
    ProgramData,
)

logger = logging.getLogger(__name__)

_DISCOVER_API = "https://api.hdhomerun.com/discover"
_LINEUP_PATH  = "/lineup.json"
_DEVICE_PATH  = "/discover.json"
_STREAM_PATH  = "/auto/v{guide_number}"

# Quality presets the HDHomeRun firmware accepts
_QUALITY_OPTIONS = [
    {"value": "hts",          "label": "Full (transcoded, HTS)"},
    {"value": "full",         "label": "Full (raw MPEG-TS, no transcoding)"},
    {"value": "internet720",  "label": "720p (internet)"},
    {"value": "internet480",  "label": "480p (internet)"},
    {"value": "internet360",  "label": "360p (internet)"},
    {"value": "internet240",  "label": "240p (internet)"},
]


class HDHomeRunScraper(BaseScraper):
    source_name      = "hdhomerun"
    display_name     = "HDHomeRun"
    scrape_interval  = 1440          # refresh once a day is enough for OTA
    config_required  = False         # works without config via LAN discovery

    config_schema = [
        ConfigField(
            key="hosts",
            label="Device IP(s) / URL(s)",
            field_type="text",
            required=False,
            placeholder="192.168.1.10, https://hdhr.example.com:5004",
            help_text=(
                "Comma-separated IPs, hostnames, or full base URLs for your "
                "HDHomeRun device(s). Use a public HTTPS/port-forward/reverse-proxy "
                "URL for a tuner reachable over WAN. Leave blank to use automatic "
                "LAN discovery."
            ),
        ),
        ConfigField(
            key="use_discovery",
            label="Use Silicondust LAN Discovery",
            field_type="toggle",
            default="1",
            help_text=(
                "Query https://api.hdhomerun.com/discover to find devices on your "
                "LAN automatically. Disable if your network blocks cloud discovery."
            ),
        ),
        ConfigField(
            key="quality",
            label="Stream Quality",
            field_type="select",
            default="hts",
            options=_QUALITY_OPTIONS,
            help_text=(
                "Transcode quality. 'Full (HTS)' works well with most IPTV players. "
                "'Full (raw)' bypasses the built-in transcoder for maximum quality."
            ),
        ),
        ConfigField(
            key="request_timeout",
            label="Request Timeout",
            field_type="number",
            required=False,
            default=20,
            placeholder="20",
            help_text=(
                "Seconds to wait for remote tuner API responses. Increase this "
                "for WAN/VPN tuners on slower links."
            ),
        ),
        ConfigField(
            key="epg_url",
            label="XMLTV EPG URL",
            field_type="text",
            required=False,
            placeholder="https://example.com/xmltv.xml.gz",
            help_text=(
                "Optional XMLTV feed for guide data. You can use the same XMLTV "
                "URL configured on TVApp2; channels are matched by guide number, "
                "channel name, and XMLTV aliases."
            ),
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        self._lock = threading.Lock()

        raw_hosts = (self.config.get("hosts") or "").strip()
        self._static_hosts: list[str] = [
            h.strip() for h in raw_hosts.split(",") if h.strip()
        ]

        use_disc = self.config.get("use_discovery", "1")
        self._use_discovery = str(use_disc).strip() not in ("0", "false", "no", "off")

        self._quality = (self.config.get("quality") or "hts").strip()
        self._epg_url = (self.config.get("epg_url") or "").strip()
        try:
            self._request_timeout = max(3, min(int(self.config.get("request_timeout") or 20), 120))
        except (TypeError, ValueError):
            self._request_timeout = 20

    @staticmethod
    def _base_url(host: str) -> str:
        host = (host or "").strip().rstrip("/")
        return host if host.startswith(("http://", "https://")) else f"http://{host}"

    @staticmethod
    def _stream_token(host: str) -> str:
        return quote((host or "").strip(), safe="")

    def _effective_epg_url(self) -> str:
        if self._epg_url:
            return self._epg_url
        try:
            from ..models import Source
            tvapp2 = Source.query.filter_by(name="tvapp2").first()
            return ((tvapp2.config or {}).get("epg_url") or "").strip() if tvapp2 else ""
        except Exception:
            return ""

    # ── Device discovery ──────────────────────────────────────────────────────

    def _discover_hosts(self) -> list[str]:
        """Return a deduplicated list of reachable device IPs."""
        hosts: list[str] = list(self._static_hosts)

        if self._use_discovery:
            try:
                r = self.session.get(_DISCOVER_API, timeout=8)
                r.raise_for_status()
                for device in r.json():
                    ip = device.get("LocalIP") or device.get("BaseURL", "")
                    if ip and ip not in hosts:
                        hosts.append(ip)
                logger.debug("[hdhomerun] discovery found %d device(s)", len(hosts))
            except Exception as exc:
                logger.warning("[hdhomerun] LAN discovery failed: %s", exc)

        if not hosts:
            logger.error(
                "[hdhomerun] No devices found. "
                "Set device IPs in Sources → HDHomeRun or enable LAN discovery."
            )
        return hosts

    def _device_info(self, host: str) -> Optional[dict]:
        """Fetch /discover.json from a device."""
        base = self._base_url(host)
        try:
            r = self.session.get(f"{base}{_DEVICE_PATH}", timeout=self._request_timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logger.warning("[hdhomerun] %s: device info failed: %s", host, exc)
            return None

    def _fetch_lineup(self, host: str) -> list[dict]:
        """Fetch /lineup.json (the channel list) from a device."""
        base = self._base_url(host)
        try:
            r = self.session.get(f"{base}{_LINEUP_PATH}", timeout=self._request_timeout)
            r.raise_for_status()
            return r.json() or []
        except Exception as exc:
            logger.warning("[hdhomerun] %s: lineup fetch failed: %s", host, exc)
            return []

    # ── Scraper interface ─────────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        hosts = self._discover_hosts()
        all_channels: list[ChannelData] = []
        seen_numbers: set[str] = set()

        for host in hosts:
            info = self._device_info(host)
            device_id = (info or {}).get("DeviceID", host)

            lineup = self._fetch_lineup(host)
            if not lineup:
                logger.warning("[hdhomerun] %s: empty lineup", host)
                continue

            for entry in lineup:
                guide_number = entry.get("GuideNumber", "")
                guide_name   = (entry.get("GuideName") or "").strip()
                if not guide_number or not guide_name:
                    continue

                # Skip duplicates when the same channel appears on multiple tuners
                uniq_key = guide_number
                if uniq_key in seen_numbers:
                    continue
                seen_numbers.add(uniq_key)

                # Pluck optional data
                hd        = entry.get("HD", 0)
                drm       = entry.get("DRM", 0)
                logo_url  = entry.get("ImageURL") or None

                if drm:
                    logger.debug(
                        "[hdhomerun] %s ch %s is DRM-protected, skipping",
                        host, guide_number,
                    )
                    continue

                # Store as a virtual URL; resolve() will rewrite it
                stream_url = f"hdhomerun://{self._stream_token(host)}/{guide_number}"

                # Best-effort channel number parsing
                try:
                    ch_num = float(guide_number)
                except ValueError:
                    ch_num = None

                all_channels.append(ChannelData(
                    source_channel_id = f"{device_id}-{guide_number}",
                    name              = guide_name,
                    stream_url        = stream_url,
                    stream_type       = "hls" if self._quality != "full" else "ts",
                    logo_url          = logo_url,
                    slug              = guide_name.lower().replace(" ", "-"),
                    category          = "HD" if hd else "SD",
                    language          = "en",
                    country           = "US",
                    number            = ch_num,
                    guide_key         = guide_number,
                    description       = entry.get("Affiliate") or None,
                ))

            logger.info(
                "[hdhomerun] %s: %d channels loaded",
                host,
                sum(1 for c in all_channels if c.stream_url.startswith(f"hdhomerun://{self._stream_token(host)}/")),
            )

        logger.info("[hdhomerun] total %d channels", len(all_channels))
        return all_channels

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """Import guide data from an optional XMLTV URL."""
        epg_url = self._effective_epg_url()
        if not epg_url:
            return []
        try:
            from .tvapp2 import TVApp2Scraper
            xmltv = TVApp2Scraper({"epg_url": epg_url})
            xmltv._progress_cb = self._progress_cb
            return xmltv.fetch_epg(channels, **kwargs)
        except Exception as exc:
            logger.warning("[hdhomerun] failed to import XMLTV EPG from %s: %s", epg_url, exc)
            return []

    # ── Stream resolution ─────────────────────────────────────────────────────

    def resolve(self, raw_url: str) -> str:
        """
        Convert  hdhomerun://<host>/<guide_number>
        to       http://<host>/auto/v<guide_number>?ClientVersion=5.3.9&Quality=<quality>
        """
        if not raw_url.startswith("hdhomerun://"):
            return raw_url

        remainder = raw_url[len("hdhomerun://"):]
        # host may itself contain colons (IPv6) — split on the LAST '/'
        idx = remainder.rfind("/")
        if idx < 0:
            logger.error("[hdhomerun] malformed url: %s", raw_url)
            return raw_url

        host         = unquote(remainder[:idx])
        guide_number = remainder[idx + 1:]
        base         = self._base_url(host)

        quality = self._quality
        url = (
            f"{base}/auto/v{guide_number}"
            f"?ClientVersion=5.3.9"
            f"&Quality={quality}"
        )
        logger.debug("[hdhomerun] resolve → %s", url)
        return url
