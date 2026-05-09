# app/scrapers/tvapp2.py
"""
tvapp2 scraper for PlaylistManager.

tvapp2 is embedded in the same Docker image and always runs on
http://127.0.0.1:4124 — no user configuration needed.

Channels are pulled from /playlist, stream URLs are unwrapped from tvapp2's
own /channel?url= wrapper so PlaylistManager stores the raw upstream URL.
PlaylistManager re-wraps at play time and proxies everything server-side.

EPG comes from Gracenote — tvapp2 tvg-ids are "<gracenote_station_id>.<call>"
(e.g. "111871.571ACC") so the Gracenote station ID is extracted directly and
stored on the channel, letting supported players use Gracenote guide
data without any XMLTV feed from tvapp2.

Dead streams are auto-disabled via stream_audit_enabled = True.
"""
from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urlsplit, parse_qs, unquote as _unquote

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_BASE_URL   = 'http://127.0.0.1:4124'
_ATTR_RE    = re.compile(r'(\S+)="([^"]*)"')


# ── M3U helpers ───────────────────────────────────────────────────────────────

def _parse_extinf(line: str) -> dict:
    attrs = {}
    for key, val in _ATTR_RE.findall(line):
        attrs[key.lower()] = val
    comma = line.rfind(',')
    if comma != -1:
        attrs['_name'] = line[comma + 1:].strip()
    return attrs


def _unwrap_channel_url(url: str) -> str:
    """
    tvapp2's playlist serves stream lines as:
        http://127.0.0.1:4124/channel?url=https%3A%2F%2Fthetvapp.to%2F...
    Extract the inner raw upstream URL so we never store the local proxy address.
    """
    try:
        parsed = urlsplit(url)
        if parsed.path == '/channel':
            inner = parse_qs(parsed.query).get('url', [None])[0]
            if inner:
                return _unquote(inner)
    except Exception:
        pass
    return url


def _extract_gracenote_id(tvg_id: str) -> str | None:
    """
    tvapp2 tvg-ids: "<gracenote_station_id>.<call_letters>"
    e.g. "111871.571ACC" → Gracenote station ID "111871"
    """
    if not tvg_id:
        return None
    prefix = tvg_id.split('.')[0]
    if prefix.isdigit() and len(prefix) >= 5:
        return prefix
    return None


_GROUP_MAP = {
    'sports':          'Sports',
    'sport':           'Sports',
    'news':            'News',
    'entertainment':   'Entertainment',
    'movies':          'Movies',
    'movie':           'Movies',
    'kids':            'Kids',
    'music':           'Music',
    'lifestyle':       'Lifestyle',
    'comedy':          'Comedy',
    'drama':           'Drama',
    'reality':         'Reality TV',
    'documentary':     'Documentary',
    'documentaries':   'Documentary',
    'sci-fi':          'Sci-Fi',
    'science fiction': 'Sci-Fi',
    'horror':          'Horror',
    'food':            'Food',
    'travel':          'Travel',
    'nature':          'Nature',
    'history':         'History',
    'general':         'Entertainment',
}


def _normalise_group(group: Optional[str]) -> Optional[str]:
    if not group:
        return None
    return _GROUP_MAP.get(group.lower().strip(), group.title())


# ── Scraper ───────────────────────────────────────────────────────────────────

class TVApp2Scraper(BaseScraper):

    source_name          = 'tvapp2'
    display_name         = 'TVApp2'
    scrape_interval      = 360       # re-sync every 6 h
    config_required      = False     # no user config needed — hardcoded to localhost
    stream_audit_enabled = True      # auto-disable dead streams

    # No config_schema — nothing for the user to fill in
    config_schema = []

    def __init__(self, config: dict = None):
        super().__init__(config)
        # Always the embedded instance — ignore any legacy config
        self._base_url = _BASE_URL

    # ── playlist ──────────────────────────────────────────────────────────────

    def _fetch_playlist(self) -> Optional[str]:
        url = f'{self._base_url}/playlist'
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error('[tvapp2] failed to fetch playlist from %s: %s', url, e)
            return None

    def _parse_playlist(self, m3u_text: str) -> list[ChannelData]:
        channels: list[ChannelData] = []
        seen_ids: set[str] = set()
        lines = m3u_text.splitlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line.startswith('#EXTINF'):
                i += 1
                continue

            attrs = _parse_extinf(line)

            j = i + 1
            stream_line = ''
            while j < len(lines):
                candidate = lines[j].strip()
                if candidate and not candidate.startswith('#'):
                    stream_line = candidate
                    break
                j += 1
            i = j + 1

            if not stream_line:
                continue

            name     = attrs.get('_name') or attrs.get('tvg-name') or ''
            tvg_id   = attrs.get('tvg-id') or ''
            logo_url = attrs.get('tvg-logo') or None
            group    = attrs.get('group-title') or None

            if not name:
                continue

            base_id    = tvg_id or name.lower().replace(' ', '-')
            group_slug = (group or '').lower().replace(' ', '').replace('/', '')
            source_channel_id = f'{base_id}.{group_slug}' if group_slug else base_id

            if source_channel_id in seen_ids:
                n = 2
                while f'{source_channel_id}.{n}' in seen_ids:
                    n += 1
                source_channel_id = f'{source_channel_id}.{n}'
            seen_ids.add(source_channel_id)

            gracenote_id = resolve_gracenote(
                'tvapp2',
                upstream_id = _extract_gracenote_id(tvg_id),
                lookup_key  = base_id,
            )

            channels.append(ChannelData(
                source_channel_id = source_channel_id,
                name              = name,
                stream_url        = _unwrap_channel_url(stream_line),
                logo_url          = logo_url,
                slug              = source_channel_id,
                category          = _normalise_group(group),
                language          = infer_language_from_metadata(name, group),
                country           = 'US',
                stream_type       = 'hls',
                gracenote_id      = gracenote_id,
                tags              = [group] if group else [],
            ))

        logger.info('[tvapp2] parsed %d channels', len(channels))
        return channels

    # ── BaseScraper interface ─────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        m3u_text = self._fetch_playlist()
        if not m3u_text:
            return []
        return self._parse_playlist(m3u_text)

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        # EPG is sourced from Gracenote via the gracenote_id on each channel.
        # tvapp2's XMLTV feed is intentionally not used.
        return []

    def resolve(self, raw_url: str) -> str:
        # raw_url is the unwrapped upstream URL stored at scrape time.
        # play.py's tvapp2_manifest_proxy wraps it in /channel?url= and
        # proxies it server-side — this method is not called in the proxy path
        # but must return a valid URL so play() routes correctly.
        return raw_url
