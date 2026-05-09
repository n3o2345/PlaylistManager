# app/scrapers/tvapp2.py
"""
tvapp2 scraper for FastChannels — internal embedded mode.

tvapp2 is bundled inside the FastChannels Docker image and runs as a
background Node.js process on localhost:4124.  It is started automatically
by entrypoint.sh before FastChannels comes up.

Set TVAPP2_ENABLED=0 in the container environment to disable the embedded
daemon (and this scraper will be a no-op).

tvapp2 exposes:
  GET http://127.0.0.1:4124/playlist.m3u8      Full IPTV M3U8 playlist
  GET http://127.0.0.1:4124/channel?url=<enc>  Per-channel token endpoint
  GET http://127.0.0.1:4124/api/health         JSON health check

stream_url stored as:  tvapp2://<channel_page_url>
resolve()  turns it into: http://127.0.0.1:4124/channel?url=<encoded_page_url>

This means a fresh token is fetched from the upstream source (TheTVApp /
TVPass) on every play request — tokens are short-lived and the tvapp2
daemon manages all the session/cookie state so FastChannels doesn't have to.

Environment variables that tune the embedded daemon (set at container level):
  TVAPP2_ENABLED           1 = run daemon (default), 0 = disabled
  TVAPP2_PORT              Daemon port (default 4124)
  TVAPP2_STREAM_QUALITY    hd | sd  (default hd)
  TVAPP2_LOG_LEVEL         0-6  (default 2 = info)
"""
from __future__ import annotations

import logging
import os
import re
from typing import Optional
from urllib.parse import quote as _quote, urlsplit, parse_qs, unquote

from .base import BaseScraper, ChannelData, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_SCHEME = 'tvapp2://'

# Internal tvapp2 daemon address — always localhost inside the container.
_TVAPP2_PORT = int(os.environ.get('TVAPP2_PORT', 4124))
_TVAPP2_BASE = f'http://127.0.0.1:{_TVAPP2_PORT}'

_ATTR_RE = re.compile(r'(\S+)="([^"]*)"')


def _parse_extinf(line: str) -> dict:
    attrs: dict = {}
    for key, val in _ATTR_RE.findall(line):
        attrs[key.lower()] = val
    comma = line.rfind(',')
    if comma != -1:
        attrs['_name'] = line[comma + 1:].strip()
    return attrs


def _extract_channel_page_url(tvapp2_proxy_url: str) -> Optional[str]:
    try:
        parsed = urlsplit(tvapp2_proxy_url)
        qs = parse_qs(parsed.query)
        candidates = qs.get('url') or qs.get('URL')
        if candidates:
            return unquote(candidates[0])
    except Exception:
        pass
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


class TVApp2Scraper(BaseScraper):
    """
    Pulls channels from the embedded tvapp2 Node.js daemon running on
    localhost inside the FastChannels container.

    No configuration is needed — tvapp2 is always at 127.0.0.1:TVAPP2_PORT.
    Add "tvapp2" as a source in the FastChannels admin UI and it will
    automatically discover and import all channels tvapp2 exposes.
    """

    source_name          = 'tvapp2'
    display_name         = 'TVApp2 (internal)'
    scrape_interval      = 720
    config_required      = False
    stream_audit_enabled = False
    config_schema        = []

    def _tvapp2_enabled(self) -> bool:
        return os.environ.get('TVAPP2_ENABLED', '1') != '0'

    def _health_ok(self) -> bool:
        try:
            r = self.session.get(f'{_TVAPP2_BASE}/api/health', timeout=10)
            return r.status_code == 200
        except Exception as e:
            logger.warning('[tvapp2] health check failed: %s', e)
            return False

    def _fetch_playlist(self) -> Optional[str]:
        url = f'{_TVAPP2_BASE}/playlist.m3u8'
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error('[tvapp2] failed to fetch playlist: %s', e)
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

            channel_page_url = _extract_channel_page_url(stream_line) or stream_line

            name   = attrs.get('_name') or attrs.get('tvg-name') or ''
            tvg_id = attrs.get('tvg-id') or ''
            logo   = attrs.get('tvg-logo') or None
            group  = attrs.get('group-title') or None

            if not name:
                continue

            if tvg_id:
                source_channel_id = tvg_id
            else:
                path = urlsplit(channel_page_url).path.strip('/')
                source_channel_id = path.split('/')[-1] if path else name.lower().replace(' ', '-')

            if source_channel_id in seen_ids:
                continue
            seen_ids.add(source_channel_id)

            channels.append(ChannelData(
                source_channel_id = source_channel_id,
                name              = name,
                stream_url        = f'{_SCHEME}{channel_page_url}',
                logo_url          = logo,
                slug              = source_channel_id,
                category          = _normalise_group(group),
                language          = infer_language_from_metadata(name, group),
                country           = 'US',
                stream_type       = 'hls',
                gracenote_id      = resolve_gracenote('tvapp2', lookup_key=source_channel_id),
                tags              = [group] if group else [],
            ))

        logger.info('[tvapp2] parsed %d channels from internal daemon', len(channels))
        return channels

    def fetch_channels(self) -> list[ChannelData]:
        if not self._tvapp2_enabled():
            logger.info('[tvapp2] daemon disabled (TVAPP2_ENABLED=0) — skipping scrape')
            return []

        if not self._health_ok():
            logger.warning('[tvapp2] daemon at %s is not responding — skipping scrape', _TVAPP2_BASE)
            return []

        m3u_text = self._fetch_playlist()
        if not m3u_text:
            return []

        return self._parse_playlist(m3u_text)

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        # tvapp2 does not serve EPG; guide data comes from Gracenote mappings.
        return []

    def resolve(self, raw_url: str) -> str:
        """
        Convert tvapp2://<page_url> to a live token URL via the internal daemon.
        The daemon fetches a fresh signed HLS token on every call.
        """
        if not raw_url.startswith(_SCHEME):
            return raw_url
        channel_page_url = raw_url[len(_SCHEME):]
        return f'{_TVAPP2_BASE}/channel?url={_quote(channel_page_url, safe="")}'
