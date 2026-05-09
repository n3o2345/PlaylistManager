# app/scrapers/tvapp2.py
"""
tvapp2 scraper for FastChannels.

tvapp2 is a self-hosted Node.js proxy that provides tokenized HLS streams for
live sports and TV channels from TheTVApp.to and TVPass.org.  It exposes:

  GET http://<host>:<port>/playlist.m3u8
      Full IPTV M3U8 playlist with channels already rewritten to go through
      tvapp2's /channel?url= token endpoint.

  GET http://<host>:<port>/channel?url=<encoded_channel_page_url>
      Token resolution endpoint — fetches a fresh signed HLS URL from the
      upstream source and proxies the variant playlist back to the client.

  GET http://<host>:<port>/api/health
      JSON health check.

Config (set via the FastChannels admin UI):
  host        tvapp2 host (default: localhost)
  port        tvapp2 port (default: 4124)
  api_key     Optional API key if tvapp2 is configured with API_KEY env var
  base_url    Full base URL override, e.g. http://192.168.1.50:4124
              (takes precedence over host+port when set)

stream_url is stored as:  tvapp2://<channel_page_url>
resolve() turns that into: http://<tvapp2>/channel?url=<encoded_channel_page_url>

This means the token is fetched fresh on every play request, matching how
tvapp2 is designed to work (tokens are short-lived and device-bound).
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote as _quote, urlsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 4124
_SCHEME = 'tvapp2://'

# Attributes we pull from #EXTINF lines.
# Example: #EXTINF:-1 tvg-id="..." tvg-name="..." tvg-logo="..." group-title="...",Channel Name
_ATTR_RE = re.compile(r'(\S+)="([^"]*)"')


def _parse_extinf(line: str) -> dict:
    """Parse key="value" attributes from a #EXTINF line."""
    attrs = {}
    for key, val in _ATTR_RE.findall(line):
        attrs[key.lower()] = val
    # Channel display name is everything after the last comma
    comma = line.rfind(',')
    if comma != -1:
        attrs['_name'] = line[comma + 1:].strip()
    return attrs


class TVApp2Scraper(BaseScraper):
    """
    Fetches the tvapp2 M3U playlist and registers each channel so FastChannels
    can resolve streams on demand through the tvapp2 token endpoint.
    """

    source_name     = 'tvapp2'
    display_name    = 'TVApp2'
    scrape_interval = 720          # tvapp2 channel list changes infrequently
    config_required = True
    stream_audit_enabled = False   # tvapp2 streams require fresh tokens; static audit is unreliable

    config_schema = [
        ConfigField(
            key='base_url',
            label='tvapp2 Base URL',
            field_type='text',
            required=False,
            placeholder='http://192.168.1.10:4124',
            help_text=(
                'Full URL to your tvapp2 instance, e.g. http://192.168.1.10:4124. '
                'When set, overrides the Host and Port fields below.'
            ),
        ),
        ConfigField(
            key='host',
            label='tvapp2 Host',
            field_type='text',
            default='localhost',
            placeholder='localhost',
            help_text='Hostname or IP of the tvapp2 server. Ignored when Base URL is set.',
        ),
        ConfigField(
            key='port',
            label='tvapp2 Port',
            field_type='number',
            default=str(_DEFAULT_PORT),
            placeholder=str(_DEFAULT_PORT),
            help_text='HTTP port tvapp2 listens on (default 4124). Ignored when Base URL is set.',
        ),
        ConfigField(
            key='api_key',
            label='API Key',
            field_type='password',
            required=False,
            secret=True,
            placeholder='leave blank if not configured',
            help_text=(
                'Optional. Set this if tvapp2 was started with the API_KEY environment variable.'
            ),
        ),
    ]

    # ── Initialisation ────────────────────────────────────────────────────────

    def __init__(self, config: dict = None):
        super().__init__(config)
        self._base_url = self._build_base_url()
        api_key = (self.config.get('api_key') or '').strip()
        if api_key:
            self.session.headers['X-API-Key'] = api_key

    def _build_base_url(self) -> str:
        explicit = (self.config.get('base_url') or '').strip().rstrip('/')
        if explicit:
            return explicit
        host = (self.config.get('host') or 'localhost').strip()
        try:
            port = int(self.config.get('port') or _DEFAULT_PORT)
        except (ValueError, TypeError):
            port = _DEFAULT_PORT
        return f'http://{host}:{port}'

    # ── Health check ──────────────────────────────────────────────────────────

    def _health_ok(self) -> bool:
        """Return True if the tvapp2 health endpoint responds 200."""
        url = f'{self._base_url}/api/health'
        try:
            r = self.session.get(url, timeout=10)
            return r.status_code == 200
        except Exception as e:
            logger.warning('[tvapp2] health check failed (%s): %s', url, e)
            return False

    # ── M3U playlist fetch ────────────────────────────────────────────────────

    def _fetch_playlist(self) -> Optional[str]:
        url = f'{self._base_url}/playlist.m3u8'
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error('[tvapp2] failed to fetch playlist from %s: %s', url, e)
            return None

    # ── M3U parsing ───────────────────────────────────────────────────────────

    def _parse_playlist(self, m3u_text: str) -> list[ChannelData]:
        """
        Parse the tvapp2 M3U playlist into ChannelData objects.

        tvapp2 rewrites channel stream URLs in the form:
            http://<tvapp2>/channel?url=<encoded_channel_page_url>

        We store the original channel page URL (decoded from the ?url= param)
        in stream_url as  tvapp2://<channel_page_url>  so resolve() can hand
        it back to tvapp2 at play time with a fresh token.
        """
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

            # Advance to the stream URL line (skip blank lines / extra tags)
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

            # Extract the original channel page URL from tvapp2's proxy URL.
            # tvapp2 serves: http://<host>/channel?url=<encoded_page_url>
            # We want the raw page URL so we can re-resolve at play time.
            channel_page_url = _extract_channel_page_url(stream_line)
            if not channel_page_url:
                # Fallback: use the raw URL as-is (e.g. direct .m3u8 links)
                channel_page_url = stream_line

            name       = attrs.get('_name') or attrs.get('tvg-name') or ''
            tvg_id     = attrs.get('tvg-id') or ''
            logo_url   = attrs.get('tvg-logo') or None
            group      = attrs.get('group-title') or None

            if not name:
                logger.debug('[tvapp2] skipping entry with no name: %s', stream_line[:80])
                continue

            # Use tvg-id as the stable channel identifier when available;
            # otherwise derive one from the channel page URL path.
            if tvg_id:
                source_channel_id = tvg_id
            else:
                # e.g. https://thetvapp.to/tv/abc-news-live/ → abc-news-live
                path = urlsplit(channel_page_url).path.strip('/')
                source_channel_id = path.split('/')[-1] if path else name.lower().replace(' ', '-')

            if source_channel_id in seen_ids:
                logger.debug('[tvapp2] duplicate channel id %s, skipping', source_channel_id)
                continue
            seen_ids.add(source_channel_id)

            language = infer_language_from_metadata(name, group)

            channels.append(ChannelData(
                source_channel_id = source_channel_id,
                name              = name,
                stream_url        = f'{_SCHEME}{channel_page_url}',
                logo_url          = logo_url,
                slug              = source_channel_id,
                category          = _normalise_group(group),
                language          = language,
                country           = 'US',
                stream_type       = 'hls',
                gracenote_id      = resolve_gracenote('tvapp2', lookup_key=source_channel_id),
                tags              = [group] if group else [],
            ))

        logger.info('[tvapp2] parsed %d channels', len(channels))
        return channels

    # ── BaseScraper interface ─────────────────────────────────────────────────

    def fetch_channels(self) -> list[ChannelData]:
        if not self._health_ok():
            logger.warning('[tvapp2] instance at %s appears unreachable; aborting scrape', self._base_url)
            return []

        m3u_text = self._fetch_playlist()
        if not m3u_text:
            return []

        return self._parse_playlist(m3u_text)

    def fetch_epg(self, channels: list[ChannelData], **kwargs) -> list[ProgramData]:
        """
        Fetch EPG from tvapp2's built-in XMLTV endpoint (GET /xmltv.xml).

        tvapp2 hosts its own guide data — the same data it used to build the
        M3U playlist — at /xmltv.xml (configurable via FILE_EPG env var in
        tvapp2, but /xmltv.xml is the default).  Channel IDs in the XMLTV file
        match the tvg-id values in the M3U, which we store as source_channel_id.
        """
        url = f'{self._base_url}/xmltv.xml'
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
            xml_text = r.text
        except Exception as e:
            logger.error('[tvapp2] failed to fetch EPG from %s: %s', url, e)
            return []

        return _parse_xmltv(xml_text)

    def resolve(self, raw_url: str) -> str:
        """
        Convert a stored tvapp2:// URL into a live tvapp2 /channel?url= URL.

        The tvapp2 /channel endpoint fetches a fresh signed token from the
        upstream source (TheTVApp / TVPass) and proxies the HLS variant
        playlist back to the caller — so every play request is properly
        authenticated without FastChannels needing to manage tokens itself.
        """
        if not raw_url.startswith(_SCHEME):
            return raw_url
        channel_page_url = raw_url[len(_SCHEME):]
        return f'{self._base_url}/channel?url={_quote(channel_page_url, safe="")}'


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_channel_page_url(tvapp2_proxy_url: str) -> Optional[str]:
    """
    Extract the original channel page URL from a tvapp2 proxy URL.

    Input:  http://192.168.1.10:4124/channel?url=https%3A%2F%2Fthetvapp.to%2Ftv%2Fabc%2F
    Output: https://thetvapp.to/tv/abc/
    """
    from urllib.parse import urlsplit, parse_qs, unquote
    try:
        parsed = urlsplit(tvapp2_proxy_url)
        qs = parse_qs(parsed.query)
        urls = qs.get('url') or qs.get('URL')
        if urls:
            return unquote(urls[0])
    except Exception:
        pass
    return None


_GROUP_MAP = {
    'sports':         'Sports',
    'sport':          'Sports',
    'news':           'News',
    'entertainment':  'Entertainment',
    'movies':         'Movies',
    'movie':          'Movies',
    'kids':           'Kids',
    'music':          'Music',
    'lifestyle':      'Lifestyle',
    'comedy':         'Comedy',
    'drama':          'Drama',
    'reality':        'Reality TV',
    'documentary':    'Documentary',
    'documentaries':  'Documentary',
    'sci-fi':         'Sci-Fi',
    'science fiction':'Sci-Fi',
    'horror':         'Horror',
    'food':           'Food',
    'travel':         'Travel',
    'nature':         'Nature',
    'history':        'History',
    'general':        'Entertainment',
}


def _normalise_group(group: Optional[str]) -> Optional[str]:
    if not group:
        return None
    key = group.lower().strip()
    return _GROUP_MAP.get(key, group.title())


# ── XMLTV EPG parsing ─────────────────────────────────────────────────────────

# XMLTV timestamps: "20240315143000 +0000" or "20240315143000 +0500"
_XMLTV_TS_RE = re.compile(r'(\d{14})\s*([+-]\d{4})?')


def _parse_xmltv_ts(value: str | None) -> Optional[datetime]:
    """Parse a XMLTV timestamp string into a UTC-aware datetime."""
    if not value:
        return None
    m = _XMLTV_TS_RE.match(value.strip())
    if not m:
        return None
    dt_str, tz_str = m.group(1), m.group(2) or '+0000'
    try:
        # Parse naive datetime then apply the offset manually.
        naive = datetime.strptime(dt_str, '%Y%m%d%H%M%S')
        sign = 1 if tz_str[0] == '+' else -1
        h, mn = int(tz_str[1:3]), int(tz_str[3:5])
        offset_secs = sign * (h * 3600 + mn * 60)
        from datetime import timedelta
        aware = naive.replace(tzinfo=timezone.utc) - timedelta(seconds=offset_secs)
        return aware
    except (ValueError, IndexError):
        return None


def _parse_xmltv(xml_text: str) -> list[ProgramData]:
    """
    Parse a standard XMLTV document into ProgramData objects.

    Handles the subset of XMLTV that tvapp2 emits:
      <programme start="..." stop="..." channel="...">
        <title>...</title>
        <desc>...</desc>
        <category>...</category>
        <icon src="..."/>
        <episode-num system="xmltv_ns">S.E.</episode-num>
        <rating><value>...</value></rating>
      </programme>
    """
    programs: list[ProgramData] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error('[tvapp2] XMLTV parse error: %s', e)
        return programs

    for prog in root.findall('programme'):
        channel_id = prog.get('channel')
        if not channel_id:
            continue

        start = _parse_xmltv_ts(prog.get('start'))
        stop  = _parse_xmltv_ts(prog.get('stop'))
        if not start or not stop:
            continue

        title_el = prog.find('title')
        title = (title_el.text or '').strip() if title_el is not None else ''
        if not title:
            continue

        desc_el = prog.find('desc')
        description = (desc_el.text or '').strip() if desc_el is not None else None

        category_el = prog.find('category')
        category = (category_el.text or '').strip() if category_el is not None else None

        icon_el = prog.find('icon')
        poster_url = icon_el.get('src') if icon_el is not None else None

        rating_el = prog.find('.//rating/value')
        rating = (rating_el.text or '').strip() if rating_el is not None else None

        # Parse episode number from xmltv_ns system: "S.E.P" (0-indexed)
        season = episode = None
        for ep_el in prog.findall('episode-num'):
            if ep_el.get('system') == 'xmltv_ns' and ep_el.text:
                parts = ep_el.text.split('.')
                try:
                    if len(parts) >= 1 and parts[0].strip():
                        season = int(parts[0].strip()) + 1
                except ValueError:
                    pass
                try:
                    if len(parts) >= 2 and parts[1].strip():
                        episode = int(parts[1].strip()) + 1
                except ValueError:
                    pass
                break

        sub_title_el = prog.find('sub-title')
        episode_title = (sub_title_el.text or '').strip() if sub_title_el is not None else None

        programs.append(ProgramData(
            source_channel_id = channel_id,
            title             = title,
            start_time        = start,
            end_time          = stop,
            description       = description or None,
            category          = category or None,
            poster_url        = poster_url or None,
            rating            = rating or None,
            season            = season,
            episode           = episode,
            episode_title     = episode_title or None,
        ))

    logger.info('[tvapp2] parsed %d EPG entries from XMLTV', len(programs))
    return programs
