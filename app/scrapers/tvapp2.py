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

import gzip
import io
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlsplit, parse_qs, unquote as _unquote
from xml.etree import ElementTree as ET

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_BASE_URL   = 'http://127.0.0.1:4124'
_ATTR_RE    = re.compile(r'(\S+)="([^"]*)"')
_XMLTV_TS_RE = re.compile(r'^(?P<date>\d{14}|\d{12}|\d{8})(?:\s*(?P<tz>Z|[+-]\d{2}:?\d{2}))?')


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


def _extract_tvg_prefix(tvg_id: str) -> str | None:
    if not tvg_id or '.' not in tvg_id:
        return None
    prefix = tvg_id.split('.', 1)[0].strip()
    return prefix or None


def _parse_xmltv_time(value: str | None) -> datetime | None:
    if not value:
        return None
    match = _XMLTV_TS_RE.match(value.strip())
    if not match:
        return None

    raw = match.group('date')
    fmt = {
        8: '%Y%m%d',
        12: '%Y%m%d%H%M',
        14: '%Y%m%d%H%M%S',
    }.get(len(raw))
    if not fmt:
        return None

    try:
        dt = datetime.strptime(raw, fmt)
    except ValueError:
        return None

    tz = match.group('tz')
    if tz:
        if tz == 'Z':
            return dt.replace(tzinfo=timezone.utc)
        if len(tz) == 5 and tz[3] != ':':
            tz = f'{tz[:3]}:{tz[3:]}'
        try:
            offset = datetime.strptime(tz, '%z').tzinfo
            return dt.replace(tzinfo=offset).astimezone(timezone.utc)
        except ValueError:
            pass

    return dt.replace(tzinfo=timezone.utc)


def _xmltv_channel_aliases(channel_el: ET.Element) -> set[str]:
    aliases: set[str] = set()
    for key in (
        'id',
        'tvg-id',
        'tvgid',
        'tvg_id',
        'xmltv-id',
        'xmltv_id',
        'channel-id',
        'channel_id',
    ):
        value = (channel_el.get(key) or '').strip()
        if value:
            aliases.add(value)

    for child in channel_el:
        tag = child.tag.rsplit('}', 1)[-1].lower()
        if tag not in {'display-name', 'display_name', 'name'}:
            continue
        value = (child.text or '').strip()
        if value:
            aliases.add(value)
    return aliases


def _parse_xmltv_content(content: bytes) -> ET.Element:
    try:
        return ET.fromstring(content)
    except ET.ParseError:
        text = content.decode('utf-8-sig', errors='replace')
        repaired_lines = []
        seen_root = False
        removed_roots = 0
        for line in text.splitlines(keepends=True):
            if re.match(r'^\s*<tv[^>]*>\s*$', line):
                if seen_root:
                    removed_roots += 1
                    continue
                seen_root = True
            repaired_lines.append(line)
        if not seen_root or not removed_roots:
            raise
        return ET.fromstring(''.join(repaired_lines).encode('utf-8'))


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
    config_schema = [
        ConfigField(
            'epg_url',
            'EPG URL',
            field_type='text',
            required=False,
            placeholder='https://example.com/xmltv.xml.gz',
            help_text='Optional XMLTV URL for tvapp2 guide data. Supports .xml and .xml.gz feeds.',
        ),
    ]

    def __init__(self, config: dict = None):
        super().__init__(config)
        # Always the embedded instance — ignore any legacy config
        self._base_url = _BASE_URL
        self._use_url_epg = bool((self.config.get('epg_url') or '').strip())

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
            ) if not self._use_url_epg else None

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
                guide_key         = tvg_id or base_id,
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
        epg_url = (self.config.get('epg_url') or '').strip()
        if not epg_url:
            # EPG is sourced from Gracenote via the gracenote_id on each channel.
            return []

        try:
            root = self._fetch_xmltv(epg_url)
        except Exception as exc:
            logger.warning('[tvapp2] failed to fetch URL EPG from %s: %s', epg_url, exc)
            return []

        channel_targets: dict[str, str] = {}
        for ch in channels:
            source_channel_id = ch.source_channel_id
            guide_key = (getattr(ch, 'guide_key', None) or '').strip()
            candidates = {
                source_channel_id,
                f'tvapp2.{source_channel_id}',
                ch.name,
            }
            if guide_key:
                candidates.add(guide_key)
                prefix = _extract_tvg_prefix(guide_key)
                if prefix:
                    candidates.add(prefix)
                if source_channel_id.startswith(f'{guide_key}.'):
                    candidates.add(guide_key)
            for candidate in candidates:
                if candidate:
                    channel_targets[candidate.casefold()] = source_channel_id

        xml_channel_targets: dict[str, str] = {}
        for channel_el in root.iter('channel'):
            xml_channel_id = (channel_el.get('id') or '').strip()
            if not xml_channel_id:
                continue
            for alias in _xmltv_channel_aliases(channel_el):
                source_channel_id = channel_targets.get(alias.casefold())
                if source_channel_id:
                    xml_channel_targets[xml_channel_id.casefold()] = source_channel_id
                    break

        programs: list[ProgramData] = []
        for prog in root.iter('programme'):
            xml_channel = (prog.get('channel') or '').strip()
            source_channel_id = (
                channel_targets.get(xml_channel.casefold())
                or xml_channel_targets.get(xml_channel.casefold())
            )
            if not source_channel_id:
                continue

            start = _parse_xmltv_time(prog.get('start'))
            stop = _parse_xmltv_time(prog.get('stop'))
            if not start or not stop:
                continue

            title = (prog.findtext('title') or '').strip() or 'Unknown'
            desc = (prog.findtext('desc') or '').strip() or None
            category = (prog.findtext('category') or '').strip() or None
            rating = (prog.findtext('rating/value') or '').strip() or None
            episode_title = (prog.findtext('sub-title') or '').strip() or None
            icon_el = prog.find('icon')
            poster_url = icon_el.get('src') if icon_el is not None else None

            programs.append(ProgramData(
                source_channel_id = source_channel_id,
                title             = title,
                start_time        = start,
                end_time          = stop,
                description       = desc,
                poster_url        = poster_url,
                category          = category,
                rating            = rating,
                episode_title     = episode_title,
            ))

        logger.info('[tvapp2] parsed %d URL EPG programs from %s', len(programs), epg_url)
        return programs

    def _fetch_xmltv(self, url: str) -> ET.Element:
        r = self.session.get(url, timeout=60, headers={'Accept-Encoding': 'gzip'})
        r.raise_for_status()
        content = r.content
        if url.lower().split('?', 1)[0].endswith('.gz') or content[:2] == b'\x1f\x8b':
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                content = gz.read()
        return _parse_xmltv_content(content)

    def resolve(self, raw_url: str) -> str:
        # raw_url is the unwrapped upstream URL stored at scrape time.
        # play.py's tvapp2_manifest_proxy wraps it in /channel?url= and
        # proxies it server-side — this method is not called in the proxy path
        # but must return a valid URL so play() routes correctly.
        return raw_url
