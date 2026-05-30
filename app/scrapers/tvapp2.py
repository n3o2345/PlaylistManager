# app/scrapers/tvapp2.py
"""
tvapp2 scraper for PlaylistManager.

tvapp2 is embedded in the same Docker image and always runs on
http://127.0.0.1:4124. Configure an XMLTV URL to import guide data.

Channels are pulled from /playlist, stream URLs are unwrapped from tvapp2's
own /channel?url= wrapper so PlaylistManager stores the raw upstream URL.
PlaylistManager re-wraps at play time and proxies everything server-side.

EPG comes only from the configured XMLTV URL. tvapp2 tvg-ids often include
numeric Gracenote/TVGuide station IDs, and those IDs are kept as guide lookup
keys for URL EPG matching instead of being stored as built-in Gracenote guide
IDs.

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


def _extract_tvguide_id(value: str) -> str | None:
    """
    Extract the numeric TVGuide/Gracenote station ID from tvapp2 identifiers.

    Handles both raw tvapp2 tvg-id values like "10179.206ESPN" and generated
    PlaylistManager ids like "tvapp2.10179.206ESPN.tvapp2.sports".
    """
    if not value:
        return None
    parts = [part.strip() for part in value.split('.') if part.strip()]
    if parts and parts[0].casefold() == 'tvapp2':
        parts = parts[1:]
    for part in parts:
        if part.isdigit() and len(part) >= 4:
            return part
    return None


def _extract_tvg_prefix(tvg_id: str) -> str | None:
    if not tvg_id or '.' not in tvg_id:
        return None
    prefix = tvg_id.split('.', 1)[0].strip()
    return prefix or None


def _source_base_id(source_channel_id: str) -> str | None:
    if not source_channel_id or '.' not in source_channel_id:
        return None
    base_id = source_channel_id.rsplit('.', 1)[0].strip()
    return base_id or None


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


def _xmltv_match_key(value: str | None) -> str:
    raw = (value or '').strip()
    raw = re.sub(r'\([^)]*\)', ' ', raw)
    raw = re.sub(r'[^a-zA-Z0-9]+', ' ', raw).casefold()
    words = [
        part for part in raw.split()
        if part not in {'channel', 'tv', 'network', 'hd', 'uhd', '4k', 'live', 'tvapp', 'tvpass'}
        and not part.isdigit()
    ]
    compact = ''.join(words)
    if compact.endswith('tvhd'):
        compact = compact[:-4]
    elif compact.endswith('hd'):
        compact = compact[:-2]
    elif compact.endswith('tv'):
        compact = compact[:-2]
    return compact


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

    config_schema = [
        ConfigField(
            'epg_url',
            'EPG URL',
            field_type='text',
            required=False,
            placeholder='https://example.com/xmltv.xml.gz',
            help_text='XMLTV URL for tvapp2 guide data. Supports .xml and .xml.gz feeds; when empty, tvapp2 imports channels without guide programs.',
        ),
    ]

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
                gracenote_id      = None,
                guide_key         = _extract_tvguide_id(tvg_id) or tvg_id or base_id,
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
            logger.info('[tvapp2] no EPG URL configured; skipping guide import')
            return []

        try:
            root = self._fetch_xmltv(epg_url)
        except Exception as exc:
            logger.warning('[tvapp2] failed to fetch URL EPG from %s: %s', epg_url, exc)
            return []

        channel_targets: dict[str, str] = {}
        normalized_channel_targets: dict[str, str] = {}
        for ch in channels:
            source_channel_id = ch.source_channel_id
            guide_key = (getattr(ch, 'guide_key', None) or '').strip()
            candidates = {
                source_channel_id,
                f'tvapp2.{source_channel_id}',
                ch.name,
            }
            base_id = _source_base_id(source_channel_id)
            if base_id:
                candidates.add(base_id)
                tvguide_id = _extract_tvguide_id(base_id)
                if tvguide_id:
                    candidates.add(tvguide_id)
            tvguide_id = _extract_tvguide_id(source_channel_id)
            if tvguide_id:
                candidates.add(tvguide_id)
            tvguide_id = _extract_tvguide_id(f'tvapp2.{source_channel_id}')
            if tvguide_id:
                candidates.add(tvguide_id)
            if guide_key:
                candidates.add(guide_key)
                tvguide_id = _extract_tvguide_id(guide_key)
                if tvguide_id:
                    candidates.add(tvguide_id)
                prefix = _extract_tvg_prefix(guide_key)
                if prefix:
                    candidates.add(prefix)
                if source_channel_id.startswith(f'{guide_key}.'):
                    candidates.add(guide_key)
            for candidate in candidates:
                if candidate:
                    channel_targets[candidate.casefold()] = source_channel_id
                    normalized = _xmltv_match_key(candidate)
                    if normalized:
                        normalized_channel_targets.setdefault(normalized, source_channel_id)

        xml_channel_targets: dict[str, str] = {}
        normalized_xml_channel_targets: dict[str, str] = {}
        for channel_el in root.iter('channel'):
            xml_channel_id = (channel_el.get('id') or '').strip()
            if not xml_channel_id:
                continue
            for alias in _xmltv_channel_aliases(channel_el):
                source_channel_id = channel_targets.get(alias.casefold())
                if not source_channel_id:
                    source_channel_id = normalized_channel_targets.get(_xmltv_match_key(alias))
                if source_channel_id:
                    xml_channel_targets[xml_channel_id.casefold()] = source_channel_id
                    normalized = _xmltv_match_key(xml_channel_id)
                    if normalized:
                        normalized_xml_channel_targets[normalized] = source_channel_id
                    break

        programs: list[ProgramData] = []
        for prog in root.iter('programme'):
            xml_channel = (prog.get('channel') or '').strip()
            source_channel_id = (
                channel_targets.get(xml_channel.casefold())
                or xml_channel_targets.get(xml_channel.casefold())
                or normalized_channel_targets.get(_xmltv_match_key(xml_channel))
                or normalized_xml_channel_targets.get(_xmltv_match_key(xml_channel))
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
