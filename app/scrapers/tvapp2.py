# app/scrapers/tvapp2.py
"""Shared M3U and XMLTV helpers for direct playlist sources.

NOTE: The tvapp2 source (local proxy at 127.0.0.1:4124) is no longer supported.
This module is retained as a utility base for M3U/XMLTV parsing used by other scrapers.
"""
from __future__ import annotations

import gzip
import io
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from xml.etree import ElementTree as ET

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata

logger = logging.getLogger(__name__)


_ATTR_RE    = re.compile(r'(\S+)="([^"]*)"')
_XMLTV_TS_RE = re.compile(r'^(?P<date>\d{14}|\d{12}|\d{8})(?:\s*(?P<tz>Z|[+-]\d{2}:?\d{2}))?')
_FORBIDDEN_GUIDE_LABEL_RE = re.compile(r'\((?:tvapp|tvpass|moj)\)', re.I)


# ── M3U helpers ───────────────────────────────────────────────────────────────

def _parse_extinf(line: str) -> dict:
    attrs = {}
    for key, val in _ATTR_RE.findall(line):
        attrs[key.lower()] = val
    comma = line.rfind(',')
    if comma != -1:
        attrs['_name'] = line[comma + 1:].strip()
    return attrs



def _extract_tvguide_id(value: str) -> str | None:
    """
    Extract the numeric TVGuide/Gracenote station ID from M3U tvg-id values.

    Handles numeric IDs like "10179.206ESPN" and generated PlaylistManager
    channel IDs like "sourcename.10179.206ESPN.sourcename.sports".
    Also handles legacy tvapp2-prefixed IDs for backward compatibility.
    """
    if not value:
        return None
    parts = [part.strip() for part in value.split('.') if part.strip()]
    # Strip known source-name prefixes (tvapp2 kept for backward compat)
    if parts and parts[0].casefold() in ('tvapp2', 'tvpass', 'tvapp'):
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


def _has_forbidden_guide_label(value: str | None) -> bool:
    return bool(_FORBIDDEN_GUIDE_LABEL_RE.search(value or ''))


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


def xmltv_epg_channel_candidates(config: dict | None, scraper_cls=None) -> list[dict]:
    """Return guide candidates directly from a configured XMLTV URL."""
    scraper_cls = scraper_cls or TVApp2Scraper
    scraper = scraper_cls(config or {})
    epg_getter = getattr(scraper, '_epg_url', None)
    epg_url = (epg_getter() if callable(epg_getter) else scraper.config.get('epg_url') or '').strip()
    if not epg_url:
        return []
    root = scraper._fetch_xmltv(epg_url)
    candidates: list[dict] = []
    for channel_el in root.iter('channel'):
        xml_channel_id = (channel_el.get('id') or '').strip()
        if not xml_channel_id:
            continue
        aliases = sorted(
            alias for alias in _xmltv_channel_aliases(channel_el)
            if alias and not _has_forbidden_guide_label(alias)
        )
        if _has_forbidden_guide_label(xml_channel_id):
            continue
        if not aliases:
            aliases = [xml_channel_id]
        display_name = next((alias for alias in aliases if alias != xml_channel_id), aliases[0])
        candidates.append({
            'guide_key': xml_channel_id,
            'name': display_name,
            'aliases': aliases,
        })
    return candidates


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
    """Base class providing M3U and XMLTV parsing utilities for direct playlist sources.

    The original tvapp2 local-proxy source (127.0.0.1:4124) has been removed.
    This class now serves only as a shared parser base for TVPassScraper, HDHomeRunScraper,
    and other direct-M3U sources.
    """

    source_name          = None
    display_name         = 'M3U/XMLTV Parser Base'
    scrape_interval      = 360
    config_required      = False
    stream_audit_enabled = True
    phase_timeouts       = {
        'init':      30,
        'bootstrap': 60,
        'channels':  120,
        'epg':       900,
    }

    config_schema = [
        ConfigField(
            'epg_url',
            'EPG URL',
            field_type='text',
            required=False,
            placeholder='https://example.com/xmltv.xml.gz',
            help_text='XMLTV URL for guide data. Supports .xml and .xml.gz feeds.',
        ),
    ]

    def _log_name(self) -> str:
        return self.source_name or 'm3u'

    # ── playlist ──────────────────────────────────────────────────────────────

    def _fetch_playlist(self) -> Optional[str]:
        """Subclasses must override to supply M3U text."""
        raise NotImplementedError('_fetch_playlist must be implemented by subclass')

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
                stream_url        = stream_line,
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

        logger.info('[%s] parsed %d channels', self._log_name(), len(channels))
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
            logger.info('[%s] no EPG URL configured; skipping guide import', self._log_name())
            return []

        skip_ids = set(kwargs.get('skip_ids') or [])
        enabled_ids = set(kwargs.get('enabled_ids') or [])
        active_channels = [
            ch for ch in channels
            if ch.source_channel_id not in skip_ids
            and (not enabled_ids or ch.source_channel_id in enabled_ids)
        ]
        total_channels = len(active_channels)
        if self._progress_cb:
            self._progress_cb('epg', 0, max(total_channels, 1))
        if not active_channels:
            logger.info('[%s] all configured EPG channels are fresh; skipping URL EPG import', self._log_name())
            if self._progress_cb:
                self._progress_cb('epg', 1, 1)
            return []

        try:
            root = self._fetch_xmltv(epg_url)
        except Exception as exc:
            logger.warning('[%s] failed to fetch URL EPG from %s: %s', self._log_name(), epg_url, exc)
            return []

        channel_targets: dict[str, str] = {}
        normalized_channel_targets: dict[str, str] = {}
        for ch in active_channels:
            source_channel_id = ch.source_channel_id
            guide_key = (getattr(ch, 'guide_key', None) or '').strip()
            candidates = {
                source_channel_id,
                f'{self._log_name()}.{source_channel_id}',
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
            tvguide_id = _extract_tvguide_id(f'{self._log_name()}.{source_channel_id}')
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
                if _has_forbidden_guide_label(alias):
                    continue
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
        matched_sids: set[str] = set()
        seen_programs = 0
        for prog in root.iter('programme'):
            seen_programs += 1
            xml_channel = (prog.get('channel') or '').strip()
            source_channel_id = (
                channel_targets.get(xml_channel.casefold())
                or xml_channel_targets.get(xml_channel.casefold())
                or normalized_channel_targets.get(_xmltv_match_key(xml_channel))
                or normalized_xml_channel_targets.get(_xmltv_match_key(xml_channel))
            )
            if not source_channel_id:
                if self._progress_cb and seen_programs % 1000 == 0:
                    self._progress_cb('epg', len(matched_sids), max(total_channels, 1))
                continue
            if source_channel_id not in matched_sids:
                matched_sids.add(source_channel_id)
                if self._progress_cb:
                    self._progress_cb('epg', len(matched_sids), max(total_channels, 1))

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

        if self._progress_cb:
            self._progress_cb('epg', total_channels, max(total_channels, 1))
        logger.info('[%s] parsed %d URL EPG programs from %s', self._log_name(), len(programs), epg_url)
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
        # Default passthrough — subclasses override to apply quality variants etc.
        return raw_url
