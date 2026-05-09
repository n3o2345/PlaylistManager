# app/scrapers/tvapp2.py
"""
tvapp2 scraper for FastChannels.

tvapp2 (TheBinaryNinja/tvapp2) is a self-hosted Node.js application that
periodically downloads pre-built M3U playlists and XMLTV EPG data from
TheTVApp, TVPass, and MoveOnJoy, then serves them locally.  It exposes:

  GET http://<host>:<port>/playlist
      Full IPTV M3U playlist.  Stream URLs are direct playable CDN URLs.
      tvapp2 does NOT proxy streams — URLs are refreshed on its own sync
      schedule (default: every 3 days, configurable via TASK_CRON_SYNC).

  GET http://<host>:<port>/epg
      XMLTV EPG guide data (uncompressed XML).

  GET http://<host>:<port>/gzip
      XMLTV EPG guide data (gzip-compressed — faster to fetch).

Config (set via the FastChannels admin UI):
  host        tvapp2 host (default: localhost)
  port        tvapp2 port (default: 4124)
  api_key     Optional API key if tvapp2 is configured with API_KEY env var
  base_url    Full base URL override, e.g. http://192.168.1.50:4124
              (takes precedence over host+port when set)

stream_url is stored as the raw CDN URL from the playlist.
resolve() returns it unchanged — tvapp2 handles its own token lifecycle.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlsplit

from .base import BaseScraper, ChannelData, ConfigField, ProgramData, infer_language_from_metadata
from ..gracenote_map import resolve_gracenote

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 4124

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
    scrape_interval = 360          # re-sync every 6h; tvapp2 refreshes on its own cron (default 3 days)
    config_required = True
    stream_audit_enabled = False   # CDN URLs are time-limited; static audit will false-positive

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

    # ── M3U playlist fetch ────────────────────────────────────────────────────

    def _fetch_playlist(self) -> Optional[str]:
        url = f'{self._base_url}/playlist'
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

        tvapp2 provides multiple entries per channel — one per upstream source
        (TVPass, TheTVApp, MoveOnJoy, etc.) — differentiated by group-title.
        Each entry becomes a separate FastChannels channel so all backup streams
        are available.  source_channel_id is made unique by combining tvg-id
        with the group slug so duplicates across sources are preserved.
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

            # Build a unique channel ID per source group so backup streams
            # aren't collapsed.  e.g. tvg-id "111871.571ACC" from group
            # "TVPass" → "111871.571ACC.tvpass", from "TheTVApp" → "111871.571ACC.thetvapp"
            base_id = tvg_id or name.lower().replace(' ', '-')
            group_slug = (group or '').lower().replace(' ', '').replace('/', '')
            source_channel_id = f'{base_id}.{group_slug}' if group_slug else base_id

            # If still colliding (same channel, same group twice), append a counter.
            if source_channel_id in seen_ids:
                n = 2
                while f'{source_channel_id}.{n}' in seen_ids:
                    n += 1
                source_channel_id = f'{source_channel_id}.{n}'
            seen_ids.add(source_channel_id)

            # tvapp2 tvg-ids are formatted "<gracenote_station_id>.<call_letters>"
            # e.g. "111871.571ACC" → station id "111871" is a valid Gracenote TMS ID.
            # Extract it directly so no CSV map entry is needed.
            gracenote_id = resolve_gracenote(
                'tvapp2',
                upstream_id = _extract_gracenote_id(tvg_id),
                lookup_key  = base_id,
            )

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
        """
        Fetch EPG from tvapp2's /epg endpoint (uncompressed XMLTV).

        The XMLTV uses bare tvg-ids as channel identifiers (e.g. "111871.571ACC"),
        but our source_channel_ids are suffixed per group (e.g. "111871.571ACC.tvpass",
        "111871.571ACC.thetvapp").  We build a mapping from bare id → all suffixed ids
        and fan out each program entry to every variant so all backup streams get EPG.
        """
        url = f'{self._base_url}/epg'
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
            xml_text = r.text
        except Exception as e:
            logger.error('[tvapp2] failed to fetch EPG from %s: %s', url, e)
            return []

        # Map bare tvg-id → list of suffixed source_channel_ids for this scrape.
        # e.g. "111871.571ACC" → ["111871.571ACC.tvpass", "111871.571ACC.thetvapp"]
        from collections import defaultdict
        bare_to_suffixed: dict[str, list[str]] = defaultdict(list)
        for ch in channels:
            # The base_id is everything before the first group-slug dot suffix.
            # We reconstruct it by stripping the group_slug we appended in _parse_playlist.
            # Simpler: look for the channel's tvg-id stored in its source_channel_id prefix.
            # Since source_channel_id = f"{base_id}.{group_slug}", and base_id may itself
            # contain dots (e.g. "111871.571ACC"), we stored base_id via gracenote lookup key.
            # Easiest reconstruction: find the longest prefix of source_channel_id that
            # matches a known bare_id from the XMLTV — but we don't have those yet.
            # Instead: any channel whose source_channel_id starts with a bare_id is a variant.
            # We'll build the reverse map after parsing, below.
            bare_to_suffixed[ch.source_channel_id].append(ch.source_channel_id)

        raw_programs = _parse_xmltv(xml_text)

        # Build bare_id → [suffixed_ids] from the channels list.
        # source_channel_id format: "{base_id}.{group_slug}" where base_id is the tvg-id.
        # We match by checking if a channel's source_channel_id starts with bare_id + '.'.
        bare_ids = {prog.source_channel_id for prog in raw_programs}
        id_map: dict[str, list[str]] = defaultdict(list)
        for ch in channels:
            cid = ch.source_channel_id
            # Find which bare_id this channel belongs to.
            for bare in bare_ids:
                if cid == bare or cid.startswith(bare + '.'):
                    id_map[bare].append(cid)
                    break
            else:
                # No match — include as-is so it at least tries to link.
                id_map[cid].append(cid)

        # Fan out: one ProgramData per suffixed channel id.
        programs: list[ProgramData] = []
        for prog in raw_programs:
            targets = id_map.get(prog.source_channel_id)
            if not targets:
                continue
            for target_id in targets:
                programs.append(ProgramData(
                    source_channel_id = target_id,
                    title             = prog.title,
                    start_time        = prog.start_time,
                    end_time          = prog.end_time,
                    description       = prog.description,
                    category          = prog.category,
                    poster_url        = prog.poster_url,
                    rating            = prog.rating,
                    season            = prog.season,
                    episode           = prog.episode,
                    episode_title     = prog.episode_title,
                ))

        logger.info('[tvapp2] fanned out %d EPG entries across %d channel variants', len(programs), len(channels))
        return programs

    def resolve(self, raw_url: str) -> str:
        """
        Return the CDN stream URL as-is.

        tvapp2 serves pre-fetched direct CDN stream URLs in /playlist.
        The client hits the CDN directly — no proxy or token reconstruction needed.
        FastChannels just 302-redirects to whatever URL tvapp2 stored.
        """
        return raw_url

    def _inject_cdn_headers(self, url: str) -> None:
        """
        Inject Origin/Referer headers into the scraper session appropriate for
        the CDN serving this tvapp2 stream URL.  Called by the manifest proxy
        path so _fetch_manifest() sends the right headers when fetching the
        HLS master playlist from TheTVApp, TVPass, or MoveOnJoy CDNs.
        """
        u = url.lower()
        if 'thetvapp' in u:
            self.session.headers.update({
                'Origin': 'https://thetvapp.to',
                'Referer': 'https://thetvapp.to/',
            })
        elif 'tvpass' in u:
            self.session.headers.update({
                'Origin': 'https://tvpass.org',
                'Referer': 'https://tvpass.org/',
            })
        elif 'moveonjoy' in u:
            self.session.headers.update({
                'Origin': 'https://moveonjoy.com',
                'Referer': 'https://moveonjoy.com/',
            })


# ── Helpers ───────────────────────────────────────────────────────────────────


def _extract_gracenote_id(tvg_id: str) -> str | None:
    """
    Extract a Gracenote station ID from a tvapp2 tvg-id.

    tvapp2 uses tvg-ids in the format "<station_id>.<call_letters>",
    e.g. "111871.571ACC" or "10035.181AETV".  The numeric prefix is a
    Gracenote TMS station ID (always 5+ digits).  Returning it as
    upstream_id lets resolve_gracenote() use it directly without a CSV map.
    """
    if not tvg_id:
        return None
    prefix = tvg_id.split('.')[0]
    if prefix.isdigit() and len(prefix) >= 5:
        return prefix
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
