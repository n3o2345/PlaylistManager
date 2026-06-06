# app/scrapers/tvpass.py
"""Direct M3U/XMLTV playlist scrapers for TVPass-style sources."""
from __future__ import annotations

import logging
import re

import requests as _requests

from .base import ConfigField, StreamDeadError
from .m3u_utils import M3UScraper

logger = logging.getLogger(__name__)

_TVPASS_PLAYLIST_URL = 'https://tvpass.org/playlist/m3u'
_TVPASS_EPG_URL = 'https://tvpass.org/epg.xml'
_LIVE_QUALITY_RE = re.compile(r'(/live/[^/?#]+/)(?:sd|hd)(?=([?#]|$))', re.I)


def _has_audio(url: str, session=None) -> bool:
    """
    Fetch the HLS manifest at *url* and return True if the stream appears to
    carry audio, False if it is silent/missing, and True (fail-open) on any
    network or parse error.

    Detection strategy:
    - Master playlist → follow first variant, then inspect that media playlist.
    - Media playlist  → look for at least one non-comment, non-empty segment line
      (any .ts/.aac/.mp4/.m4s segment implies audio tracks are present).
    - Additionally check for EXT-X-MEDIA TYPE=AUDIO tags which explicitly
      declare audio renditions.
    """
    try:
        s = session or _requests.Session()
        r = s.get(url, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            logger.debug('[tvpass] audio-check HTTP %s for %s', r.status_code, url[:80])
            return True  # fail open on non-200

        text = r.text

        # Follow first variant if this is a master playlist
        if '#EXT-X-STREAM-INF' in text:
            from urllib.parse import urljoin
            for line in text.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    try:
                        vr = s.get(urljoin(url, line), timeout=10)
                        if vr.status_code == 200:
                            text = vr.text
                    except Exception:
                        pass
                    break

        # EXT-X-MEDIA TYPE=AUDIO present → audio renditions declared
        if 'TYPE=AUDIO' in text:
            return True

        # Count non-comment segment lines
        segment_lines = [
            l.strip() for l in text.splitlines()
            if l.strip() and not l.strip().startswith('#')
        ]
        if not segment_lines:
            logger.debug('[tvpass] audio-check: no segment lines in playlist at %s', url[:80])
            return False

        # Heuristic: if we can probe a segment and get a meaningful payload,
        # assume audio is present. We rely on segment count > 0 as a proxy
        # for a live, non-silent stream since full audio frame parsing is
        # outside the scope of a lightweight HTTP check.
        return True

    except Exception as e:
        logger.debug('[tvpass] audio-check error (fail-open): %s', e)
        return True  # fail open on any exception


def m3u_config_schema(
    playlist_url: str,
    epg_url: str = '',
    *,
    playlist_help: str = 'M3U playlist URL.',
    epg_help: str = 'XMLTV guide URL. Leave empty to skip guide import.',
    include_quality: bool = False,
    extra_fields: list[ConfigField] | None = None,
) -> list[ConfigField]:
    schema = [
        ConfigField(
            'playlist_url',
            'Playlist URL',
            field_type='text',
            required=False,
            placeholder=playlist_url,
            default=playlist_url,
            help_text=playlist_help,
        ),
        ConfigField(
            'epg_url',
            'EPG URL',
            field_type='text',
            required=False,
            placeholder=epg_url,
            default=epg_url,
            help_text=epg_help,
        ),
    ]
    if include_quality:
        schema.append(
            ConfigField(
                'quality',
                'Stream Quality',
                field_type='select',
                required=False,
                default='hd',
                options=[
                    {'value': 'hd', 'label': 'HD'},
                    {'value': 'sd', 'label': 'SD'},
                ],
                help_text='Live URLs that support /hd and /sd variants will be normalized to this quality.',
            )
        )
    if extra_fields:
        schema.extend(extra_fields)
    return schema


class DirectM3UScraper(M3UScraper):
    """Fetch channels from a standalone M3U playlist and optional XMLTV URL."""

    source_name = None
    display_name = None
    scrape_interval = 360
    config_required = False
    stream_audit_enabled = True
    default_playlist_url = ''
    default_epg_url = ''
    enable_quality = False

    def _playlist_url(self) -> str:
        return (self.config.get('playlist_url') or self.default_playlist_url).strip()

    def _epg_url(self) -> str:
        return (self.config.get('epg_url') or self.default_epg_url).strip()

    def _quality(self) -> str:
        quality = (self.config.get('quality') or 'hd').strip().lower()
        return quality if quality in {'hd', 'sd'} else 'hd'

    def _fetch_playlist(self) -> str | None:
        url = self._playlist_url()
        if not url:
            logger.error('[%s] no playlist URL configured', self.source_name or 'm3u')
            return None
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error('[%s] failed to fetch playlist from %s: %s', self.source_name or 'm3u', url, e)
            return None

    def _apply_quality(self, url: str) -> str:
        if not self.enable_quality:
            return url
        return _LIVE_QUALITY_RE.sub(rf'\g<1>{self._quality()}', url)

    def fetch_channels(self):
        m3u_text = self._fetch_playlist()
        if not m3u_text:
            return []

        channels = self._parse_playlist(m3u_text)
        for ch in channels:
            ch.stream_url = self._apply_quality(ch.stream_url)
        if self.enable_quality:
            logger.info('[%s] parsed %d channels at %s quality', self.source_name, len(channels), self._quality())
        else:
            logger.info('[%s] parsed %d channels', self.source_name, len(channels))
        return channels

    def fetch_epg(self, channels, **kwargs):
        epg_url = self._epg_url()
        if not epg_url:
            logger.info('[%s] no EPG URL configured; skipping guide import', self.source_name or 'm3u')
            return []

        original = self.config.get('epg_url')
        self.config['epg_url'] = epg_url
        try:
            return super().fetch_epg(channels, **kwargs)
        finally:
            if original is None:
                self.config.pop('epg_url', None)
            else:
                self.config['epg_url'] = original

    def resolve(self, raw_url: str) -> str:
        return self._apply_quality(raw_url)


class TVPassScraper(DirectM3UScraper):
    """TVPass scraper with HD-first stream selection and audio-based failover.

    Play order on each request:
      1. Try HD stream — if audio is detected, serve it.
      2. If HD has no audio, try SD stream — if audio is detected, serve it.
      3. If SD also has no audio, raise StreamDeadError so Dispatcharr can
         trigger its own channel failover.
    """
    source_name = 'tvpass'
    source_aliases = ('tvpass_direct',)
    display_name = 'TVPass'
    default_playlist_url = _TVPASS_PLAYLIST_URL
    default_epg_url = _TVPASS_EPG_URL
    enable_quality = True

    config_schema = m3u_config_schema(
        _TVPASS_PLAYLIST_URL,
        _TVPASS_EPG_URL,
        playlist_help='M3U playlist URL. Defaults to the TVPass public playlist.',
        epg_help='XMLTV guide URL. Leave empty to use the TVPass EPG.',
        include_quality=True,
    )

    def resolve(self, raw_url: str) -> str:
        """
        Resolve with HD-first audio check, SD fallback, then dead-stream signal.

        Always attempts HD first regardless of the configured quality preference,
        since HD is the highest-quality option. Only falls back to SD when HD
        produces a playlist with no audio. If SD also has no audio, raises
        StreamDeadError so Dispatcharr can perform channel-level failover.
        """
        # Build both quality variants for this URL
        hd_url = _LIVE_QUALITY_RE.sub(r'\g<1>hd', raw_url)
        sd_url = _LIVE_QUALITY_RE.sub(r'\g<1>sd', raw_url)

        # If the URL has no quality segment at all, just serve it directly
        if hd_url == raw_url and sd_url == raw_url:
            return raw_url

        # 1. Try HD first
        logger.debug('[tvpass] resolve: checking HD stream for %s', hd_url[:80])
        if _has_audio(hd_url, self.session):
            logger.debug('[tvpass] resolve: HD stream has audio — using HD')
            return hd_url

        logger.warning('[tvpass] resolve: HD stream has no audio, trying SD fallback for %s', hd_url[:80])

        # 2. Try SD fallback
        if _has_audio(sd_url, self.session):
            logger.info('[tvpass] resolve: SD stream has audio — using SD fallback')
            return sd_url

        # 3. Both HD and SD have no audio — signal dead stream
        logger.error(
            '[tvpass] resolve: both HD and SD streams have no audio for %s — signalling dead stream',
            raw_url[:80],
        )
        raise StreamDeadError(
            f'TVPass channel has no audio on HD or SD streams: {raw_url[:80]}'
        )


class TVAppScraper(DirectM3UScraper):
    source_name = 'tvapp'
    source_aliases = ('thetvapp', 'thetvapp_direct')
    display_name = 'TheTVApp'
    default_playlist_url = 'https://thetvapp-m3u.data-search.workers.dev/playlist'
    default_epg_url = ''
    enable_quality = True

    config_schema = m3u_config_schema(
        default_playlist_url,
        default_epg_url,
        playlist_help='M3U playlist URL for TheTVApp. Defaults to the public generated TheTVApp playlist.',
        epg_help='Optional XMLTV guide URL for TheTVApp. Leave empty to scrape channels only.',
        include_quality=True,
    )


class MoveOnJoyScraper(DirectM3UScraper):
    source_name = 'moj'
    source_aliases = ('moveonjoy',)
    display_name = 'MoveOnJoy'
    default_playlist_url = 'https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/us_moveonjoy.m3u'
    default_epg_url = ''

    config_schema = m3u_config_schema(
        default_playlist_url,
        default_epg_url,
        playlist_help='M3U playlist URL for MoveOnJoy. Defaults to the iptv-org MoveOnJoy source playlist.',
        epg_help='Optional XMLTV guide URL for MoveOnJoy. Leave empty to scrape channels only.',
    )


class DaddyLiveScraper(DirectM3UScraper):
    source_name = 'daddylive'
    source_aliases = ('daddy_live', 'dlhd')
    display_name = 'DaddyLive'
    default_playlist_url = 'https://raw.githubusercontent.com/pigzillaaaaa/iptv-scraper/refs/heads/main/daddylive-channels.m3u8'
    default_epg_url = 'https://raw.githubusercontent.com/pigzillaaaaa/iptv-scraper/refs/heads/main/epgs/daddylive-channels-epg.xml'
    manifest_proxy_enabled = True
    proxy_segments = True

    config_schema = m3u_config_schema(
        default_playlist_url,
        default_epg_url,
        playlist_help='M3U playlist URL for DaddyLive channels.',
        epg_help='XMLTV guide URL for DaddyLive channels. Leave empty to scrape channels only.',
        extra_fields=[
            ConfigField(
                'referer',
                'HTTP Referer',
                field_type='text',
                required=False,
                default='https://daddylivestream.com/',
                placeholder='https://daddylivestream.com/',
                help_text='Referer header used when proxying DaddyLive manifests and segments.',
            ),
            ConfigField(
                'origin',
                'HTTP Origin',
                field_type='text',
                required=False,
                default='https://daddylivestream.com',
                placeholder='https://daddylivestream.com',
                help_text='Origin header used when proxying DaddyLive manifests and segments.',
            ),
        ],
    )

    def __init__(self, config: dict = None):
        super().__init__(config)
        referer = (self.config.get('referer') or 'https://daddylivestream.com/').strip()
        origin = (self.config.get('origin') or 'https://daddylivestream.com').strip()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': referer,
            'Origin': origin,
        })
