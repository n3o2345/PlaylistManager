# app/scrapers/tvpass.py
"""
Native TVPass scraper.

This bypasses the embedded tvapp2 Node proxy entirely.  Channels are imported
from TVPass' public M3U playlist and playback URLs are kept as stable
https://tvpass.org/live/<channel>/<quality> URLs so clients can resolve the
current CDN redirect themselves.
"""
from __future__ import annotations

import logging
import re

from .base import ConfigField
from .tvapp2 import TVApp2Scraper

logger = logging.getLogger(__name__)

_DEFAULT_PLAYLIST_URL = 'https://tvpass.org/playlist.m3u'
_DEFAULT_EPG_URL = 'https://tvpass.org/epg.xml'
_LIVE_QUALITY_RE = re.compile(r'(/live/[^/?#]+/)(?:sd|hd)(?=([?#]|$))', re.I)


class TVPassScraper(TVApp2Scraper):
    source_name = 'tvpass'
    source_aliases = ('thetvapp', 'thetvapp_direct', 'tvpass_direct')
    display_name = 'TVPass Direct'
    scrape_interval = 360
    config_required = False
    stream_audit_enabled = True

    config_schema = [
        ConfigField(
            'playlist_url',
            'Playlist URL',
            field_type='text',
            required=False,
            placeholder=_DEFAULT_PLAYLIST_URL,
            default=_DEFAULT_PLAYLIST_URL,
            help_text='M3U playlist URL. Defaults to TVPass public playlist.',
        ),
        ConfigField(
            'epg_url',
            'EPG URL',
            field_type='text',
            required=False,
            placeholder=_DEFAULT_EPG_URL,
            default=_DEFAULT_EPG_URL,
            help_text='XMLTV guide URL. Leave empty to use the TVPass EPG.',
        ),
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
            help_text='TVPass live URLs support /hd and /sd variants. HD is the default.',
        ),
    ]

    def _playlist_url(self) -> str:
        return (self.config.get('playlist_url') or _DEFAULT_PLAYLIST_URL).strip()

    def _epg_url(self) -> str:
        return (self.config.get('epg_url') or _DEFAULT_EPG_URL).strip()

    def _quality(self) -> str:
        quality = (self.config.get('quality') or 'hd').strip().lower()
        return quality if quality in {'hd', 'sd'} else 'hd'

    def _fetch_playlist(self) -> str | None:
        url = self._playlist_url()
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error('[tvpass] failed to fetch playlist from %s: %s', url, e)
            return None

    def _apply_quality(self, url: str) -> str:
        quality = self._quality()
        return _LIVE_QUALITY_RE.sub(rf'\g<1>{quality}', url)

    def fetch_channels(self):
        m3u_text = self._fetch_playlist()
        if not m3u_text:
            return []

        channels = self._parse_playlist(m3u_text)
        for ch in channels:
            ch.stream_url = self._apply_quality(ch.stream_url)
        logger.info('[tvpass] parsed %d channels at %s quality', len(channels), self._quality())
        return channels

    def fetch_epg(self, channels, **kwargs):
        epg_url = self._epg_url()
        if not epg_url:
            logger.info('[tvpass] no EPG URL configured; skipping guide import')
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
        # Return the stable TVPass URL directly; /play will issue a plain 302.
        return self._apply_quality(raw_url)
