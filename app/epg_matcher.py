# app/epg_matcher.py
"""
XMLTV EPG channel matcher for TVPass and HDHomeRun sources.

Builds a searchable index from a remote XMLTV file and provides:
  - auto_match()  : fuzzy name matching using token normalization
  - search()      : typeahead search for manual assignment in the UI
  - apply_match() : persists guide_key to Channel row

The XMLTV channel id is the Gracenote/TMS station ID (numeric string, e.g.
"35312").  Writing it to Channel.guide_key causes _tvg_id() in m3u.py to emit
it as tvg-id, which joins with the XMLTV EPG that TVPass / HDHomeRun point to.

Index cache is mtime-keyed on the on-disk XMLTV file and expires after
EPG_CACHE_TTL_SECONDS when the source is remote.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests

logger = logging.getLogger(__name__)

# ── Cache paths ───────────────────────────────────────────────────────────────
_EPG_CACHE_PATH = Path('/data/epg_matcher_cache.xml')
_EPG_CACHE_TTL_SECONDS = 12 * 3600  # refresh remote XMLTV every 12 h

# ── In-process index ──────────────────────────────────────────────────────────
_index_lock = threading.Lock()
_index: 'XmltvIndex | None' = None
_index_url: str = ''
_index_fetched_at: float = 0.0


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class XmltvChannel:
    """One <channel> entry from the XMLTV file."""
    xmltv_id: str                       # value of id= attribute (TMS station ID)
    display_names: list[str]            # all <display-name> values, in order
    icon_url: Optional[str] = None

    @property
    def primary_name(self) -> str:
        return self.display_names[0] if self.display_names else self.xmltv_id

    @property
    def network_name(self) -> str:
        """Second display-name is typically the network/affil name."""
        return self.display_names[1] if len(self.display_names) > 1 else self.primary_name

    @property
    def callsign(self) -> str:
        """First display-name is typically the OTA callsign."""
        return self.display_names[0] if self.display_names else ''


@dataclass
class XmltvIndex:
    """Searchable index over all channels in an XMLTV file."""
    channels: list[XmltvChannel] = field(default_factory=list)
    # token-normalised name → list of matching channels
    _by_norm: dict[str, list[XmltvChannel]] = field(default_factory=dict, repr=False)
    built_at: float = 0.0

    def _build_lookup(self) -> None:
        self._by_norm.clear()
        for ch in self.channels:
            for name in ch.display_names:
                key = _norm(name)
                self._by_norm.setdefault(key, []).append(ch)
                # also index each token independently
                for tok in _tokens(name):
                    self._by_norm.setdefault(tok, []).append(ch)

    def search(self, query: str, limit: int = 20) -> list[XmltvChannel]:
        """Return channels whose display-names best match *query*."""
        q_norm = _norm(query)
        q_toks = _tokens(query)

        scored: dict[str, tuple[float, XmltvChannel]] = {}
        candidates: list[XmltvChannel] = []

        # exact normalised match
        for ch in self._by_norm.get(q_norm, []):
            candidates.append(ch)

        # token overlap fallback
        if not candidates:
            for tok in q_toks:
                for ch in self._by_norm.get(tok, []):
                    candidates.append(ch)

        # deduplicate and score
        for ch in candidates:
            if ch.xmltv_id in scored:
                continue
            scored[ch.xmltv_id] = (_score(query, ch), ch)

        ranked = sorted(scored.values(), key=lambda t: -t[0])
        return [ch for _, ch in ranked[:limit]]

    def auto_match(self, channel_name: str) -> Optional[XmltvChannel]:
        """Return the best-scoring XMLTV channel for *channel_name*, or None."""
        results = self.search(channel_name, limit=5)
        if not results:
            return None
        best = results[0]
        if _score(channel_name, best) >= 0.6:
            return best
        return None

    def by_id(self, xmltv_id: str) -> Optional[XmltvChannel]:
        for ch in self.channels:
            if ch.xmltv_id == xmltv_id:
                return ch
        return None


# ── Text normalisation ────────────────────────────────────────────────────────

_STRIP_RE = re.compile(r'[^a-z0-9\s]')
_NOISE = frozenset({
    'hd', 'sd', 'tv', 'channel', 'the', 'network', 'broadcasting',
    'service', 'television', 'national', 'public', 'broadcasting',
    'company', 'entertainment',
})


def _norm(s: str) -> str:
    return _STRIP_RE.sub('', s.lower()).strip()


def _tokens(s: str) -> list[str]:
    return [t for t in _norm(s).split() if t not in _NOISE and len(t) > 1]


def _score(query: str, ch: XmltvChannel) -> float:
    """Simple Jaccard-like score between query tokens and channel name tokens."""
    q_toks = set(_tokens(query))
    if not q_toks:
        return 0.0

    best = 0.0
    for name in ch.display_names:
        n_toks = set(_tokens(name))
        if not n_toks:
            continue
        inter = q_toks & n_toks
        union = q_toks | n_toks
        j = len(inter) / len(union)

        # Bonus: exact normalised match on any display-name
        if _norm(query) == _norm(name):
            j = min(1.0, j + 0.4)

        # Bonus: query is a prefix of a token in the name
        for nt in n_toks:
            if nt.startswith(_norm(query)) and len(_norm(query)) >= 3:
                j = min(1.0, j + 0.2)
                break

        best = max(best, j)

    return best


# ── XMLTV parsing ─────────────────────────────────────────────────────────────

def _parse_xmltv(path: Path) -> XmltvIndex:
    """Parse an XMLTV file and return a populated XmltvIndex.

    Tolerates the double-<tv> wrapper that zap2xml produces.
    """
    channels: list[XmltvChannel] = []
    try:
        for event, elem in ET.iterparse(str(path), events=('end',)):
            if elem.tag == 'channel':
                cid = (elem.get('id') or '').strip()
                names = [dn.text.strip() for dn in elem.findall('display-name')
                         if dn.text and dn.text.strip()]
                icon_el = elem.find('icon')
                icon = icon_el.get('src') if icon_el is not None else None
                if cid and names:
                    channels.append(XmltvChannel(
                        xmltv_id=cid,
                        display_names=names,
                        icon_url=icon,
                    ))
                elem.clear()
    except ET.ParseError as exc:
        # Truncated / double-root files: log and continue with what we got
        logger.warning('[epg-matcher] XMLTV parse warning (%s); indexed %d channels so far', exc, len(channels))

    idx = XmltvIndex(channels=channels, built_at=time.time())
    idx._build_lookup()
    logger.info('[epg-matcher] indexed %d XMLTV channels', len(channels))
    return idx


# ── Remote fetch & cache ──────────────────────────────────────────────────────

def _fetch_and_cache(url: str) -> Path:
    """Download XMLTV from *url* to the cache path.  Returns the cache path."""
    logger.info('[epg-matcher] fetching XMLTV from %s', url)
    _EPG_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60, stream=True)
    r.raise_for_status()
    with _EPG_CACHE_PATH.open('wb') as fh:
        for chunk in r.iter_content(65536):
            fh.write(chunk)
    logger.info('[epg-matcher] cached XMLTV to %s (%d bytes)', _EPG_CACHE_PATH,
                _EPG_CACHE_PATH.stat().st_size)
    return _EPG_CACHE_PATH


def _cache_is_fresh() -> bool:
    if not _EPG_CACHE_PATH.exists():
        return False
    age = time.time() - _EPG_CACHE_PATH.stat().st_mtime
    return age < _EPG_CACHE_TTL_SECONDS


# ── Public API ────────────────────────────────────────────────────────────────

def get_index(epg_url: str = '') -> Optional['XmltvIndex']:
    """Return (or build) the XMLTV index for *epg_url*.

    Caches the parsed index in-process.  Re-parses when the URL changes or the
    on-disk cache is refreshed.
    """
    global _index, _index_url, _index_fetched_at

    with _index_lock:
        url_changed = epg_url and epg_url != _index_url

        if url_changed or _index is None:
            # Determine source path
            parsed = urlparse(epg_url or '')
            if parsed.scheme in ('http', 'https'):
                if url_changed or not _cache_is_fresh():
                    try:
                        _fetch_and_cache(epg_url)
                    except Exception as exc:
                        logger.error('[epg-matcher] failed to fetch XMLTV: %s', exc)
                        if not _EPG_CACHE_PATH.exists():
                            return None
                src_path = _EPG_CACHE_PATH
            elif parsed.scheme == 'file' or not parsed.scheme:
                # Local file path
                src_path = Path(epg_url.replace('file://', ''))
                if not src_path.exists():
                    logger.error('[epg-matcher] local XMLTV not found: %s', src_path)
                    return None
            else:
                logger.error('[epg-matcher] unsupported URL scheme: %s', epg_url)
                return None

            _index = _parse_xmltv(src_path)
            _index_url = epg_url or ''
            _index_fetched_at = time.time()

        return _index


def invalidate_index() -> None:
    """Force the next call to get_index() to re-parse the XMLTV."""
    global _index
    with _index_lock:
        _index = None


def run_auto_match(source_names: list[str], epg_url: str, dry_run: bool = False) -> dict:
    """Auto-match all channels from *source_names* against *epg_url*.

    Writes guide_key to Channel rows that don't already have one pinned/locked.
    Returns a summary dict with matched / skipped / failed counts.
    """
    from .models import Channel, Source
    from .extensions import db

    idx = get_index(epg_url)
    if idx is None:
        return {'error': 'Could not build XMLTV index', 'matched': 0, 'skipped': 0}

    sources = Source.query.filter(Source.name.in_(source_names)).all()
    source_ids = [s.id for s in sources]
    channels = Channel.query.filter(
        Channel.source_id.in_(source_ids),
        Channel.is_active == True,
    ).all()

    matched = skipped = already_set = 0
    results = []

    for ch in channels:
        # Don't overwrite a pinned guide_key
        if ch.guide_key and ch.guide_key.strip():
            already_set += 1
            results.append({
                'channel_id': ch.id,
                'name': ch.name,
                'status': 'already_set',
                'guide_key': ch.guide_key,
            })
            continue

        xmltv_ch = idx.auto_match(ch.name)
        if xmltv_ch:
            if not dry_run:
                ch.guide_key = xmltv_ch.xmltv_id
            matched += 1
            results.append({
                'channel_id': ch.id,
                'name': ch.name,
                'status': 'matched',
                'guide_key': xmltv_ch.xmltv_id,
                'matched_name': xmltv_ch.primary_name,
                'network': xmltv_ch.network_name,
                'icon_url': xmltv_ch.icon_url,
            })
        else:
            skipped += 1
            results.append({
                'channel_id': ch.id,
                'name': ch.name,
                'status': 'unmatched',
                'guide_key': None,
            })

    if not dry_run:
        db.session.commit()
        logger.info('[epg-matcher] auto-match: %d matched, %d skipped, %d already set',
                    matched, skipped, already_set)

    return {
        'matched': matched,
        'skipped': skipped,
        'already_set': already_set,
        'dry_run': dry_run,
        'results': results,
    }
