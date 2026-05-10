"""
app/ts_stitcher.py

MPEG-TS PCR/PTS/DTS timestamp stitcher for Pluto TV SSAI ad breaks.

Pluto inserts ad segments whose DTS timelines reset to near-zero at every
ad/content boundary.  Without stitching the output MPEG-TS has huge timestamp
jumps that cause downstream players (Channels DVR, Plex, Emby, etc.) to show
a black screen for the duration of the ad pod.

How it works
------------
Each TS segment is a sequence of 188-byte packets.  Three timestamp fields
carry the presentation clock:

  PCR  – Program Clock Reference, in the adaptation field.  Controls the
         decoder's clock.  Lives in the MPEG-TS layer.

  PTS  – Presentation Timestamp.  When to display the decoded sample.
         Lives in the PES header (first packet of each elementary stream unit).

  DTS  – Decoding Timestamp.  When the decoder must have the data ready.
         Present alongside PTS when they differ (e.g. B-frames).

All three are 33-bit values counting at 90 kHz (PCR base) or 27 MHz
(PCR extension, which we preserve unchanged).

Algorithm
---------
Per channel we track:
  offset   – a signed integer added to every raw timestamp before writing.
  last_dts – the last output DTS seen (after offset was applied).

On each segment:
  1. Find the first DTS in the segment (raw, before any offset).
  2. Compute effective_first = (first_dts + offset) mod 2^33.
  3. Compare effective_first to last_dts.
  4. If the absolute difference exceeds DISCONTINUITY_THRESHOLD (2 s at 90 kHz),
     an SSAI boundary has been detected.  Recalculate offset so that
     effective_first == last_dts (seamless continuation).
  5. Apply the offset to every PCR/PTS/DTS in the segment buffer.
  6. Record the last output DTS for the next iteration.

At content → ad boundary:
  - last_dts ≈ T_content  (e.g. 54 000 000 ticks into broadcast)
  - first_dts_ad ≈ 1 000  (ad segment resets to near zero)
  - new offset = T_content − 1 000 ≈ T_content
  - output DTS starts at T_content, advances through the ad pod  ✓

At ad → content boundary (live channel; content advanced during ads):
  - last_dts ≈ T_content + ad_duration   (output end of ad pod)
  - first_dts_content ≈ T_content + ad_duration  (live content caught up)
  - diff ≈ +offset (huge) → discontinuity detected
  - new offset ≈ 0
  - output DTS continues seamlessly from end of ad pod  ✓

Usage
-----
    from .ts_stitcher import stitch_segment, clear_channel_state

    processed = stitch_segment(channel_id, raw_ts_bytes)

    # Call when a channel is known to have reconnected from scratch:
    clear_channel_state(channel_id)
"""

import logging
import threading
import time
from collections import OrderedDict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PACKET_SIZE             = 188
SYNC_BYTE               = 0x47
TS_MASK                 = 0x1FFFFFFFF     # 33-bit wrap mask
_HALF_TS                = TS_MASK // 2    # for signed diff calculation
DISCONTINUITY_THRESHOLD = 180_000         # 2 seconds at 90 kHz
_MAX_CHANNELS           = 256             # LRU eviction ceiling


# ---------------------------------------------------------------------------
# Per-channel state
# ---------------------------------------------------------------------------

class _State:
    __slots__ = ('offset', 'last_dts', 'last_used', 'lock')

    def __init__(self):
        self.offset    = 0
        self.last_dts  = None       # type: int | None   — output domain
        self.last_used = time.monotonic()
        self.lock      = threading.Lock()


_registry: OrderedDict[str, _State] = OrderedDict()
_registry_lock = threading.Lock()


def _get_state(channel_id: str) -> _State:
    with _registry_lock:
        if channel_id in _registry:
            _registry.move_to_end(channel_id)
            st = _registry[channel_id]
            st.last_used = time.monotonic()
            return st
        if len(_registry) >= _MAX_CHANNELS:
            _registry.popitem(last=False)
        st = _State()
        _registry[channel_id] = st
        return st


def clear_channel_state(channel_id: str) -> None:
    """Reset stitcher state for *channel_id* (call on reconnect / source switch)."""
    with _registry_lock:
        _registry.pop(channel_id, None)


# ---------------------------------------------------------------------------
# Bit-level read/write helpers
# ---------------------------------------------------------------------------

def _read_ts(buf: bytes | bytearray, off: int) -> int:
    """Read a 33-bit PTS/DTS value from the 5-byte field at *off*.

    Encoding (ISO 13818-1 §2.4.3.7):
      byte[0]  bits 3:1 = ts[32:30],  bit 0 = marker '1'
      byte[1]  bits 7:0 = ts[29:22]
      byte[2]  bits 7:1 = ts[21:15],  bit 0 = marker '1'
      byte[3]  bits 7:0 = ts[14:7]
      byte[4]  bits 7:1 = ts[6:0],    bit 0 = marker '1'
    """
    return (((buf[off]   & 0x0E) << 29) |
             (buf[off+1]          << 22) |
            ((buf[off+2] & 0xFE) << 14) |
             (buf[off+3]          <<  7) |
            ((buf[off+4] & 0xFE) >>  1))


def _write_ts(buf: bytearray, off: int, value: int) -> None:
    """Write *value* (33-bit) into the PTS/DTS field at *off*, preserving prefix nibble."""
    v = value & TS_MASK
    prefix     = buf[off] & 0xF0          # '0010', '0011', or '0001' prefix nibble
    buf[off]   = prefix | ((v >> 29) & 0x0E) | 0x01
    buf[off+1] = (v >> 22) & 0xFF
    buf[off+2] = ((v >> 14) & 0xFE) | 0x01
    buf[off+3] = (v >> 7)  & 0xFF
    buf[off+4] = ((v << 1) & 0xFE) | 0x01


def _read_pcr_base(buf: bytes | bytearray, off: int) -> int:
    """Read the 33-bit PCR base from the 6-byte PCR field at *off*.

    PCR layout (ISO 13818-1 §2.4.3.5):
      byte[0]         = base[32:25]
      byte[1]         = base[24:17]
      byte[2]         = base[16:9]
      byte[3]         = base[8:1]
      byte[4] bit 7   = base[0]
      byte[4] bits 6:1 = reserved (all 1)
      byte[4] bit 0   = ext[8]
      byte[5]         = ext[7:0]
    """
    return ((buf[off]   << 25) |
            (buf[off+1] << 17) |
            (buf[off+2] <<  9) |
            (buf[off+3] <<  1) |
            (buf[off+4] >>  7))


def _write_pcr_base(buf: bytearray, off: int, base: int) -> None:
    """Write *base* (33-bit) into the PCR base field, preserving the extension."""
    b = base & TS_MASK
    buf[off]   = (b >> 25) & 0xFF
    buf[off+1] = (b >> 17) & 0xFF
    buf[off+2] = (b >>  9) & 0xFF
    buf[off+3] = (b >>  1) & 0xFF
    # bit7 = base[0]; bits6:1 = reserved (1); bit0 = ext[8] (preserve)
    buf[off+4] = ((b & 0x01) << 7) | 0x7E | (buf[off+4] & 0x01)
    # buf[off+5]: PCR extension low byte — unchanged


# ---------------------------------------------------------------------------
# Segment scanning
# ---------------------------------------------------------------------------

def _find_dts(buf: bytes | bytearray, *, last: bool = False) -> int | None:
    """
    Return the first (or last if *last*) DTS value found in *buf*.
    Falls back to PTS when a packet carries only PTS (pts_dts_flags == 2).
    Returns None if no timestamped PES packet is found.
    """
    result = None
    n = len(buf)

    # Locate sync byte (handles small preambles / misalignment)
    i = 0
    while i < n and buf[i] != SYNC_BYTE:
        i += 1

    while i + PACKET_SIZE <= n:
        if buf[i] != SYNC_BYTE:
            i += 1
            continue
        try:
            adapt_ctrl = (buf[i + 3] >> 4) & 0x3
            pos = i + 4

            if adapt_ctrl in (2, 3):         # adaptation field present
                adapt_len = buf[pos]
                pos += 1 + adapt_len

            if adapt_ctrl in (1, 3) and (buf[i + 1] >> 6) & 1:   # PUSI set
                if (pos + 9 <= i + PACKET_SIZE
                        and buf[pos] == 0x00
                        and buf[pos+1] == 0x00
                        and buf[pos+2] == 0x01):
                    flags = (buf[pos + 7] >> 6) & 0x3
                    if flags == 3 and pos + 19 <= i + PACKET_SIZE:
                        # DTS present (bytes 14–18 of PES header)
                        val = _read_ts(buf, pos + 14)
                        if not last:
                            return val
                        result = val
                    elif flags == 2 and pos + 14 <= i + PACKET_SIZE:
                        # PTS only — use as proxy for DTS
                        val = _read_ts(buf, pos + 9)
                        if not last:
                            return val
                        result = val
        except (IndexError, ValueError):
            pass
        i += PACKET_SIZE

    return result


def _apply_offset(buf: bytearray, offset: int) -> None:
    """Rewrite every PCR/PTS/DTS in the 188-byte-packet stream *buf* by adding *offset*."""
    if not offset:
        return

    n = len(buf)
    i = 0
    while i < n and buf[i] != SYNC_BYTE:
        i += 1

    while i + PACKET_SIZE <= n:
        if buf[i] != SYNC_BYTE:
            i += 1
            continue
        try:
            adapt_ctrl = (buf[i + 3] >> 4) & 0x3
            pos = i + 4

            # ---- Adaptation field: PCR and OPCR --------------------------------
            if adapt_ctrl in (2, 3):
                adapt_len = buf[pos]
                if adapt_len > 0:
                    flags = buf[pos + 1]
                    if flags & 0x10 and pos + 8 <= i + PACKET_SIZE:   # PCR
                        _write_pcr_base(buf, pos + 2,
                                        _read_pcr_base(buf, pos + 2) + offset)
                    if flags & 0x08 and pos + 14 <= i + PACKET_SIZE:  # OPCR
                        _write_pcr_base(buf, pos + 8,
                                        _read_pcr_base(buf, pos + 8) + offset)
                pos += 1 + adapt_len

            # ---- PES header: PTS and DTS ----------------------------------------
            if adapt_ctrl in (1, 3) and (buf[i + 1] >> 6) & 1:  # PUSI
                if (pos + 9 <= i + PACKET_SIZE
                        and buf[pos] == 0x00
                        and buf[pos+1] == 0x00
                        and buf[pos+2] == 0x01):
                    flags = (buf[pos + 7] >> 6) & 0x3
                    if flags >= 2 and pos + 14 <= i + PACKET_SIZE:    # PTS
                        _write_ts(buf, pos + 9,
                                  _read_ts(buf, pos + 9) + offset)
                    if flags == 3 and pos + 19 <= i + PACKET_SIZE:    # DTS
                        _write_ts(buf, pos + 14,
                                  _read_ts(buf, pos + 14) + offset)
        except (IndexError, ValueError):
            pass
        i += PACKET_SIZE


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def stitch_segment(channel_id: str, data: bytes) -> bytes:
    """
    Rewrite all PCR/PTS/DTS timestamps in *data* so the segment is
    continuous with the previous segment for *channel_id*.

    Returns a new bytes object with modified timestamps, or *data* unchanged
    on error or if no offset adjustment is needed.
    """
    if not channel_id:
        return data

    state = _get_state(channel_id)

    with state.lock:
        try:
            first_dts = _find_dts(data)
            if first_dts is None:
                return data

            # ------------------------------------------------------------------
            # Discontinuity detection
            # ------------------------------------------------------------------
            if state.last_dts is not None:
                effective_first = (first_dts + state.offset) & TS_MASK

                # Compute signed difference (handles 33-bit wrap)
                diff = effective_first - state.last_dts
                if diff > _HALF_TS:
                    diff -= (TS_MASK + 1)
                elif diff < -_HALF_TS:
                    diff += (TS_MASK + 1)

                if abs(diff) > DISCONTINUITY_THRESHOLD:
                    old_offset = state.offset
                    # Slide segment to start exactly where previous ended
                    state.offset = (state.last_dts - first_dts) & TS_MASK
                    logger.debug(
                        '[ts-stitcher] ch=%s SSAI boundary: '
                        'first_dts=%d last_dts=%d diff=%d '
                        'offset %d → %d',
                        channel_id, first_dts, state.last_dts,
                        diff, old_offset, state.offset,
                    )

            # ------------------------------------------------------------------
            # Apply offset
            # ------------------------------------------------------------------
            if state.offset == 0:
                # No-op path: skip copy, just record last DTS
                last_raw = _find_dts(data, last=True)
                if last_raw is not None:
                    state.last_dts = last_raw & TS_MASK
                return data

            buf = bytearray(data)
            _apply_offset(buf, state.offset)
            result = bytes(buf)

            last_raw = _find_dts(result, last=True)
            if last_raw is not None:
                state.last_dts = last_raw & TS_MASK

            return result

        except Exception as exc:
            logger.warning(
                '[ts-stitcher] ch=%s processing error — passing through unchanged: %s',
                channel_id, exc,
            )
            return data
