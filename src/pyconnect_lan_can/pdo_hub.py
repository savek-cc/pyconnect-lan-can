"""PDO broadcast fan-out and mirror-sweep warm-up.

Subscribes to PDO-class CAN frames via a `CanRouter` and routes every
matching broadcast to per-pdid callback lists. Sessions register with
`subscribe(pdid, callback)` when they receive `CnRpdoRequest` and
`unsubscribe_all(session_tag)` on session teardown.

PDO CAN-ID encoding:

    can_id = (pdo_id << 14) | 0x40 | src_node

Top-5 bits are zero — that's what distinguishes PDO frames from RMI
(top-5 = 0b11111) and NMT (top-5 = 0b10000).

Also provides:

- `rtr_pdid()` — sends an RTR on a pdid's CAN-ID to prompt the
  producer (the HRU, usually node 1) to re-publish the current value.
  A real gateway does this per-CnRpdoRequest so the zero-placeholder
  notification is quickly followed by the real value.
- `sweep()` — RTRs every pdid we have ever seen a subscription for.
  Reproduces the gateway's per-connect warm-up behaviour: without it,
  the app's first subscription burst has to wait for on-change events.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Callable

import can

from .can_router import CanRouter

# HRU CAN node ID — the ComfoAir Q is node 1 on every captured bus.
DEFAULT_PRODUCER_NODE = 0x01

log = logging.getLogger(__name__)

PDO_TOP5_MASK = 0b11111 << 24  # any of the upper 5 bits set → not PDO

# CnRpdoRequest.type → zero-placeholder byte count:
#
#     0  CN_BOOL      → 1 B
#     1  CN_UINT8     → 1 B
#     2  CN_UINT16    → 2 B
#     3  CN_UINT32    → 4 B
#     5  CN_INT8      → 1 B
#     6  CN_INT16     → 2 B
#     8  CN_INT64     → 8 B
#     10 CN_TIME      → 4 B (seconds-since-2000, LE-uint32)
#     11 CN_VERSION   → 4 B (packed state/major/minor/patch)
#     9  CN_STRING    → variable; no zero placeholder
#
# Types 4 and 7 are reserved-unused. Observed app sessions only
# exercise {0, 1, 2, 3, 6, 8}; the rest are covered for completeness.
PDO_TYPE_SIZE: dict[int, int] = {
    0: 1,   # CN_BOOL
    1: 1,   # CN_UINT8
    2: 2,   # CN_UINT16
    3: 4,   # CN_UINT32
    5: 1,   # CN_INT8
    6: 2,   # CN_INT16
    8: 8,   # CN_INT64
    10: 4,  # CN_TIME
    11: 4,  # CN_VERSION
}

# App-observed CnRpdoRequest set — the ~90 unique pdids the app issues
# in a single connect + control-action session. Used as the default
# sweep seed so the HRU's publish cache is warmed for exactly the pdids
# the app asks about, even on the emulator's very first session.
APP_OBSERVED_SUBSCRIPTIONS: tuple[tuple[int, int], ...] = (
    (16, 1), (18, 1), (33, 1), (37, 1), (42, 1), (49, 1),
    (53, 1), (54, 1), (55, 1), (56, 1), (57, 1), (58, 1),
    (65, 1), (66, 1), (67, 1), (70, 1), (71, 1), (73, 1), (74, 1),
    (81, 3), (82, 3), (85, 3), (86, 3), (87, 3), (89, 3), (90, 3),
    (117, 1), (118, 1), (119, 2), (120, 2), (121, 2), (122, 2),
    (128, 2), (129, 2), (130, 2),
    (144, 2), (145, 2), (146, 2),
    (176, 1), (192, 2),
    (208, 1), (209, 6), (210, 0), (211, 0), (212, 6),
    (213, 2), (214, 2), (215, 2), (216, 2), (217, 2), (218, 2), (219, 2),
    (220, 6), (221, 6),
    (224, 1), (225, 1), (226, 2), (227, 1), (228, 1), (230, 8),
    (274, 6), (275, 6), (278, 6),
    (290, 1), (291, 1), (292, 1), (294, 1),
    (321, 2), (325, 2), (330, 2),
    (337, 3), (338, 3), (341, 3), (342, 3), (343, 3), (345, 3), (346, 3),
    (369, 1), (370, 1), (371, 1), (372, 1),
    (384, 6), (386, 0),
    (400, 6), (401, 1), (402, 0),
    (416, 6), (417, 6), (418, 1), (419, 0),
    (784, 1), (802, 6),
)


PdoHandler = Callable[[int, bytes], None]  # (pdid, data) → void


def pdo_can_id(pdid: int, producer_node: int = DEFAULT_PRODUCER_NODE) -> int:
    """Compose the producer's CAN-ID for a pdid."""
    return (pdid << 14) | 0x40 | (producer_node & 0x3F)


class PdoHub:
    """Route PDO broadcasts from the CAN bus to interested sessions."""

    def __init__(self, router: CanRouter, bus: can.BusABC,
                 *, producer_node: int = DEFAULT_PRODUCER_NODE):
        self._bus = bus
        self._producer = producer_node & 0x3F
        self._subs: dict[int, list[tuple[object, PdoHandler]]] = defaultdict(list)
        # Persistent "subscription table" analog. pdid → most-recently-
        # seen rpdo_type. Feeds `sweep()` so reconnects can warm the HRU
        # cache without re-observing every CnRpdoRequest first.
        self._known_subs: dict[int, int] = {}
        self._sweep_lock = asyncio.Lock()
        router.register(self._is_pdo, self._on_rx)

    @staticmethod
    def _is_pdo(msg: can.Message) -> bool:
        # PDO frames have top-5 bits = 0. Remote frames are requests from
        # consumers — we don't need them here (we're also a consumer).
        if msg.is_remote_frame:
            return False
        if msg.is_error_frame:
            return False
        return (msg.arbitration_id & PDO_TOP5_MASK) == 0

    def _on_rx(self, msg: can.Message) -> None:
        pdid = (msg.arbitration_id >> 14) & 0x7FFF
        subs = self._subs.get(pdid)
        if not subs:
            return
        data = bytes(msg.data)
        # Iterate a copy — callbacks may unsubscribe themselves.
        for _tag, cb in list(subs):
            try:
                cb(pdid, data)
            except Exception:
                log.exception("PDO subscriber for pdid=%d raised", pdid)

    # ── Subscription API ────────────────────────────────────────────────
    def subscribe(self, tag: object, pdid: int, cb: PdoHandler,
                  *, rpdo_type: int = 1) -> None:
        self._subs[pdid].append((tag, cb))
        # Learn this pdid so future sweeps include it even when the
        # session that asked has gone.
        self._known_subs[pdid] = rpdo_type

    def unsubscribe_all(self, tag: object) -> None:
        for pdid in list(self._subs):
            kept = [(t, c) for t, c in self._subs[pdid] if t is not tag]
            if kept:
                self._subs[pdid] = kept
            else:
                del self._subs[pdid]

    # ── Mirror-sweep warm-up ────────────────────────────────────────────
    def seed_known_subs(self, entries: list[tuple[int, int]] |
                        tuple[tuple[int, int], ...]) -> None:
        """Preload the sweep table with (pdid, rpdo_type) pairs."""
        for pdid, rtype in entries:
            self._known_subs.setdefault(pdid, rtype)

    def known_pdids(self) -> list[int]:
        return sorted(self._known_subs)

    async def rtr_pdid(self, pdid: int) -> None:
        """Ask the producer to (re)publish this pdid's current value."""
        msg = can.Message(
            arbitration_id=pdo_can_id(pdid, self._producer),
            is_extended_id=True, is_remote_frame=True, data=b"",
        )
        try:
            await asyncio.to_thread(self._bus.send, msg)
        except Exception:
            log.exception("RTR send failed for pdid=%d", pdid)

    async def sweep(self, *, pacing_s: float = 0.02) -> int:
        """RTR every known pdid to warm the HRU publish cache.

        Mirrors the per-connect sweep a real gateway runs. Only one
        sweep runs at a time; concurrent callers wait. Returns the
        number of RTRs sent.

        Pacing matches the observed tens-of-ms burst. We briefly ran
        at 100 ms while chasing silent CAN RX loss on the bridge; root
        cause was the ESP-IDF TWAI driver's default ~32-slot rx_queue
        overrunning under burst load. Fixed by setting
        `rx_queue_len: 256` in the bridge's canbus YAML.
        """
        async with self._sweep_lock:
            pdids = self.known_pdids()
            if not pdids:
                return 0
            log.info("PDO mirror-sweep: RTRing %d pdids", len(pdids))
            sent = 0
            for pdid in pdids:
                await self.rtr_pdid(pdid)
                sent += 1
                if pacing_s > 0:
                    await asyncio.sleep(pacing_s)
            log.info("PDO mirror-sweep: done (%d RTRs)", sent)
            return sent


def payload_size_for_type(rpdo_type: int) -> int:
    """Zero-placeholder size for a CnRpdoRequest.type value.

    Falls back to 1 for unknown types — the app tolerates extra or
    short bytes gracefully.
    """
    return PDO_TYPE_SIZE.get(rpdo_type, 1)
