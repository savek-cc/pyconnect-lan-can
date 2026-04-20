"""RMI passthrough over python-can.

Wraps a `can.BusABC` (in practice `Ecb1Bus`) with the CAN-level RMI
framing:

    can_id = 0x1f000000
           | (seq & 0x3)   << 17
           | is_request    << 16
           | err           << 15
           | is_multi      << 14
           | (dst_node)    << 6
           | src_node

Payloads ≤ 8 bytes ride a single frame. Larger payloads are split into
7-byte chunks, each prefixed with `chunk_counter | (0x80 if last)`.

Frames are delivered by a shared `CanRouter` (one reader per bus). The
`request()` coroutine serialises in-flight requests with a lock — the
app issues RMIs sequentially, so a single outstanding request is
sufficient for v1.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import can

from .can_router import CanRouter

log = logging.getLogger(__name__)

RMI_TOP5 = 0b11111 << 24
RMI_BASE = 0x1F000000


class RmiFragmentLoss(Exception):
    """Raised when a multi-frame RMI response has gaps in its counter.

    A bridge can occasionally drop CAN frames on the bridge→host TCP
    path under burst load. Silently concatenating the surviving
    fragments produces a byte-truncated payload that decodes but whose
    internal structure is corrupt; the app detects this at the
    property-table level and aborts the session. Raising this
    exception lets `_do_rmi` map the failure to NOT_REACHABLE so the
    app retries the individual RMI instead of giving up entirely.
    """

    def __init__(self, *, received: int, expected: int,
                 missing: tuple[int, ...]):
        self.received = received
        self.expected = expected
        self.missing = missing
        super().__init__(
            f"got {received}/{expected} fragments, missing={list(missing)}"
        )

# "Host application" node ID: values in 0x30–0x3E are safe for callers
# that are not real bus devices. We pick 0x3E so an emulator can sit
# alongside a real LAN-C on the same bus without an RMI src-node
# collision. NMT heartbeat still announces the LAN-C node because the
# app looks for the gateway there — see emulator.py --nmt-node.
DEFAULT_HOST_NODE = 0x3E
RESPONSE_TIMEOUT_S = 3.0
FRAGMENT_LOSS_RETRIES = 2


def encode_rmi_id(seq: int, is_request: int, err: int, is_multi: int,
                  dst_node: int, src_node: int) -> int:
    return (
        RMI_BASE
        | ((seq & 0x3) << 17)
        | ((is_request & 0x1) << 16)
        | ((err & 0x1) << 15)
        | ((is_multi & 0x1) << 14)
        | ((dst_node & 0x3F) << 6)
        | (src_node & 0x3F)
    )


def decode_rmi_id(can_id: int
                  ) -> Optional[tuple[int, int, int, int, int, int]]:
    """Return (seq, is_req, err, is_multi, dst, src) or None if not RMI."""
    if (can_id & RMI_TOP5) != RMI_BASE:
        return None
    return (
        (can_id >> 17) & 0x3,
        (can_id >> 16) & 0x1,
        (can_id >> 15) & 0x1,
        (can_id >> 14) & 0x1,
        (can_id >> 6) & 0x3F,
        can_id & 0x3F,
    )


class RmiTransport:
    """Single-in-flight RMI client bound to one CAN bus.

    Call `start()` after the owning `CanRouter` has been registered on
    the asyncio loop where `request(...)` will run.
    """

    def __init__(self, bus: can.BusABC, router: CanRouter,
                 *, src_node: int = DEFAULT_HOST_NODE):
        self._bus = bus
        self._src = src_node & 0x3F
        self._seq = 0
        self._lock = asyncio.Lock()
        self._frame_q: asyncio.Queue[can.Message] = asyncio.Queue(maxsize=1024)
        router.register(self._is_rmi, self._on_rx)

    def start(self) -> None:
        # Kept for API symmetry with the old interface — no work to do
        # since the router-owned thread is already running.
        log.info("RMI transport ready src_node=0x%02x", self._src)

    def close(self) -> None:
        # Router lifecycle is owned by the caller.
        pass

    # ── Router callbacks (run on asyncio loop) ──────────────────────────
    @staticmethod
    def _is_rmi(msg: can.Message) -> bool:
        return decode_rmi_id(msg.arbitration_id) is not None

    def _on_rx(self, msg: can.Message) -> None:
        try:
            self._frame_q.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("RMI RX queue full; dropping id=0x%08x",
                        msg.arbitration_id)

    # ── Public request API ──────────────────────────────────────────────
    async def request(self, dst_node: int, payload: bytes,
                      timeout: float = RESPONSE_TIMEOUT_S
                      ) -> tuple[bool, bytes]:
        """Send an RMI request and wait for the matching response.

        Returns `(is_err, response_bytes)` where `is_err` reflects the
        `err` bit on the response CAN-ID. Raises `asyncio.TimeoutError`
        if no response arrives within `timeout`.
        """
        async with self._lock:
            # Retry locally on fragment loss so the caller (and the app
            # one hop up) never has to cope with it. A bridge may drop
            # a small fraction of CAN frames on long multi-frame
            # responses; a fresh request almost always succeeds on
            # retry. We do NOT retry on TimeoutError — that signals a
            # genuinely unresponsive CAN node, not a bridge hiccup,
            # and the caller wants to know about it quickly.
            for attempt in range(FRAGMENT_LOSS_RETRIES + 1):
                while not self._frame_q.empty():
                    self._frame_q.get_nowait()
                seq = self._seq
                self._seq = (self._seq + 1) & 0x3
                await self._send(seq, dst_node, payload)
                try:
                    return await asyncio.wait_for(
                        self._collect(seq, dst_node), timeout=timeout,
                    )
                except RmiFragmentLoss as e:
                    if attempt >= FRAGMENT_LOSS_RETRIES:
                        raise
                    log.info(
                        "RMI fragment loss (dst=0x%02x attempt=%d/%d): %s "
                        "— retrying",
                        dst_node, attempt + 1,
                        FRAGMENT_LOSS_RETRIES + 1, e,
                    )
            # Unreachable: the loop either returns or re-raises.
            raise AssertionError("unreachable")

    # ── TX ──────────────────────────────────────────────────────────────
    async def _send(self, seq: int, dst_node: int, payload: bytes) -> None:
        if len(payload) == 0:
            raise ValueError("RMI payload must be non-empty")
        if len(payload) <= 8:
            cid = encode_rmi_id(seq, 1, 0, 0, dst_node, self._src)
            msg = can.Message(
                arbitration_id=cid, is_extended_id=True,
                is_remote_frame=False, data=payload,
            )
            await asyncio.to_thread(self._bus.send, msg)
            return
        cid = encode_rmi_id(seq, 1, 0, 1, dst_node, self._src)
        chunks = [payload[i:i + 7] for i in range(0, len(payload), 7)]
        for i, chunk in enumerate(chunks):
            header = (i & 0x7F) | (0x80 if i == len(chunks) - 1 else 0)
            data = bytes([header]) + chunk
            msg = can.Message(
                arbitration_id=cid, is_extended_id=True,
                is_remote_frame=False, data=data,
            )
            await asyncio.to_thread(self._bus.send, msg)

    # ── RX collection ───────────────────────────────────────────────────
    async def _collect(self, seq: int, dst_node: int
                       ) -> tuple[bool, bytes]:
        # Multi-frame responses carry a 7-bit counter in the first byte
        # of each frame (low 7 bits), with the high bit set on the final
        # fragment. We index chunks by counter so that (a) dropped
        # fragments are detectable (gap in the counter sequence) rather
        # than silently concatenated and (b) we can surface RmiFragmentLoss
        # to the caller who can then return NOT_REACHABLE to the app
        # instead of a truncated payload that would fail higher-level
        # parsing. Fragment loss can happen under burst load.
        by_counter: dict[int, bytes] = {}
        last_counter: int | None = None
        while True:
            msg = await self._frame_q.get()
            meta = decode_rmi_id(msg.arbitration_id)
            if meta is None:
                continue
            m_seq, is_req, err, is_multi, dst, src = meta
            # Responses addressed to us from our target, replying to the
            # same seq. The HRU doesn't enforce seq matching, but since
            # we serialise requests a mismatch means stray traffic.
            if is_req != 0 or dst != self._src or src != dst_node:
                continue
            if m_seq != seq:
                log.debug("RMI stray frame seq=%d expected=%d", m_seq, seq)
                continue
            data = bytes(msg.data)
            if is_multi == 0:
                return (err == 1, data)
            if not data:
                log.warning("RMI multi-frame with empty data, ignoring")
                continue
            header = data[0]
            counter = header & 0x7F
            is_last = bool(header & 0x80)
            if counter in by_counter:
                log.warning(
                    "RMI duplicate fragment counter=%d, replacing",
                    counter,
                )
            by_counter[counter] = data[1:]
            if is_last:
                last_counter = counter
            if last_counter is not None:
                expected = set(range(last_counter + 1))
                missing = expected - by_counter.keys()
                if missing:
                    log.warning(
                        "RMI fragment loss seq=%d dst=0x%02x "
                        "got=%d/%d missing=%s",
                        seq, dst_node, len(by_counter),
                        last_counter + 1, sorted(missing),
                    )
                    raise RmiFragmentLoss(
                        received=len(by_counter),
                        expected=last_counter + 1,
                        missing=tuple(sorted(missing)),
                    )
                ordered = b"".join(by_counter[i]
                                   for i in range(last_counter + 1))
                return (err == 1, ordered)
