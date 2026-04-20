"""NMT (node-management) presence for the emulated LAN-C node.

Implements the discovery/heartbeat CAN frame class:

    can_id = 0x10000000 | (fc << 17) | node_id

    fc=0  heartbeat broadcast  0x1000_0000 | node   (~0.5 Hz, empty)
    fc=1  presence ping/reply  0x1002_0000 | node
          RTR = ping from poller, non-RTR+empty = reply from `node`
    fc=2  identity payload     0x1004_0000 | node   (not answered yet
          — emulator stays silent until the payload layout is
          confirmed from bus observations)

What the emulator has to do to stay "visible" on the bus like a real
gateway does:

  a) Broadcast its own heartbeat every ~2 s.
  b) Reply to presence-ping RTRs addressed to its node-ID.
"""

from __future__ import annotations

import asyncio
import logging
import threading

import can

from .can_router import CanRouter

log = logging.getLogger(__name__)

NMT_TOP5_MASK = 0b11111 << 24
NMT_BASE = 0x10000000
NMT_FC_SHIFT = 17
NMT_FC_MASK = 0b11 << NMT_FC_SHIFT

FC_HEARTBEAT = 0
FC_PRESENCE = 1
FC_IDENTITY = 2

DEFAULT_HEARTBEAT_INTERVAL_S = 2.0


def nmt_can_id(fc: int, node_id: int) -> int:
    return NMT_BASE | ((fc & 0x3) << NMT_FC_SHIFT) | (node_id & 0x3F)


def decode_nmt_id(can_id: int) -> tuple[int, int] | None:
    """Return (fc, node_id) for an NMT frame, or None if it isn't one."""
    if (can_id & NMT_TOP5_MASK) != NMT_BASE:
        return None
    return ((can_id >> NMT_FC_SHIFT) & 0x3, can_id & 0x3F)


class NmtPresence:
    """Heartbeat broadcaster + presence-ping responder for one node ID."""

    def __init__(self, router: CanRouter, bus: can.BusABC,
                 *, node_id: int,
                 heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S):
        self._bus = bus
        self._node = node_id & 0x3F
        self._interval = heartbeat_interval_s
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tx_lock = threading.Lock()
        self._pings_answered = 0
        self._sweep_lock = asyncio.Lock()
        # Node IDs that answered our most recent presence sweep.
        self._last_seen_nodes: set[int] = set()
        router.register(self._is_our_presence_ping, self._on_ping)
        router.register(self._is_presence_reply, self._on_presence_reply)

    def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(
            self._heartbeat_loop(), name="nmt-heartbeat",
        )
        log.info(
            "NMT presence: node=0x%02x heartbeat every %.1fs",
            self._node, self._interval,
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None

    @property
    def pings_answered(self) -> int:
        return self._pings_answered

    @property
    def node_id(self) -> int:
        return self._node

    # ── Heartbeat TX ────────────────────────────────────────────────────
    async def _heartbeat_loop(self) -> None:
        if self._interval <= 0:
            return
        while not self._stop.is_set():
            try:
                await asyncio.to_thread(self._send_heartbeat)
            except Exception:
                log.exception("NMT heartbeat send failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._interval,
                )
                return
            except asyncio.TimeoutError:
                pass

    def _send_heartbeat(self) -> None:
        msg = can.Message(
            arbitration_id=nmt_can_id(FC_HEARTBEAT, self._node),
            is_extended_id=True, is_remote_frame=False, data=b"",
        )
        with self._tx_lock:
            self._bus.send(msg)

    # ── Presence-ping RX (runs on asyncio loop via CanRouter) ───────────
    def _is_our_presence_ping(self, msg: can.Message) -> bool:
        meta = decode_nmt_id(msg.arbitration_id)
        if meta is None:
            return False
        fc, node = meta
        # Pings are RTR-form; replies are non-RTR. We only want the
        # inbound pings targeted at us.
        return (fc == FC_PRESENCE and node == self._node
                and msg.is_remote_frame)

    def _on_ping(self, msg: can.Message) -> None:
        """Send an empty non-RTR reply on the same CAN-ID."""
        reply = can.Message(
            arbitration_id=nmt_can_id(FC_PRESENCE, self._node),
            is_extended_id=True, is_remote_frame=False, data=b"",
        )
        try:
            # Sync send — we're already off the bus reader thread.
            with self._tx_lock:
                self._bus.send(reply)
            self._pings_answered += 1
            log.debug("NMT presence-ping from id=0x%08x → answered",
                      msg.arbitration_id)
        except Exception:
            log.exception("NMT presence reply send failed")

    # ── Active presence sweep ──────────────────────────────────────────
    def _is_presence_reply(self, msg: can.Message) -> bool:
        meta = decode_nmt_id(msg.arbitration_id)
        if meta is None:
            return False
        fc, node = meta
        # Non-RTR presence frames are replies (or our own echoed reply —
        # which by design self-identifies and we skip).
        return (fc == FC_PRESENCE and not msg.is_remote_frame
                and node != self._node)

    def _on_presence_reply(self, msg: can.Message) -> None:
        _, node = decode_nmt_id(msg.arbitration_id) or (0, 0)
        self._last_seen_nodes.add(node)
        log.debug("NMT presence reply from node=0x%02x", node)

    @property
    def last_seen_nodes(self) -> set[int]:
        return set(self._last_seen_nodes)

    async def sweep(self, *, node_range: tuple[int, int] = (0x01, 0x3F),
                    pacing_s: float = 0.02) -> int:
        """RTR every node in [lo, hi] to discover live peers.

        Reproduces the LAN C's per-app-connect presence scan. Replies
        land via `_on_presence_reply` and update `last_seen_nodes`.
        Only one sweep runs at a time. Returns number of RTRs sent.
        """
        lo, hi = node_range
        lo = max(0x01, lo & 0x3F)
        hi = min(0x3F, hi & 0x3F)
        async with self._sweep_lock:
            self._last_seen_nodes.clear()
            log.info("NMT presence sweep: nodes 0x%02x..0x%02x", lo, hi)
            sent = 0
            for node in range(lo, hi + 1):
                if node == self._node:
                    continue  # don't RTR ourselves
                try:
                    await asyncio.to_thread(self._send_ping, node)
                    sent += 1
                except Exception:
                    log.exception("NMT presence ping send failed node=0x%02x",
                                  node)
                if pacing_s > 0:
                    await asyncio.sleep(pacing_s)
            # Small grace period so late replies are counted before the
            # caller reads `last_seen_nodes`.
            await asyncio.sleep(0.05)
            log.info("NMT presence sweep: done (sent=%d seen=%d)",
                     sent, len(self._last_seen_nodes))
            return sent

    def _send_ping(self, node: int) -> None:
        msg = can.Message(
            arbitration_id=nmt_can_id(FC_PRESENCE, node),
            is_extended_id=True, is_remote_frame=True, data=b"",
        )
        with self._tx_lock:
            self._bus.send(msg)
