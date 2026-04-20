"""Single-reader CAN dispatcher.

python-can buses have one `recv()` consumer — if two threads race on it
each only sees half the bus. `CanRouter` owns the sole reader thread
and dispatches each received frame to every matching subscriber on the
asyncio loop thread.

Subscribers register a predicate (fast sync check against the raw
`can.Message`) plus a handler (also sync, invoked via
`call_soon_threadsafe`). Handlers should do the minimum amount of work
and hand off to their own asyncio-native state (queues, events).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Callable

import can

log = logging.getLogger(__name__)

Predicate = Callable[[can.Message], bool]
Handler = Callable[[can.Message], None]


class CanRouter:
    def __init__(self, bus: can.BusABC, recv_timeout: float = 0.2):
        self._bus = bus
        self._recv_timeout = recv_timeout
        self._subs: list[tuple[Predicate, Handler]] = []
        self._stop = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def register(self, predicate: Predicate, handler: Handler) -> None:
        self._subs.append((predicate, handler))

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("CanRouter already started")
        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(
            target=self._rx_loop, name="can-router", daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.5)
            self._thread = None

    def _rx_loop(self) -> None:
        assert self._loop is not None
        while not self._stop.is_set():
            try:
                msg = self._bus.recv(timeout=self._recv_timeout)
            except Exception:
                log.exception("CAN router recv crashed; stopping")
                return
            if msg is None:
                continue
            # Snapshot subs to a local — register() may be called from
            # the asyncio thread while we iterate.
            subs = list(self._subs)
            for pred, handler in subs:
                try:
                    matches = pred(msg)
                except Exception:
                    log.exception("router predicate crashed; skipping")
                    continue
                if matches:
                    self._loop.call_soon_threadsafe(handler, msg)
