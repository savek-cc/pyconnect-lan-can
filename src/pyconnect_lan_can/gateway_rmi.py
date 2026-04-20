"""Gateway-originated periodic RMIs.

A real gateway issues three RMI requests on a ~60 s cadence even when
no app is connected:

    01 1d 01 10 08
    └─ GETSINGLEPROPERTY TEMPHUMCONTROL/1 flags=ACTUAL prop=0x08
       → `comfortTemperatureMode` (enum ADAPTIVE=0 / FIXED=1)

    02 01 1e 01 12 0c 0b
    └─ GETMULTIPLEPROPERTIES VENTILATIONCONFIG/1, 1 group,
       cnt|flags=0x12 (2 props + flags=ACTUAL)
       props: 0x0C `switchOffDelayBoost` (UINT8 minutes)
              0x0B `switchOnDelayBoost`  (UINT16 seconds)

    01 1e 01 10 0d
    └─ GETSINGLEPROPERTY VENTILATIONCONFIG/1 flags=ACTUAL prop=0x0D
       → `switchOffDelayMode` (enum FIXEDMODE=0 / MIRRORMODE=1)

These are config-cache polls around the comfort-temperature mode and
the boost on/off-delay configuration — slow-changing RW properties
the gateway refreshes in case the installer or HMI has changed them
locally. Not sensor telemetry, and not correlated with anything the
app subscribes to as PDOs.

Responses are logged and discarded — we don't forward them.
"""

from __future__ import annotations

import asyncio
import logging

from .rmi_transport import RmiTransport

log = logging.getLogger(__name__)

# HRU is node 1 on every captured bus.
HRU_NODE = 0x01

# Default cadence — each payload fires once per interval. Observed real
# LAN-C spacing is ~60 s; keep that unless overridden.
DEFAULT_INTERVAL_S = 60.0

GATEWAY_PAYLOADS: tuple[bytes, ...] = (
    bytes.fromhex("011d011008"),          # GETSINGLE TEMPHUMCONTROL/1
    bytes.fromhex("02011e01120c0b"),      # GETMULTI  VENTILATIONCONFIG/1
    bytes.fromhex("011e01100d"),          # GETSINGLE VENTILATIONCONFIG/1
)


class GatewayRmiLoop:
    """Background task issuing the three periodic gateway RMIs."""

    def __init__(self, rmi: RmiTransport, *,
                 interval_s: float = DEFAULT_INTERVAL_S,
                 dst_node: int = HRU_NODE):
        self._rmi = rmi
        self._interval = interval_s
        self._dst = dst_node & 0x3F
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="gateway-rmi-loop")
        log.info("gateway RMI loop started (interval=%.1fs dst=0x%02x)",
                 self._interval, self._dst)

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

    async def _run(self) -> None:
        # Tiny initial delay so we don't collide with the startup mirror-
        # sweep on the very first connect.
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=2.0)
            return
        except asyncio.TimeoutError:
            pass
        while not self._stop.is_set():
            for payload in GATEWAY_PAYLOADS:
                if self._stop.is_set():
                    return
                try:
                    is_err, resp = await self._rmi.request(self._dst, payload)
                    log.debug(
                        "gateway RMI %s → err=%s resp=%s",
                        payload.hex(), is_err, resp.hex(),
                    )
                except asyncio.TimeoutError:
                    log.info("gateway RMI %s timed out", payload.hex())
                except Exception:
                    log.exception("gateway RMI %s failed", payload.hex())
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._interval,
                )
                return
            except asyncio.TimeoutError:
                pass
