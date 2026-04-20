"""Entry point for the LAN-C emulator.

v0.8: UDP discovery + TCP session handshake + RMI passthrough +
CnRpdoRequest subscribe (zero-placeholder + bus-to-session PDO
forwarding) + mirror-sweep warm-up on session start + the three
gateway-originated 60 s-cadence RMIs the real LAN C emits +
NMT heartbeat / presence-ping reply on the CAN bus.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import uuid as _uuid
from pathlib import Path

from .capture import open_sink
from .discovery import (
    DISCOVERY_PORT,
    DiscoveryIdentity,
    local_ip_toward,
    serve_discovery,
)
from .can_router import CanRouter
from .ecb1.bus import Ecb1Bus
from .ecb1 import wire as ecb1_wire
from .gateway_rmi import DEFAULT_INTERVAL_S, GatewayRmiLoop
from .nmt import DEFAULT_HEARTBEAT_INTERVAL_S, NmtPresence
from .pdo_hub import APP_OBSERVED_SUBSCRIPTIONS, PdoHub
from .pin_store import DEFAULT_STATE_FILE, PinStore
from .rmi_transport import DEFAULT_HOST_NODE, RmiTransport
from .server import COMFOCONNECT_PORT, ComfoConnectServer

# Stable emulator uuid. Pattern mirrors the real LAN-C's shape
# (00000000-0021-1017-8001-<mac>) but in the locally-administered range
# so nothing real will ever collide with it.
DEFAULT_EMULATOR_UUID = _uuid.UUID("00000000-0021-1017-8001-fe00c0deface")


async def _run(args: argparse.Namespace) -> int:
    ip = args.advertise_ip or local_ip_toward(args.ip_for)
    gateway_uuid = _uuid.UUID(args.uuid)
    ident = DiscoveryIdentity(
        uuid=gateway_uuid.bytes,
        ipaddress=ip,
        version=args.version,
    )
    capture = open_sink(args.capture, source="emulator")
    udp_transport, udp_proto = await serve_discovery(
        ident, bind_host=args.bind, port=args.udp_port, capture=capture,
    )

    bus: Ecb1Bus | None = None
    router: CanRouter | None = None
    rmi: RmiTransport | None = None
    pdo_hub: PdoHub | None = None
    gw_rmi: GatewayRmiLoop | None = None
    nmt: NmtPresence | None = None
    # Emulator-wide "we are this CAN node on the bus" value. NMT
    # heartbeat, LAN-C self-RMI matching, and the CnNodeNotification
    # burst all key off this. Defaults to 0x0C (what the 2026-04-20
    # paired-capture unit used); override when impersonating a unit
    # that sat at a different ID.
    lanc_node = args.lanc_node
    if args.bridge_host:
        bus = Ecb1Bus(
            host=args.bridge_host, port=args.bridge_port,
            role=ecb1_wire.Role.CONTROL,
        )
        router = CanRouter(bus)
        rmi = RmiTransport(bus, router, src_node=args.src_node)
        pdo_hub = PdoHub(router, bus)
        if not args.no_warm_sweep:
            pdo_hub.seed_known_subs(APP_OBSERVED_SUBSCRIPTIONS)
        nmt_node = args.nmt_node if args.nmt_node is not None else lanc_node
        nmt = NmtPresence(
            router, bus, node_id=nmt_node,
            heartbeat_interval_s=args.heartbeat_interval,
        )
        router.start()
        rmi.start()
        nmt.start()
        if args.gateway_rmi_interval > 0:
            gw_rmi = GatewayRmiLoop(rmi, interval_s=args.gateway_rmi_interval)
            gw_rmi.start()
    else:
        logging.getLogger(__name__).warning(
            "no --bridge-host set: RMI/PDO requests will be answered NOT_REACHABLE"
        )

    pin_store = PinStore(Path(args.pin_file))
    tcp_server = ComfoConnectServer(
        gateway_uuid=gateway_uuid, rmi=rmi, pdo_hub=pdo_hub, nmt=nmt,
        lanc_node=lanc_node, capture=capture, pin_store=pin_store,
    )
    await tcp_server.start(host=args.bind, port=args.tcp_port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        udp_transport.close()
        tcp_server.close()
        await tcp_server.wait_closed()
        if gw_rmi is not None:
            await gw_rmi.stop()
        if nmt is not None:
            await nmt.stop()
        if router is not None:
            router.close()
        if bus is not None:
            bus.shutdown()
        if capture is not None:
            capture.close()
    logging.getLogger(__name__).info(
        "shutdown; udp_replies=%d", udp_proto.replies,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--udp-port", type=int, default=DISCOVERY_PORT,
                    help="UDP discovery listen port")
    ap.add_argument("--tcp-port", type=int, default=COMFOCONNECT_PORT,
                    help="TCP session listen port")
    ap.add_argument("--uuid", default=str(DEFAULT_EMULATOR_UUID))
    ap.add_argument("--version", type=int, default=1)
    ap.add_argument(
        "--advertise-ip", default=None,
        help="Override the IP we announce; default is autodetected by "
             "asking the kernel how it would reach --ip-for.",
    )
    ap.add_argument(
        "--ip-for", default="8.8.8.8",
        help="Target used only for local-IP autodetection (not contacted).",
    )
    ap.add_argument(
        "--bridge-host", default=None,
        help="ECB1 bridge host (ESP device IP). Without this, RMI is "
             "unavailable and requests get NOT_REACHABLE.",
    )
    ap.add_argument("--bridge-port", type=int, default=28081)
    ap.add_argument(
        "--lanc-node", type=lambda s: int(s, 0), default=0x0C,
        help="CAN node ID to impersonate as the LAN-C. Drives the "
             "CnNodeNotification burst (as productId=5, zoneId=255), "
             "the self-RMI short-circuit, and (unless --nmt-node is "
             "given) the NMT heartbeat source. Observed real units "
             "sit at 0x0C or 0x20; override per deployment.",
    )
    ap.add_argument(
        "--src-node", type=lambda s: int(s, 0), default=DEFAULT_HOST_NODE,
        help="Local CAN node ID used when issuing RMIs (default 0x3E).",
    )
    ap.add_argument(
        "--no-warm-sweep", action="store_true",
        help="Disable the PDO mirror-sweep seeded from observed app "
             "subscriptions (a real gateway warms on every connect).",
    )
    ap.add_argument(
        "--capture", default=None,
        help="Write a JSONL capture to this path ('-' for stdout). "
             "One record per UDP datagram and per TCP envelope, with "
             "wall + monotonic timestamps so paired CAN + TCP "
             "captures can be realigned.",
    )
    ap.add_argument(
        "--gateway-rmi-interval", type=float, default=DEFAULT_INTERVAL_S,
        help="Period (s) for the three gateway-originated RMIs the real "
             "LAN C issues. 0 disables (default %(default)ss).",
    )
    ap.add_argument(
        "--nmt-node", type=lambda s: int(s, 0), default=None,
        help="Override the NMT heartbeat / presence-reply node ID. "
             "Defaults to --lanc-node. Deliberately decoupled from "
             "--src-node so the emulator can be seen as the LAN-C "
             "by the HRU while issuing RMIs from a non-colliding "
             "host-application ID.",
    )
    ap.add_argument(
        "--heartbeat-interval", type=float,
        default=DEFAULT_HEARTBEAT_INTERVAL_S,
        help="NMT self-heartbeat period (s). 0 disables.",
    )
    ap.add_argument(
        "--pin-file", default=str(DEFAULT_STATE_FILE),
        help="JSON state file holding the persisted gateway PIN. "
             "Created with pin=0 (factory default) if missing. Delete "
             "to simulate a factory reset (default %(default)s).",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
