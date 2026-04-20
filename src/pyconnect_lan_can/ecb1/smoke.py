"""End-to-end sanity check of the ECB1 backend against a live bridge.

Usage: python -m pyconnect_lan_can.ecb1.smoke <host> [--seconds N]
       [--role monitor|control]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import can

from . import wire
from .bus import Ecb1Bus


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=28081)
    ap.add_argument("--seconds", type=float, default=10.0)
    ap.add_argument("--role", choices=("monitor", "control"), default="monitor")
    ap.add_argument(
        "--tx-echo", action="store_true",
        help="(CONTROL only) send a harmless RTR and confirm the echo comes back",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    role = wire.Role.CONTROL if args.role == "control" else wire.Role.MONITOR
    bus = Ecb1Bus(host=args.host, port=args.port, role=role)
    print(
        f"connected role={bus.hello_resp.granted_role} "
        f"caps=0x{bus.hello_resp.caps:016x} "
        f"opts=0x{bus.hello_resp.active_options:08x} "
        f"bitrate={bus.hello_resp.nominal_bitrate}"
    )

    tx_echo_id = 0x1002_007F  # NMT-class, unowned node — safe sink
    echo_seen = False
    if args.tx_echo:
        if role != wire.Role.CONTROL:
            print("--tx-echo requires --role control", file=sys.stderr)
            bus.shutdown()
            return 2
        bus.send(can.Message(
            arbitration_id=tx_echo_id, is_extended_id=True,
            is_remote_frame=True, dlc=0, data=b"",
        ))

    end = time.monotonic() + args.seconds
    rx = 0
    pdo_nodes: dict[int, int] = {}
    rmi = 0
    nmt = 0
    while time.monotonic() < end:
        msg = bus.recv(timeout=1.0)
        if msg is None:
            continue
        rx += 1
        if args.tx_echo and msg.arbitration_id == tx_echo_id and msg.is_remote_frame:
            echo_seen = True
        top5 = (msg.arbitration_id >> 24) & 0b11111
        if top5 == 0b00000:
            node = msg.arbitration_id & 0x3F
            pdo_nodes[node] = pdo_nodes.get(node, 0) + 1
        elif top5 == 0b11111:
            rmi += 1
        elif top5 == 0b10000:
            nmt += 1

    print(f"received {rx} frames in {args.seconds:.1f}s "
          f"(PDO nodes={dict(sorted(pdo_nodes.items()))} RMI={rmi} NMT={nmt})")
    if args.tx_echo:
        print(f"tx-echo: {'ok' if echo_seen else 'MISSING'}")
    bus.shutdown()
    if rx == 0:
        return 1
    if args.tx_echo and not echo_seen:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
