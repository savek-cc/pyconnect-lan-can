"""Raw CAN dumper for the ECB1 bridge — JSONL output.

Opens `Ecb1Bus` in MONITOR role (non-intrusive — no TX, no local echo
because we never TX) and appends every frame the bridge surfaces to
an output stream as a single JSON object per line, unfiltered.

Per-frame object keys:

    t_iso       ISO-8601 UTC wall-clock, microsecond precision
    t_mono_ns   host monotonic-clock ns (stable across wall-clock jumps)
    dev_ts_us   ECB1 device timestamp in microseconds, 0 if unavailable
    id          29/11-bit CAN id as 0x-prefixed hex string
    ext         1 if extended id
    rtr         1 if remote-transmission-request
    err         1 if error frame
    rx          1 if bridge marked this RX (not a local-echo)
    dlc         data length code
    data        payload bytes as lowercase hex (empty for RTR/error)

A single header line (JSON object with `type: "header"`) is written
first with the HELLO_RESP capabilities so post-processing knows which
bridge and bitrate produced the log. A trailer (`type: "trailer"`)
records total frames seen when the dumper stops.

Usage:
    python3 -m pyconnect_lan_can.can_dump --host <esp-ip> \\
        --out captures/can_<ts>.jsonl
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import signal
import sys
import time
from typing import Any, TextIO

from .ecb1 import wire as ecb1_wire
from .ecb1.bus import Ecb1Bus


def _frame_json(msg) -> dict[str, Any]:
    dev_ts = int(msg.timestamp * 1_000_000) if msg.timestamp else 0
    data = bytes(msg.data or b"").hex()
    return {
        "t_iso": _dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec="microseconds"),
        "t_mono_ns": time.monotonic_ns(),
        "dev_ts_us": dev_ts,
        "id": f"0x{msg.arbitration_id:08x}",
        "ext": int(bool(msg.is_extended_id)),
        "rtr": int(bool(msg.is_remote_frame)),
        "err": int(bool(msg.is_error_frame)),
        "rx": int(bool(getattr(msg, "is_rx", True))),
        "dlc": msg.dlc,
        "data": data,
    }


def _emit(out: TextIO, obj: dict[str, Any]) -> None:
    out.write(json.dumps(obj, separators=(",", ":")) + "\n")
    out.flush()


def _run(args: argparse.Namespace, out: TextIO) -> int:
    bus = Ecb1Bus(
        host=args.host, port=args.port,
        role=ecb1_wire.Role.MONITOR,
        options=int(ecb1_wire.Opt.DEVICE_TIMESTAMPS),
    )
    hello = bus.hello_resp
    _emit(out, {
        "type": "header",
        "t_iso": _dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec="microseconds"),
        "t_mono_ns": time.monotonic_ns(),
        "source": "ecb1",
        "host": args.host,
        "port": args.port,
        "bitrate": hello.nominal_bitrate,
        "caps": f"0x{hello.caps:016x}",
        "active_options": f"0x{hello.active_options:08x}",
        "granted_role": int(hello.granted_role),
        "channel_count": hello.channel_count,
        "max_dlc": hello.max_dlc,
    })

    stop = {"flag": False}

    def _on_sig(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _on_sig)
    signal.signal(signal.SIGTERM, _on_sig)

    count = 0
    try:
        while not stop["flag"]:
            msg = bus.recv(timeout=1.0)
            if msg is None:
                continue
            _emit(out, _frame_json(msg))
            count += 1
    finally:
        bus.shutdown()
        _emit(out, {
            "type": "trailer",
            "t_iso": _dt.datetime.now(_dt.timezone.utc).isoformat(
                timespec="microseconds"),
            "t_mono_ns": time.monotonic_ns(),
            "frames": count,
        })
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True,
                    help="ECB1 bridge host (ESP device IP)")
    ap.add_argument("--port", type=int, default=28081)
    ap.add_argument("--out", default="-",
                    help="Output file path, or '-' for stdout (default)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.out == "-":
        return _run(args, sys.stdout)
    with open(args.out, "w", buffering=1) as fp:
        return _run(args, fp)


if __name__ == "__main__":
    sys.exit(main())
