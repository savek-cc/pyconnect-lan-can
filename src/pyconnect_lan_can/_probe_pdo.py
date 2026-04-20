"""Subscribe to a handful of PDOs and listen for notifications.

Exercises the zero-placeholder behaviour (emitted immediately after
CnRpdoConfirm) plus real PDO broadcasts forwarded from the HRU. Prints
each CnRpdoNotification as it arrives.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid as _uuid
from typing import List

from . import framing, protocol as pr
from ._proto import zehnder_pb2 as pb

log = logging.getLogger(__name__)

# Small default set drawn from observed app sessions. Types per
# CnRpdoRequest: 16/type=1, 65/type=1, 117/type=1, 221/type=2.
DEFAULT_SUBS: list[tuple[int, int]] = [
    (16, 1),   # ventilationUnit_mode (enum)
    (65, 1),   # PRESET scheduler activityValue
    (117, 1),  # f12ExhaustFan_fanDutyCycle
    (221, 2),  # a temperature-sensor reading (u16)
]


async def _send(writer, app_uuid, gw_uuid, ref, op_type, body):
    env = pr.Envelope(op_type=op_type, body=body,
                      result=pr.Result.OK, reference=ref)
    frame = framing.Frame(
        src_uuid=app_uuid, dst_uuid=gw_uuid,
        operation=env.encode_header(), body=env.encode_body(),
    )
    await framing.write_frame(writer, frame)
    log.info("→ %s ref=%d", pr.op_name(op_type), ref)


async def probe(host: str, port: int, pin: int, duration: float,
                subs: List[tuple[int, int]]) -> int:
    reader, writer = await asyncio.open_connection(host, port)
    app_uuid = _uuid.uuid4()
    gw_uuid = _uuid.UUID(int=0)
    ref = 0

    async def expect_reply():
        frame = await framing.read_frame(reader)
        env = pr.decode(frame.operation, frame.body)
        log.info("← %s result=%d ref=%s body=%s",
                 pr.op_name(env.op_type), env.result,
                 env.reference, type(env.body).__name__ if env.body else "-")
        return env

    reg = pb.RegisterAppRequest()
    reg.uuid = app_uuid.bytes
    reg.pin = pin
    reg.devicename = "probe-pdo"
    await _send(writer, app_uuid, gw_uuid, ref,
                pr.OpType.RegisterAppRequestType, reg)
    ref += 1
    r = await expect_reply()
    if r.result != pr.Result.OK:
        print("RegisterApp failed", file=sys.stderr)
        return 1

    ss = pb.StartSessionRequest()
    ss.takeover = True
    await _send(writer, app_uuid, gw_uuid, ref,
                pr.OpType.StartSessionRequestType, ss)
    ref += 1
    r = await expect_reply()
    if r.result != pr.Result.OK:
        print("StartSession failed", file=sys.stderr)
        return 2

    # Consume the 3 node notifications that follow StartSessionConfirm.
    for _ in range(3):
        await expect_reply()

    for pdid, rtype in subs:
        req = pb.CnRpdoRequest()
        req.pdid = pdid
        req.zone = 1
        req.type = rtype
        req.timeout = 0xFFFFFF
        await _send(writer, app_uuid, gw_uuid, ref,
                    pr.OpType.CnRpdoRequestType, req)
        ref += 1

    # Drain all incoming frames for `duration` seconds.
    counts: dict[int, int] = {}
    placeholders: dict[int, int] = {}
    deadline = asyncio.get_running_loop().time() + duration
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            frame = await asyncio.wait_for(
                framing.read_frame(reader), timeout=remaining)
        except asyncio.TimeoutError:
            break
        env = pr.decode(frame.operation, frame.body)
        if env.op_type == pr.OpType.CnRpdoNotificationType:
            body: pb.CnRpdoNotification = env.body  # type: ignore[assignment]
            data = bytes(body.data)
            counts[body.pdid] = counts.get(body.pdid, 0) + 1
            is_placeholder = (set(data) == {0} or data == b"")
            if is_placeholder:
                placeholders[body.pdid] = placeholders.get(body.pdid, 0) + 1
            log.info(
                "← notif pdid=%d zone=%d data=(%dB) %s%s",
                body.pdid, body.zone, len(data), data.hex(),
                "  [PLACEHOLDER]" if is_placeholder else "",
            )
        else:
            log.info("← %s result=%d ref=%s", pr.op_name(env.op_type),
                     env.result, env.reference)

    # Summary.
    print()
    print("=== PDO notification summary ===")
    for pdid, rtype in subs:
        total = counts.get(pdid, 0)
        ph = placeholders.get(pdid, 0)
        real = total - ph
        print(f"  pdid={pdid:>4}  type={rtype}  "
              f"total={total:>3}  placeholder={ph}  real={real}")

    close = pb.CloseSessionRequest()
    await _send(writer, app_uuid, gw_uuid, ref,
                pr.OpType.CloseSessionRequestType, close)
    try:
        await asyncio.wait_for(expect_reply(), timeout=1.0)
    except asyncio.TimeoutError:
        pass
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=56747)
    ap.add_argument("--pin", type=int, default=1)
    ap.add_argument("--duration", type=float, default=5.0,
                    help="Seconds to listen for notifications")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(probe(args.host, args.port, args.pin,
                             args.duration, DEFAULT_SUBS))


if __name__ == "__main__":
    sys.exit(main())
