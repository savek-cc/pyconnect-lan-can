"""Minimal RMI probe — talks to the emulator over TCP and issues one RMI.

Uses a gateway-style self-test payload:
    GETSINGLE TEMPHUMCONTROL/1 prop=0x08 flags=ACTUAL
which is what a real gateway itself polls every 60 s. If the bridge +
HRU are live, this returns non-empty response bytes; if the HRU is
unreachable the emulator replies NOT_REACHABLE.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid as _uuid

from . import framing, protocol as pr
from ._proto import zehnder_pb2 as pb

log = logging.getLogger(__name__)


OP_GETSINGLEPROPERTY = 0x01
UNIT_TEMPHUMCONTROL = 0x1D
FLAG_ACTUAL = 0x10


async def _exchange(reader, writer, app_uuid, gw_uuid, ref, op_type, body):
    env = pr.Envelope(op_type=op_type, body=body,
                      result=pr.Result.OK, reference=ref)
    frame = framing.Frame(
        src_uuid=app_uuid, dst_uuid=gw_uuid,
        operation=env.encode_header(), body=env.encode_body(),
    )
    log.info("→ %s ref=%d", pr.op_name(op_type), ref)
    await framing.write_frame(writer, frame)
    resp_frame = await framing.read_frame(reader)
    resp = pr.decode(resp_frame.operation, resp_frame.body)
    log.info("← %s result=%d body=%s", pr.op_name(resp.op_type),
             resp.result,
             type(resp.body).__name__ if resp.body is not None else "-")
    return resp


async def probe(host: str, port: int, node_id: int, pin: int) -> int:
    reader, writer = await asyncio.open_connection(host, port)
    app_uuid = _uuid.uuid4()
    gw_uuid = _uuid.UUID(int=0)  # first frame teaches us the real one
    ref = 0

    reg = pb.RegisterAppRequest()
    reg.uuid = app_uuid.bytes
    reg.pin = pin
    reg.devicename = "probe-rmi"
    resp = await _exchange(reader, writer, app_uuid, gw_uuid, ref,
                           pr.OpType.RegisterAppRequestType, reg)
    if resp.result != pr.Result.OK:
        print(f"RegisterApp failed: result={resp.result}", file=sys.stderr)
        return 1
    # Update gw_uuid from the response frame for subsequent sends.
    # (Our _exchange helper doesn't return the Frame; re-fetch from the
    # first write attempt — simpler to just reopen with discovery. For
    # now, the emulator accepts any dst uuid as long as src is stable,
    # so we reuse the zero UUID throughout.)
    ref += 1

    ss = pb.StartSessionRequest()
    ss.takeover = True
    resp = await _exchange(reader, writer, app_uuid, gw_uuid, ref,
                           pr.OpType.StartSessionRequestType, ss)
    if resp.result != pr.Result.OK:
        print(f"StartSession failed: result={resp.result}", file=sys.stderr)
        return 2
    ref += 1

    # Drain the three CnNodeNotifications the emulator emits after
    # StartSessionConfirm. They arrive before our next reply.
    for _ in range(3):
        notif_frame = await asyncio.wait_for(
            framing.read_frame(reader), timeout=2.0)
        notif = pr.decode(notif_frame.operation, notif_frame.body)
        log.info("← %s (notification)", pr.op_name(notif.op_type))

    # GETMULTIPLEPROPERTIES NODE/1 [prop 0x03,0x04,0x06,0x05,0x14] flags=ACTUAL.
    # This is the first RMI the app sends after StartSession — asks
    # the HRU for its serial + product name. Returns a multi-frame
    # response (~32 bytes), exercising chunk reassembly.
    payload = bytes([0x02, 0x01, 0x01, 0x01, 0x05 | FLAG_ACTUAL,
                     0x03, 0x04, 0x06, 0x05, 0x14])
    rmi = pb.CnRmiRequest()
    rmi.nodeId = node_id
    rmi.message = payload
    resp = await _exchange(reader, writer, app_uuid, gw_uuid, ref,
                           pr.OpType.CnRmiRequestType, rmi)
    ref += 1
    if resp.result != pr.Result.OK:
        print(f"RMI failed outer: result={resp.result}", file=sys.stderr)
        return 3
    rresp: pb.CnRmiResponse = resp.body  # type: ignore[assignment]
    print(f"RMI ok: result={rresp.result} "
          f"message=({len(rresp.message)}B) {rresp.message.hex()}")

    # Close cleanly.
    close = pb.CloseSessionRequest()
    await _exchange(reader, writer, app_uuid, gw_uuid, ref,
                    pr.OpType.CloseSessionRequestType, close)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return 0 if rresp.result == 0 else 4


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=56747)
    ap.add_argument("--node-id", type=lambda s: int(s, 0), default=1,
                    help="Target ComfoNet node (1=HRU)")
    ap.add_argument("--pin", type=int, default=1,
                    help="Non-zero pin (emulator rejects pin=0)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(probe(args.host, args.port, args.node_id, args.pin))


if __name__ == "__main__":
    sys.exit(main())
