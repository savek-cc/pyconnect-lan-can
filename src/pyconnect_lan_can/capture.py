"""JSONL capture sink for the emulator.

One object per line, newline-terminated. Every record carries a
wall-clock `t_iso` (UTC microsecond) plus a monotonic-clock
`t_mono_ns` so paired CAN + TCP captures can be realigned even
across wall-clock jumps.

Record types:

    header / trailer          written once at open/close

    udp_recv / udp_send       discovery datagrams (peer, raw_hex,
                              decoded text if parseable)

    tcp_frame                 one per ComfoConnect envelope, including
                              raw wire bytes and decoded fields
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
import time
from typing import Any, Optional, TextIO

from google.protobuf import text_format

from . import framing, protocol as pr
from ._proto import zehnder_pb2 as pb


class JsonlSink:
    """Thread-safe JSONL writer for emulator capture."""

    def __init__(self, fp: TextIO, *, source: str):
        self._fp = fp
        self._lock = threading.Lock()
        self.write({"type": "header", "source": source})

    @staticmethod
    def _timestamps() -> dict[str, Any]:
        return {
            "t_iso": _dt.datetime.now(_dt.timezone.utc).isoformat(
                timespec="microseconds"),
            "t_mono_ns": time.monotonic_ns(),
        }

    def write(self, obj: dict[str, Any]) -> None:
        rec = {**self._timestamps(), **obj}
        line = json.dumps(rec, separators=(",", ":"))
        with self._lock:
            self._fp.write(line + "\n")
            self._fp.flush()

    def close(self) -> None:
        try:
            self.write({"type": "trailer"})
        except Exception:
            pass

    def tcp_frame(self, *, conn: int, direction: str,
                  frame: framing.Frame, env: pr.Envelope) -> None:
        try:
            result_name = pb.GatewayOperation.GatewayResult.Name(env.result)
        except Exception:
            result_name = str(env.result)
        body_text = ""
        if env.body is not None:
            try:
                body_text = text_format.MessageToString(
                    env.body, as_one_line=True).strip()
            except Exception as e:
                body_text = f"<pb err: {e}>"
        raw = frame.encode()
        self.write({
            "type": "tcp_frame",
            "conn": conn,
            "dir": direction,
            "src_uuid": str(frame.src_uuid),
            "dst_uuid": str(frame.dst_uuid),
            "op_type": int(env.op_type),
            "op_name": pr.op_name(env.op_type),
            "reference": env.reference,
            "result": int(env.result),
            "result_name": result_name,
            "body_type": (type(env.body).__name__
                          if env.body is not None else None),
            "body_text": body_text,
            "op_hex": frame.operation.hex(),
            "body_hex": frame.body.hex(),
            "raw_hex": raw.hex(),
        })

    def udp(self, *, kind: str, peer, data: bytes,
            decoded: Optional[pb.DiscoveryOperation] = None) -> None:
        rec: dict[str, Any] = {
            "type": f"udp_{kind}",
            "peer": (f"{peer[0]}:{peer[1]}" if peer else None),
            "raw_hex": data.hex(),
        }
        if decoded is not None:
            try:
                rec["decoded"] = text_format.MessageToString(
                    decoded, as_one_line=True).strip()
            except Exception as e:
                rec["decoded_err"] = str(e)
        self.write(rec)


def open_sink(path: Optional[str], *, source: str) -> Optional[JsonlSink]:
    """Return a JsonlSink writing to `path` ('-' for stdout), or None."""
    if not path:
        return None
    if path == "-":
        import sys
        return JsonlSink(sys.stdout, source=source)
    fp = open(path, "w", buffering=1)
    return JsonlSink(fp, source=source)
