"""TCP framing for the LAN-C protocol.

Frame layout (big-endian throughout):

    off  size   field
    ───  ────   ──────────────────────────────────────────
      0    4    length            (uint32 BE; bytes after this field)
      4   16    src_uuid          (two uint64 BE: MSB, LSB)
     20   16    dst_uuid
     36    2    op_len            (uint16 BE)
     38 op_len  operation         (GatewayOperation pb bytes)
    38+op_len …  body             (typed pb bytes; optional)
"""
from __future__ import annotations

import asyncio
import struct
import uuid
from dataclasses import dataclass

MAX_FRAME = 4096
MIN_FRAME = 32  # 16+16 UUIDs, 0-byte op/body technically invalid but permitted


class WireError(Exception):
    """Raised on any wire-layer framing violation."""


@dataclass
class Frame:
    src_uuid: uuid.UUID
    dst_uuid: uuid.UUID
    operation: bytes
    body: bytes = b""

    def encode(self) -> bytes:
        op_len = len(self.operation)
        if op_len > 0xFFFF:
            raise WireError(f"operation too long: {op_len}")
        length = 16 + 16 + 2 + op_len + len(self.body)
        if length > MAX_FRAME:
            raise WireError(f"frame too long: {length}")
        hdr = struct.pack(
            ">IQQQQH",
            length,
            (self.src_uuid.int >> 64) & 0xFFFFFFFFFFFFFFFF,
            self.src_uuid.int & 0xFFFFFFFFFFFFFFFF,
            (self.dst_uuid.int >> 64) & 0xFFFFFFFFFFFFFFFF,
            self.dst_uuid.int & 0xFFFFFFFFFFFFFFFF,
            op_len,
        )
        return hdr + self.operation + self.body


def decode_frame(buf: bytes) -> Frame:
    """Parse a *complete* frame (without the leading 4-byte length field).

    `buf` must start at the src_uuid MSB and run to the end of the body.
    Use `read_frame` for the streaming path that reads from an
    `asyncio.StreamReader`.
    """
    if len(buf) < 32 + 2:
        raise WireError(f"frame too short: {len(buf)} bytes")
    src_msb, src_lsb, dst_msb, dst_lsb, op_len = struct.unpack_from(
        ">QQQQH", buf, 0
    )
    off = 34
    if op_len > len(buf) - off:
        raise WireError(f"op_len {op_len} exceeds frame")
    operation = bytes(buf[off : off + op_len])
    body = bytes(buf[off + op_len :])
    src = uuid.UUID(int=(src_msb << 64) | src_lsb)
    dst = uuid.UUID(int=(dst_msb << 64) | dst_lsb)
    return Frame(src_uuid=src, dst_uuid=dst, operation=operation, body=body)


async def read_frame(reader: asyncio.StreamReader) -> Frame:
    """Read one framed message from the TCP stream.

    Raises:
        WireError — framing violation (caller should drop the connection).
        asyncio.IncompleteReadError — peer closed mid-frame / on EOF.
    """
    length_bytes = await reader.readexactly(4)
    (length,) = struct.unpack(">I", length_bytes)
    if length < MIN_FRAME:
        raise WireError(f"frame too short: length={length}")
    if length > MAX_FRAME:
        raise WireError(f"frame too long: length={length}")
    payload = await reader.readexactly(length)
    return decode_frame(payload)


async def write_frame(writer: asyncio.StreamWriter, frame: Frame) -> None:
    writer.write(frame.encode())
    await writer.drain()
