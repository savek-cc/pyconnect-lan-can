"""Byte-level codec for the ECB1 wire protocol.

Spec: esphome-zehnder-comfoair/docs/ecb1-protocol.md §5, §6, §9, §10.
All multi-byte fields are big-endian. No struct padding — serialization
is explicit.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import IntEnum, IntFlag

MAGIC = 0x45434231  # "ECB1"
VERSION = 1
HEADER_SIZE = 20
MAX_PAYLOAD = 1024


class Msg(IntEnum):
    HELLO_REQ = 0x01
    HELLO_RESP = 0x02
    ERROR_RESP = 0x03
    PING_REQ = 0x04
    PONG_RESP = 0x05
    GET_STATS_REQ = 0x06
    GET_STATS_RESP = 0x07
    SET_OPTIONS_REQ = 0x08
    SET_OPTIONS_RESP = 0x09
    SET_FILTERS_REQ = 0x0A
    SET_FILTERS_RESP = 0x0B
    RX_CAN_FRAME_EVT = 0x20
    TX_CAN_FRAME_REQ = 0x21
    TX_CAN_FRAME_RESP = 0x22
    TX_STATUS_EVT = 0x23
    BUS_ERROR_EVT = 0x24
    BUS_STATE_EVT = 0x25


class Status(IntEnum):
    OK = 0
    UNSUPPORTED_VERSION = 1
    UNSUPPORTED_MESSAGE = 2
    UNSUPPORTED_FEATURE = 3
    INVALID_PAYLOAD = 4
    INVALID_FRAME = 5
    INVALID_FILTER = 6
    NOT_READY = 7
    QUEUE_FULL = 8
    BUS_OFF = 9
    READ_ONLY = 10
    BUSY = 11
    INTERNAL_ERROR = 12


class Role(IntEnum):
    MONITOR = 0
    CONTROL = 1


class CanFlag(IntFlag):
    EXT = 1 << 0
    RTR = 1 << 1
    ERR = 1 << 2
    FD = 1 << 3
    BRS = 1 << 4
    ESI = 1 << 5
    RX = 1 << 6
    ECHO = 1 << 7


class Opt(IntFlag):
    LOCAL_ECHO = 1 << 0
    LISTEN_ONLY = 1 << 1
    DEVICE_TIMESTAMPS = 1 << 2


# Struct formats (big-endian, explicit layout).
_HEADER = struct.Struct(">IBBHIII")  # magic, ver, type, flags, seq, corr, plen
_HELLO_REQ = struct.Struct(">HHBBHQII")
_HELLO_RESP = struct.Struct(">HHBBHQIIHH")
_FRAME_FIXED = struct.Struct(">IBBBBQI")  # can_id, flags, dlc, chan, dlen, ts_us, host_tx
_TX_RESP = struct.Struct(">IHH")  # host_tx_id, status, queue_depth
_ERROR_RESP = struct.Struct(">H")

assert _HEADER.size == HEADER_SIZE


@dataclass
class Envelope:
    type: int
    seq: int
    corr: int
    payload: bytes = b""
    flags: int = 0

    def pack(self) -> bytes:
        return _HEADER.pack(
            MAGIC, VERSION, self.type, self.flags,
            self.seq, self.corr, len(self.payload),
        ) + self.payload


class ProtocolError(RuntimeError):
    pass


def decode_header(buf: bytes) -> tuple[int, int, int, int, int, int]:
    """Decode the 20-byte header. Returns (type, flags, seq, corr, plen, ver)."""
    if len(buf) < HEADER_SIZE:
        raise ProtocolError(f"header short: {len(buf)}/{HEADER_SIZE}")
    magic, ver, mtype, flags, seq, corr, plen = _HEADER.unpack_from(buf, 0)
    if magic != MAGIC:
        raise ProtocolError(f"bad magic 0x{magic:08x}")
    if plen > MAX_PAYLOAD:
        raise ProtocolError(f"payload {plen} exceeds {MAX_PAYLOAD}")
    return mtype, flags, seq, corr, plen, ver


@dataclass
class HelloReq:
    min_version: int = VERSION
    max_version: int = VERSION
    role: Role = Role.MONITOR
    caps: int = 0
    options: int = 0

    def pack(self) -> bytes:
        return _HELLO_REQ.pack(
            self.min_version, self.max_version,
            int(self.role), 0, 0,
            self.caps, self.options, 0,
        )


@dataclass
class HelloResp:
    status: int
    selected_version: int
    granted_role: int
    caps: int
    active_options: int
    nominal_bitrate: int
    channel_count: int
    max_dlc: int

    @classmethod
    def unpack(cls, payload: bytes) -> "HelloResp":
        if len(payload) < _HELLO_RESP.size:
            raise ProtocolError(f"HELLO_RESP short: {len(payload)}")
        status, ver, role, _r0, _r1, caps, opts, bitrate, chans, max_dlc = \
            _HELLO_RESP.unpack_from(payload, 0)
        return cls(status, ver, role, caps, opts, bitrate, chans, max_dlc)


@dataclass
class CanFrame:
    can_id: int
    flags: int = 0
    dlc: int = 0
    channel: int = 0
    timestamp_us: int = 0
    host_tx_id: int = 0
    data: bytes = b""

    def pack(self) -> bytes:
        if len(self.data) > 8:
            raise ValueError(f"data_len {len(self.data)} > 8")
        return _FRAME_FIXED.pack(
            self.can_id, self.flags, self.dlc, self.channel,
            len(self.data), self.timestamp_us, self.host_tx_id,
        ) + self.data

    @classmethod
    def unpack(cls, payload: bytes) -> "CanFrame":
        if len(payload) < _FRAME_FIXED.size:
            raise ProtocolError(f"frame short: {len(payload)}")
        can_id, flags, dlc, chan, dlen, ts_us, host_tx = \
            _FRAME_FIXED.unpack_from(payload, 0)
        end = _FRAME_FIXED.size + dlen
        if len(payload) < end:
            raise ProtocolError(f"frame data short: {len(payload)} < {end}")
        return cls(can_id, flags, dlc, chan, ts_us, host_tx,
                   bytes(payload[_FRAME_FIXED.size:end]))


@dataclass
class TxResp:
    host_tx_id: int
    status: int
    queue_depth: int

    @classmethod
    def unpack(cls, payload: bytes) -> "TxResp":
        if len(payload) < _TX_RESP.size:
            raise ProtocolError(f"TX_RESP short: {len(payload)}")
        return cls(*_TX_RESP.unpack_from(payload, 0))


def unpack_error(payload: bytes) -> int:
    if len(payload) < _ERROR_RESP.size:
        raise ProtocolError(f"ERROR_RESP short: {len(payload)}")
    return _ERROR_RESP.unpack_from(payload, 0)[0]
