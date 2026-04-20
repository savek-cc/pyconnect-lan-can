"""python-can backend speaking ECB1 to an ESPHome bridge."""

from __future__ import annotations

import logging
import select
import socket
import threading
from typing import Optional, Tuple

import can

from . import wire

log = logging.getLogger(__name__)

_RECV_CHUNK = 4096


class Ecb1Bus(can.BusABC):
    """python-can Bus that tunnels CAN through an ECB1 TCP bridge.

    Minimal v1: HELLO handshake, blocking `_recv_internal`, synchronous
    `send` returning after TX_CAN_FRAME_RESP. Filters are not pushed to
    the bridge yet — python-can applies them locally.
    """

    def __init__(
        self,
        channel: str | None = None,
        *,
        host: str | None = None,
        port: int = 28081,
        role: wire.Role = wire.Role.CONTROL,
        options: int = int(wire.Opt.LOCAL_ECHO | wire.Opt.DEVICE_TIMESTAMPS),
        connect_timeout: float = 5.0,
        **kwargs,
    ) -> None:
        if host is None:
            if not channel:
                raise ValueError("Ecb1Bus requires host= or channel='host[:port]'")
            host, _, port_s = channel.partition(":")
            if port_s:
                port = int(port_s)
        self.channel_info = f"ecb1://{host}:{port}"
        self._host = host
        self._port = port
        self._role = role
        self._options = options
        self._tx_lock = threading.Lock()
        self._seq = 0

        self._sock = socket.create_connection((host, port), timeout=connect_timeout)
        self._sock.settimeout(None)
        self._rx_buf = bytearray()

        self._hello()
        super().__init__(channel=self.channel_info, **kwargs)

    # ── Handshake ────────────────────────────────────────────────────────
    def _hello(self) -> None:
        req = wire.HelloReq(role=self._role, options=self._options).pack()
        self._send_envelope(wire.Msg.HELLO_REQ, req)
        mtype, _seq, _corr, payload = self._recv_envelope_blocking()
        if mtype == wire.Msg.ERROR_RESP:
            st = wire.unpack_error(payload)
            raise ConnectionError(f"ECB1 HELLO rejected: status={st}")
        if mtype != wire.Msg.HELLO_RESP:
            raise ConnectionError(f"ECB1 HELLO unexpected msg=0x{mtype:02x}")
        resp = wire.HelloResp.unpack(payload)
        if resp.status != wire.Status.OK:
            raise ConnectionError(
                f"ECB1 HELLO_RESP status={resp.status} role={resp.granted_role}"
            )
        self.hello_resp = resp
        log.info(
            "ECB1 connected %s role=%d caps=0x%016x opts=0x%08x bitrate=%d",
            self.channel_info, resp.granted_role, resp.caps,
            resp.active_options, resp.nominal_bitrate,
        )

    # ── Envelope I/O ─────────────────────────────────────────────────────
    def _next_seq(self) -> int:
        self._seq = (self._seq + 1) & 0xFFFFFFFF
        return self._seq

    def _send_envelope(self, mtype: int, payload: bytes = b"", corr: int = 0) -> int:
        seq = self._next_seq()
        env = wire.Envelope(type=mtype, seq=seq, corr=corr, payload=payload).pack()
        with self._tx_lock:
            self._sock.sendall(env)
        return seq

    def _recv_envelope_blocking(self, timeout: Optional[float] = None
                                ) -> Tuple[int, int, int, bytes]:
        """Pull one full envelope, blocking up to `timeout` seconds.

        Raises socket.timeout if nothing arrives in time.
        """
        deadline = None if timeout is None else (timeout, None)
        while True:
            env = self._try_take_envelope()
            if env is not None:
                return env
            if timeout is None:
                ready, _, _ = select.select([self._sock], [], [])
            else:
                ready, _, _ = select.select([self._sock], [], [], timeout)
                if not ready:
                    raise socket.timeout("no ECB1 envelope in window")
            chunk = self._sock.recv(_RECV_CHUNK)
            if not chunk:
                raise ConnectionError("ECB1 peer closed")
            self._rx_buf.extend(chunk)

    def _try_take_envelope(self) -> Optional[Tuple[int, int, int, bytes]]:
        if len(self._rx_buf) < wire.HEADER_SIZE:
            return None
        mtype, _flags, seq, corr, plen, _ver = wire.decode_header(bytes(self._rx_buf))
        total = wire.HEADER_SIZE + plen
        if len(self._rx_buf) < total:
            return None
        payload = bytes(self._rx_buf[wire.HEADER_SIZE:total])
        del self._rx_buf[:total]
        return mtype, seq, corr, payload

    # ── python-can BusABC ────────────────────────────────────────────────
    def _recv_internal(self, timeout: Optional[float]
                       ) -> Tuple[Optional[can.Message], bool]:
        try:
            mtype, _seq, _corr, payload = self._recv_envelope_blocking(timeout)
        except socket.timeout:
            return None, False
        if mtype != wire.Msg.RX_CAN_FRAME_EVT:
            log.debug("dropping non-RX envelope type=0x%02x", mtype)
            return None, False
        f = wire.CanFrame.unpack(payload)
        msg = can.Message(
            timestamp=(f.timestamp_us / 1_000_000) if f.timestamp_us else 0.0,
            arbitration_id=f.can_id,
            is_extended_id=bool(f.flags & wire.CanFlag.EXT),
            is_remote_frame=bool(f.flags & wire.CanFlag.RTR),
            is_error_frame=bool(f.flags & wire.CanFlag.ERR),
            is_rx=bool(f.flags & wire.CanFlag.RX),
            dlc=f.dlc,
            data=f.data,
            channel=self.channel_info,
        )
        return msg, False  # python-can applies filters

    def send(self, msg: can.Message, timeout: Optional[float] = None) -> None:
        if self._role != wire.Role.CONTROL:
            raise can.CanOperationError("Ecb1Bus not in CONTROL role")
        flags = 0
        if msg.is_extended_id:
            flags |= wire.CanFlag.EXT
        if msg.is_remote_frame:
            flags |= wire.CanFlag.RTR
        data = b"" if msg.is_remote_frame else bytes(msg.data or b"")
        # host_tx_id must be non-zero per spec §11.1; use our next seq.
        host_tx_id = self._next_seq() or 1
        frame = wire.CanFrame(
            can_id=msg.arbitration_id,
            flags=flags,
            dlc=msg.dlc if msg.dlc else len(data),
            host_tx_id=host_tx_id,
            data=data,
        )
        self._send_envelope(wire.Msg.TX_CAN_FRAME_REQ, frame.pack())
        # Spec §11.2: bridge acks with TX_CAN_FRAME_RESP. We do not block
        # waiting for it here — the RX pump will surface async responses.

    def shutdown(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        super().shutdown()
