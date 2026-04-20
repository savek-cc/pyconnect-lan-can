"""TCP server for ComfoConnect sessions on port 56747.

Session handshake and the notifications/confirms a real gateway emits
in the first second of a session:

    A→G RegisterAppRequestType        → RegisterAppConfirm (OK once the
                                         pin matches the stored one;
                                         otherwise NOT_ALLOWED)
    A→G StartSessionRequestType       takeover → StartSessionConfirm OK
    G→A CnNodeNotificationType        nodeId=1  productId=1 zoneId=1   NORMAL
    G→A CnNodeNotificationType        nodeId=25 productId=7 zoneId=1   NORMAL
    G→A CnNodeNotificationType        nodeId=<self> productId=5 zoneId=255 NORMAL
    A→G GetRemoteAccessIdRequestType  → empty confirm (feature disabled)
    A→G VersionRequestType            → gatewayVersion + serial + comfoNetVersion
    A→G GetSupportIdRequestType       → empty confirm
    A→G GetWebIdRequestType           → empty confirm
    A→G CnTimeRequestType             → CnTimeConfirm(currentTime=...)
    A→G KeepAliveType                 → (silent; the gateway emits none)
    A→G CloseSessionRequestType       → CloseSessionConfirm
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from . import framing, protocol as pr
from ._proto import zehnder_pb2 as pb
from .capture import JsonlSink
from .nmt import NmtPresence
from .pdo_hub import PdoHub, payload_size_for_type
from .pin_store import PinStore
from .rmi_transport import RmiFragmentLoss, RmiTransport

COMFOCONNECT_PORT = 56747

log = logging.getLogger(__name__)


# Gateway version identifiers. The app decodes these as packed LE-uint32
# bitfields: state << 30 | major << 20 | minor << 10 | patch, with
# state ∈ {U=0, D=1, P=2, R=3} (release). The app displays these but
# does not gate on them, so values only need to be structurally valid.
_GATEWAY_VERSION = 0xC0101401
_COMFONET_VERSION = 0xC0100000
_SERIAL_NUMBER = "PYCCLANC00001"

# ComfoNet counts seconds from 2000-01-01, not from the Unix epoch. We
# return UTC seconds-since-2000; the app just displays this value, so
# users in non-UTC TZs will see a time offset but no behavioural break.
_COMFONET_EPOCH_S = int(
    datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
)

# Fixed bus topology observed in captures:
#   HRU=1 (productId=1, zoneId=1), LAN-C=<varies>, KNX-C=25 (productId=7,
#   zoneId=1). A real gateway emits these exactly once in the order
#   1 → self → 25 right after StartSessionConfirm. Node ID of the LAN-C
#   itself is therefore parameterized — see `_node_burst()`.
_NODE_NORMAL = pb.CnNodeNotification.NodeModeType.Value("NODE_NORMAL")


def _node_burst(lanc_node: int) -> list[tuple[int, int, int, int]]:
    """(nodeId, productId, zoneId, mode) triples in the observed order."""
    return [
        (1, 1, 1, _NODE_NORMAL),
        (lanc_node & 0x3F, 5, 255, _NODE_NORMAL),
        (25, 7, 1, _NODE_NORMAL),
    ]


# Canned reply to the app's "product-info" self-RMI — the GETMULTI the
# app issues at every connect to populate the gateway row in its node
# list. Request payload is:
#     02 01 01 01 15 03 04 06 05 14
#       │  │  │  │  └─┴─┴─┴─┴─ property ids (0x15 serial, …, 0x14 name)
#       │  │  │  └─ ?
#       │  │  └─ subunit
#       │  └─ unit
#       └─ opcode (GETMULTI)
# The reply is:
#     00  <serial>\0  01 14 10 C0 01  <product>\0
# Serial-number and product-name strings are delimited with trailing
# NULs. The 5 fixed bytes in the middle are the non-string property
# values (type + value for properties 0x03/0x04/0x06/0x05). We
# parameterise the serial to stay in sync with `_SERIAL_NUMBER`.
def _lanc_self_properties_response(serial: str = _SERIAL_NUMBER) -> bytes:
    return (
        b"\x00"
        + serial.encode("ascii") + b"\x00"
        + bytes.fromhex("011410c001")
        + b"ComfoConnect LAN C\x00"
    )


_LANC_SELF_PROPERTIES_REQUEST = bytes.fromhex("02010101150304060514")


@dataclass
class Session:
    """Per-TCP-connection state. Owns the TCP writer for replies."""

    conn_id: int
    peer: str
    gateway_uuid: _uuid.UUID
    writer: asyncio.StreamWriter
    app_uuid: Optional[_uuid.UUID] = None
    registered: bool = False
    started: bool = False
    close_requested: bool = False
    _seen_frames: int = 0
    _write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # pdid -> (zone, rpdo_type). Tracks active CnRpdoRequest subscriptions
    # so we can unregister them from the bus-level PdoHub on teardown.
    pdo_subs: dict[int, tuple[int, int]] = field(default_factory=dict)
    capture: Optional[JsonlSink] = None

    async def write(self, env: pr.Envelope) -> bool:
        """Emit an envelope back to the app, src=gateway, dst=app.

        Returns True on success, False if the peer has gone away. In the
        failure case `close_requested` is set so the connection loop
        exits cleanly at its next check — callers don't need to bail
        out mid-handler just because one reply couldn't flush.
        """
        if self.app_uuid is None:
            raise RuntimeError("cannot write before app_uuid is known")
        frame = framing.Frame(
            src_uuid=self.gateway_uuid,
            dst_uuid=self.app_uuid,
            operation=env.encode_header(),
            body=env.encode_body(),
        )
        try:
            async with self._write_lock:
                await framing.write_frame(self.writer, frame)
        except (ConnectionError, BrokenPipeError, OSError) as e:
            # Peer closed mid-RMI (3s CAN timeout is long enough for
            # the app to give up and reset TCP) or any other
            # non-recoverable write error. Mark the session dead and
            # swallow — no more frames are going out on this socket.
            log.info("[c%d] write failed (peer gone): %s",
                     self.conn_id, e)
            self.close_requested = True
            return False
        if self.capture is not None:
            self.capture.tcp_frame(
                conn=self.conn_id, direction="G→A",
                frame=frame, env=env,
            )
        log.info("[c%d] → %s ref=%s result=%s body=%s",
                 self.conn_id, pr.op_name(env.op_type), env.reference,
                 env.result,
                 type(env.body).__name__ if env.body is not None else "-")
        return True


class ComfoConnectServer:
    def __init__(self, gateway_uuid: _uuid.UUID,
                 rmi: Optional[RmiTransport] = None,
                 pdo_hub: Optional[PdoHub] = None,
                 nmt: Optional[NmtPresence] = None,
                 *, lanc_node: int = 0x0C,
                 capture: Optional[JsonlSink] = None,
                 pin_store: Optional[PinStore] = None):
        self.gateway_uuid = gateway_uuid
        self._rmi = rmi
        self._pdo_hub = pdo_hub
        self._nmt = nmt
        self._lanc_node = lanc_node & 0x3F
        self._capture = capture
        self._pin_store = pin_store
        self._conn_counter = 0
        self._server: asyncio.base_events.Server | None = None
        # Active session (for takeover semantics). There is always at
        # most one active session — additional concurrent connections
        # are denied until the active one gets taken over.
        self._active: Optional[Session] = None
        self._active_lock = asyncio.Lock()

    # ── lifecycle ─────────────────────────────────────────────────────
    async def start(self, host: str = "0.0.0.0",
                    port: int = COMFOCONNECT_PORT) -> None:
        self._server = await asyncio.start_server(self._handle_conn, host, port)
        sock = self._server.sockets[0].getsockname() if self._server.sockets else (host, port)
        log.info("TCP server listening on %s (gateway uuid=%s)",
                 sock, self.gateway_uuid)

    async def serve_forever(self) -> None:
        if self._server is None:
            raise RuntimeError("start() not called")
        await self._server.serve_forever()

    def close(self) -> None:
        if self._server is not None:
            self._server.close()

    async def wait_closed(self) -> None:
        if self._server is not None:
            await self._server.wait_closed()

    # ── per-connection loop ───────────────────────────────────────────
    async def _handle_conn(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        self._conn_counter += 1
        peer = writer.get_extra_info("peername")
        session = Session(
            conn_id=self._conn_counter,
            peer=f"{peer[0]}:{peer[1]}" if peer else "?",
            gateway_uuid=self.gateway_uuid,
            writer=writer,
            capture=self._capture,
        )
        log.info("[c%d] connected from %s", session.conn_id, session.peer)
        try:
            while True:
                try:
                    frame = await framing.read_frame(reader)
                except asyncio.IncompleteReadError:
                    log.info("[c%d] peer closed", session.conn_id)
                    return
                except framing.WireError as e:
                    log.warning("[c%d] wire error: %s — dropping",
                                session.conn_id, e)
                    return
                session._seen_frames += 1
                if session.app_uuid is None:
                    session.app_uuid = frame.src_uuid
                    log.info("[c%d] app_uuid=%s",
                             session.conn_id, session.app_uuid)
                try:
                    env = pr.decode(frame.operation, frame.body)
                except Exception as e:
                    log.warning("[c%d] pb decode failed: %s", session.conn_id, e)
                    continue
                if self._capture is not None:
                    self._capture.tcp_frame(
                        conn=session.conn_id, direction="A→G",
                        frame=frame, env=env,
                    )
                log.info(
                    "[c%d] ← %s ref=%s result=%s body=%s",
                    session.conn_id,
                    pr.op_name(env.op_type),
                    env.reference,
                    env.result,
                    type(env.body).__name__ if env.body is not None else "-",
                )
                try:
                    await self._dispatch(session, env)
                except Exception:
                    log.exception("[c%d] dispatch failed", session.conn_id)
                if session.close_requested:
                    return
        finally:
            await self._teardown_session(session)

    async def _teardown_session(self, session: Session) -> None:
        async with self._active_lock:
            if self._active is session:
                self._active = None
        if self._pdo_hub is not None and session.pdo_subs:
            self._pdo_hub.unsubscribe_all(session)
        try:
            session.writer.close()
            await session.writer.wait_closed()
        except Exception:
            pass
        log.info("[c%d] closed (saw %d frames)",
                 session.conn_id, session._seen_frames)

    # ── dispatch ──────────────────────────────────────────────────────
    async def _dispatch(self, s: Session, env: pr.Envelope) -> None:
        OpType = pr.OpType
        t = env.op_type

        if t == OpType.RegisterAppRequestType:
            return await self._do_register(s, env)
        if t == OpType.StartSessionRequestType:
            return await self._do_start_session(s, env)
        if t == OpType.CloseSessionRequestType:
            return await self._do_close_session(s, env)
        if t == OpType.ChangePinRequestType:
            return await self._do_change_pin(s, env)
        if t == OpType.VersionRequestType:
            return await self._do_version(s, env)
        if t in (OpType.GetRemoteAccessIdRequestType,
                 OpType.GetSupportIdRequestType,
                 OpType.GetWebIdRequestType):
            return await self._do_empty_id_confirm(s, env)
        if t == OpType.CnTimeRequestType:
            return await self._do_cn_time(s, env)
        if t == OpType.KeepAliveType:
            # Real LAN C emits no reply; liveness is app-driven via
            # CnTime every ~5 s. Silently accept.
            return
        if t == OpType.CnRmiRequestType:
            return await self._do_rmi(s, env)
        if t == OpType.CnRpdoRequestType:
            return await self._do_rpdo(s, env)
        if t == OpType.CnNodeRequestType:
            # Android app never issues this against the real LAN C in
            # our capture; stay silent-compliant with NOT_ALLOWED.
            return await self._do_not_allowed(s, env)

        log.warning("[c%d] unhandled op %s — replying NOT_ALLOWED",
                    s.conn_id, pr.op_name(t))
        # Best-effort NOT_ALLOWED. Some ops have no confirm mapping;
        # those we just drop (matches real-gateway leniency).
        try:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))
        except KeyError:
            pass

    # ── handlers ──────────────────────────────────────────────────────
    async def _do_register(self, s: Session, env: pr.Envelope) -> None:
        req: pb.RegisterAppRequest = env.body  # type: ignore[assignment]
        if req is None:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))
            return
        # A real gateway verifies `pin` against its stored gateway
        # PIN. NOT_ALLOWED on mismatch causes the app to prompt and
        # retry. When no PinStore is wired, stay permissive.
        if self._pin_store is not None and int(req.pin) != self._pin_store.pin:
            log.info("[c%d] Register: pin=%d rejected (stored=%d)",
                     s.conn_id, int(req.pin), self._pin_store.pin)
            await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))
            return
        s.registered = True
        await s.write(pr.make_reply(env, result=pr.Result.OK))

    async def _do_start_session(self, s: Session, env: pr.Envelope) -> None:
        req: pb.StartSessionRequest | None = env.body  # type: ignore[assignment]
        takeover = bool(req and req.takeover)
        async with self._active_lock:
            if self._active is not None and self._active is not s:
                if takeover:
                    log.info("[c%d] takeover: evicting c%d",
                             s.conn_id, self._active.conn_id)
                    self._active.close_requested = True
                    try:
                        self._active.writer.close()
                    except Exception:
                        pass
                    self._active = s
                else:
                    # Real LAN-C returns OTHER_SESSION when an app asks
                    # for a non-takeover session while another is active.
                    await s.write(pr.make_reply(env, result=pr.Result.OTHER_SESSION))
                    return
            else:
                self._active = s
        s.started = True
        await s.write(pr.make_reply(env, result=pr.Result.OK))
        await self._emit_node_burst(s)
        # A real gateway RTR-sweeps its PDO subscription table on
        # every app connect/reconnect. Fire the sweep as a background
        # task — it overlaps with the app's own CnRpdoRequest burst
        # that typically lands in the next ~15 ms, and by the time
        # those arrive the HRU has refreshed its cache.
        if self._pdo_hub is not None:
            asyncio.create_task(self._pdo_hub.sweep())
        # A real gateway also NMT-pings nodes 0x01..0x3F on every app
        # connect, so other devices on the bus stay visible to the app.
        if self._nmt is not None:
            asyncio.create_task(self._nmt.sweep())

    async def _emit_node_burst(self, s: Session) -> None:
        for node_id, product_id, zone_id, mode in _node_burst(self._lanc_node):
            n = pb.CnNodeNotification()
            n.nodeId = node_id
            n.productId = product_id
            n.zoneId = zone_id
            n.mode = mode
            await s.write(pr.make_notification(
                pr.OpType.CnNodeNotificationType, n))

    async def _do_close_session(self, s: Session, env: pr.Envelope) -> None:
        await s.write(pr.make_reply(env, result=pr.Result.OK))
        s.close_requested = True

    async def _do_change_pin(self, s: Session, env: pr.Envelope) -> None:
        """Verify oldpin against the store, persist newpin on success.

        Mirrors real gateway behaviour — the old pin is verified
        before the new one is accepted. The app only consults the
        outer GatewayOperation.result on the reply; the
        ChangePinConfirm body is empty and ignored. Without a PinStore
        we fall back to accept-any, matching the old permissive
        posture.
        """
        req: pb.ChangePinRequest | None = env.body  # type: ignore[assignment]
        if req is None:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))
            return
        if self._pin_store is not None:
            if int(req.oldpin) != self._pin_store.pin:
                log.info("[c%d] ChangePin: oldpin=%d rejected (stored=%d)",
                         s.conn_id, int(req.oldpin), self._pin_store.pin)
                await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))
                return
            self._pin_store.set_pin(int(req.newpin))
            log.info("[c%d] ChangePin: pin updated to %d",
                     s.conn_id, int(req.newpin))
        else:
            log.info("[c%d] ChangePin: oldpin=%d newpin=%d (no store, not persisted)",
                     s.conn_id, int(req.oldpin), int(req.newpin))
        await s.write(pr.make_reply(env, result=pr.Result.OK))

    async def _do_version(self, s: Session, env: pr.Envelope) -> None:
        vc = pb.VersionConfirm()
        vc.gatewayVersion = _GATEWAY_VERSION
        vc.serialNumber = _SERIAL_NUMBER
        vc.comfoNetVersion = _COMFONET_VERSION
        await s.write(pr.make_reply(env, body=vc, result=pr.Result.OK))

    async def _do_empty_id_confirm(self, s: Session, env: pr.Envelope) -> None:
        # Real LAN-C answers these with a confirm that has no fields
        # set. The app interprets missing uuid = "feature disabled".
        OpType = pr.OpType
        body_by_op = {
            OpType.GetRemoteAccessIdRequestType: pb.GetRemoteAccessIdConfirm(),
            OpType.GetSupportIdRequestType:      pb.GetSupportIdConfirm(),
            OpType.GetWebIdRequestType:          pb.GetWebIdConfirm(),
        }
        body = body_by_op[env.op_type]
        await s.write(pr.make_reply(env, body=body, result=pr.Result.OK))

    async def _do_cn_time(self, s: Session, env: pr.Envelope) -> None:
        tc = pb.CnTimeConfirm()
        tc.currentTime = int(time.time()) - _COMFONET_EPOCH_S
        await s.write(pr.make_reply(env, body=tc, result=pr.Result.OK))

    async def _do_not_allowed(self, s: Session, env: pr.Envelope) -> None:
        await s.write(pr.make_reply(env, result=pr.Result.NOT_ALLOWED))

    async def _do_rpdo(self, s: Session, env: pr.Envelope) -> None:
        """Subscribe the session to a pdid and emit a zero placeholder.

        Observed gateway pattern: CnRpdoConfirm (empty body) followed
        immediately by one CnRpdoNotification with zero-filled `data`
        sized per the `type` field, then the real value arrives later
        when the HRU's next PDO broadcast flows through. We replicate
        that same order — the app tolerates (and depends on) it.
        """
        req: pb.CnRpdoRequest | None = env.body  # type: ignore[assignment]
        if self._pdo_hub is None or req is None:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return

        pdid = int(req.pdid)
        zone = int(req.zone) if req.HasField("zone") else 1
        rpdo_type = int(req.type) if req.HasField("type") else 1
        first_subscription = pdid not in s.pdo_subs
        s.pdo_subs[pdid] = (zone, rpdo_type)
        if first_subscription:
            loop = asyncio.get_running_loop()
            self._pdo_hub.subscribe(
                s, pdid,
                lambda p, d, sess=s, z=zone, loop=loop:
                    loop.create_task(self._forward_pdo(sess, p, z, d)),
                rpdo_type=rpdo_type,
            )

        await s.write(pr.make_reply(env, result=pr.Result.OK))

        placeholder = pb.CnRpdoNotification()
        placeholder.pdid = pdid
        placeholder.zone = zone
        placeholder.data = bytes(payload_size_for_type(rpdo_type))
        await s.write(pr.make_notification(
            pr.OpType.CnRpdoNotificationType, placeholder,
        ))

        # Prompt the producer to re-broadcast the current value so the
        # app sees real data within ~tens of ms rather than having to
        # wait for the next on-change event. Matches the two-notification
        # pattern a real gateway emits per CnRpdoRequest.
        await self._pdo_hub.rtr_pdid(pdid)

    async def _forward_pdo(self, s: Session, pdid: int, zone: int,
                           data: bytes) -> None:
        """Relay a bus-side PDO broadcast to a subscribed session."""
        if s.close_requested or s.writer.is_closing():
            return
        notif = pb.CnRpdoNotification()
        notif.pdid = pdid
        notif.zone = zone
        notif.data = data
        try:
            await s.write(pr.make_notification(
                pr.OpType.CnRpdoNotificationType, notif,
            ))
        except Exception:
            log.exception("[c%d] failed to forward PDO pdid=%d",
                          s.conn_id, pdid)

    async def _do_rmi(self, s: Session, env: pr.Envelope) -> None:
        req: pb.CnRmiRequest | None = env.body  # type: ignore[assignment]
        if req is None or not req.message:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return
        # RMIs addressed to our own LAN-C node never reach the CAN bus
        # on a real system — the gateway answers them locally from its
        # own property table. We replicate the product-info reply and
        # NOT_REACHABLE the rest so the app stops waiting for a CAN
        # reply that can never come (and, critically, so it doesn't
        # rip the TCP session down after its 3 s watchdog expires).
        if req.nodeId == self._lanc_node:
            if bytes(req.message) == _LANC_SELF_PROPERTIES_REQUEST:
                body = pb.CnRmiResponse()
                # Leave body.result unset: proto2 `optional uint32` with
                # default=0 serializes a spurious `08 00` prefix when
                # explicitly set, which diverges from observed gateway
                # replies (no field 1 on success). Assigning `message`
                # directly keeps result at HasField()=False.
                body.message = _lanc_self_properties_response()
                log.info("[c%d] RMI to self (node=0x%02x) → product-info",
                         s.conn_id, req.nodeId)
                await s.write(pr.make_reply(env, body=body,
                                            result=pr.Result.OK))
                return
            log.info("[c%d] RMI to self (node=0x%02x, msg=%s) → NOT_REACHABLE",
                     s.conn_id, req.nodeId, bytes(req.message).hex())
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return
        if self._rmi is None:
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return
        try:
            is_err, payload = await self._rmi.request(
                req.nodeId, bytes(req.message),
            )
        except asyncio.TimeoutError:
            log.info("[c%d] RMI timeout (node=%d)", s.conn_id, req.nodeId)
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return
        except RmiFragmentLoss as e:
            log.warning("[c%d] RMI fragment loss (node=%d): %s",
                        s.conn_id, req.nodeId, e)
            await s.write(pr.make_reply(env, result=pr.Result.NOT_REACHABLE))
            return
        except Exception:
            log.exception("[c%d] RMI dispatch error", s.conn_id)
            await s.write(pr.make_reply(env, result=pr.Result.INTERNAL_ERROR))
            return
        body = pb.CnRmiResponse()
        # The CAN-layer `err` bit surfaces as the body-level result. The
        # outer GatewayOperation.result stays OK whenever we successfully
        # exchanged frames — the app distinguishes transport from RMI
        # errors by which field is non-zero. Leave `result` unset on
        # success: CnRmiResponse.result is proto2 `optional uint32` with
        # default=0, and explicitly assigning 0 serializes a spurious
        # `08 00` prefix. A real gateway omits field 1 on success.
        if is_err:
            body.result = 1
        body.message = payload
        await s.write(pr.make_reply(env, body=body, result=pr.Result.OK))
