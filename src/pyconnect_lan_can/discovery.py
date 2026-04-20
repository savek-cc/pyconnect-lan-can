"""UDP discovery responder for ComfoConnect on port 56747.

The Android app broadcasts a `DiscoveryOperation{searchGatewayRequest}`
envelope; the LAN-C (or this emulator impersonating one) replies with
`DiscoveryOperation{searchGatewayResponse{uuid, ipaddress, version,
type=lanc}}` unicast back to the broadcaster.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import uuid as _uuid
from dataclasses import dataclass
from typing import Tuple

from ._proto import zehnder_pb2 as pb
from .capture import JsonlSink

DISCOVERY_PORT = 56747

log = logging.getLogger(__name__)


def local_ip_toward(target: str) -> str:
    """Return the local IP the kernel would use to reach `target`.

    Does not actually send anything — connect() on UDP just sets the peer
    which triggers source-address selection.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((target, 1))
        return s.getsockname()[0]
    finally:
        s.close()


@dataclass
class DiscoveryIdentity:
    """What we advertise on the bus. uuid is 16 raw bytes per protobuf."""
    uuid: bytes
    ipaddress: str
    version: int = 1
    gateway_type: int = pb.SearchGatewayResponse.GatewayType.Value("lanc")

    def __post_init__(self) -> None:
        if len(self.uuid) != 16:
            raise ValueError(f"uuid must be 16 bytes, got {len(self.uuid)}")


class DiscoveryResponder(asyncio.DatagramProtocol):
    def __init__(self, identity: DiscoveryIdentity,
                 capture: JsonlSink | None = None):
        self.identity = identity
        self.capture = capture
        self.transport: asyncio.DatagramTransport | None = None
        self.replies = 0

    def connection_made(self, transport):  # type: ignore[override]
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        op = pb.DiscoveryOperation()
        parsed: pb.DiscoveryOperation | None
        try:
            op.ParseFromString(data)
            parsed = op
        except Exception as e:
            log.debug("UDP ← %s undecodable (%d bytes): %s", addr, len(data), e)
            parsed = None
        if self.capture is not None:
            self.capture.udp(kind="recv", peer=addr, data=data, decoded=parsed)
        if parsed is None or not op.HasField("searchGatewayRequest"):
            if parsed is not None:
                log.debug("UDP ← %s not a search request, ignoring", addr)
            return
        resp = pb.DiscoveryOperation()
        resp.searchGatewayResponse.uuid = self.identity.uuid
        resp.searchGatewayResponse.ipaddress = self.identity.ipaddress
        resp.searchGatewayResponse.version = self.identity.version
        resp.searchGatewayResponse.type = self.identity.gateway_type
        payload = resp.SerializeToString()
        assert self.transport is not None
        self.transport.sendto(payload, addr)
        self.replies += 1
        if self.capture is not None:
            self.capture.udp(kind="send", peer=addr, data=payload, decoded=resp)
        log.info("UDP ← %s SearchGatewayRequest — replying uuid=%s ip=%s",
                 addr, _uuid.UUID(bytes=self.identity.uuid),
                 self.identity.ipaddress)


async def serve_discovery(identity: DiscoveryIdentity,
                          bind_host: str = "0.0.0.0",
                          port: int = DISCOVERY_PORT,
                          capture: JsonlSink | None = None,
                          ) -> Tuple[asyncio.DatagramTransport, DiscoveryResponder]:
    """Bind the discovery UDP socket and return the running transport+proto.

    Caller is responsible for calling transport.close() at shutdown.
    """
    loop = asyncio.get_running_loop()
    transport, proto = await loop.create_datagram_endpoint(
        lambda: DiscoveryResponder(identity, capture=capture),
        local_addr=(bind_host, port),
        allow_broadcast=True,
        reuse_port=False,
    )
    log.info("discovery bound on %s:%d as uuid=%s ip=%s",
             bind_host, port, _uuid.UUID(bytes=identity.uuid),
             identity.ipaddress)
    return transport, proto  # type: ignore[return-value]
