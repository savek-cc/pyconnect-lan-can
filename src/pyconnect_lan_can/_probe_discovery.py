"""Minimal UDP discovery client, for roundtrip-testing the emulator.

Sends one `searchGatewayRequest` to <host>:<port>, waits up to
`timeout` seconds for the reply, and prints the decoded response.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid as _uuid

from ._proto import zehnder_pb2 as pb


async def probe(host: str, port: int, timeout: float = 2.0) -> int:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()

    class _DP(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):  # type: ignore[override]
            if not fut.done():
                fut.set_result((data, addr))

    transport, _ = await loop.create_datagram_endpoint(_DP, local_addr=("0.0.0.0", 0))
    try:
        req = pb.DiscoveryOperation()
        req.searchGatewayRequest.SetInParent()
        transport.sendto(req.SerializeToString(), (host, port))
        try:
            data, addr = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            print("no reply", file=sys.stderr)
            return 1
    finally:
        transport.close()

    op = pb.DiscoveryOperation()
    op.ParseFromString(data)
    if not op.HasField("searchGatewayResponse"):
        print("reply lacked searchGatewayResponse", file=sys.stderr)
        return 2
    r = op.searchGatewayResponse
    print(f"from={addr} uuid={_uuid.UUID(bytes=bytes(r.uuid))} "
          f"ip={r.ipaddress} version={r.version} "
          f"type={pb.SearchGatewayResponse.GatewayType.Name(r.type)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=56747)
    ap.add_argument("--timeout", type=float, default=2.0)
    args = ap.parse_args()
    return asyncio.run(probe(args.host, args.port, args.timeout))


if __name__ == "__main__":
    sys.exit(main())
