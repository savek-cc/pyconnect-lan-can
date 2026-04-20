#!/bin/sh
set -eu

: "${BRIDGE_HOST:?set BRIDGE_HOST to the ESP device IP running the ECB1 bridge}"

set -- --pin-file /data/lanc_state.json --bridge-host "$BRIDGE_HOST" "$@"

[ -n "${BRIDGE_PORT-}" ]            && set -- "$@" --bridge-port "$BRIDGE_PORT"
[ -n "${LANC_NODE-}" ]              && set -- "$@" --lanc-node "$LANC_NODE"
[ -n "${SRC_NODE-}" ]               && set -- "$@" --src-node "$SRC_NODE"
[ -n "${NMT_NODE-}" ]               && set -- "$@" --nmt-node "$NMT_NODE"
[ -n "${ADVERTISE_IP-}" ]           && set -- "$@" --advertise-ip "$ADVERTISE_IP"
[ -n "${IP_FOR-}" ]                 && set -- "$@" --ip-for "$IP_FOR"
[ -n "${UUID-}" ]                   && set -- "$@" --uuid "$UUID"
[ -n "${VERSION-}" ]                && set -- "$@" --version "$VERSION"
[ -n "${TCP_PORT-}" ]               && set -- "$@" --tcp-port "$TCP_PORT"
[ -n "${UDP_PORT-}" ]               && set -- "$@" --udp-port "$UDP_PORT"
[ -n "${GATEWAY_RMI_INTERVAL-}" ]   && set -- "$@" --gateway-rmi-interval "$GATEWAY_RMI_INTERVAL"
[ -n "${HEARTBEAT_INTERVAL-}" ]     && set -- "$@" --heartbeat-interval "$HEARTBEAT_INTERVAL"
[ "${NO_WARM_SWEEP-}" = "true" ]    && set -- "$@" --no-warm-sweep
[ "${VERBOSE-}" = "true" ]          && set -- "$@" --verbose

exec python -m pyconnect_lan_can.emulator "$@"
