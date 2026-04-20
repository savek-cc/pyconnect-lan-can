# pyconnect-lan-can

A software emulator for the Zehnder **ComfoConnect LAN C** gateway.

The real LAN C is the Wi-Fi box that bridges a Zehnder ComfoAir Q ventilation
unit to the *ComfoControl* Android app. This project reproduces the gateway
side of that conversation in Python, so the app sees a working "gateway"
while the other end speaks raw CAN to the HRU through an ESP32 running the
companion ECB1 CAN bridge firmware.

```
  ComfoControl app        pyconnect-lan-can            ESP32 (ecb1_bridge)    ComfoAir Q
  ┌──────────────┐   TCP  ┌──────────────────┐   TCP   ┌─────────────────┐    ┌────────┐
  │  56747       │◀──────▶│  emulator        │◀───────▶│  CAN<->TCP      │◀──▶│  CAN   │
  │  56747/UDP   │◀──────▶│                  │  28081  │  (ECB1 v1)      │    │        │
  └──────────────┘        └──────────────────┘         └─────────────────┘    └────────┘
```

The companion ESPHome firmware with the matching bridge component lives at
<https://github.com/savek-cc/esphome-zehnder-comfoair/tree/ecb1-bridge>. The
wire format between the emulator and the ESP32 is documented in
[`esphome-zehnder-comfoair/docs/ecb1-protocol.md`](https://github.com/savek-cc/esphome-zehnder-comfoair/blob/ecb1-bridge/docs/ecb1-protocol.md).

## What it does

- **UDP discovery (port 56747)** — answers the app's `SearchGatewayRequest`
  broadcast so the gateway shows up in the app's device list.
- **TCP session (port 56747)** — framed protobuf envelopes, with Start /
  RegisterApp / CloseSession / Keepalive / ChangePin handled in-process.
- **RMI passthrough** — `CnRmiRequest` payloads are forwarded verbatim to
  the bus as ComfoNet RMIs; replies are reassembled and returned to the
  app with the envelope's `reference` preserved. Handles multi-frame
  fragmentation and retries on lost fragments.
- **PDO subscribe / forward** — `CnRpdoRequest` subscriptions are tracked
  per session; matching PDO frames from the bus are wrapped in
  `CnRpdoNotification` messages and fanned out to interested sessions.
  Every subscribe is answered first with a zero-filled placeholder, then
  the real value once the next broadcast lands, matching the real
  gateway's behaviour.
- **PDO warm-sweep** — on every app connect, RTR-sweeps the known
  subscription set so cached values are fresh before the app starts
  reading them.
- **NMT heartbeat + presence reply** — advertises the emulated LAN-C on
  the CAN bus and answers the HRU's presence pings, so other devices on
  the bus continue to see the gateway.
- **Gateway-originated RMIs** — reproduces the 60 s-cadence
  `TEMPHUMCONTROL` / `VENTILATIONCONFIG` queries the real gateway emits.
- **PIN persistence** — `ChangePinRequest` is honoured and the new PIN
  is stored locally; delete the state file to simulate a factory reset.
- **Session takeover** — `StartSessionRequest.takeover=true` evicts the
  prior session (matches real gateway behaviour).
- **Optional JSONL capture** — `--capture` writes a line-delimited
  capture of UDP datagrams and TCP envelopes with wall + monotonic
  timestamps for offline analysis.
- **python-can backend** — the TCP bridge is exposed as a python-can
  interface (`can.Bus(interface="ecb1", host=..., port=28081)`), so the
  bridge can also be used directly by unrelated python-can tooling.

Firmware update and remote/cloud features of the real gateway are
intentionally out of scope.

## Requirements

- Python **3.10** or newer
- [python-can](https://pypi.org/project/python-can/) 4.3+
- [protobuf](https://pypi.org/project/protobuf/) 4.25+
- An ESP32 running the ECB1 bridge from the ESPHome project linked above,
  wired to the HRU's CAN bus at 50 kbps.

## Install

```sh
git clone https://github.com/savek-cc/pyconnect-lan-can.git
cd pyconnect-lan-can
pip install -e .
```

Installing the package also registers the ECB1 python-can backend via
the `can.interface` entry point.

## Quick start

1. Flash the ECB1 bridge firmware onto an ESP32 wired to the CAN bus.
   Use the `ecb1-bridge` branch of the companion repo and any of the
   board YAMLs (they all include the `ecb1` package now):

   ```sh
   git clone -b ecb1-bridge https://github.com/savek-cc/esphome-zehnder-comfoair.git
   cd esphome-zehnder-comfoair
   cp secrets.yaml.example secrets.yaml   # then edit
   make compile upload BOARD=esp32-evb-eth
   ```

2. Run the emulator, pointing it at the ESP's IP:

   ```sh
   python -m pyconnect_lan_can.emulator --bridge-host <esp-ip>
   ```

   Useful flags:

   - `--advertise-ip <ip>` — IP to publish in the UDP discovery reply
     (auto-detected if omitted).
   - `--lanc-node <id>` — CAN node ID to impersonate. Default `0x0C`.
   - `--capture /tmp/lanc.jsonl` — write a JSONL session capture.
   - `-v` — debug logging.

3. Open the ComfoControl Android app on a device that can reach the
   host the emulator is running on. The emulated gateway appears in the
   device list; pair with PIN `0` by default (or whatever PIN was last
   persisted).

## Docker

A `Dockerfile` and `docker-compose.yaml` are included. The compose file
uses host networking (needed so the emulator receives the app's UDP
discovery broadcast) and mounts `./config` for PIN persistence.

```sh
cp .env.example .env   # then edit BRIDGE_HOST
docker compose up -d --build
```

All CLI flags are available as env vars (see `.env.example`); the
container writes `lanc_state.json` into `./config/`.

## Using the python-can backend on its own

Once the package is installed, any python-can tool can talk through the
ECB1 bridge without running the emulator:

```python
import can

bus = can.Bus(interface="ecb1", host="192.0.2.10", port=28081)
for msg in bus:
    print(msg)
```

The `CONTROL` role is assigned to the first client; subsequent clients
get `MONITOR` and receive RX + echoed TX frames read-only.

## Status

Development preview. Interfaces and CLI flags may change without notice
until a tagged release.

## License

MIT.
