"""JSON-backed emulator PIN store.

A real gateway persists the PIN across reboots and resets it to 0000
on factory reset. The emulator mirrors that with a JSON file: default
0 on first run, updated via `ChangePinRequestType`, verified against
`RegisterAppRequest.pin` and `ChangePinRequest.oldpin`. Delete the
file to simulate factory reset.

Writes are atomic (`os.replace` of a `.tmp` sibling) so a crash mid-
write can never leave a half-written JSON file that would fail to
load on next boot.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from threading import Lock

log = logging.getLogger(__name__)

DEFAULT_PIN = 0  # factory default
DEFAULT_STATE_FILE = Path("./lanc_state.json")


class PinStore:
    """Thread-safe PIN holder with lazy init and atomic persistence."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._lock = Lock()
        self._pin = self._load_or_init()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def pin(self) -> int:
        with self._lock:
            return self._pin

    def set_pin(self, new_pin: int) -> None:
        with self._lock:
            self._pin = int(new_pin) & 0xFFFFFFFF
            self._write(self._pin)
            log.info("PinStore: pin updated in %s", self._path)

    # ── internal ───────────────────────────────────────────────────────
    def _load_or_init(self) -> int:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text())
                pin = int(data["pin"]) & 0xFFFFFFFF
                log.info("PinStore: loaded pin from %s", self._path)
                return pin
            except (OSError, ValueError, KeyError,
                    json.JSONDecodeError) as e:
                log.warning(
                    "PinStore: %s unreadable (%s) — resetting to default",
                    self._path, e,
                )
        self._write(DEFAULT_PIN)
        log.info("PinStore: initialised %s with default pin", self._path)
        return DEFAULT_PIN

    def _write(self, pin: int) -> None:
        parent = self._path.parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(json.dumps({"pin": pin}))
        os.replace(tmp, self._path)
