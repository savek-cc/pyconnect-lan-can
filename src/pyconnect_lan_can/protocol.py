"""OperationType ↔ protobuf message mapping.

This layer knows protobuf types and operation enums, nothing about
sockets or CAN. It turns wire-level (operation_bytes, body_bytes) into
typed `Envelope` objects and back.
"""
from __future__ import annotations

from dataclasses import dataclass

from google.protobuf.message import Message

from ._proto import zehnder_pb2 as pb

OpType = pb.GatewayOperation.OperationType
Result = pb.GatewayOperation.GatewayResult


# ── OperationType → (direction, body_class) ─────────────────────────────────
# direction:
#   "req"  — request from app to gateway; body is the Request message
#   "cnf"  — confirm/response from gateway to app; body is the Confirm message
#   "ntf"  — notification (unsolicited, gateway → app); body is the Notification
#   "req_empty" — request that carries no body (e.g. CnNodeRequest, KeepAlive)
#
# Body can be None if the OperationType carries no payload (KeepAlive).
# This table only contains in-scope types. Anything else is reported as
# NOT_ALLOWED at the session layer.
_TYPE_MAP: dict[int, tuple[str, type[Message] | None]] = {
    # Pairing + session
    OpType.SetAddressRequestType:    ("req", pb.SetAddressRequest),
    OpType.SetAddressConfirmType:    ("cnf", pb.SetAddressConfirm),
    OpType.RegisterAppRequestType:   ("req", pb.RegisterAppRequest),
    OpType.RegisterAppConfirmType:   ("cnf", pb.RegisterAppConfirm),
    OpType.StartSessionRequestType:  ("req", pb.StartSessionRequest),
    OpType.StartSessionConfirmType:  ("cnf", pb.StartSessionConfirm),
    OpType.CloseSessionRequestType:  ("req", pb.CloseSessionRequest),
    OpType.CloseSessionConfirmType:  ("cnf", pb.CloseSessionConfirm),
    OpType.ChangePinRequestType:     ("req", pb.ChangePinRequest),
    OpType.ChangePinConfirmType:     ("cnf", pb.ChangePinConfirm),

    # Backend-access identity slots. The Android app polls these right
    # after StartSession; the real LAN C answers with an empty Confirm
    # (no uuid set) when the slot is unused, and the app treats that as
    # "feature disabled". We do the same — nothing to persist.
    OpType.GetRemoteAccessIdRequestType: ("req_empty", None),
    OpType.GetRemoteAccessIdConfirmType: ("cnf", pb.GetRemoteAccessIdConfirm),
    OpType.GetSupportIdRequestType:      ("req_empty", None),
    OpType.GetSupportIdConfirmType:      ("cnf", pb.GetSupportIdConfirm),
    OpType.GetWebIdRequestType:          ("req_empty", None),
    OpType.GetWebIdConfirmType:          ("cnf", pb.GetWebIdConfirm),

    # Version
    OpType.VersionRequestType:  ("req_empty", None),
    OpType.VersionConfirmType:  ("cnf", pb.VersionConfirm),

    # Keepalive — no body, no confirm type (KeepAlive is a one-way ping)
    OpType.KeepAliveType:       ("req_empty", None),

    # ComfoNet core
    OpType.CnTimeRequestType:       ("req", pb.CnTimeRequest),
    OpType.CnTimeConfirmType:       ("cnf", pb.CnTimeConfirm),
    OpType.CnNodeRequestType:       ("req_empty", None),
    OpType.CnNodeNotificationType:  ("ntf", pb.CnNodeNotification),

    OpType.CnRmiRequestType:   ("req", pb.CnRmiRequest),
    OpType.CnRmiResponseType:  ("cnf", pb.CnRmiResponse),

    OpType.CnRpdoRequestType:       ("req", pb.CnRpdoRequest),
    OpType.CnRpdoConfirmType:       ("cnf", pb.CnRpdoConfirm),
    OpType.CnRpdoNotificationType:  ("ntf", pb.CnRpdoNotification),

    OpType.CnAlarmNotificationType: ("ntf", pb.CnAlarmNotification),

    # Notifications (wraps alarm with push-UUIDs — v1 gateway does not emit)
    OpType.GatewayNotificationType: ("ntf", pb.GatewayNotification),
}


def body_class_for(op_type: int) -> type[Message] | None:
    """Return the body pb class for an OperationType, or None if no body."""
    entry = _TYPE_MAP.get(op_type)
    if entry is None:
        return None
    return entry[1]


def is_known(op_type: int) -> bool:
    return op_type in _TYPE_MAP


# Every request type → its paired confirm/response type (used when
# answering or composing the outer GatewayOperation.type on replies).
REQUEST_TO_CONFIRM: dict[int, int] = {
    OpType.SetAddressRequestType:    OpType.SetAddressConfirmType,
    OpType.RegisterAppRequestType:   OpType.RegisterAppConfirmType,
    OpType.StartSessionRequestType:  OpType.StartSessionConfirmType,
    OpType.CloseSessionRequestType:  OpType.CloseSessionConfirmType,
    OpType.ChangePinRequestType:     OpType.ChangePinConfirmType,
    OpType.GetRemoteAccessIdRequestType: OpType.GetRemoteAccessIdConfirmType,
    OpType.GetSupportIdRequestType:      OpType.GetSupportIdConfirmType,
    OpType.GetWebIdRequestType:          OpType.GetWebIdConfirmType,
    OpType.VersionRequestType:       OpType.VersionConfirmType,
    OpType.CnTimeRequestType:        OpType.CnTimeConfirmType,
    OpType.CnRmiRequestType:         OpType.CnRmiResponseType,
    OpType.CnRpdoRequestType:        OpType.CnRpdoConfirmType,
}


@dataclass
class Envelope:
    """Header + body pair — the decoded form of a wire Frame.

    `op_type`, `result`, `reference` mirror the header fields. `body` is
    the decoded pb message (or None for bodiless ops like KeepAlive).
    `result_description` is set only on error replies. `reference=None`
    means the field was absent on the wire (proto2 presence); notifications
    must leave it None, replies must echo the request value verbatim —
    the Android app drops replies where `hasReference()` is false and drops
    notifications where it is true (see GatewayConnection.onMessageReceived).
    """
    op_type: int
    body: Message | None = None
    result: int = Result.OK
    reference: int | None = None
    result_description: str = ""

    def encode_header(self) -> bytes:
        op = pb.GatewayOperation()
        op.type = self.op_type
        op.result = self.result
        if self.reference is not None:
            op.reference = self.reference
        if self.result_description:
            op.resultDescription = self.result_description
        return op.SerializeToString()

    def encode_body(self) -> bytes:
        if self.body is None:
            return b""
        return self.body.SerializeToString()


def decode(operation_bytes: bytes, body_bytes: bytes) -> Envelope:
    """Parse (Frame.operation, Frame.body) into a typed Envelope.

    Unknown OperationType values are preserved so the session layer can
    reject them with NOT_ALLOWED.
    """
    op = pb.GatewayOperation()
    op.ParseFromString(operation_bytes)
    op_type = op.type
    body: Message | None = None
    body_cls = body_class_for(op_type)
    if body_cls is not None and body_bytes:
        body = body_cls()
        body.ParseFromString(body_bytes)
    elif body_cls is None and body_bytes:
        # OperationType has no body but the frame carries bytes — tolerate
        # and ignore (matches real-gateway leniency).
        pass
    return Envelope(
        op_type=op_type,
        body=body,
        result=op.result,
        reference=op.reference if op.HasField("reference") else None,
        result_description=op.resultDescription,
    )


# ── Convenience constructors ────────────────────────────────────────────────
def make_reply(
    request: Envelope,
    *,
    body: Message | None = None,
    result: int = Result.OK,
    description: str = "",
    op_type: int | None = None,
) -> Envelope:
    """Build the response envelope for a received request.

    Echoes `reference`. If `op_type` is not given, uses
    REQUEST_TO_CONFIRM[request.op_type]. The caller is responsible for
    providing the right `body` pb type for that op.
    """
    if op_type is None:
        op_type = REQUEST_TO_CONFIRM.get(request.op_type)
        if op_type is None:
            raise KeyError(
                f"no confirm/response type for request {request.op_type}"
            )
    return Envelope(
        op_type=op_type,
        body=body,
        result=result,
        reference=request.reference,
        result_description=description,
    )


def make_notification(op_type: int, body: Message) -> Envelope:
    """Build an unsolicited notification envelope (no reference).

    The absent reference tells the app this is a notification rather
    than an orphaned reply (see Envelope docstring).
    """
    return Envelope(op_type=op_type, body=body, result=Result.OK, reference=None)


def op_name(op_type: int) -> str:
    """Human-readable name for an OperationType enum value (for logs)."""
    try:
        return OpType.Name(op_type)
    except ValueError:
        return f"OperationType({op_type})"


# Re-exported for callers that want to build pb messages without importing
# zehnder_pb2 themselves.
__all__ = [
    "Envelope",
    "OpType",
    "Result",
    "REQUEST_TO_CONFIRM",
    "body_class_for",
    "decode",
    "is_known",
    "make_notification",
    "make_reply",
    "op_name",
    "pb",
]
