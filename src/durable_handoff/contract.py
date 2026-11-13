"""The handoff contract: the one object every agent in this example agrees on.

Three agents touch a request over its life, often in different processes and
hours apart:

    FrontDeskAgent (live session) -> verification workflow (Temporal) -> FollowUpAgent (SMS)

They never share memory. What they share is a ``HandoffContext``: a typed,
versioned, JSON-serializable snapshot of who the user is, what they asked for,
what was said, and where the task stands.

Compatibility rules (see README "Patterns"):

1. Writers always emit ``CURRENT_VERSION``.
2. Readers accept every version back to 1 and upcast to current on read
   (``parse_context``). A workflow started by last week's deployment carries a
   v1 payload in its Temporal history; today's worker must still run it.
3. A version newer than the reader knows is an explicit error
   (``UnsupportedContextVersion``), never a silent best-effort parse. That makes
   "roll out readers before writers" enforceable.
4. The transcript is stored as plain ``Turn`` records, not as the framework's
   ``ChatContext.to_dict()`` output. That dict is an internal format with no
   version field; ``Turn`` is ours and stable. ``to_chat_ctx``/``from_chat_ctx``
   convert at the edges.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

CURRENT_VERSION = 2


class UnsupportedContextVersion(ValueError):
    pass


class TaskStatus(StrEnum):
    COLLECTING = "collecting"  # front desk still talking to the user
    SUBMITTED = "submitted"  # handed to the background workflow
    VERIFYING = "verifying"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"  # nobody decided before the approval deadline

    @property
    def is_final(self) -> bool:
        return self in (TaskStatus.APPROVED, TaskStatus.REJECTED, TaskStatus.EXPIRED)


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Turn(_Model):
    role: Literal["user", "assistant"]
    text: str
    agent: str  # which agent said it / heard it, e.g. "front_desk"
    channel: Literal["voice", "text", "sms"]


class Contact(_Model):
    name: str
    phone: str = Field(pattern=r"^\+[1-9]\d{6,14}$", description="E.164")


class Request(_Model):
    kind: str = Field(description="what the user wants verified, e.g. 'rental_application'")
    documents: tuple[str, ...] = ()
    notes: str = ""


class TaskState(_Model):
    status: TaskStatus = TaskStatus.COLLECTING
    workflow_id: str | None = None
    detail: str = ""  # human-readable outcome, e.g. the approver's reason
    updated_at: datetime | None = None


class Hop(_Model):
    """One handoff between agents. The list of hops is an audit trail."""

    from_agent: str
    to_agent: str
    at: datetime
    reason: str


class HandoffContextV1(_Model):
    """The first shipped shape. Flat contact fields, transcript as one string.

    Kept so old payloads (in Temporal histories, in the store) stay readable.
    New code never writes it.
    """

    schema_version: Literal[1] = 1
    conversation_id: str
    user_name: str
    user_phone: str
    request_kind: str
    documents: list[str] = []
    transcript: str = ""  # "user: ...\nassistant: ..." lines
    status: str = "submitted"
    workflow_id: str | None = None


class HandoffContextV2(_Model):
    schema_version: Literal[2] = 2
    conversation_id: str
    contact: Contact
    request: Request
    transcript: tuple[Turn, ...] = ()
    task: TaskState = TaskState()
    hops: tuple[Hop, ...] = ()

    # --- the operations agents actually perform -----------------------------

    def handoff(self, *, to_agent: str, reason: str, at: datetime | None = None) -> HandoffContextV2:
        """Record that control of this conversation moves to ``to_agent``."""
        from_agent = self.hops[-1].to_agent if self.hops else "front_desk"
        hop = Hop(from_agent=from_agent, to_agent=to_agent, at=at or _now(), reason=reason)
        return self.model_copy(update={"hops": (*self.hops, hop)})

    def with_task(self, *, at: datetime | None = None, **changes: Any) -> HandoffContextV2:
        task = self.task.model_copy(update={**changes, "updated_at": at or _now()})
        return self.model_copy(update={"task": task})

    def with_turns(self, *turns: Turn) -> HandoffContextV2:
        return self.model_copy(update={"transcript": (*self.transcript, *turns)})

    @property
    def current_agent(self) -> str:
        return self.hops[-1].to_agent if self.hops else "front_desk"


HandoffContext = HandoffContextV2
"""Alias for the current version. Import this, not a numbered class."""

_AnyVersion = Annotated[HandoffContextV1 | HandoffContextV2, Field(discriminator="schema_version")]
_any_version: TypeAdapter[HandoffContextV1 | HandoffContextV2] = TypeAdapter(_AnyVersion)


def _upcast_v1(old: HandoffContextV1) -> HandoffContextV2:
    turns: list[Turn] = []
    for line in old.transcript.splitlines():
        role, _, text = line.partition(":")
        if role.strip() in ("user", "assistant") and text.strip():
            turns.append(
                Turn(role=role.strip(), text=text.strip(), agent="front_desk", channel="voice")  # type: ignore[arg-type]
            )
    try:
        status = TaskStatus(old.status)
    except ValueError:
        status = TaskStatus.SUBMITTED
    return HandoffContextV2(
        conversation_id=old.conversation_id,
        contact=Contact(name=old.user_name, phone=old.user_phone),
        request=Request(kind=old.request_kind, documents=tuple(old.documents)),
        transcript=tuple(turns),
        task=TaskState(status=status, workflow_id=old.workflow_id),
    )


def parse_context(data: dict[str, Any] | str | bytes) -> HandoffContext:
    """Read any known version and return the current one."""
    raw: Any = data
    if isinstance(data, (str, bytes)):
        import json

        raw = json.loads(data)
    if not isinstance(raw, dict):
        raise TypeError("handoff context must be a JSON object")
    version = raw.get("schema_version", 1)  # v1 payloads predate the field being required
    if not isinstance(version, int) or version < 1:
        raise UnsupportedContextVersion(f"bad schema_version: {version!r}")
    if version > CURRENT_VERSION:
        raise UnsupportedContextVersion(
            f"context is v{version}, this reader understands up to v{CURRENT_VERSION}; "
            "deploy readers before writers"
        )
    try:
        parsed = _any_version.validate_python({**raw, "schema_version": version})
    except ValidationError:
        raise
    return _upcast_v1(parsed) if isinstance(parsed, HandoffContextV1) else parsed


def dump_context(ctx: HandoffContext) -> dict[str, Any]:
    return ctx.model_dump(mode="json")


def _now() -> datetime:
    return datetime.now(UTC)
