"""The handoff contract round-trips across agents and across versions.

No Temporal here: these are the fast checks on the contract itself, plus a
front desk -> JSON -> follow-up agent round trip through real AgentSessions.
The cross-version test against a running workflow is in test_durability.py.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from livekit.agents import AgentSession
from pydantic import ValidationError

from durable_handoff.agents import FollowUpAgent, FrontDeskAgent, SessionState, build_context
from durable_handoff.contract import (
    CURRENT_VERSION,
    Contact,
    HandoffContext,
    Request,
    TaskStatus,
    Turn,
    UnsupportedContextVersion,
    dump_context,
    parse_context,
)
from durable_handoff.scripted_llm import Rule, ScriptedLLM, ToolCall, after_tool, last_user_says

T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)

V1_PAYLOAD: dict[str, Any] = {
    # What the first deployment wrote. Note: no schema_version field at all.
    "conversation_id": "conv-legacy",
    "user_name": "Ana Lima",
    "user_phone": "+5511987654321",
    "request_kind": "lease_renewal",
    "documents": ["signed lease"],
    "transcript": "user: I need my lease renewal checked\nassistant: Sure, send it over\nnoise line",
    "status": "submitted",
}


def sample() -> HandoffContext:
    return (
        HandoffContext(
            conversation_id="conv-1",
            contact=Contact(name="Dana Reyes", phone="+14155550123"),
            request=Request(kind="rental_application", documents=("pay stub", "photo id")),
            transcript=(
                Turn(role="user", text="I need my application verified", agent="front_desk", channel="voice"),
                Turn(role="assistant", text="Submitted, I'll text you", agent="front_desk", channel="voice"),
            ),
        )
        .handoff(to_agent="verifier", reason="long-running", at=T0)
        .with_task(status=TaskStatus.APPROVED, workflow_id="wf-1", detail="looks good", at=T0)
    )


def test_current_version_round_trips_through_json() -> None:
    ctx = sample()
    wire = json.dumps(dump_context(ctx))
    assert json.loads(wire)["schema_version"] == CURRENT_VERSION
    assert parse_context(wire) == ctx
    assert parse_context(dump_context(parse_context(wire))) == ctx  # stable under repeated round trips


def test_v1_payload_is_upcast_to_current() -> None:
    ctx = parse_context(V1_PAYLOAD)
    assert ctx.schema_version == CURRENT_VERSION
    assert ctx.contact == Contact(name="Ana Lima", phone="+5511987654321")
    assert ctx.request.kind == "lease_renewal" and ctx.request.documents == ("signed lease",)
    assert [(t.role, t.text) for t in ctx.transcript] == [
        ("user", "I need my lease renewal checked"),
        ("assistant", "Sure, send it over"),
    ]  # the unparseable line is dropped, not turned into garbage
    assert ctx.task.status is TaskStatus.SUBMITTED
    # once upcast, it round-trips as current
    assert parse_context(dump_context(ctx)) == ctx


def test_explicit_v1_version_field_is_accepted() -> None:
    assert parse_context({**V1_PAYLOAD, "schema_version": 1}) == parse_context(V1_PAYLOAD)


def test_newer_version_than_reader_is_refused_loudly() -> None:
    future = {**dump_context(sample()), "schema_version": CURRENT_VERSION + 1, "new_field": 1}
    with pytest.raises(UnsupportedContextVersion, match="deploy readers before writers"):
        parse_context(future)


def test_unknown_fields_are_rejected_not_silently_dropped() -> None:
    with pytest.raises(ValidationError):
        parse_context({**dump_context(sample()), "surprise": True})


def test_invalid_phone_is_rejected_at_the_boundary() -> None:
    with pytest.raises(ValidationError):
        Contact(name="x", phone="555-0123")


def test_hops_form_an_audit_trail() -> None:
    ctx = sample().handoff(to_agent="follow_up", reason="done", at=T0)
    assert [(h.from_agent, h.to_agent) for h in ctx.hops] == [
        ("front_desk", "verifier"),
        ("verifier", "follow_up"),
    ]
    assert ctx.current_agent == "follow_up"


async def test_context_round_trips_from_front_desk_session_to_follow_up_agent() -> None:
    """Front desk session -> HandoffContext -> JSON (the process boundary) ->
    FollowUpAgent in a brand-new session. The follow-up model must see the
    front desk conversation, without tool-call noise."""
    front_llm = ScriptedLLM(
        [
            Rule(
                last_user_says("Dana"),
                tool_calls=[ToolCall("record_contact", {"name": "Dana Reyes", "phone": "+14155550123"})],
            ),
            Rule(after_tool("record_contact"), text="Thanks Dana. What should we verify?"),
        ]
    )
    state = SessionState(conversation_id="conv-rt", temporal=None, channel="voice")  # type: ignore[arg-type]
    async with front_llm, AgentSession[SessionState](llm=front_llm, userdata=state) as session:
        await session.start(FrontDeskAgent())
        await session.run(user_input="Hi, this is Dana, my number is +14155550123")
        ctx = build_context(state, session.history, kind="rental_application", documents=["pay stub"])

    wire = json.dumps(dump_context(ctx))  # the only thing that crosses to the next agent

    restored = parse_context(wire).with_task(status=TaskStatus.APPROVED, workflow_id="wf-x", at=T0)
    follow_llm = ScriptedLLM([Rule(last_user_says("status"), text="It was approved.")])
    follow_state = SessionState(conversation_id=restored.conversation_id, temporal=None, prior=restored)  # type: ignore[arg-type]
    async with follow_llm, AgentSession[SessionState](llm=follow_llm, userdata=follow_state) as session:
        await session.start(FollowUpAgent(restored))
        result = await session.run(user_input="what's the status?")
        result.expect.next_event().is_message(role="assistant")

    seen = follow_llm.requests[-1]
    texts = [i.text_content for i in seen.items if i.type == "message" and i.role in ("user", "assistant")]
    assert texts[:2] == ["Hi, this is Dana, my number is +14155550123", "Thanks Dana. What should we verify?"]
    assert not any(i.type == "function_call" for i in seen.items[:-1]), (
        "tool calls must not leak across agents"
    )
    instructions = seen.items[0].text_content if seen.items[0].type == "message" else ""
    assert instructions is not None and "approved" in instructions and "Dana Reyes" in instructions
