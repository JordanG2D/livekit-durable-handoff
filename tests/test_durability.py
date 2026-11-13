"""Fault injection. Every scenario must end with exactly one user notification
and the right final state.

The worker is a real subprocess, killed with SIGKILL (no cleanup, no goodbye).
State lives in the Temporal dev server, the fake SMS gateway's SQLite file and
the conversation store, never in the worker.
"""

from __future__ import annotations

from typing import Any

import pytest
from livekit.agents import AgentSession
from temporalio.client import WorkflowExecutionStatus, WorkflowHandle

from durable_handoff.agents import FrontDeskAgent, SessionState, start_verification
from durable_handoff.contract import HandoffContext, TaskStatus, parse_context
from durable_handoff.scripted_llm import Rule, ScriptedLLM, ToolCall, after_tool, last_user_says
from durable_handoff.sms_channel import SmsChannel
from durable_handoff.workflow import Decision, VerificationResult, VerificationWorkflow

from .conftest import FAST, Harness, eventually, serve
from .test_contract import V1_PAYLOAD

pytestmark = pytest.mark.integration

PHONE = "+14155550123"


def front_desk_llm() -> ScriptedLLM:
    return ScriptedLLM(
        [
            Rule(
                last_user_says("Dana"),
                tool_calls=[ToolCall("record_contact", {"name": "Dana Reyes", "phone": PHONE})],
            ),
            Rule(
                after_tool("record_contact"),
                tool_calls=[
                    ToolCall(
                        "submit_for_verification",
                        {
                            "kind": "rental_application",
                            "documents": ["pay stub", "photo id", "reference letter"],
                        },
                    )
                ],
            ),
            Rule(
                after_tool("submit_for_verification"), text="You're all set. We'll text you when it's done."
            ),
        ]
    )


async def run_front_desk(h: Harness, conversation_id: str, *, user_input: str | None = None) -> SessionState:
    """A complete live session that submits a request and then *ends*."""
    state = SessionState(
        conversation_id=conversation_id,
        temporal=h.client,
        channel="voice",
        task_queue=h.task_queue,
        timings=FAST,
    )
    model = front_desk_llm()
    async with model, AgentSession[SessionState](llm=model, userdata=state) as session:
        await session.start(FrontDeskAgent())
        result = await session.run(
            user_input=user_input
            or f"Hi, I'm Dana Reyes, {PHONE}. Please verify my rental application: "
            "pay stub, photo id, reference letter."
        )
        result.expect.contains_function_call(name="submit_for_verification")
    assert state.workflow_ids, "front desk should have started a workflow"
    return state


def handle(h: Harness, wf_id: str) -> WorkflowHandle[Any, Any]:
    return h.client.get_workflow_handle_for(VerificationWorkflow.run, wf_id)


async def status_of(h: Harness, wf_id: str) -> TaskStatus | None:
    raw = await handle(h, wf_id).query(VerificationWorkflow.context)
    return parse_context(raw).task.status if raw else None


async def heartbeat_progress(h: Harness, wf_id: str) -> int:
    desc = await handle(h, wf_id).describe()
    for act in desc.raw_description.pending_activities:
        if act.activity_type.name == "verify_documents" and act.heartbeat_details.payloads:
            return int(act.heartbeat_details.payloads[0].data)
    return 0


async def approve(
    h: Harness, wf_id: str, event_id: str = "evt-1", approved: bool = True, reason: str = "income verified"
) -> None:
    await handle(h, wf_id).signal(
        VerificationWorkflow.decide, Decision(event_id=event_id, approved=approved, reason=reason)
    )


async def finish(h: Harness, wf_id: str) -> tuple[VerificationResult, HandoffContext]:
    result: VerificationResult = await handle(h, wf_id).result()
    return result, parse_context(result.context)


def assert_exactly_one_notification(h: Harness, *, contains: str) -> None:
    sms = h.gateway.outbox(PHONE)
    assert len(sms) == 1, f"expected exactly one SMS, got {[m.body for m in sms]}"
    assert contains in sms[0].body


# --- the scenarios -------------------------------------------------------------


async def test_session_ends_before_task_finishes_then_user_is_texted_and_can_reply(
    harness: Harness, conversation_id: str
) -> None:
    h = harness
    h.start_worker()
    state = await run_front_desk(h, conversation_id)  # returns only after the session has closed
    wf_id = state.workflow_ids[0]

    # Session is gone; the task is not.
    assert (await handle(h, wf_id).describe()).status == WorkflowExecutionStatus.RUNNING
    assert h.gateway.outbox(PHONE) == []

    await eventually(lambda: status_of(h, wf_id), timeout=30)
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, wf_id)
    result, ctx = await finish(h, wf_id)

    assert ctx.task.status is TaskStatus.APPROVED
    assert [hop.to_agent for hop in ctx.hops] == ["verifier", "follow_up"]
    assert_exactly_one_notification(h, contains="approved")

    # The user replies by SMS, through the gateway's webhook, hours "later".
    follow_llm = ScriptedLLM(
        [Rule(last_user_says("which"), text="Your rental application, with 3 documents.")]
    )
    channel = SmsChannel(
        store=h.store(), gateway=h.gateway_client, temporal=h.client, llm_factory=lambda: follow_llm
    )
    url, cleanup = await serve(channel.routes())
    h.gateway.webhook_url = f"{url}/sms/inbound"
    try:
        await h.gateway_client.simulate_inbound(from_phone=PHONE, body="which application was that?")
    finally:
        await cleanup()  # type: ignore[misc]

    bodies = [m.body for m in h.gateway.outbox(PHONE)]
    assert bodies[1:] == ["Your rental application, with 3 documents."]
    # The follow-up agent saw the front desk conversation from the earlier, closed session.
    seen = [i.text_content for i in follow_llm.requests[-1].items if i.type == "message"]
    assert any("Please verify my rental application" in (t or "") for t in seen)
    assert any(bodies[0] == t for t in seen), "the follow-up agent knows what it already texted"
    stored = h.store().get(conversation_id)
    assert stored is not None and stored.transcript[-1].text == bodies[1]


async def test_worker_killed_mid_verification_resumes_from_heartbeat(
    harness: Harness, conversation_id: str
) -> None:
    h = harness
    first = h.start_worker()
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]

    await eventually(lambda: heartbeat_progress(h, wf_id), timeout=30)  # at least one document done
    h.kill(first)  # SIGKILL mid-activity
    assert h.gateway.outbox(PHONE) == []

    h.start_worker()
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=60)
    await approve(h, wf_id)
    result, ctx = await finish(h, wf_id)

    assert ctx.task.status is TaskStatus.APPROVED
    assert result.verify_resumed_from >= 1, "retry should continue from the heartbeat, not start over"
    assert_exactly_one_notification(h, contains="approved")


async def test_worker_killed_while_waiting_for_approval_and_decision_arrives_while_down(
    harness: Harness, conversation_id: str
) -> None:
    h = harness
    first = h.start_worker()
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)

    h.kill(first)
    await approve(h, wf_id)  # no worker running: the signal is durably recorded anyway

    h.start_worker()
    _, ctx = await finish(h, wf_id)
    assert ctx.task.status is TaskStatus.APPROVED
    assert_exactly_one_notification(h, contains="approved")


async def test_worker_dies_after_sms_sent_but_before_activity_completes(
    harness: Harness, conversation_id: str
) -> None:
    """The nasty one: the side effect happened, Temporal never heard. The retry
    must not text the user again."""
    h = harness
    doomed = h.start_worker(faults="crash_after_send")
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, wf_id)

    await eventually(lambda: doomed.poll() is not None, timeout=30)  # it crashed itself after sending
    assert len(h.gateway.outbox(PHONE)) == 1

    h.start_worker()  # same fault config; faults only fire on attempt 1
    result, ctx = await finish(h, wf_id)

    assert ctx.task.status is TaskStatus.APPROVED
    assert_exactly_one_notification(h, contains="approved")
    attempts = [dup for key, dup in h.gateway.send_attempts() if key == f"{wf_id}/final"]
    assert attempts == [False, True], "second attempt reached the gateway and was deduplicated"
    assert result.notification_id == h.gateway.outbox(PHONE)[0].id


async def test_duplicate_and_conflicting_completion_events(harness: Harness, conversation_id: str) -> None:
    h = harness
    h.start_worker()
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)

    await approve(h, wf_id, event_id="evt-1")
    await approve(h, wf_id, event_id="evt-1")  # webhook redelivery
    await approve(h, wf_id, event_id="evt-2", approved=False, reason="second reviewer disagrees")
    result, ctx = await finish(h, wf_id)

    assert ctx.task.status is TaskStatus.APPROVED and ctx.task.detail == "income verified"
    assert result.duplicate_decisions_ignored == 2
    assert_exactly_one_notification(h, contains="approved")


async def test_duplicate_submissions_start_one_workflow(harness: Harness, conversation_id: str) -> None:
    h = harness
    h.start_worker()
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]

    # The same request again: a model retry, a user repeating themselves, a second session.
    ctx = parse_context((await handle(h, wf_id).query(VerificationWorkflow.context)) or {})
    _, existed = await start_verification(state, ctx.model_copy(update={"hops": ()}))
    assert existed

    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, wf_id)
    await finish(h, wf_id)

    # ...and once more after it finished: still refused, still one SMS.
    _, existed = await start_verification(state, ctx)
    assert existed
    assert_exactly_one_notification(h, contains="approved")


async def test_redelivered_inbound_sms_gets_one_reply(harness: Harness, conversation_id: str) -> None:
    h = harness
    h.start_worker()
    state = await run_front_desk(h, conversation_id)
    wf_id = state.workflow_ids[0]
    await eventually(lambda: _is(status_of(h, wf_id), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, wf_id)
    await finish(h, wf_id)

    follow_llm = ScriptedLLM([Rule(last_user_says("thanks"), text="You're welcome!")])
    channel = SmsChannel(
        store=h.store(), gateway=h.gateway_client, temporal=h.client, llm_factory=lambda: follow_llm
    )
    for _ in range(2):
        await channel.handle_inbound(message_id="IN-1", from_phone=PHONE, body="thanks!")
    assert [m.body for m in h.gateway.outbox(PHONE)][1:] == ["You're welcome!"]
    stored = h.store().get(conversation_id)
    assert stored is not None and [t.text for t in stored.transcript].count("thanks!") == 1


async def test_follow_up_agent_hands_back_to_front_desk_over_sms(
    harness: Harness, conversation_id: str
) -> None:
    """In-process handoff inside the SMS session (tool returns an Agent), then a
    second durable handoff. Contact details carry through both."""
    h = harness
    h.start_worker()
    state = await run_front_desk(h, conversation_id)
    first_wf = state.workflow_ids[0]
    await eventually(lambda: _is(status_of(h, first_wf), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, first_wf)
    await finish(h, first_wf)

    sms_llm = ScriptedLLM(
        [
            Rule(last_user_says("parking"), tool_calls=[ToolCall("start_new_request")]),
            Rule(after_tool("start_new_request"), text="Sure. Which documents do you have for the permit?"),
            Rule(
                last_user_says("registration"),
                tool_calls=[
                    ToolCall(
                        "submit_for_verification",
                        {"kind": "parking_permit", "documents": ["car registration"]},
                    )
                ],
            ),
            Rule(after_tool("submit_for_verification"), text="Parking permit submitted; we'll text you."),
        ]
    )
    channel = SmsChannel(
        store=h.store(), gateway=h.gateway_client, temporal=h.client, llm_factory=lambda: sms_llm,
        task_queue=h.task_queue, timings=FAST,
    )  # fmt: skip
    reply = await channel.handle_inbound(
        message_id="IN-2", from_phone=PHONE, body="I also need a parking permit"
    )
    assert reply == "Sure. Which documents do you have for the permit?"
    assert isinstance(channel.last_agent, FrontDeskAgent)  # handed off inside the SMS session
    held = h.store().get(conversation_id)
    assert held is not None and held.current_agent == "front_desk"  # ...and that survives the session

    # Next text, next (new) session: the front desk is rebuilt, not the follow-up agent.
    reply = await channel.handle_inbound(message_id="IN-3", from_phone=PHONE, body="just my car registration")
    assert reply == "Parking permit submitted; we'll text you."
    assert isinstance(channel.last_agent, FrontDeskAgent)
    wf_ids = [wf async for wf in h.client.list_workflows(f"TaskQueue = '{h.task_queue}'")]
    second = next(w.id for w in wf_ids if w.id != first_wf)
    second_ctx = parse_context((await handle(h, second).query(VerificationWorkflow.context)) or {})
    assert second_ctx.contact.phone == PHONE and second_ctx.request.kind == "parking_permit"
    # the new request carries the whole history, including the first request's SMS thread
    assert any("rental application" in t.text for t in second_ctx.transcript)


async def test_workflow_started_with_v1_context_completes_on_current_worker(harness: Harness) -> None:
    """Across versions, for real: an old deployment started this workflow with a
    v1 payload. Today's worker replays that history and must finish the job."""
    h = harness
    h.start_worker()
    payload = {**V1_PAYLOAD, "user_phone": PHONE, "conversation_id": "conv-v1"}
    wf = await h.client.start_workflow(
        "VerificationWorkflow",  # by name, with a raw dict: exactly what an older client would have sent
        {"context": payload, "timings": FAST.model_dump()},
        id="verify-conv-v1-legacy",
        task_queue=h.task_queue,
    )
    await eventually(lambda: _is(status_of(h, wf.id), TaskStatus.AWAITING_APPROVAL), timeout=30)
    await approve(h, wf.id)
    _, ctx = await finish(h, wf.id)

    assert ctx.schema_version == 2 and ctx.contact.name == "Ana Lima"
    assert_exactly_one_notification(h, contains="lease renewal has been approved")
    stored = h.store().get("conv-v1")
    assert stored == ctx  # persisted in the current format


async def _is(value: Any, expected: Any) -> bool:
    return (await value) == expected
