"""The two LiveKit agents in the example.

``FrontDeskAgent`` runs in the live session (voice or text). It collects the
request and delegates the slow part to the verification workflow through a
function tool, then the session is free to end.

``FollowUpAgent`` is created later, in a fresh headless ``AgentSession``, when
the user replies over SMS. It is constructed from the ``HandoffContext`` the
workflow left behind, so it knows who it's talking to and what was said.

Two kinds of handoff appear here, and the difference is the point:

- In-process: ``FollowUpAgent.start_new_request`` returns a ``FrontDeskAgent``.
  That's the framework's own handoff (a tool returning an ``Agent``), and state
  rides along in ``userdata`` and ``chat_ctx``.
- Durable, cross-process: ``FrontDeskAgent.submit_for_verification`` starts a
  Temporal workflow with a serialized ``HandoffContext``. Nothing in memory
  survives this one, so everything the next agent needs is in the contract.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Literal

from livekit.agents import Agent, RunContext, ToolError, function_tool
from pydantic import ValidationError
from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from .contract import Contact, HandoffContext, Request, TaskState, TaskStatus, dump_context
from .transcript import from_chat_ctx, to_chat_ctx
from .workflow import TASK_QUEUE, Timings, VerificationInput, VerificationWorkflow

logger = logging.getLogger("durable_handoff.agents")

Channel = Literal["voice", "text", "sms"]


@dataclass
class SessionState:
    """``AgentSession.userdata``: per-session, in-memory, gone when the session ends.

    Anything that must outlive the session goes into a ``HandoffContext``.
    """

    conversation_id: str
    temporal: Client
    channel: Channel = "text"
    contact: Contact | None = None
    prior: HandoffContext | None = None  # set when the session was resumed from a context
    task_queue: str = TASK_QUEUE
    timings: Timings = field(default_factory=Timings)
    workflow_ids: list[str] = field(default_factory=list)


FRONT_DESK_INSTRUCTIONS = """\
You are the front desk for a property management office. People contact you to
get documents verified for a rental application, a lease renewal, or a parking
permit.

Collect, in as few turns as you can: the person's full name, a mobile number
that can receive texts (in international format, like +14155550123), what they
need verified, and which documents they are providing. Call record_contact as
soon as you have name and number, then submit_for_verification.

Verification takes anywhere from a few minutes to a couple of days because a
human reviewer signs off. Never promise a result. After submitting, tell them
they'll get a text when it's done and that they can end the conversation.
Keep replies short; your output may be spoken aloud.
"""


class FrontDeskAgent(Agent):
    def __init__(self, *, chat_ctx: object | None = None) -> None:
        super().__init__(instructions=FRONT_DESK_INSTRUCTIONS, chat_ctx=chat_ctx)  # type: ignore[arg-type]

    @function_tool
    async def record_contact(self, ctx: RunContext[SessionState], name: str, phone: str) -> str:
        """Save the person's name and the mobile number we'll text when verification finishes.

        Args:
            name: The person's full name.
            phone: Mobile number in E.164 format, for example +14155550123.
        """
        try:
            ctx.userdata.contact = Contact(name=name.strip(), phone=phone.replace(" ", "").replace("-", ""))
        except ValidationError:
            raise ToolError(
                f"{phone!r} isn't a valid international number; ask for it like +14155550123"
            ) from None
        return f"Saved {name} at {ctx.userdata.contact.phone}."

    @function_tool
    async def submit_for_verification(
        self, ctx: RunContext[SessionState], kind: str, documents: list[str], notes: str = ""
    ) -> str:
        """Hand the request to the background verification team. Returns immediately;
        the result is texted to the person later.

        Args:
            kind: What is being verified: rental_application, lease_renewal or parking_permit.
            documents: Short names of the documents provided, for example ["pay stub", "photo id"].
            notes: Anything the reviewer should know.
        """
        state = ctx.userdata
        if state.contact is None:
            raise ToolError("call record_contact first; we need a number to text the result to")

        context = build_context(state, self.session.history, kind=kind, documents=documents, notes=notes)
        handle, already = await start_verification(state, context)
        if already:
            return "That exact request is already with the verification team; no need to resubmit."
        return (
            f"Submitted (reference {handle.id}). The person will get a text at "
            f"{state.contact.phone} when it's done. The conversation can end now."
        )

    @function_tool
    async def check_status(self, ctx: RunContext[SessionState]) -> str:
        """Check how the most recent submitted request is going."""
        if not ctx.userdata.workflow_ids:
            return "Nothing has been submitted in this conversation yet."
        return await describe_status(ctx.userdata.temporal, ctx.userdata.workflow_ids[-1])


class FollowUpAgent(Agent):
    """Picks the conversation back up over SMS, hours later, in a new process."""

    def __init__(self, context: HandoffContext) -> None:
        task = context.task
        super().__init__(
            instructions=(
                "You are following up by text message with someone who earlier asked the "
                f"front desk to verify their {context.request.kind.replace('_', ' ')}. "
                f"Their name is {context.contact.name}. The verification status is "
                f"'{task.status.value}'" + (f" ({task.detail})" if task.detail else "") + ". "
                "The conversation so far, including what the front desk said and the text "
                "you already sent, is in your history. Answer questions about the result "
                "briefly; this is SMS, so one or two sentences, no markdown. If they want to "
                "submit something new, call start_new_request."
            ),
            chat_ctx=to_chat_ctx(context.transcript),
        )
        self.context = context

    @function_tool
    async def get_task_status(self, ctx: RunContext[SessionState]) -> str:
        """Look up the latest status of the person's verification request."""
        wf_id = self.context.task.workflow_id
        if wf_id is None:
            return f"Status: {self.context.task.status.value}."
        return await describe_status(ctx.userdata.temporal, wf_id)

    @function_tool
    async def start_new_request(self, ctx: RunContext[SessionState]) -> tuple[Agent, str]:
        """Use when the person wants to submit a new or additional request."""
        # The framework's in-process handoff: return the next agent. Contact
        # details are already known, so the front desk won't ask again.
        ctx.userdata.contact = self.context.contact
        return FrontDeskAgent(
            chat_ctx=self.chat_ctx.copy(exclude_instructions=True)
        ), "Handing over to the front desk."


# --- helpers shared by the agents, the demo and the tests --------------------


def build_context(
    state: SessionState, history: object, *, kind: str, documents: list[str], notes: str = ""
) -> HandoffContext:
    """Snapshot everything the background agent and the follow-up agent will need."""
    assert state.contact is not None
    new_turns = from_chat_ctx(history, agent="front_desk", channel=state.channel)  # type: ignore[arg-type]
    prior_turns = state.prior.transcript if state.prior else ()
    known = {(t.role, t.text) for t in prior_turns}
    turns = (*prior_turns, *(t for t in new_turns if (t.role, t.text) not in known))
    return HandoffContext(
        conversation_id=state.conversation_id,
        contact=state.contact,
        request=Request(kind=kind, documents=tuple(documents), notes=notes),
        transcript=turns,
        task=TaskState(status=TaskStatus.SUBMITTED),
        hops=state.prior.hops if state.prior else (),
    )


def workflow_id_for(context: HandoffContext) -> str:
    """Deterministic: the same request from the same conversation maps to the same
    workflow, so a repeated tool call (the model retrying, the user saying it
    twice) can't start a second one."""
    digest = hashlib.sha256(
        "|".join([context.request.kind, *sorted(context.request.documents)]).encode()
    ).hexdigest()[:10]
    return f"verify-{context.conversation_id}-{digest}"


async def start_verification(
    state: SessionState, context: HandoffContext
) -> tuple[WorkflowHandle[VerificationWorkflow, object], bool]:
    """Start the workflow, or attach to it if this exact request is already running
    or finished. Returns ``(handle, already_existed)``."""
    wf_id = workflow_id_for(context)
    try:
        handle = await state.temporal.start_workflow(
            VerificationWorkflow.run,
            VerificationInput(context=dump_context(context), timings=state.timings),
            id=wf_id,
            task_queue=state.task_queue,
            # Running duplicate -> error. Finished duplicate -> error too, unless
            # the earlier run failed, in which case a resubmission may retry it.
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        )
        existed = False
    except WorkflowAlreadyStartedError:
        handle = state.temporal.get_workflow_handle_for(VerificationWorkflow.run, wf_id)
        existed = True
    if wf_id not in state.workflow_ids:
        state.workflow_ids.append(wf_id)
    logger.info("verification %s %s", wf_id, "already existed" if existed else "started")
    return handle, existed


async def describe_status(client: Client, workflow_id: str) -> str:
    handle = client.get_workflow_handle(workflow_id)
    try:
        raw = await handle.query(VerificationWorkflow.context)
    except Exception:  # no worker up right now, or history retention passed
        desc = await handle.describe()
        return f"Request {workflow_id} is {desc.status.name.lower() if desc.status else 'unknown'}."
    if raw is None:
        return f"Request {workflow_id} is queued."
    from .contract import parse_context

    task = parse_context(raw).task
    return f"Request {workflow_id}: {task.status.value}" + (f" ({task.detail})" if task.detail else "") + "."
