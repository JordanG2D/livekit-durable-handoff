"""The background verifier: a Temporal workflow that owns a request after handoff.

Why a workflow and not an async tool: an async tool is an ``asyncio.Task`` in
the agent's process. When the session closes, the framework cancels it, and if
the process dies it's simply gone. This work has to outlive both, so it runs
on a Temporal worker, and its state lives in the Temporal server's event
history rather than in any process.

Shape of the workflow:

    parse + upcast context  ->  verify_documents (activity, heartbeats, resumable)
                            ->  wait for an approval signal (minutes to hours, durable timer)
                            ->  hand off to the follow-up agent: notify_user (activity, idempotent)
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .contract import HandoffContext, TaskStatus, Turn, dump_context, parse_context

TASK_QUEUE = "durable-handoff"


class Timings(BaseModel):
    """Production defaults; tests shrink them. Part of the workflow input, so a
    running workflow keeps the timings it started with."""

    heartbeat_timeout_s: float = 30
    verify_timeout_s: float = 30 * 60
    approval_timeout_s: float = 48 * 3600
    notify_timeout_s: float = 30


class VerificationInput(BaseModel):
    # A raw dict, not a typed HandoffContext, on purpose: Temporal stores this
    # payload in history and replays it to whatever worker version is running
    # later. Parsing it ourselves (parse_context) means a v1 payload from an old
    # deployment is upcast instead of failing deserialization.
    context: dict[str, Any]
    timings: Timings = Timings()


class VerifyReport(BaseModel):
    ok: bool
    checked: list[str]
    summary: str
    resumed_from: int = 0  # >0 when a retry continued from a heartbeat instead of restarting


class Decision(BaseModel):
    """An approval decision from the outside world (a reviewer, a webhook)."""

    event_id: str  # the sender's id for this event; duplicates reuse it
    approved: bool
    reason: str = ""


class NotifyInput(BaseModel):
    context: dict[str, Any]
    body: str
    idempotency_key: str


class VerificationResult(BaseModel):
    context: dict[str, Any]
    notification_id: str
    duplicate_decisions_ignored: int
    verify_resumed_from: int = 0


@workflow.defn
class VerificationWorkflow:
    def __init__(self) -> None:
        self._ctx: HandoffContext | None = None
        self._decision: Decision | None = None
        self._ignored_decisions = 0

    @workflow.run
    async def run(self, inp: VerificationInput) -> VerificationResult:
        t = inp.timings
        now = workflow.now()
        ctx = parse_context(inp.context)
        ctx = ctx.handoff(to_agent="verifier", reason="long-running verification", at=now)
        ctx = ctx.with_task(status=TaskStatus.VERIFYING, workflow_id=workflow.info().workflow_id, at=now)
        self._ctx = ctx

        report: VerifyReport = await workflow.execute_activity(
            "verify_documents",
            dump_context(ctx),
            result_type=VerifyReport,
            start_to_close_timeout=timedelta(seconds=t.verify_timeout_s),
            heartbeat_timeout=timedelta(seconds=t.heartbeat_timeout_s),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=10),
        )

        if not report.ok:
            ctx = ctx.with_task(status=TaskStatus.REJECTED, detail=report.summary, at=workflow.now())
        else:
            ctx = ctx.with_task(status=TaskStatus.AWAITING_APPROVAL, detail=report.summary, at=workflow.now())
            self._ctx = ctx
            try:
                # A durable timer: no process is "waiting" here. The worker can
                # restart any number of times; the server holds the deadline.
                await workflow.wait_condition(
                    lambda: self._decision is not None, timeout=timedelta(seconds=t.approval_timeout_s)
                )
            except TimeoutError:
                ctx = ctx.with_task(
                    status=TaskStatus.EXPIRED, detail="no decision before the deadline", at=workflow.now()
                )
            else:
                d = self._decision
                assert d is not None
                status = TaskStatus.APPROVED if d.approved else TaskStatus.REJECTED
                ctx = ctx.with_task(status=status, detail=d.reason, at=workflow.now())

        ctx = ctx.handoff(
            to_agent="follow_up", reason=f"task {ctx.task.status.value}; continue over SMS", at=workflow.now()
        )
        body = _notification_text(ctx)
        # The follow-up agent speaks first, so its opening message is part of the transcript it inherits.
        ctx = ctx.with_turns(Turn(role="assistant", text=body, agent="follow_up", channel="sms"))
        self._ctx = ctx

        sent: dict[str, Any] = await workflow.execute_activity(
            "notify_user",
            NotifyInput(
                context=dump_context(ctx),
                body=body,
                # One key per workflow run's final notification. Activity retries
                # reuse it, so the gateway delivers at most once.
                idempotency_key=f"{workflow.info().workflow_id}/final",
            ),
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=t.notify_timeout_s),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=20),
        )
        return VerificationResult(
            context=dump_context(ctx),
            notification_id=sent["id"],
            duplicate_decisions_ignored=self._ignored_decisions,
            verify_resumed_from=report.resumed_from,
        )

    @workflow.signal
    def decide(self, decision: Decision) -> None:
        """First decision wins. Redelivered webhooks and second opinions are
        counted and ignored, so they can't trigger a second notification or flip
        a decision the user was already told about."""
        if self._decision is None:
            self._decision = decision
        else:
            self._ignored_decisions += 1
            workflow.logger.info("ignoring decision %s: already decided", decision.event_id)

    @workflow.query
    def context(self) -> dict[str, Any] | None:
        return dump_context(self._ctx) if self._ctx else None


def _notification_text(ctx: HandoffContext) -> str:
    first = ctx.contact.name.split()[0]
    kind = ctx.request.kind.replace("_", " ")
    match ctx.task.status:
        case TaskStatus.APPROVED:
            outcome = "has been approved"
        case TaskStatus.REJECTED:
            outcome = "was not approved"
        case _:
            outcome = "timed out waiting for a reviewer"
    detail = f" ({ctx.task.detail})" if ctx.task.detail else ""
    return f"Hi {first}, your {kind} {outcome}{detail}. Reply here with any questions."
