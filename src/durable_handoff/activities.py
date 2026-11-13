"""Activities: the side-effecting steps of the workflow.

Temporal retries an activity until it succeeds, so each one must be safe to
run more than once:

- ``verify_documents`` heartbeats its progress and resumes from the last
  heartbeat after a crash instead of starting over.
- ``notify_user`` writes the context with an idempotent upsert and sends the
  SMS with an idempotency key. If the worker dies between "sent" and "Temporal
  recorded that it was sent", the retry hits the same key and the gateway
  returns the original message.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from temporalio import activity

from .contract import parse_context
from .sms_gateway import GatewayClient
from .store import ConversationStore
from .workflow import NotifyInput, VerifyReport

FAULTS_ENV = "DURABLE_HANDOFF_FAULTS"
"""Comma-separated fault points for tests (currently ``crash_after_send``). Each one
fires only on an activity's first attempt, simulating a worker that dies."""


def _fault(point: str) -> None:
    if point in os.environ.get(FAULTS_ENV, "").split(",") and activity.info().attempt == 1:
        activity.logger.warning("fault injection: hard-exiting worker at %s", point)
        os._exit(137)  # like SIGKILL: no cleanup, no completion reported


class Activities:
    def __init__(self, *, gateway_url: str, store_path: Path, step_seconds: float = 2.0) -> None:
        self.gateway = GatewayClient(gateway_url)
        self.store = ConversationStore(store_path)
        self.step_seconds = step_seconds

    @activity.defn(name="verify_documents")
    async def verify_documents(self, context: dict[str, Any]) -> VerifyReport:
        ctx = parse_context(context)
        docs = list(ctx.request.documents)
        # Resume where a previous attempt stopped. Heartbeat details are stored
        # by the server, so they survive the worker that wrote them.
        details = activity.info().heartbeat_details
        start = int(details[0]) if details else 0
        if start:
            activity.logger.info("resuming verification at document %d/%d", start, len(docs))
        for i in range(start, len(docs)):
            await asyncio.sleep(self.step_seconds)  # stand-in for OCR, a registry lookup, a model call...
            activity.heartbeat(i + 1)
        if not docs:
            return VerifyReport(ok=False, checked=[], summary="no documents were provided")
        return VerifyReport(
            ok=True, checked=docs, summary=f"{len(docs)} document(s) checked", resumed_from=start
        )

    @activity.defn(name="notify_user")
    async def notify_user(self, inp: NotifyInput) -> dict[str, Any]:
        ctx = parse_context(inp.context)
        self.store.save(ctx)  # first, so a reply that races the SMS finds the context
        result = await self.gateway.send(
            to=ctx.contact.phone, body=inp.body, idempotency_key=inp.idempotency_key
        )
        _fault("crash_after_send")
        return result
