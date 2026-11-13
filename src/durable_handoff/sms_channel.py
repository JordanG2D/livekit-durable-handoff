"""The text channel: each inbound SMS becomes one turn of a short-lived headless session.

SMS doesn't fit the "one long session per room" model. Replies arrive minutes
or days apart, there's no room to join, and keeping a session open between
texts would hold memory and a process for nothing. So every inbound message:

1. loads the conversation's ``HandoffContext`` from the store,
2. starts a headless ``AgentSession`` (no room; text in, text out) with a
   ``FollowUpAgent`` built from that context,
3. runs exactly one turn with ``session.run(user_input=...)``,
4. appends the new turns to the context, saves it, closes the session,
5. sends the reply, keyed on the inbound message id so a redelivered webhook
   doesn't produce a second reply.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from aiohttp import web
from livekit.agents import Agent, AgentSession, llm
from livekit.agents.voice.run_result import ChatMessageEvent, RunResult
from temporalio.client import Client

from .agents import FollowUpAgent, FrontDeskAgent, SessionState
from .contract import HandoffContext, TaskStatus, Turn
from .sms_gateway import GatewayClient
from .store import ConversationStore
from .transcript import to_chat_ctx
from .workflow import TASK_QUEUE, Timings

logger = logging.getLogger("durable_handoff.sms")

NO_CONTEXT_REPLY = (
    "Hi! We don't have an open request for this number. Call or chat with the front desk to start one."
)


class SmsChannel:
    def __init__(
        self,
        *,
        store: ConversationStore,
        gateway: GatewayClient,
        temporal: Client,
        llm_factory: Callable[[], llm.LLM[Any]],
        task_queue: str = TASK_QUEUE,
        timings: Timings | None = None,
    ) -> None:
        self.task_queue = task_queue  # used if the follow-up hands off to a new request
        self.timings = timings or Timings()
        self.store = store
        self.gateway = gateway
        self.temporal = temporal
        self.llm_factory = llm_factory
        self._locks: dict[str, asyncio.Lock] = {}
        self._handled: dict[str, str] = {}  # inbound id -> reply, for redelivered webhooks
        self.last_agent: Agent | None = None  # for tests: which agent ended the turn

    async def handle_inbound(self, *, message_id: str, from_phone: str, body: str) -> str:
        # One turn at a time per conversation. Two texts sent back-to-back must
        # not run as two concurrent sessions that each overwrite the context.
        lock = self._locks.setdefault(from_phone, asyncio.Lock())
        async with lock:
            if message_id in self._handled:
                return self._handled[message_id]

            context = self.store.latest_for_phone(from_phone)
            if context is None:
                reply = NO_CONTEXT_REPLY
            else:
                state = SessionState(
                    conversation_id=context.conversation_id,
                    temporal=self.temporal,
                    channel="sms",
                    contact=context.contact,
                    prior=context,
                    workflow_ids=[context.task.workflow_id] if context.task.workflow_id else [],
                    task_queue=self.task_queue,
                    timings=self.timings,
                )
                model = self.llm_factory()
                async with model, AgentSession[SessionState](llm=model, userdata=state) as session:
                    await session.start(agent_for(context))
                    result: RunResult[None] = await session.run(user_input=body)
                    self.last_agent = session.current_agent
                    texts = [
                        e.item.text_content
                        for e in result.events
                        if isinstance(e, ChatMessageEvent)
                        and e.item.role == "assistant"
                        and e.item.text_content
                    ]
                reply = " ".join(t.strip() for t in texts) or "Sorry, I didn't catch that."
                context = self._after_turn(context, state, body=body, reply=reply)
                self.store.save(context)

            await self.gateway.send(to=from_phone, body=reply, idempotency_key=f"reply/{message_id}")
            self._handled[message_id] = reply
            return reply

    def _after_turn(
        self, context: HandoffContext, state: SessionState, *, body: str, reply: str
    ) -> HandoffContext:
        agent_name = "follow_up" if isinstance(self.last_agent, FollowUpAgent) else "front_desk"
        context = context.with_turns(
            Turn(role="user", text=body, agent=agent_name, channel="sms"),
            Turn(role="assistant", text=reply, agent=agent_name, channel="sms"),
        )
        # The framework's in-process handoff lives only as long as the session.
        # This session ends now, so record who holds the conversation in the
        # contract; the next text rebuilds that agent (see agent_for).
        if agent_name != context.current_agent:
            context = context.handoff(to_agent=agent_name, reason="in-session handoff over SMS")
        new_wf = state.workflow_ids[-1] if state.workflow_ids else None
        if new_wf and new_wf != context.task.workflow_id:
            context = context.with_task(status=TaskStatus.SUBMITTED, workflow_id=new_wf, detail="")
        return context

    def routes(self) -> list[web.RouteDef]:
        async def webhook(request: web.Request) -> web.Response:
            data: dict[str, Any] = await request.json()
            reply = await self.handle_inbound(
                message_id=data["id"], from_phone=data["from"], body=data["body"]
            )
            return web.json_response({"reply": reply})

        return [web.post("/sms/inbound", webhook)]


def agent_for(context: HandoffContext) -> Agent:
    """Rebuild whichever agent held the conversation when the last session ended."""
    if context.current_agent == "front_desk":
        return FrontDeskAgent(chat_ctx=to_chat_ctx(context.transcript))
    return FollowUpAgent(context)
