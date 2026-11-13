"""Convert between the framework's ``ChatContext`` and the contract's ``Turn`` records.

Kept apart from ``contract.py`` so the contract (and the Temporal workflow that
imports it) doesn't depend on livekit-agents at all.
"""

from __future__ import annotations

from typing import Literal

from livekit.agents import llm

from .contract import Turn

Channel = Literal["voice", "text", "sms"]


def from_chat_ctx(chat_ctx: llm.ChatContext, *, agent: str, channel: Channel) -> list[Turn]:
    """Keep only what the next agent should see: user and assistant text.

    Tool calls, tool outputs, config updates and handoff markers are dropped on
    purpose. They describe *how* the previous agent worked, reference tools the
    next agent may not have, and would make the transcript depend on framework
    internals.
    """
    turns: list[Turn] = []
    for item in chat_ctx.items:
        if item.type != "message" or item.role not in ("user", "assistant"):
            continue
        text = (item.text_content or "").strip()
        if text:
            turns.append(Turn(role=item.role, text=text, agent=agent, channel=channel))
    return turns


def to_chat_ctx(turns: tuple[Turn, ...] | list[Turn], *, max_turns: int = 40) -> llm.ChatContext:
    """Rebuild a ``ChatContext`` the next agent can be constructed with.

    Only the last ``max_turns`` are kept: a conversation that spans days can
    grow without bound, and the follow-up agent needs recency, not everything.
    """
    ctx = llm.ChatContext.empty()
    for turn in list(turns)[-max_turns:]:
        ctx.add_message(role=turn.role, content=turn.text)
    return ctx
