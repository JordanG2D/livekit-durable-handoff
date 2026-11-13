"""A deterministic, offline ``llm.LLM`` for tests and the no-key demo.

The framework's own test suite ships a fake LLM under ``tests/`` but not in the
package, so examples have to bring their own. This one is a small rule table:
each rule looks at the chat context the agent is about to send and, if it
matches, returns canned text and/or tool calls. No network, no tokens.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from livekit.agents import llm
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS, NOT_GIVEN, APIConnectOptions, NotGivenOr

Matcher = Callable[[llm.ChatContext], bool]

_call_ids = itertools.count(1)


@dataclass
class ToolCall:
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class Rule:
    """``when`` decides if this rule answers; ``text`` and ``tool_calls`` are the answer."""

    when: Matcher
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)


def last_user_says(fragment: str) -> Matcher:
    """Match when the newest user message contains ``fragment`` (case-insensitive)
    and the model hasn't answered it yet (the last item is that user message)."""

    def match(ctx: llm.ChatContext) -> bool:
        item = ctx.items[-1] if ctx.items else None
        return (
            item is not None
            and item.type == "message"
            and item.role == "user"
            and fragment.lower() in (item.text_content or "").lower()
        )

    return match


def after_tool(name: str) -> Matcher:
    """Match right after the output of tool ``name`` was added to the context."""

    def match(ctx: llm.ChatContext) -> bool:
        item = ctx.items[-1] if ctx.items else None
        return item is not None and item.type == "function_call_output" and item.name == name

    return match


def instructions_contain(fragment: str) -> Matcher:
    """Match ``generate_reply(instructions=...)`` turns, which arrive as a trailing
    system/developer message."""

    def match(ctx: llm.ChatContext) -> bool:
        item = ctx.items[-1] if ctx.items else None
        return (
            item is not None
            and item.type == "message"
            and item.role in ("system", "developer")
            and fragment.lower() in (item.text_content or "").lower()
        )

    return match


class ScriptedLLM(llm.LLM[Any]):
    def __init__(self, rules: list[Rule], *, fallback: str = "Sorry, could you say that again?"):
        super().__init__()
        self.rules = rules
        self.fallback = fallback
        self.requests: list[llm.ChatContext] = []  # every context the agent sent, for asserts

    @property
    def model(self) -> str:
        return "scripted"

    @property
    def provider(self) -> str:
        return "offline"

    def chat(
        self,
        *,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool] | None = None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        parallel_tool_calls: NotGivenOr[bool] = NOT_GIVEN,
        tool_choice: NotGivenOr[llm.ToolChoice] = NOT_GIVEN,
        extra_kwargs: NotGivenOr[dict[str, Any]] = NOT_GIVEN,
    ) -> llm.LLMStream:
        self.requests.append(chat_ctx.copy())
        rule = next((r for r in self.rules if r.when(chat_ctx)), None)
        return _ScriptedStream(self, rule, chat_ctx=chat_ctx, tools=tools or [], conn_options=conn_options)


class _ScriptedStream(llm.LLMStream):
    def __init__(
        self,
        owner: ScriptedLLM,
        rule: Rule | None,
        *,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool],
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(owner, chat_ctx=chat_ctx, tools=tools, conn_options=conn_options)
        self._owner = owner
        self._rule = rule

    async def _run(self) -> None:
        text = self._rule.text if self._rule else self._owner.fallback
        calls = [
            llm.FunctionToolCall(
                name=c.name, arguments=json.dumps(c.arguments), call_id=f"call_{next(_call_ids)}"
            )
            for c in (self._rule.tool_calls if self._rule else [])
        ]
        self._event_ch.send_nowait(
            llm.ChatChunk(
                id="scripted",
                delta=llm.ChoiceDelta(role="assistant", content=text or None, tool_calls=calls),
            )
        )
