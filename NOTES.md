# Notes: how LiveKit Agents handles sessions, state and handoff today

Studied on 2026-11-13 against **livekit-agents 1.8.11** (`livekit/agents@b36a4af`), the
current docs at docs.livekit.io, and the open **Agents 2.0 beta** PR
([livekit/agents#7757](https://github.com/livekit/agents/pull/7757), branch head `2215be2`).
Source links are pinned to those commits. Claims marked *verified* were checked by
running code, not just by reading it.

## 1. Sessions

- An `AgentSession` is the runtime for one conversation. It owns the STT/LLM/TTS
  pipeline, the active `Agent`, and the chat history, and it moves through
  initializing → listening/thinking/speaking → closing.
  [Agent sessions](https://docs.livekit.io/agents/logic/sessions.md)
- In production a session lives inside a **job**, one process per room, started by
  `AgentServer` and `@server.rtc_session()`. When the session ends, `shutdown()` or
  `aclose()` runs, then shutdown hooks run. The hooks get **10 seconds by default**
  before the process is force-killed (`shutdown_process_timeout`).
  [Job lifecycle](https://docs.livekit.io/agents/server/job.md)
- A session does not need a room. `AgentSession(llm=...)` plus
  `session.start(agent)` runs headless, and `session.run(user_input=...)` drives one
  text turn and returns a `RunResult` of typed events. This is the framework's
  testing idiom ([Unit tests](https://docs.livekit.io/testing/unit-tests.md),
  [`examples/frontdesk/test_agent.py`](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/examples/frontdesk/test_agent.py)),
  and this example also uses it as the runtime for the SMS channel.
- Text in a room arrives over data streams through `RoomIO` (`TextInputOptions`).
  Audio can be turned off for text-only sessions (`RoomOptions(audio_input=False,
  audio_output=False)`), and `console --text` gives a local text console.
  [Text and transcriptions](https://docs.livekit.io/agents/multimodality/text.md)

## 2. State

The framework gives a session two places to keep state. Both live in memory:

| | What | Lifetime |
|---|---|---|
| `session.userdata` | any object you pass in, usually a dataclass, typed as `AgentSession[T]` and read from tools as `RunContext[T].userdata` | the session |
| `ChatContext` | the conversation: messages, tool calls and outputs, handoff markers, config updates | the session; each `Agent` holds its own `chat_ctx` |

- [Passing state](https://docs.livekit.io/agents/logic/agents-handoffs.md#passing-state)
  recommends a dataclass in `userdata`. Nothing persists it.
- `ChatContext.to_dict()` and `ChatContext.from_dict()` exist
  ([chat_context.py](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/livekit-agents/livekit/agents/llm/chat_context.py#L773)),
  so you *can* snapshot a conversation. The dict has **no version field**, and it
  includes framework-internal item types (`agent_config_update`, `agent_handoff`,
  per-item `metrics`). It's a serialization of the current object model, not a
  documented storage format. *Verified*: the dump of a one-turn session starts with
  an `agent_handoff` item and an `agent_config_update` item, before the first user
  message.
- There is no 1.x API for "end this session, start a new one later where it left
  off". The documented building block is constructing an `Agent(chat_ctx=...)`.
  Developers who want pause/resume save the chat context themselves and rebuild it
  ([#3740](https://github.com/livekit/agents/issues/3740)).

## 3. Handoff

**In-process handoff** is well supported and well documented.
[Agents & handoffs](https://docs.livekit.io/agents/logic/agents-handoffs.md),
[Tasks & task groups](https://docs.livekit.io/agents/logic/tasks.md),
[Workflows](https://docs.livekit.io/agents/logic/workflows.md)

- A function tool returns an `Agent` (or `(Agent, result)`), and the session swaps
  the active agent
  ([generation.py](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/livekit-agents/livekit/agents/voice/generation.py#L1193-L1240)).
  `session.update_agent()` does the same thing from code.
- `AgentTask[T]` takes control temporarily and returns a typed result.
  `TaskGroup` (beta) chains tasks.
- **A new agent starts with an empty history** unless you pass
  `chat_ctx=old.chat_ctx.copy(...)`, and `userdata` is shared across agents
  ([Context preservation](https://docs.livekit.io/agents/logic/agents-handoffs.md#context-preservation)).
- *Verified, and it affects SMS*: the reply right after a handoff is generated
  while the old activity drains, with `tool_choice="none"`
  ([agent_activity.py](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/livekit-agents/livekit/agents/voice/agent_activity.py#L4317-L4320)).
  So the new agent can't call tools until the next user turn. In a call that turn
  comes moments later in the same session. Over SMS it comes in a different session,
  so **which agent holds the conversation has to be persisted**, or the next text
  goes to the wrong agent. This example persists it in the contract
  (`HandoffContext.hops`) and rebuilds that agent on each inbound text
  (`sms_channel.agent_for`).

**Cross-process handoff**, meaning to a different worker, a later session or a
different channel, has no 1.x primitive. You serialize something yourself.

## 4. Where long-running work falls outside a session

The framework's answer to slow work is the **async tool**: a tool that calls
`ctx.update()` becomes non-blocking, can stream progress, opts into cancellation
with `ToolFlag.CANCELLABLE`, and has `on_duplicate=` policies. `AsyncToolset`
carries such a tool across in-session handoffs.
[Async tools](https://docs.livekit.io/agents/logic/tools/async.md),
[Subagent delegation](https://docs.livekit.io/agents/logic/patterns/subagent-delegation.md)

That works for seconds to a few minutes inside a live conversation. It doesn't
work for minutes to hours:

1. **An async tool is an `asyncio.Task` in the job process**
   ([tool_executor.py](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/livekit-agents/livekit/agents/voice/tool_executor.py#L513-L557)).
   If the process dies, the work and its result are gone. Nothing records that it
   was running.
2. **Closing the session waits for it.** When an activity closes, it cancels
   cancellable tools and *awaits* the rest
   ([agent_activity.py](https://github.com/livekit/agents/blob/b36a4af9b3aec580d26cc21a25cf4a5c8fe30aa8/livekit-agents/livekit/agents/voice/agent_activity.py#L1705-L1725)).
   *Verified*: `session.aclose()` blocked for the full 20 s of a non-cancellable
   async tool. In a job, the 10 s process-shutdown timeout kills it partway through.
   So a long non-cancellable tool either holds the session open or is lost, and the
   docs don't describe what happens to running async tools at session end.
3. **The result can only be delivered into the same session.** The follow-up is
   "a reply in a later turn". When the user has hung up, there's no turn, and the
   framework has no API for the agent to start a message on another channel.

So this example moves the work out of the session entirely, into a Temporal
workflow. The front desk's tool *starts* the workflow and returns at once. The
workflow owns the request from then on, and it notifies the user over SMS.

## 5. Text and SMS

- 1.x text means text in a LiveKit room (data streams), or a headless session you
  drive yourself. There is no SMS channel or adapter. The
  [Twilio connector](https://docs.livekit.io/telephony/connectors/twilio.md) and the
  [WhatsApp connector](https://docs.livekit.io/telephony/connectors/whatsapp.md)
  bridge **calls**.
- An SMS conversation is a run of single turns, minutes or days apart. A long-lived
  session per phone number would hold a process for nothing. The shape that fits
  is "load state → one headless turn → save state", and that's what
  `sms_channel.py` does.

## 6. What developers are asking for

From an issue survey (about 110 searches, issue bodies plus linked PRs; comments
and Discussions weren't readable from this environment):

- **Durable state, resume after restart.** [#3740](https://github.com/livekit/agents/issues/3740)
  (pause/resume by saving the chat context), [#3668](https://github.com/livekit/agents/issues/3668)
  (context lost when the agent restarts mid-call), [#242](https://github.com/livekit/agents/issues/242)
  (recovery from agent shutdown), [#4626](https://github.com/livekit/agents/issues/4626)
  (events that won't serialize when persisted), [#7720](https://github.com/livekit/agents/issues/7720)
  (history duplicated when an external checkpointer owns state, open).
- **Background work.** [#7215](https://github.com/livekit/agents/issues/7215)
  (async tool progress vs. final output, and "how does an external platform notify
  the room", open), [#5936](https://github.com/livekit/agents/issues/5936)
  (handoff after `ctx.update()` dropped, open), [#8366](https://github.com/livekit/agents/issues/8366)
  (public job-scoped `create_task()`, open). I found nothing about work that must
  *outlive* the job.
- **Handoff context.** [#4106](https://github.com/livekit/agents/issues/4106)
  (the triggering user message is missing after a handoff), [#5284](https://github.com/livekit/agents/issues/5284),
  [#8307](https://github.com/livekit/agents/issues/8307) (open), [#7320](https://github.com/livekit/agents/issues/7320)
  (a handoff silently dropped when batched with another tool), [#5697](https://github.com/livekit/agents/issues/5697)
  (conditional `TaskGroup`, open).
- **Text.** [#791](https://github.com/livekit/agents/issues/791), [#2487](https://github.com/livekit/agents/issues/2487),
  [#5178](https://github.com/livekit/agents/issues/5178), [#6577](https://github.com/livekit/agents/issues/6577)
  (console text mode doesn't show agent-initiated messages, open). I found no SMS
  issue.
- **Duplicates.** [#3702](https://github.com/livekit/agents/issues/3702) (an
  interrupted tool re-run creates duplicate orders), [#1063](https://github.com/livekit/agents/issues/1063)
  (duplicate job dispatch).
- **Testing.** [#3394](https://github.com/livekit/agents/issues/3394) asked to ship
  the test fakes (`tests/fake_llm.py`) in the package. It's closed, and they still
  aren't shipped, so this example brings its own `ScriptedLLM`.

## 7. Where the framework is going: Agents 2.0 (beta, not released)

[#7757](https://github.com/livekit/agents/pull/7757) answers much of this. Its docs
(`docs/` on the PR branch) say:

- **Threads and storage.** Each conversation is a durable thread with its own
  database: history, typed state, the agent stack (which agent is speaking), and
  tasks. It's loaded before each run and committed after. "Nothing lives in the
  process between runs, so any worker can take the next message." A resent
  `messageId` "runs nothing new" (`docs/storage.md`;
  bug [#8367](https://github.com/livekit/agents/issues/8367), fix
  [#8436](https://github.com/livekit/agents/pull/8436)).
- **Text as a first-class endpoint** over A2A, with inbox policies (`batch`,
  `interrupt`, `queue`) for messages that arrive mid-turn (`docs/text-endpoints.md`,
  [#8204](https://github.com/livekit/agents/pull/8204)).
- **Channels.** Web chat, API, partner agents and calls are built. **SMS, WhatsApp
  and email are planned**, keyed by `phone:+E164` (`docs/planned-channels.md`).
- **Background agent tasks** with `Working(timeout=timedelta(days=3))` and
  `on_timeout`, which "survive the turn, the worker and deploys", are **designed,
  not built** (`docs/planned-agent-tasks.md`). That doc also says "outside calls
  are keyed on `ctx.task_id` since a hook may run again after a crash", which is the
  same at-least-once rule this example follows.
- Still open in 2.0's own docs:
  - "A message the agent starts on its own ... a background task's question, is an
    open design point" (`planned-channels.md`, "Messages the agent starts").
  - "A run whose worker died is not run again: its task fails `worker-lost` ... and
    the client resends" (`storage.md`). That rule works for a person typing, but a
    background task has no client.
  - "There is one backend. Local development and the tests use the developer's own
    staging project; nothing stands in for it" (`storage.md`). PR
    [#8200](https://github.com/livekit/agents/pull/8200) adds a local fake for the
    framework's own tests.

**How this example relates:** it's built on stable 1.8.x, using the framework's own
pieces (`AgentSession`, `Agent`, function tools, tool-returns-`Agent` handoff,
`session.run`, `chat_ctx`) and adding only what 1.x lacks: a durable owner for the
long-running work, a versioned handoff contract, and an SMS edge. Most of what it
builds by hand maps onto a 2.0 concept: the `HandoffContext` onto thread state and
the agent stack, the SMS channel onto planned channels, and the workflow onto
planned background tasks. The parts 2.0 leaves open (agent-initiated messages,
re-driving a background task whose worker died, offline tests) are where the drafts
in [DRAFT_ISSUES.md](DRAFT_ISSUES.md) focus.
