# Draft issues for livekit/agents (for review, not filed)

These come out of building this example. **Nothing here has been filed or posted.**
Joshua reviews and decides which, if any, to send.

How duplicates were checked: about 110 unauthenticated GitHub searches over issues
and PRs on 2026-11-13 (the queries for each draft are listed under it), plus a read
of the Agents 2.0 beta docs on the [#7757](https://github.com/livekit/agents/pull/7757)
branch. **Limits:** issue comments and GitHub Discussions couldn't be read from the
build environment, so check
[Discussions](https://github.com/livekit/agents/discussions) before filing.

| # | Draft | Targets | Recommendation |
|---|---|---|---|
| 1 | Document (and bound) what session close does to running async tools | 1.x | **File.** Concrete, reproducible, small. |
| 2 | Re-driving a background task whose worker died | 2.0 planned | **File**, framed as design feedback on `planned-agent-tasks.md`. |
| 3 | Agent-initiated messages need an idempotency key | 2.0 planned | **File, or fold into 2**. It's an open point in their own docs. |
| 4 | A supported local storage backend for offline tests | 2.0 | Comment on [#8200](https://github.com/livekit/agents/pull/8200) first. File only if it's meant to stay internal. |
| 5 | `ChatContext.to_dict()` as a storage format | 1.x | Optional. Lower value if 2.0 lands soon. |
| — | Shipping a scripted fake LLM | 1.x | **Don't file.** It duplicates closed [#3394](https://github.com/livekit/agents/issues/3394). There's a suggested comment at the end. |

---

## 1. Document (and bound) what `AgentSession` close does to running async tools

**Searched:** `async tool session close cancel`, `outlive session`, `after session ends`,
`disconnect continue tool`, `durable`. Nothing on-topic.
Related, but different: [#8366](https://github.com/livekit/agents/issues/8366) asks
for job-scoped tasks that *end* with the job.

**Body:**

> **What happens**
>
> When a session closes, `AgentActivity.aclose()` cancels async tools that opted
> into `ToolFlag.CANCELLABLE`, then *awaits* the rest before teardown
> (`agent_activity.py`, `self._tool_executor.drain()` then `aclose()`). So
> `session.aclose()` blocks for as long as a non-cancellable async tool runs:
>
> ```python
> class A(Agent):
>     @function_tool
>     async def slow(self, ctx: RunContext) -> str:
>         await ctx.update("working on it")
>         await asyncio.sleep(20)
>         return "done"
>
> # ... after a turn that calls `slow`:
> t = time.monotonic(); await session.aclose()
> print(time.monotonic() - t)   # 20.0 on livekit-agents 1.8.11
> ```
>
> In a job, shutdown hooks get `shutdown_process_timeout` (10 s by default) before
> the process is killed. So a non-cancellable tool that takes longer either holds
> the session open after the user leaves, or is killed partway through with no
> record that it ran. The [async tools page](https://docs.livekit.io/agents/logic/tools/async.md)
> says async tools "finish what they're doing regardless of what the user does",
> but doesn't say what happens when the *session* ends.
>
> **Suggestion**
>
> 1. Docs: a "Lifecycle" section on the async tools page covering session close,
>    `drain()`, the job shutdown timeout, and the guidance "for work that may
>    outlive the session, hand it to a durable system and return".
> 2. API, optional: a bound on how long close waits for tools (e.g.
>    `AgentSession(tool_close_timeout=...)`), and/or a per-tool hook that runs on
>    close so a tool can hand off its work (persist a job id, enqueue it) before
>    it's cancelled.
>
> Context: I built an example where a front-desk agent delegates hours-long work
> to a Temporal workflow and texts the user later. It works by never running the
> work in the session at all. That's the right call, but nothing in the docs
> points developers to it before they discover the close behavior in production.

---

## 2. [agents-2.0] Background tasks: who re-drives a run whose worker died?

**Searched:** `worker-lost`, `worker died`, `background task`, `lease expire`,
`resume after crash`, and the 2.0 docs. Nothing filed. #8367 and #8436 cover
`messageId` resend dedup, not this.

**Body:**

> `docs/storage.md` says: "A run whose worker died is not run again: its task
> fails `worker-lost` when the next holder finds it, and the client resends."
> That works when a person is typing. They resend, or the A2A client retries.
>
> `docs/planned-agent-tasks.md` designs background tasks that "survive the turn,
> the worker and deploys" (`Working(timeout=timedelta(days=3))`, `on_timeout`). For
> those there's often **no client to resend**. The task was started by
> `A2ADelegate.send()` without awaiting, by the clock (`delay=`), or by a system
> posting back. If the worker dies in `on_enter`, or between a `Working` transition
> and its commit, `worker-lost` looks like a dead end.
>
> **Questions / proposal**
>
> - Should a background task's hook be re-driven from the last committed state
>   when its lease expires (at least once), instead of failing `worker-lost`? The
>   planned doc already says "outside calls are keyed on `ctx.task_id` since a hook
>   may run again after a crash", which assumes re-execution. Making it the
>   documented behavior for background tasks would complete that contract.
> - If not, who is notified on `worker-lost` for a task nobody awaits? A push
>   notification to the delegating lane would at least let the caller's next turn
>   decide.
> - A retry policy per task (max attempts, backoff), shaped like durable-execution
>   engines, would cover both cases.
>
> I hit this building the same flow on 1.x with Temporal. The tests that mattered
> most were "SIGKILL the worker mid-step" and "the worker dies after the side effect
> but before the commit". Happy to share them as test cases.

---

## 3. [agents-2.0] Agent-initiated messages: include an idempotency key in the design

**Searched:** `notification`, `outbound text`, `send sms`, `proactive message`,
`agent initiated`. No issues. In 2.0 docs this is an explicit open point:
`planned-channels.md`, "Messages the agent starts".

**Body:**

> `planned-channels.md` lists "a message the agent starts on its own, a link sent
> mid-call, a reminder, a background task's question" as an open design point.
> A data point from building that flow (a background verification finishes hours
> later and texts the user):
>
> The sender is almost always something that runs **at least once**: a background
> task hook that may re-run after a crash, a timer, a retried webhook. If the send
> verb has no idempotency key, every caller has to build its own dedup, or the user
> gets the same text twice. My example's hardest test is "the worker dies after the
> SMS was sent but before the step committed", and it only passes because the
> gateway honors `Idempotency-Key`.
>
> **Suggestion:** whatever the verb is, give it a caller-supplied key with
> at-most-once delivery per key per thread, e.g.
>
> ```python
> await ctx.thread.send("Your application was approved.", channel="sms", key=f"{ctx.task_id}/approved")
> ```
>
> and record the send in the journal, so the re-run hook sees it happened. Also: the
> message should be written to the lane's history, so the next inbound reply is
> answered with "what we told them" in context.

*(This could be folded into #2 as one issue on "background tasks and the outside
world" if a single issue reads better.)*

---

## 4. [agents-2.0] A supported local storage backend for offline tests and CI

**Recommendation: comment on [#8200](https://github.com/livekit/agents/pull/8200)
first** ("tests: run the agent-store tests on a local fake"). That PR may already
be the answer. File this only if the fake is meant to stay internal.

**Searched:** `local storage`, `offline test`, `agent-db local`, `sqlite`,
`bring your own storage`. No issues.

**Body / comment:**

> `docs/storage.md`: "There is one backend. Local development and the tests use the
> developer's own staging project; nothing stands in for it."
>
> For example repos and app CI that's a real barrier. The durability properties
> worth testing (a worker killed mid-turn, a resent `messageId`, inbox batching)
> are exactly the ones you want in CI, and CI often has no network or credentials.
> Could the local fake from #8200 be exported as a supported
> `livekit.agents.storage.LocalStorage` (in-memory or SQLite), clearly labeled
> "not for production"? Then the testing docs could show a "kill the worker"
> test that runs anywhere.

---

## 5. `ChatContext.to_dict()` / `from_dict()`: stable format, or not?

**Searched:** `ChatContext to_dict`, `from_dict`, `ChatContext serialization`,
`chat context save restore`. No on-topic issues. [#3740](https://github.com/livekit/agents/issues/3740)
uses save/restore as a workaround.

**Body:**

> Developers persist `chat_ctx.to_dict()` to resume conversations later (#3740
> describes it as a workaround). The dict has no version field, and it includes
> framework-internal items (`agent_config_update`, `agent_handoff`, per-message
> `metrics`). So it isn't clear whether stored dicts will load after an upgrade.
>
> Could the docs say which it is?
> - **Stable:** add `"version"` to the output and a migration path in `from_dict`.
> - **Not stable:** say so on the chat context page, and recommend storing only
>   messages, or the protobuf `livekit.agent.ChatContext.ChatItem` JSON that 2.0's
>   thread storage uses, if that's meant to be the stable form.

---

## Not a new issue: suggested comment on #3394 (closed)

[#3394](https://github.com/livekit/agents/issues/3394) asked to ship `tests/fake_*.py`.
It's closed, and the fakes still aren't in the 1.8.11 package. A short comment with a
concrete use case is more useful than a duplicate:

> Another use case for this: example repos that test agents in CI with no model
> calls. I ended up writing a ~100-line scripted `llm.LLM` that pushes `ChatChunk`s
> into `LLMStream._event_ch`, which is internal. Even a minimal
> `livekit.agents.testing.ScriptedLLM(rules)` next to `mock_tools` would remove the
> need to touch internals. Is this still something you'd accept a PR for?
