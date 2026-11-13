"""End-to-end demo, entirely on your machine.

    uv run python -m durable_handoff.demo                # offline: scripted model, no keys
    uv run python -m durable_handoff.demo --kill-worker  # ...and SIGKILL the worker mid-task
    uv run python -m durable_handoff.demo --live         # real model, $5 hard cap (see budget.py)

What happens:

1. A Temporal dev server, the fake SMS gateway and a worker process start.
2. A front desk text session takes a request and hands it to the workflow.
   The session then ends.
3. The workflow verifies documents (optionally surviving a killed worker),
   then waits for a reviewer. The demo plays the reviewer, and also redelivers
   the approval webhook to show it's ignored.
4. The user gets exactly one SMS, replies to it, and the follow-up agent
   answers with the earlier conversation in context.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from aiohttp import web
from livekit.agents import AgentSession, llm
from livekit.agents.voice.run_result import RunResult
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

from .agents import FrontDeskAgent, SessionState
from .budget import Budget
from .contract import TaskStatus, parse_context
from .scripted_llm import Rule, ScriptedLLM, ToolCall, after_tool, last_user_says
from .sms_channel import SmsChannel
from .sms_gateway import FakeSmsGateway, GatewayClient
from .store import ConversationStore
from .workflow import Decision, Timings, VerificationWorkflow

PHONE = "+14155550123"
USER_LINES = [
    "Hi, I'm Dana Reyes, my mobile is +14155550123.",
    "I need my rental application verified. I have a pay stub, a photo ID and a reference letter.",
]
SMS_REPLY = "Great, thanks! Do I need to bring anything to the office?"
DEMO_TIMINGS = Timings(
    heartbeat_timeout_s=3, verify_timeout_s=120, approval_timeout_s=300, notify_timeout_s=10
)


def scripted_front_desk() -> ScriptedLLM:
    return ScriptedLLM(
        [
            Rule(
                last_user_says("Dana"),
                tool_calls=[ToolCall("record_contact", {"name": "Dana Reyes", "phone": PHONE})],
            ),
            Rule(
                after_tool("record_contact"),
                text="Thanks, Dana! What should we verify, and which documents do you have?",
            ),
            Rule(
                last_user_says("rental application"),
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
                after_tool("submit_for_verification"),
                text=(
                    "Done. It's with our verification team, and we'll text you when it's finished. "
                    "You can hang up."
                ),
            ),
        ]
    )


def scripted_follow_up() -> ScriptedLLM:
    return ScriptedLLM(
        [Rule(last_user_says("bring"), text="Just your photo ID when you pick up the keys. Congrats!")]
    )


def live_llm() -> llm.LLM[Any]:
    if os.environ.get("OPENAI_API_KEY"):
        from livekit.plugins import openai  # pip install 'livekit-durable-handoff[live]'

        model: llm.LLM[Any] = openai.LLM(model=os.environ.get("LLM_MODEL", "gpt-4.1-mini"))
        return model
    if os.environ.get("LIVEKIT_API_KEY"):
        from livekit.agents import inference

        return inference.LLM(os.environ.get("LLM_MODEL", "openai/gpt-4.1-mini"))
    sys.exit("--live needs OPENAI_API_KEY or LIVEKIT_API_KEY/LIVEKIT_API_SECRET in the environment")


def say(who: str, text: str) -> None:
    print(f"  {who:>10} | {text}")


def step(text: str) -> None:
    print(f"\n== {text}")


async def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--live", action="store_true", help="use a real model (keys from env, $5 cap)")
    p.add_argument("--kill-worker", action="store_true", help="SIGKILL the worker mid-verification")
    p.add_argument("--budget-usd", type=float, default=5.0)
    args = p.parse_args(argv)

    work = Path(".demo")
    work.mkdir(exist_ok=True)  # noqa: ASYNC240 - once, at startup
    budget = Budget(args.budget_usd)
    if args.live:
        budget.check()
    make_front = live_llm if args.live else scripted_front_desk
    make_follow = live_llm if args.live else scripted_follow_up

    step("starting Temporal dev server, fake SMS gateway, worker")
    env = await WorkflowEnvironment.start_local(
        data_converter=pydantic_data_converter,
        download_dest_dir=".temporal-cache",
        dev_server_log_level="error",
    )
    run_id = uuid.uuid4().hex[:6]
    gateway = FakeSmsGateway(work / f"sms-{run_id}.db")
    await gateway.start()
    store_path = work / f"conversations-{run_id}.db"
    task_queue = f"demo-{run_id}"

    def start_worker() -> subprocess.Popen[bytes]:
        cmd = [sys.executable, "-m", "durable_handoff.worker",
               "--address", env.client.service_client.config.target_host,
               "--task-queue", task_queue, "--gateway-url", gateway.url, "--store", str(store_path),
               "--step-seconds", "1.5"]  # fmt: skip
        return subprocess.Popen(
            cmd, stdout=open(work / f"worker-{run_id}.log", "ab"), stderr=subprocess.STDOUT
        )  # noqa: SIM115

    worker = start_worker()
    try:
        step("live session with the front desk (text mode)")
        state = SessionState(
            conversation_id=f"demo-{run_id}", temporal=env.client, channel="text", task_queue=task_queue,
            timings=DEMO_TIMINGS,
        )  # fmt: skip
        model = make_front()
        async with model, AgentSession[SessionState](llm=model, userdata=state) as session:
            await session.start(FrontDeskAgent())
            for line in USER_LINES:
                if args.live:
                    budget.check()
                say("user", line)
                result: RunResult[None] = await session.run(user_input=line)
                for ev in result.events:
                    if ev.type == "function_call":
                        say("(tool)", f"{ev.item.name}({ev.item.arguments})")
                    elif ev.type == "message" and ev.item.role == "assistant":
                        say("front desk", ev.item.text_content or "")
            if args.live:
                print(
                    f"  (estimated model spend this session: ${budget.charge(session.usage.model_usage):.4f})"
                )
        print("  -- session closed --")
        if not state.workflow_ids:
            sys.exit("the model never called submit_for_verification; try again or run without --live")
        wf = env.client.get_workflow_handle_for(VerificationWorkflow.run, state.workflow_ids[0])

        async def status() -> TaskStatus | None:
            raw = await wf.query(VerificationWorkflow.context)
            return parse_context(raw).task.status if raw else None

        step(f"background verification: workflow {wf.id}")
        if args.kill_worker:
            while not any(
                a.heartbeat_details.payloads for a in (await wf.describe()).raw_description.pending_activities
            ):
                await asyncio.sleep(0.2)
            worker.send_signal(signal.SIGKILL)
            worker.wait()
            print("  worker SIGKILLed mid-verification. Starting a new one...")
            worker = start_worker()
        while await status() != TaskStatus.AWAITING_APPROVAL:
            await asyncio.sleep(0.3)
        print("  documents checked; waiting on a human reviewer")

        step("reviewer approves (and the webhook is redelivered)")
        decision = Decision(event_id="review-1", approved=True, reason="income verified")
        await wf.signal(VerificationWorkflow.decide, decision)
        await wf.signal(VerificationWorkflow.decide, decision)
        result_wf = await wf.result()
        final = parse_context(result_wf.context)
        print(
            f"  final status: {final.task.status.value}; "
            f"duplicates ignored: {result_wf.duplicate_decisions_ignored}"
        )
        if result_wf.verify_resumed_from:
            print(f"  verification resumed at document {result_wf.verify_resumed_from + 1}, not from scratch")
        print(f"  handoff trail: {' -> '.join(['front_desk', *[h.to_agent for h in final.hops]])}")

        step("the user's phone")
        for m in gateway.outbox(PHONE):
            say("sms in", m.body)

        step("user replies by SMS; a new headless session picks the conversation up")
        follow_model = make_follow()
        channel = SmsChannel(
            store=ConversationStore(store_path), gateway=GatewayClient(gateway.url), temporal=env.client,
            llm_factory=lambda: follow_model, task_queue=task_queue, timings=DEMO_TIMINGS,
        )  # fmt: skip
        app = web.Application()
        app.add_routes(channel.routes())
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        gateway.webhook_url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/sms/inbound"  # type: ignore[union-attr]
        if args.live:
            budget.check()
        say("sms out", SMS_REPLY)
        await GatewayClient(gateway.url).simulate_inbound(from_phone=PHONE, body=SMS_REPLY)
        say("sms in", gateway.outbox(PHONE)[-1].body)
        await runner.cleanup()

        sends = gateway.send_attempts()
        print(f"\nSMS delivered: {len(gateway.outbox(PHONE))}, send attempts: {len(sends)}")
        if args.live:
            print(f"total live-model spend so far: ${budget.spent:.4f} of ${budget.cap:.2f}")
    finally:
        if worker.poll() is None:
            worker.terminate()
            worker.wait()
        await gateway.stop()
        await env.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
