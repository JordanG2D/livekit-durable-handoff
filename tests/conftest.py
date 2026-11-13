"""Shared fixtures.

Integration tests run against a real Temporal dev server (``start_local``),
because the property under test is that state lives *outside* the worker. The
worker itself runs as a subprocess so tests can SIGKILL it.

Nothing here calls a model: every agent uses ``ScriptedLLM``.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from aiohttp import web
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

from durable_handoff.sms_gateway import FakeSmsGateway, GatewayClient
from durable_handoff.store import ConversationStore
from durable_handoff.workflow import Timings

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".temporal-cache"

# Fast timings: a killed worker's activity is retried ~2s after its last heartbeat.
FAST = Timings(heartbeat_timeout_s=2, verify_timeout_s=60, approval_timeout_s=60, notify_timeout_s=4)
STEP_SECONDS = 0.4


@pytest.fixture(scope="session")
async def temporal() -> AsyncIterator[WorkflowEnvironment]:
    CACHE.mkdir(exist_ok=True)
    env = await WorkflowEnvironment.start_local(
        data_converter=pydantic_data_converter,
        download_dest_dir=str(CACHE),
        dev_server_log_level="error",
    )
    yield env
    await env.shutdown()


@pytest.fixture
def client(temporal: WorkflowEnvironment) -> Client:
    return temporal.client


@dataclass
class Harness:
    """Everything a scenario needs, isolated per test (own task queue, own DBs)."""

    client: Client
    address: str
    task_queue: str
    gateway: FakeSmsGateway
    gateway_client: GatewayClient
    store_path: Path
    tmp: Path
    workers: list[subprocess.Popen[bytes]] = field(default_factory=list)

    def start_worker(self, *, faults: str = "") -> subprocess.Popen[bytes]:
        env = {**os.environ, "DURABLE_HANDOFF_FAULTS": faults, "PYTHONUNBUFFERED": "1"}
        log = open(self.tmp / f"worker-{len(self.workers)}.log", "wb")  # noqa: SIM115
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "durable_handoff.worker",
                "--address", self.address,
                "--task-queue", self.task_queue,
                "--gateway-url", self.gateway.url,
                "--store", str(self.store_path),
                "--step-seconds", str(STEP_SECONDS),
            ],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )  # fmt: skip
        self.workers.append(proc)
        return proc

    @staticmethod
    def kill(proc: subprocess.Popen[bytes]) -> None:
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    def store(self) -> ConversationStore:
        return ConversationStore(self.store_path)

    def worker_logs(self) -> str:
        return "\n".join(p.read_text(errors="replace") for p in sorted(self.tmp.glob("worker-*.log")))


@pytest.fixture
async def harness(client: Client, temporal: WorkflowEnvironment, tmp_path: Path) -> AsyncIterator[Harness]:
    gateway = FakeSmsGateway(tmp_path / "sms.db")
    await gateway.start()
    h = Harness(
        client=client,
        address=client.service_client.config.target_host,
        task_queue=f"tq-{uuid.uuid4().hex[:8]}",
        gateway=gateway,
        gateway_client=GatewayClient(gateway.url),
        store_path=tmp_path / "conversations.db",
        tmp=tmp_path,
    )
    yield h
    for p in h.workers:
        if p.poll() is None:
            h.kill(p)
    await gateway.stop()


async def serve(routes: list[web.RouteDef]) -> tuple[str, Callable[[], object]]:
    """Run an aiohttp app on a free port; returns (url, async cleanup)."""
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return f"http://127.0.0.1:{port}", runner.cleanup


async def eventually(check: Callable[[], object], *, timeout: float = 30, interval: float = 0.1) -> object:
    """Poll an (async or sync) check until it returns truthy."""
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


@pytest.fixture
def conversation_id() -> Iterator[str]:
    yield f"conv-{uuid.uuid4().hex[:8]}"
