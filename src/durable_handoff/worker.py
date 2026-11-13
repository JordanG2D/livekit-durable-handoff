"""Run a Temporal worker for the verification workflow.

    python -m durable_handoff.worker --gateway-url http://127.0.0.1:8099

Kill it at any point (Ctrl+C, ``kill -9``) and start it again: the workflow
picks up where the history says it was.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from .activities import Activities
from .workflow import TASK_QUEUE, VerificationWorkflow


async def connect(address: str) -> Client:
    return await Client.connect(address, data_converter=pydantic_data_converter)


def build_worker(client: Client, activities: Activities, *, task_queue: str = TASK_QUEUE) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[VerificationWorkflow],
        activities=[activities.verify_documents, activities.notify_user],
    )


async def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--address", default="localhost:7233")
    p.add_argument("--task-queue", default=TASK_QUEUE)
    p.add_argument("--gateway-url", required=True)
    p.add_argument("--store", type=Path, default=Path("conversations.db"))
    p.add_argument("--step-seconds", type=float, default=2.0, help="simulated time per document")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s worker %(levelname)s %(message)s")

    client = await connect(args.address)
    acts = Activities(gateway_url=args.gateway_url, store_path=args.store, step_seconds=args.step_seconds)
    worker = build_worker(client, acts, task_queue=args.task_queue)
    logging.info("worker polling task queue %r", args.task_queue)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
