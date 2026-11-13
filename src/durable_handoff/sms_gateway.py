"""A local fake SMS gateway, shaped like a real one (Twilio, Telnyx, ...).

Outbound: ``POST /v1/messages`` with an ``Idempotency-Key`` header. A repeated
key returns the original message and sends nothing. This is the property the
whole example leans on: Temporal runs activities *at least once*, so the
side effect at the end must be idempotent for the user to see *exactly one*
notification.

Inbound: ``POST /v1/simulate/inbound`` stands in for "the user typed a reply on
their phone". The gateway records it and forwards it to the app's webhook, the
way a real provider would.

State is in SQLite so it survives the worker processes the tests kill.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbound (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    to_phone TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS send_attempts (
    n INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL,
    duplicate INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS inbound (
    id TEXT PRIMARY KEY,
    from_phone TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass(frozen=True)
class Message:
    id: str
    to: str
    body: str
    idempotency_key: str


class FakeSmsGateway:
    def __init__(self, db_path: Path, *, webhook_url: str | None = None) -> None:
        self.db_path = db_path
        self.webhook_url = webhook_url
        self._db = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._runner: web.AppRunner | None = None
        self.url = ""

    # --- inspection helpers for tests and the demo ---------------------------

    def outbox(self, to: str | None = None) -> list[Message]:
        q = "SELECT id, to_phone, body, idempotency_key FROM outbound"
        rows = self._db.execute(
            q + (" WHERE to_phone = ?" if to else "") + " ORDER BY rowid", (to,) if to else ()
        ).fetchall()
        return [Message(id=r[0], to=r[1], body=r[2], idempotency_key=r[3]) for r in rows]

    def send_attempts(self) -> list[tuple[str, bool]]:
        rows = self._db.execute("SELECT idempotency_key, duplicate FROM send_attempts ORDER BY n").fetchall()
        return [(r[0], bool(r[1])) for r in rows]

    # --- HTTP ----------------------------------------------------------------

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> str:
        app = web.Application()
        app.router.add_post("/v1/messages", self._send)
        app.router.add_get("/v1/messages", self._list)
        app.router.add_post("/v1/simulate/inbound", self._inbound)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        sock = site._server.sockets[0]  # type: ignore[union-attr]
        self.url = f"http://{host}:{sock.getsockname()[1]}"
        return self.url

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
        self._db.close()

    async def _send(self, request: web.Request) -> web.Response:
        key = request.headers.get("Idempotency-Key")
        if not key:
            return web.json_response({"error": "Idempotency-Key header is required"}, status=400)
        data = await request.json()
        msg_id = f"SM{uuid.uuid4().hex[:16]}"
        cur = self._db.execute(
            "INSERT OR IGNORE INTO outbound (id, idempotency_key, to_phone, body) VALUES (?, ?, ?, ?)",
            (msg_id, key, data["to"], data["body"]),
        )
        duplicate = cur.rowcount == 0
        self._db.execute(
            "INSERT INTO send_attempts (idempotency_key, duplicate) VALUES (?, ?)", (key, int(duplicate))
        )
        row = self._db.execute("SELECT id FROM outbound WHERE idempotency_key = ?", (key,)).fetchone()
        return web.json_response({"id": row[0], "duplicate": duplicate}, status=200 if duplicate else 201)

    async def _list(self, request: web.Request) -> web.Response:
        to = request.query.get("to")
        return web.json_response([m.__dict__ for m in self.outbox(to)])

    async def _inbound(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg_id = f"IN{uuid.uuid4().hex[:16]}"
        self._db.execute(
            "INSERT INTO inbound (id, from_phone, body) VALUES (?, ?, ?)",
            (msg_id, data["from"], data["body"]),
        )
        if not self.webhook_url:
            return web.json_response({"id": msg_id, "forwarded": False}, status=202)
        payload = {"id": msg_id, "from": data["from"], "body": data["body"]}
        async with aiohttp.ClientSession() as http, http.post(self.webhook_url, json=payload) as resp:
            resp.raise_for_status()
        return web.json_response({"id": msg_id, "forwarded": True}, status=202)


class GatewayClient:
    """What the app uses to send. Talks HTTP, so it works from any process."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def send(self, *, to: str, body: str, idempotency_key: str) -> dict[str, Any]:
        async with (
            aiohttp.ClientSession() as http,
            http.post(
                f"{self.base_url}/v1/messages",
                json={"to": to, "body": body},
                headers={"Idempotency-Key": idempotency_key},
            ) as resp,
        ):
            resp.raise_for_status()
            result: dict[str, Any] = await resp.json()
            return result

    async def simulate_inbound(self, *, from_phone: str, body: str) -> dict[str, Any]:
        async with (
            aiohttp.ClientSession() as http,
            http.post(
                f"{self.base_url}/v1/simulate/inbound", json={"from": from_phone, "body": body}
            ) as resp,
        ):
            resp.raise_for_status()
            result: dict[str, Any] = await resp.json()
            return result
