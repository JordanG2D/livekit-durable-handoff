"""Where a conversation's ``HandoffContext`` lives between sessions.

The Temporal workflow owns the context while the task runs. When it finishes,
the notify activity writes the final context here, and the SMS channel reads it
back when the user replies. SQLite keeps the example dependency-free; any
key-value store with a compare-and-set works the same way.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .contract import HandoffContext, dump_context, parse_context

_SCHEMA = """
CREATE TABLE IF NOT EXISTS contexts (
    conversation_id TEXT PRIMARY KEY,
    phone TEXT NOT NULL,
    body TEXT NOT NULL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS contexts_phone ON contexts (phone, updated_at);
"""


class ConversationStore:
    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)

    def save(self, ctx: HandoffContext) -> None:
        """Upsert. Saving the same context twice is a no-op, so retries are safe."""
        self._db.execute(
            "INSERT INTO contexts (conversation_id, phone, body) VALUES (?, ?, ?) "
            "ON CONFLICT(conversation_id) DO UPDATE SET body = excluded.body, "
            "phone = excluded.phone, updated_at = CURRENT_TIMESTAMP",
            (ctx.conversation_id, ctx.contact.phone, json.dumps(dump_context(ctx))),
        )

    def get(self, conversation_id: str) -> HandoffContext | None:
        row = self._db.execute(
            "SELECT body FROM contexts WHERE conversation_id = ?", (conversation_id,)
        ).fetchone()
        return parse_context(row[0]) if row else None

    def latest_for_phone(self, phone: str) -> HandoffContext | None:
        row = self._db.execute(
            "SELECT body FROM contexts WHERE phone = ? ORDER BY updated_at DESC, rowid DESC LIMIT 1", (phone,)
        ).fetchone()
        return parse_context(row[0]) if row else None

    def close(self) -> None:
        self._db.close()
