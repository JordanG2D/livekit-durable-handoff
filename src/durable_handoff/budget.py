"""A hard spending cap for the live demo.

Prices are deliberately pessimistic (well above any small model's list price),
so the estimate overstates real spend. The ledger is a local file, so the cap
holds across runs, not just within one. Delete ``.budget.json`` to reset.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from livekit.agents.metrics.usage import LLMModelUsage

# USD per million tokens. Pessimistic on purpose; edit if you use a pricier model.
INPUT_PER_M = 5.0
OUTPUT_PER_M = 20.0


class BudgetExceeded(RuntimeError):
    pass


class Budget:
    def __init__(self, cap_usd: float = 5.0, ledger: Path = Path(".budget.json")) -> None:
        self.cap = cap_usd
        self.ledger = ledger

    @property
    def spent(self) -> float:
        try:
            return float(json.loads(self.ledger.read_text())["spent_usd"])
        except (FileNotFoundError, KeyError, ValueError):
            return 0.0

    def check(self) -> None:
        """Call before every model turn."""
        if self.spent >= self.cap:
            raise BudgetExceeded(f"live-model budget of ${self.cap:.2f} is used up (${self.spent:.4f} spent)")

    def charge(self, usage: Sequence[object]) -> float:
        """Add a finished session's usage to the ledger; returns this session's cost."""
        cost = 0.0
        for u in usage:
            if isinstance(u, LLMModelUsage):
                cost += u.input_tokens / 1e6 * INPUT_PER_M + u.output_tokens / 1e6 * OUTPUT_PER_M
        self.ledger.write_text(json.dumps({"spent_usd": round(self.spent + cost, 6)}))
        return cost
