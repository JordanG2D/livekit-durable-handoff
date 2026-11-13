"""The live-demo spending cap refuses further model turns once it's used up."""

from __future__ import annotations

from pathlib import Path

import pytest
from livekit.agents.metrics.usage import LLMModelUsage

from durable_handoff.budget import Budget, BudgetExceeded


def test_cap_is_enforced_and_persists_across_runs(tmp_path: Path) -> None:
    ledger = tmp_path / "budget.json"
    budget = Budget(cap_usd=5.0, ledger=ledger)
    budget.check()  # fresh ledger: allowed

    # 1M output tokens at the pessimistic $20/M is $20, well past the cap.
    budget.charge([LLMModelUsage(provider="x", model="y", input_tokens=0, output_tokens=1_000_000)])
    with pytest.raises(BudgetExceeded):
        budget.check()
    with pytest.raises(BudgetExceeded):
        Budget(cap_usd=5.0, ledger=ledger).check()  # a new run reads the same ledger


def test_small_demo_costs_cents(tmp_path: Path) -> None:
    budget = Budget(cap_usd=5.0, ledger=tmp_path / "budget.json")
    cost = budget.charge([LLMModelUsage(provider="x", model="y", input_tokens=8_000, output_tokens=1_000)])
    assert cost < 0.10
    budget.check()
