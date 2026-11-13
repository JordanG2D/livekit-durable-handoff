"""Durable, long-running agent work with handoff across voice and text."""

from .contract import CURRENT_VERSION, HandoffContext, parse_context

__all__ = ["CURRENT_VERSION", "HandoffContext", "parse_context"]
