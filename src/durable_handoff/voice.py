"""Optional: run the front desk as a real LiveKit agent (voice, or text console).

    python -m durable_handoff.voice console --text   # framework's text console, no audio
    python -m durable_handoff.voice console          # local mic/speaker
    python -m durable_handoff.voice dev              # join rooms on your LiveKit project

Needs ``LIVEKIT_URL``/``LIVEKIT_API_KEY``/``LIVEKIT_API_SECRET`` (models go
through LiveKit Inference), a Temporal server at ``TEMPORAL_ADDRESS``
(default localhost:7233), and a worker (``python -m durable_handoff.worker``).
Tests never import this module.
"""

from __future__ import annotations

import os

from livekit.agents import AgentServer, AgentSession, JobContext, cli, inference

from .agents import FrontDeskAgent, SessionState
from .worker import connect

server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: JobContext) -> None:
    temporal = await connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    # The room name doubles as the conversation id. A real deployment would use
    # its own id (a ticket, a CRM record) and put it in the room metadata.
    state = SessionState(conversation_id=ctx.room.name, temporal=temporal, channel="voice")
    session = AgentSession[SessionState](
        stt=inference.STT(os.environ.get("STT_MODEL", "deepgram/flux-general"), language="en"),
        llm=inference.LLM(os.environ.get("LLM_MODEL", "openai/gpt-4.1-mini")),
        tts=inference.TTS(os.environ.get("TTS_MODEL", "cartesia/sonic-3")),
        userdata=state,
    )
    await session.start(agent=FrontDeskAgent(), room=ctx.room)
    await session.generate_reply(instructions="Greet the caller and ask what they need verified.")


if __name__ == "__main__":
    cli.run_app(server)
