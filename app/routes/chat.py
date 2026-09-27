"""
POST /v1/chat — the only public-facing endpoint (LLD §3.1).

Streams SSE events back to the client: token → done → citations.
"""

from __future__ import annotations

import json

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from app.models.requests import ChatRequest

logger = structlog.get_logger()

router = APIRouter()


@router.post(
    "/v1/chat",
    summary="RAG chat — the single front door",
    description="Orchestrates IQS → Retrieval → LLM Gateway and streams the answer back via SSE",
)
async def chat(request: Request, body: ChatRequest):
    """
    Client-facing RAG endpoint.

    Returns an SSE stream:
      event: token   → {"text": "..."}
      event: done    → {"finishReason": "stop", "turnId": "...", ...}
      event: citations → {"sources": [...]}
    """
    orchestrator = request.app.state.orchestrator

    async def _event_stream():
        async for event in orchestrator.execute(request=body):
            yield {
                "event": event["event"],
                "data": event["data"],
            }

    return EventSourceResponse(_event_stream())
