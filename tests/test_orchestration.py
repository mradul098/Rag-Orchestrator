"""
Tests for the RAG Orchestration Service.

Uses mock downstream clients — no real IQS/Retrieval/LLM Gateway needed.
"""

from __future__ import annotations

import json
from typing import AsyncIterator, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import Settings
from app.interfaces.conversation_store import ConversationStore
from app.models.requests import ChatRequest, QueryFilters, RequestingUser
from app.services.downstream_clients import (
    DownstreamError,
    IQSClient,
    LLMGatewayClient,
    RetrievalClient,
)
from app.services.orchestrator import Orchestrator
from app.services.prompt_builder import build_prompt_messages


# ── Mock conversation store ──────────────────────────────────────────────

class FakeConversationStore(ConversationStore):
    def __init__(self):
        self.updated_turns = {}

    async def get_recent_turns(self, session_id, tenant_id, limit=5):
        return []

    async def update_turn_answer(self, turn_id, final_answer, retrieved_chunk_ids=None):
        self.updated_turns[turn_id] = {
            "final_answer": final_answer,
            "retrieved_chunk_ids": retrieved_chunk_ids,
        }


# ── Mock IQS response ───────────────────────────────────────────────────

MOCK_IQS_NORMAL = {
    "turnId": "turn-abc-123",
    "query": {"text": "What were total assets in Q2 2026?", "topK": 5, "searchMode": "hybrid"},
    "intent": "needs_retrieval",
    "warnings": [],
}

MOCK_IQS_SHORT_CIRCUIT = {
    "turnId": "turn-short-1",
    "shortCircuit": True,
    "reason": "non_document_query",
    "suggestedResponse": "You're welcome! Let me know if you have any other questions.",
}

# ── Mock Retrieval response ──────────────────────────────────────────────

MOCK_RETRIEVAL = {
    "results": [
        {
            "chunkId": "chunk-001",
            "chunkText": "Total assets as of Q2 2026 were $482.3 million.",
            "documentId": "doc-001",
            "documentName": "Q2-2026-10Q.pdf",
            "pageNo": 3,
            "score": 0.92,
        },
        {
            "chunkId": "chunk-002",
            "chunkText": "Current assets increased by 12% year-over-year.",
            "documentId": "doc-001",
            "documentName": "Q2-2026-10Q.pdf",
            "pageNo": 4,
            "score": 0.85,
        },
    ],
    "latencyMs": {"embed": 20, "search": 30, "total": 50},
}

# ── Helpers ──────────────────────────────────────────────────────────────

VALID_REQUEST = {
    "sessionId": "sess-test-1",
    "tenantId": "00103",
    "rawQuery": "What were total assets in Q2?",
    "requestingUser": {
        "userId": "analyst@example.com",
        "roles": ["analyst"],
        "groups": ["grp-finance-au"],
    },
    "filters": {
        "documentModel": ["sampledocs"],
        "isDeleted": False,
        "legalHold": False,
    },
}


def _make_request(
    session_id="sess-1",
    tenant_id="00103",
    raw_query="test",
    user_id="analyst@example.com",
    roles=None,
    groups=None,
    document_model=None,
) -> ChatRequest:
    """Helper: construct a ChatRequest for tests."""
    return ChatRequest(
        sessionId=session_id,
        tenantId=tenant_id,
        rawQuery=raw_query,
        requestingUser=RequestingUser(userId=user_id, roles=roles, groups=groups) if user_id else None,
        filters=QueryFilters(documentModel=document_model) if document_model else None,
    )


def _build_orchestrator(
    iqs_response=None,
    iqs_error=False,
    retrieval_response=None,
    retrieval_error=False,
    llm_tokens=None,
    llm_error=False,
) -> Tuple[Orchestrator, FakeConversationStore]:
    """Create an orchestrator with mocked downstream clients."""

    # IQS
    iqs = IQSClient(base_url="http://fake:8003")
    if iqs_error:
        iqs.rewrite = AsyncMock(side_effect=DownstreamError("IQS down", service="iqs"))
    else:
        iqs.rewrite = AsyncMock(return_value=iqs_response or MOCK_IQS_NORMAL)

    # Retrieval
    retrieval = RetrievalClient(base_url="http://fake:8002")
    if retrieval_error:
        retrieval.retrieve = AsyncMock(
            side_effect=DownstreamError("Retrieval down", service="hybrid_retrieval")
        )
    else:
        retrieval.retrieve = AsyncMock(return_value=retrieval_response or MOCK_RETRIEVAL)

    # LLM Gateway (async generator)
    llm = LLMGatewayClient(base_url="http://fake:8001")
    tokens = llm_tokens or ["Total ", "assets ", "were $482.3M."]

    async def _mock_stream(*args, **kwargs):
        if llm_error:
            raise DownstreamError("LLM Gateway down", service="llm_gateway")
        for token in tokens:
            yield ("token", {"text": token})
        yield ("done", {"finishReason": "stop", "usage": {"inputTokens": 100, "outputTokens": 20}})

    llm.stream_generate = _mock_stream

    store = FakeConversationStore()
    settings = Settings(environment="local", sqlite_db_path="/tmp/fake.db")

    orch = Orchestrator(
        iqs_client=iqs,
        retrieval_client=retrieval,
        llm_client=llm,
        conversation_store=store,
        settings=settings,
    )
    return orch, store


async def _collect_events(orchestrator, session_id="sess-1", tenant_id="00103", raw_query="test"):
    events = []
    request = _make_request(session_id=session_id, tenant_id=tenant_id, raw_query=raw_query)
    async for event in orchestrator.execute(request=request):
        events.append(event)
    return events


# ── Prompt Builder Tests ─────────────────────────────────────────────────

class TestPromptBuilder:
    def test_basic_prompt(self):
        messages = build_prompt_messages(
            rewritten_query="What were total assets?",
            chunks=[
                {"chunkText": "Total assets were $482.3M.", "documentName": "10Q.pdf", "pageNo": 3},
            ],
        )
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert "[1]" in messages[1]["content"]
        assert "10Q.pdf" in messages[1]["content"]
        assert "What were total assets?" in messages[1]["content"]

    def test_no_chunks(self):
        messages = build_prompt_messages(
            rewritten_query="What is this?",
            chunks=[],
        )
        assert "No relevant documents" in messages[1]["content"]

    def test_with_history(self):
        messages = build_prompt_messages(
            rewritten_query="What about Q3?",
            chunks=[{"chunkText": "Q3 data.", "documentName": "10Q.pdf", "pageNo": 5}],
            history=[{"raw_query": "Q2 assets?", "final_answer": "Total assets were $482.3M."}],
        )
        assert "Previous conversation" in messages[1]["content"]
        assert "Q2 assets?" in messages[1]["content"]


# ── Orchestrator Pipeline Tests ──────────────────────────────────────────

class TestOrchestrator:

    @pytest.mark.asyncio
    async def test_normal_flow(self):
        orch, store = _build_orchestrator()
        events = await _collect_events(orch)

        # Should have: 3 tokens + 1 done + 1 citations = 5 events
        event_types = [e["event"] for e in events]
        assert event_types.count("token") == 3
        assert "done" in event_types
        assert "citations" in event_types

        # Verify token content
        token_texts = [json.loads(e["data"])["text"] for e in events if e["event"] == "token"]
        assert "".join(token_texts) == "Total assets were $482.3M."

        # Verify citations
        cit_event = [e for e in events if e["event"] == "citations"][0]
        cit_data = json.loads(cit_event["data"])
        assert len(cit_data["sources"]) == 2
        assert cit_data["sources"][0]["documentName"] == "Q2-2026-10Q.pdf"

        # Verify done event includes turnId
        done_event = [e for e in events if e["event"] == "done"][0]
        done_data = json.loads(done_event["data"])
        assert done_data["turnId"] == "turn-abc-123"

        # Verify store was updated
        assert "turn-abc-123" in store.updated_turns
        assert store.updated_turns["turn-abc-123"]["final_answer"] == "Total assets were $482.3M."

    @pytest.mark.asyncio
    async def test_short_circuit(self):
        orch, _ = _build_orchestrator(iqs_response=MOCK_IQS_SHORT_CIRCUIT)
        events = await _collect_events(orch)

        # Short-circuit: 1 token + 1 done = 2 events (no retrieval, no citations)
        assert len(events) == 2
        token_data = json.loads(events[0]["data"])
        assert "welcome" in token_data["text"].lower()

        done_data = json.loads(events[1]["data"])
        assert done_data["shortCircuit"] is True

    @pytest.mark.asyncio
    async def test_iqs_failure_falls_back_to_raw_query(self):
        orch, _ = _build_orchestrator(iqs_error=True)
        events = await _collect_events(orch, raw_query="What about Q2?")

        # Should still work — falls back to raw query
        event_types = [e["event"] for e in events]
        assert "token" in event_types
        assert "done" in event_types
        assert "citations" in event_types

    @pytest.mark.asyncio
    async def test_retrieval_failure_returns_error(self):
        orch, _ = _build_orchestrator(retrieval_error=True)
        events = await _collect_events(orch)

        # Should return a single error event
        assert len(events) == 1
        assert events[0]["event"] == "error"
        err = json.loads(events[0]["data"])
        assert err["code"] == "RETRIEVAL_FAILED"
        assert err["stage"] == "hybrid_retrieval"

    @pytest.mark.asyncio
    async def test_llm_gateway_failure_returns_error(self):
        orch, _ = _build_orchestrator(llm_error=True)
        events = await _collect_events(orch)

        error_events = [e for e in events if e["event"] == "error"]
        assert len(error_events) == 1
        err = json.loads(error_events[0]["data"])
        assert err["code"] == "LLM_GATEWAY_FAILED"

    @pytest.mark.asyncio
    async def test_zero_chunks_still_calls_llm(self):
        """LLD TBD-RO-12: zero chunks → model generates 'no info found' response."""
        orch, _ = _build_orchestrator(
            retrieval_response={"results": []},
            llm_tokens=["I couldn't find relevant information."],
        )
        events = await _collect_events(orch)

        event_types = [e["event"] for e in events]
        assert "token" in event_types
        assert "done" in event_types
        # Citations should be empty
        cit = [e for e in events if e["event"] == "citations"][0]
        assert len(json.loads(cit["data"])["sources"]) == 0


# ── Request Validation Tests ─────────────────────────────────────────────

class TestRequestValidation:

    def _client(self):
        from app.routes.chat import router
        app = FastAPI()
        app.include_router(router)
        orch, _ = _build_orchestrator()
        app.state.orchestrator = orch
        return TestClient(app)

    def test_missing_session_id(self):
        client = self._client()
        resp = client.post("/v1/chat", json={"tenantId": "00103", "rawQuery": "test"})
        assert resp.status_code == 422

    def test_missing_tenant_id(self):
        client = self._client()
        resp = client.post("/v1/chat", json={"sessionId": "s1", "rawQuery": "test"})
        assert resp.status_code == 422

    def test_empty_raw_query(self):
        client = self._client()
        resp = client.post("/v1/chat", json={"sessionId": "s1", "tenantId": "00103", "rawQuery": ""})
        assert resp.status_code == 422

    def test_valid_request_streams_sse(self):
        client = self._client()
        resp = client.post("/v1/chat", json=VALID_REQUEST)
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")
