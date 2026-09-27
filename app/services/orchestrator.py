"""
Orchestrator — the single-pass pipeline (LLD §4).

Steps:
  1. Call IQS  →  rewritten query + turnId (or short-circuit)
  2. Call Retrieval Service  →  ranked chunks
  3. Fetch conversation history  →  for prompt assembly
  4. Assemble prompt (template + query + chunks + history)
  5. Stream LLM Gateway  →  relay tokens to client
  6. Update Conversation Store  →  final_answer + chunk_ids
  7. Append citations event
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import Dict, List, Optional

import structlog

from app.config import Settings
from app.interfaces.conversation_store import ConversationStore
from app.models.requests import ChatRequest
from app.models.responses import CitationSource, CitationsEvent, ErrorResponse
from app.services.downstream_clients import (
    DownstreamError,
    IQSClient,
    LLMGatewayClient,
    RetrievalClient,
)
from app.services.prompt_builder import build_prompt_messages, load_system_prompt

logger = structlog.get_logger()


class Orchestrator:
    """Single-pass RAG orchestrator — sequences IQS → Retrieval → LLM Gateway."""

    def __init__(
        self,
        iqs_client: IQSClient,
        retrieval_client: RetrievalClient,
        llm_client: LLMGatewayClient,
        conversation_store: ConversationStore,
        settings: Settings,
    ):
        self._iqs = iqs_client
        self._retrieval = retrieval_client
        self._llm = llm_client
        self._store = conversation_store
        self._settings = settings
        self._system_prompt = load_system_prompt(version=settings.prompt_template_version)

    async def execute(self, request: ChatRequest) -> AsyncIterator[dict]:
        """
        Execute the full RAG pipeline. Yields SSE event dicts:
          {"event": "token", "data": ...}
          {"event": "done", "data": ...}
          {"event": "citations", "data": ...}

        On unrecoverable failure, yields a single error event.
        """
        start = time.monotonic()
        session_id = request.session_id
        tenant_id = request.tenant_id
        raw_query = request.raw_query

        # Resolve user identity — comes from upstream auth, or synthetic fallback
        if request.requesting_user:
            user_id = request.requesting_user.user_id
            user_roles = request.requesting_user.roles
            user_groups = request.requesting_user.groups
        else:
            # TBD-RO-13: synthesise from tenantId until upstream auth is wired
            user_id = f"user@{tenant_id}"
            user_roles = None
            user_groups = None

        # Build filters dict to forward to Retrieval Service
        retrieval_filters: Optional[Dict] = None
        if request.filters:
            f = request.filters
            retrieval_filters = {
                "isDeleted": f.is_deleted,
                "legalHold": f.legal_hold,
            }
            if f.document_model:
                retrieval_filters["documentModel"] = f.document_model
            if f.version_series_ids:
                retrieval_filters["versionSeriesIds"] = f.version_series_ids
            if f.classification:
                retrieval_filters["classification"] = f.classification
            if f.date_range:
                retrieval_filters["dateRange"] = {
                    "from": f.date_range.from_date,
                    "to": f.date_range.to_date,
                }

        warnings = []

        # ── Step 1: Call IQS ─────────────────────────────────────────────
        iqs_start = time.monotonic()
        turn_id = None
        rewritten_query = raw_query  # fallback
        search_mode = "hybrid"
        top_k = 8

        try:
            iqs_resp = await self._iqs.rewrite(
                session_id=session_id,
                tenant_id=tenant_id,
                user_id=user_id,
                raw_query=raw_query,
            )

            turn_id = iqs_resp.get("turnId")

            # Check for short-circuit (LLD §3.3)
            if iqs_resp.get("shortCircuit"):
                suggested = iqs_resp.get("suggestedResponse", "I can help you with document-related questions.")
                # For short-circuit: emit as a single token + done, no retrieval needed
                yield {"event": "token", "data": json.dumps({"text": suggested})}
                yield {"event": "done", "data": json.dumps({
                    "finishReason": "short_circuit",
                    "turnId": turn_id,
                    "shortCircuit": True,
                })}
                return

            # Normal path
            query_data = iqs_resp.get("query", {})
            rewritten_query = query_data.get("text", raw_query)
            search_mode = query_data.get("searchMode", "hybrid")
            top_k = query_data.get("topK", 8)

        except DownstreamError as e:
            # LLD §7: IQS failure → fall back to raw query
            logger.warning("iqs_call_failed", error=str(e))
            warnings.append(f"IQS unavailable: using raw query")

        iqs_ms = int((time.monotonic() - iqs_start) * 1000)
        logger.info("step_iqs_complete", turn_id=turn_id, rewritten_query=rewritten_query[:80], iqs_ms=iqs_ms)

        # ── Step 2: Call Retrieval Service ────────────────────────────────
        retrieval_start = time.monotonic()
        chunks = []
        try:
            retrieval_resp = await self._retrieval.retrieve(
                tenant_id=tenant_id,
                user_id=user_id,
                query_text=rewritten_query,
                top_k=top_k,
                search_mode=search_mode,
                turn_id=turn_id,
                # Full contract fields — forwarded verbatim
                request_id=request.request_id,
                user_roles=user_roles,
                user_groups=user_groups,
                filters=retrieval_filters,
                traceparent=request.traceparent,
            )
            chunks = retrieval_resp.get("results", [])
        except DownstreamError as e:
            # LLD §7: Retrieval failure → fatal, return error
            logger.error("retrieval_call_failed", error=str(e))
            yield {"event": "error", "data": json.dumps(
                ErrorResponse(
                    code="RETRIEVAL_FAILED", stage="hybrid_retrieval",
                    message="Could not retrieve relevant information at this time.",
                    session_id=session_id,
                ).model_dump(by_alias=True)
            )}
            return

        retrieval_ms = int((time.monotonic() - retrieval_start) * 1000)
        logger.info("step_retrieval_complete", chunk_count=len(chunks), retrieval_ms=retrieval_ms)

        # ── Step 3: Fetch conversation history ────────────────────────────
        history = []
        try:
            history = await self._store.get_recent_turns(
                session_id=session_id,
                tenant_id=tenant_id,
                limit=self._settings.history_window_size,
            )
        except Exception as e:
            # LLD §7: history failure → proceed without history
            logger.warning("history_fetch_failed", error=str(e))

        # ── Step 4: Assemble prompt ───────────────────────────────────────
        messages = build_prompt_messages(
            rewritten_query=rewritten_query,
            chunks=chunks,
            history=history,
            system_prompt=self._system_prompt,
        )

        # ── Step 5: Stream LLM Gateway → relay to client ─────────────────
        llm_start = time.monotonic()
        full_answer_parts = []
        request_id = turn_id or f"rag-{session_id}"

        try:
            async for event_type, data in self._llm.stream_generate(
                messages=messages,
                tenant_id=tenant_id,
                request_id=request_id,
            ):
                if event_type == "token":
                    text = data.get("text", "")
                    full_answer_parts.append(text)
                    yield {"event": "token", "data": json.dumps({"text": text})}

                elif event_type == "done":
                    data["turnId"] = turn_id
                    yield {"event": "done", "data": json.dumps(data)}

                elif event_type == "error":
                    yield {"event": "error", "data": json.dumps(data)}
                    return

        except DownstreamError as e:
            logger.error("llm_gateway_failed", error=str(e))
            yield {"event": "error", "data": json.dumps(
                ErrorResponse(
                    code="LLM_GATEWAY_FAILED", stage="llm_gateway",
                    message="The language model is temporarily unavailable. Please try again.",
                    session_id=session_id,
                ).model_dump(by_alias=True)
            )}
            return

        llm_ms = int((time.monotonic() - llm_start) * 1000)
        full_answer = "".join(full_answer_parts)

        # ── Step 6: Update Conversation Store ─────────────────────────────
        if turn_id:
            chunk_ids = [c.get("chunkId", c.get("chunk_id", "")) for c in chunks]
            try:
                await self._store.update_turn_answer(
                    turn_id=turn_id,
                    final_answer=full_answer,
                    retrieved_chunk_ids=chunk_ids,
                )
            except Exception as e:
                # LLD §7: store write failure → log, don't fail
                logger.warning("turn_update_failed", turn_id=turn_id, error=str(e))

        # ── Step 7: Append citations event ────────────────────────────────
        citations = []
        for chunk in chunks:
            citations.append(CitationSource(
                document_id=chunk.get("documentId", chunk.get("document_id", "")),
                document_name=chunk.get("documentName", chunk.get("document_name", "Unknown")),
                page_no=chunk.get("pageNo", chunk.get("page_no")),
                chunk_id=chunk.get("chunkId", chunk.get("chunk_id", "")),
            ))

        citations_event = CitationsEvent(sources=citations)
        yield {"event": "citations", "data": json.dumps(
            citations_event.model_dump(by_alias=True)
        )}

        total_ms = int((time.monotonic() - start) * 1000)
        logger.info(
            "orchestration_complete",
            turn_id=turn_id,
            tenant_id=tenant_id,
            iqs_ms=iqs_ms,
            retrieval_ms=retrieval_ms,
            llm_ms=llm_ms,
            total_ms=total_ms,
            chunks_used=len(chunks),
            answer_length=len(full_answer),
        )
