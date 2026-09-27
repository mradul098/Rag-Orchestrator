# RAG Orchestration Service — Progress

## Status: COMPLETE ✅ (contract gap fixed 2026-09-27)

## Completed
- [x] Project skeleton (pyproject.toml, requirements.txt, .env.example)
- [x] Config (app/config.py)
- [x] Package __init__.py files (app, routes, models, services, adapters, interfaces)
- [x] Request model — **full LLD §3.1 contract**: requestingUser (roles+groups), filters (documentModel, dateRange, classification), requestId, traceparent
- [x] Response models (app/models/responses.py) — LLD §3.2, §3.3
- [x] Downstream HTTP clients — RetrievalClient now forwards full contract (roles, groups, filters, requestId, traceparent)
- [x] Orchestrator — accepts ChatRequest, extracts user identity + filters, passes verbatim to Retrieval
- [x] Prompt builder (app/services/prompt_builder.py) — LLD §5

## Contract Gap Fixed
Initial ChatRequest only carried sessionId + tenantId + rawQuery. Fixed:
- `requestingUser` (userId, roles, groups) → forwarded to Retrieval for future ACL
- `filters` (documentModel, dateRange, classification, legalHold, isDeleted) → forwarded for search scoping
- `requestId`, `schemaVersion`, `traceparent` → distributed tracing
- Retrieval `_defense_in_depth_check` now enforces documentModel ACL (MVP: tenantId + documentModel must match)
- [x] Prompt template v1 (app/templates/qa-with-citations/v1.txt)
- [x] Conversation Store interface (app/interfaces/conversation_store.py)
- [x] SQLite conversation store adapter (app/adapters/sqlite_conversation.py)
- [x] Postgres conversation store adapter (app/adapters/postgres_conversation.py)
- [x] Orchestration pipeline (app/services/orchestrator.py) — LLD §4
- [x] POST /v1/chat route with SSE streaming (app/routes/chat.py)
- [x] FastAPI app entry point (app/main.py)
- [x] Tests — 13 passing (prompt builder, pipeline, failures, validation)
- [x] README.md
- [x] Dockerfile
- [x] End-to-end integration test — all 4 services running, full pipeline verified

## Verified E2E Flow
```
Client → POST /v1/chat (:8000)
  → IQS (:8003) rewrites "What were total assets in Q2?" → standalone query
  → Retrieval (:8002) returns 8 chunks from seeded data
  → LLM Gateway (:8001) streams answer referencing [1] Q2_2026_Balance_Sheet.pdf
  → Citations appended with all 8 source documents
```

## Open Decisions (from LLD)
- TBD-RO-8: Latency SLO — sum of all downstream service latencies
- TBD-RO-10: History window shared with IQS — using same value (5) for MVP
- TBD-RO-11: Citations — all prompt chunks for MVP (not model-referenced only)
- TBD-RO-12: Zero-chunks — using model-generated "no info found" response
- TBD-RO-13: Upstream auth mechanism — assumes tenantId in body for now
