# RAG Orchestration Service

The single front door for the Phase 2 RAG pipeline. The **only** internet-facing service — receives the client's raw query and orchestrates IQS → Retrieval → LLM Gateway, streaming the final answer back via SSE.

## Architecture

```
Client
  │
  ▼  POST /v1/chat (SSE stream)
┌─────────────────────────────────────┐
│  RAG Orchestration Service (:8000)  │
│                                     │
│  1. Call IQS (:8003)                │  → rewritten query + turnId
│  2. Call Retrieval (:8002)          │  → ranked chunks
│  3. Fetch conversation history      │  → from shared conversation store
│  4. Assemble prompt (template v1)   │  → system + context + history + query
│  5. Stream LLM Gateway (:8001)      │  → relay token events to client
│  6. Update conversation store       │  → final_answer + chunk_ids
│  7. Append citations event          │  → document sources
└─────────────────────────────────────┘
```

## SSE Response Format

```
event: token
data: {"text": "Total "}

event: token
data: {"text": "assets "}

event: token
data: {"text": "were $482.3M."}

event: done
data: {"finishReason": "stop", "turnId": "...", "usage": {...}}

event: citations
data: {"sources": [{"documentId": "...", "documentName": "Q2-2026-10Q.pdf", "pageNo": 3, "chunkId": "..."}]}
```

## Running Locally

Requires all 3 downstream services running:
```bash
# Terminal 1: LLM Gateway
cd ../llm-gateway-service && source venv/bin/activate && ENVIRONMENT=local uvicorn app.main:app --port 8001

# Terminal 2: Retrieval Service (seed first!)
cd ../retrieval-service && source venv/bin/activate && ENVIRONMENT=local uvicorn app.main:app --port 8002

# Terminal 3: IQS
cd ../iqs-service && source venv/bin/activate && ENVIRONMENT=local uvicorn app.main:app --port 8003

# Terminal 4: RAG Orchestration
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
ENVIRONMENT=local uvicorn app.main:app --port 8000
```

## Testing

```bash
# Unit tests (mocked downstream — no services needed)
python -m pytest tests/ -v

# End-to-end (all 4 services running)
curl -N http://localhost:8000/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "sessionId": "sess-1",
    "tenantId": "00103",
    "rawQuery": "What were total assets in Q2?"
  }'
```

## Failure Handling (LLD §7)

| Failure | Behavior |
|---|---|
| IQS fails | Fall back to raw query, proceed with retrieval |
| Retrieval fails | Return structured error, stop pipeline |
| Zero chunks returned | Call LLM with "no context" instruction (model generates honest response) |
| LLM Gateway fails | Return structured error with `LLM_GATEWAY_FAILED` |
| Conversation Store fails | Log and proceed — never blocks the response |
