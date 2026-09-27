# Phase 2 RAG Platform — Complete End-to-End Data Flow

**A single user question ("What were total assets in Q2?") traced through every service, every schema, every database call, every LLM prompt, and every response.**

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Step 0 — Client Sends Request](#step-0--client-sends-request)
3. [Step 1 — RAG Orchestrator receives and validates](#step-1--rag-orchestrator-receives-and-validates)
4. [Step 2 — RAG Orchestrator calls IQS](#step-2--rag-orchestrator-calls-iqs)
5. [Step 3 — IQS reads Conversation History (DB)](#step-3--iqs-reads-conversation-history-db)
6. [Step 4 — IQS calls LLM Gateway (query rewrite)](#step-4--iqs-calls-llm-gateway-query-rewrite)
7. [Step 5 — IQS writes Turn to DB and responds to Orchestrator](#step-5--iqs-writes-turn-to-db-and-responds-to-orchestrator)
8. [Step 6 — RAG Orchestrator calls Retrieval Service](#step-6--rag-orchestrator-calls-retrieval-service)
9. [Step 7 — Retrieval calls Embedding API](#step-7--retrieval-calls-embedding-api)
10. [Step 8 — Retrieval fires two Elasticsearch queries](#step-8--retrieval-fires-two-elasticsearch-queries)
11. [Step 9 — Retrieval fuses results and responds](#step-9--retrieval-fuses-results-and-responds)
12. [Step 10 — RAG Orchestrator assembles LLM prompt](#step-10--rag-orchestrator-assembles-llm-prompt)
13. [Step 11 — RAG Orchestrator calls LLM Gateway (generation)](#step-11--rag-orchestrator-calls-llm-gateway-generation)
14. [Step 12 — RAG Orchestrator streams tokens to client](#step-12--rag-orchestrator-streams-tokens-to-client)
15. [Step 13 — RAG Orchestrator writes final answer to DB](#step-13--rag-orchestrator-writes-final-answer-to-db)
16. [Step 14 — RAG Orchestrator sends Citations event](#step-14--rag-orchestrator-sends-citations-event)
17. [Database Schemas](#database-schemas)
18. [Complete Latency Breakdown](#complete-latency-breakdown)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT (Browser / App)                         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  POST /v1/chat  (SSE stream back)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               RAG ORCHESTRATION SERVICE   (:8000)                           │
│   ┌──────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐   │
│   │ Validate     │  │ Assemble Prompt  │  │ Stream SSE back to client   │   │
│   │ ChatRequest  │  │ (template + ctx) │  │ token → done → citations    │   │
│   └──────┬───────┘  └────────┬────────┘  └─────────────────────────────┘   │
└──────────┼───────────────────┼─────────────────────────────────────────────┘
           │                   │
    ┌──────▼──────┐     ┌──────▼──────────────────────────────┐
    │     IQS     │     │        LLM Gateway (:8001)           │
    │   (:8003)   │────▶│  POST /v1/generate  (streaming)      │
    │  /internal  │     │  Ollama (local) / Vertex AI (prod)   │
    │  /rewrite   │     └──────────────────────────────────────┘
    └──────┬──────┘
           │ reads/writes
    ┌──────▼──────────────────────────┐
    │  Conversation Store             │
    │  SQLite (local) / Postgres (prod)│
    │  conversation_turns table        │
    └─────────────────────────────────┘
           │
    ┌──────▼──────────────────────────┐
    │  Retrieval Service  (:8002)     │
    │  /internal/retrieve             │
    │  ┌──────────┐  ┌─────────────┐  │
    │  │ FAISS    │  │ SQLite FTS5 │  │  ← LOCAL
    │  │ (kNN)    │  │ (BM25)      │  │
    │  └──────────┘  └─────────────┘  │
    │  ┌──────────────────────────┐   │
    │  │ Elasticsearch (prod)     │   │  ← PRODUCTION
    │  │ rag-chunks-{tenantId}    │   │
    │  └──────────────────────────┘   │
    └─────────────────────────────────┘
```

---

## Step 0 — Client Sends Request

The client (browser, mobile app, API consumer) fires a single `POST /v1/chat` to the RAG Orchestration Service. This is the **only** public-facing endpoint in the entire system.

```http
POST http://rag-orchestrator:8000/v1/chat
Content-Type: application/json
Authorization: Bearer <upstream-jwt>   ← validated by API gateway, NOT by any microservice
```

### Request Body (ChatRequest)

```json
{
  "schemaVersion": "1.0",
  "requestId": "b7a1e9d0-3c2f-4e91-9c31-2f6a9b7d4e10",
  "sessionId": "sess-user123-2026-09-27",
  "tenantId": "00103",
  "rawQuery": "What were total assets in Q2?",
  "requestingUser": {
    "userId": "analyst@acmecorp.com",
    "roles": ["analyst"],
    "groups": ["grp-finance-au"]
  },
  "filters": {
    "documentModel": ["sampledocs"],
    "isDeleted": false,
    "legalHold": false,
    "dateRange": {
      "from": "2026-01-01T00:00:00Z",
      "to": "2026-09-27T00:00:00Z"
    },
    "classification": null
  },
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
}
```

### Field Semantics

| Field | Who populates it | Purpose |
|---|---|---|
| `requestId` | Client (or auto-generated by Orchestrator) | End-to-end trace correlation. Echoed in every downstream call. |
| `sessionId` | Client | Groups multiple turns into one conversation. Shared key for Conversation Store. |
| `tenantId` | API Gateway (from JWT claim) | Hard tenant isolation — never trusted from user input. Applied as pre-filter on every ES/FAISS query. |
| `requestingUser` | API Gateway (from JWT claims) | Already-resolved identity. No service downstream validates JWTs. |
| `filters.documentModel` | Client | Scope search to specific document folders. MVP ACL: tenantId + documentModel must both match. |
| `traceparent` | Client / API Gateway | W3C distributed tracing header — forwarded to all downstream services. |

---

## Step 1 — RAG Orchestrator receives and validates

**Service:** RAG Orchestration Service  
**File:** `app/routes/chat.py` → `app/services/orchestrator.py`

The Orchestrator validates the `ChatRequest` (Pydantic), then resolves the user identity it will use for all downstream calls:

```python
# If requestingUser is present (normal prod path):
user_id  = "analyst@acmecorp.com"
roles    = ["analyst"]
groups   = ["grp-finance-au"]

# If requestingUser absent (fallback, dev only):
user_id  = "user@00103"   # synthesised from tenantId
roles    = None
groups   = None
```

It also converts `filters` to the dict that will be forwarded verbatim to Retrieval:

```python
retrieval_filters = {
    "isDeleted": False,
    "legalHold": False,
    "documentModel": ["sampledocs"],
    "dateRange": {
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-09-27T00:00:00Z"
    }
}
```

---

## Step 2 — RAG Orchestrator calls IQS

**Caller:** RAG Orchestration Service  
**Endpoint:** `POST http://iqs:8003/internal/rewrite`

### Request Payload (IQSClient.rewrite)

```json
{
  "sessionId": "sess-user123-2026-09-27",
  "tenantId": "00103",
  "requestingUser": {
    "userId": "analyst@acmecorp.com"
  },
  "rawQuery": "What were total assets in Q2?"
}
```

> **Note:** IQS only needs `userId` for logging/history lookup. `roles` and `groups` are not forwarded to IQS because IQS doesn't make ACL decisions — it only rewrites the query.

---

## Step 3 — IQS reads Conversation History (DB)

**Service:** IQS  
**File:** `app/adapters/sqlite_conversation.py`

IQS queries the shared `conversation_turns` table to fetch the last N turns for this session:

```sql
SELECT turn_id, raw_query, rewritten_query, final_answer, turn_index
FROM conversation_turns
WHERE session_id = 'sess-user123-2026-09-27'
  AND tenant_id  = '00103'
ORDER BY turn_index DESC
LIMIT 5;
```

**For turn 1 (first ever message):** returns empty list `[]`.

**For turn 2+ (multi-turn, e.g. "What about Q3?"):** returns:

```json
[
  {
    "turn_id": "a1b2c3d4-...",
    "raw_query": "What were total assets in Q2?",
    "rewritten_query": "What were total assets in Q2 2026?",
    "final_answer": "According to [1] (Q2_2026_Balance_Sheet.pdf, Page 3), total assets for the quarter ended June 30, 2026 were $482.3M.",
    "turn_index": 1
  }
]
```

---

## Step 4 — IQS calls LLM Gateway (query rewrite)

**Caller:** IQS  
**Endpoint:** `POST http://llm-gateway:8001/v1/generate`  
**Mode:** `stream: false` (IQS needs the full rewrite synchronously, not streamed)

### Request Payload

```json
{
  "tenantId": "00103",
  "requestId": "iqs-rewrite-2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1",
  "stream": false,
  "messages": [
    {
      "role": "system",
      "content": "You are a query rewriting assistant for a document retrieval system...\n(see full system prompt below)"
    },
    {
      "role": "user",
      "content": "Conversation history:\n[none — first turn]\n\nUser query: What were total assets in Q2?\n\nTask: Rewrite the query as a standalone search query. Return JSON only."
    }
  ]
}
```

### IQS System Prompt (full)

```
You are a query rewriting assistant for a financial document retrieval system.

Your job is to take a user's raw query and conversation history, and produce:
1. A standalone, self-contained search query (removes pronouns, resolves references)
2. An intent classification

INTENT VALUES:
- needs_retrieval: user wants information from documents
- short_circuit: small talk, greetings, thanks, off-topic (e.g. "thanks!", "hello", "what's the weather")

RULES:
- If the query references previous context ("it", "that", "the same", "what about Q3"), 
  resolve the reference using the conversation history.
- Keep the rewritten query as a precise financial search query.
- Never add information not present in the history or query.
- Return ONLY valid JSON, no markdown, no explanation.

OUTPUT FORMAT:
{
  "rewrittenQuery": "...",
  "intent": "needs_retrieval" | "short_circuit",
  "searchMode": "hybrid" | "lexical" | "semantic",
  "topK": 8
}
```

### LLM Gateway Response (non-streaming)

```json
{
  "text": "{\"rewrittenQuery\": \"What were the total assets in Q2 2026?\", \"intent\": \"needs_retrieval\", \"searchMode\": \"hybrid\", \"topK\": 8}",
  "finishReason": "stop",
  "usage": {
    "inputTokens": 295,
    "outputTokens": 30
  },
  "modelUsed": "primary"
}
```

---

## Step 5 — IQS writes Turn to DB and responds to Orchestrator

### DB Write (INSERT)

IQS creates a new row in `conversation_turns` with the information it has right now. The `final_answer` and `retrieved_chunk_ids` are `NULL` — the RAG Orchestrator will fill them in later.

```sql
INSERT INTO conversation_turns (
  turn_id, session_id, tenant_id, raw_query,
  rewritten_query, intent, final_answer, retrieved_chunk_ids, turn_index
) VALUES (
  '2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1',
  'sess-user123-2026-09-27',
  '00103',
  'What were total assets in Q2?',
  'What were the total assets in Q2 2026?',
  'needs_retrieval',
  NULL,           -- ← RAG Orchestrator fills this in Step 13
  NULL,           -- ← RAG Orchestrator fills this in Step 13
  1               -- turn index (1 = first message in session)
);
```

### IQS Response to RAG Orchestrator

```json
{
  "turnId": "2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1",
  "intent": "needs_retrieval",
  "shortCircuit": false,
  "query": {
    "text": "What were the total assets in Q2 2026?",
    "topK": 8,
    "searchMode": "hybrid"
  },
  "warnings": [],
  "latencyMs": {
    "history": 1,
    "rewrite": 314,
    "store": 2,
    "total": 321
  }
}
```

> **Short-circuit example** — if query was `"thanks!"`:
> ```json
> {
>   "turnId": "50a48902-...",
>   "shortCircuit": true,
>   "reason": "non_document_query",
>   "suggestedResponse": "You're welcome! Let me know if you have any other questions.",
>   "warnings": []
> }
> ```
> When `shortCircuit: true`, the Orchestrator skips Steps 6–14, emits one token + done event, and returns. No retrieval, no LLM generation.

**RAG Orchestrator now knows:**
- `turnId = "2c9f5fa0-..."`  (to update after generation)
- `rewritten_query = "What were the total assets in Q2 2026?"`
- `top_k = 8`, `search_mode = "hybrid"`

---

## Step 6 — RAG Orchestrator calls Retrieval Service

**Caller:** RAG Orchestration Service  
**Endpoint:** `POST http://retrieval:8002/internal/retrieve`

### Request Payload (full LLD §3.1 contract)

```json
{
  "schemaVersion": "1.0",
  "requestId": "b7a1e9d0-3c2f-4e91-9c31-2f6a9b7d4e10",
  "tenantId": "00103",
  "turnId": "2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1",
  "requestingUser": {
    "userId": "analyst@acmecorp.com",
    "roles": ["analyst"],
    "groups": ["grp-finance-au"]
  },
  "query": {
    "text": "What were the total assets in Q2 2026?",
    "retrievalUnit": "chunk",
    "topK": 8,
    "searchMode": "hybrid",
    "filters": {
      "documentModel": ["sampledocs"],
      "isDeleted": false,
      "legalHold": false,
      "dateRange": {
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-09-27T00:00:00Z"
      },
      "classification": null
    },
    "rerank": false
  },
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
}
```

**Key things the Retrieval Service does with each field:**
- `tenantId` → mandatory ES pre-filter on both search legs (primary ACL)
- `requestingUser.roles/groups` → reserved for future ACL (Option C/D in LLD §9.3)
- `query.text` → embedded via Gemini for kNN leg; passed as-is for lexical leg
- `query.filters.documentModel` → ES pre-filter + defense-in-depth re-check (MVP ACL)
- `query.filters.dateRange` → ES range filter on `created_at`
- `turnId` → opaque passthrough, echoed in response

---

## Step 7 — Retrieval calls Embedding API

**Caller:** Retrieval Service  
**Target:** Ollama (local) / Gemini Embedding API (prod)

```http
POST http://localhost:11434/api/embeddings
Content-Type: application/json

{
  "model": "nomic-embed-text",
  "prompt": "What were the total assets in Q2 2026?"
}
```

**Response:**
```json
{
  "embedding": [0.0123, -0.0456, 0.7821, ..., 0.9788]
}
```
→ 768-dimensional float vector. **Must use the exact same model** as the Embedding & Indexing Service used at index time. If they diverge, cosine similarity compares vectors from different embedding spaces and results are garbage.

---

## Step 8 — Retrieval fires two Elasticsearch queries

Both queries run **in parallel** (`asyncio.gather`). The same mandatory filters are applied as **pre-filters** on both legs — never post-filters (LLD §9.4: pre-filtering ensures `k` relevant documents are returned, not `k` documents that then get filtered down).

### Leg A — Lexical (BM25) search

```json
POST rag-chunks-00103/_search

{
  "query": {
    "bool": {
      "must": [
        { "match": { "chunk_text": "What were the total assets in Q2 2026?" } }
      ],
      "filter": [
        { "term":  { "tenant_id":      "00103"       } },
        { "term":  { "is_deleted":     false          } },
        { "term":  { "legal_hold":     false          } },
        { "terms": { "document_model": ["sampledocs"] } },
        { "range": { "created_at":     { "gte": "2026-01-01", "lte": "2026-09-27" } } }
      ]
    }
  },
  "size": 40,
  "_source": ["chunk_id", "chunk_text", "tenant_id", "document_id", "document_name",
              "document_model", "chunk_type", "page_no", "heading", "section",
              "document_uri", "is_deleted", "legal_hold"]
}
```

### Leg B — kNN (Vector) search

```json
POST rag-chunks-00103/_search

{
  "knn": {
    "field": "embedding",
    "query_vector": [0.0123, -0.0456, 0.7821, ..., 0.9788],
    "k": 40,
    "num_candidates": 200,
    "filter": [
      { "term":  { "tenant_id":      "00103"       } },
      { "term":  { "is_deleted":     false          } },
      { "term":  { "legal_hold":     false          } },
      { "terms": { "document_model": ["sampledocs"] } },
      { "range": { "created_at":     { "gte": "2026-01-01", "lte": "2026-09-27" } } }
    ]
  },
  "_source": ["chunk_id", "chunk_text", "tenant_id", "document_id", "document_name",
              "document_model", "chunk_type", "page_no", "heading", "section",
              "document_uri", "is_deleted", "legal_hold"]
}
```

### Elasticsearch Document Schema (stored at index time by Embedding & Indexing Service)

```json
{
  "_index": "rag-chunks-00103",
  "_id":    "DOC001#chunk-1",
  "_source": {
    "chunk_id":          "DOC001#chunk-1",
    "tenant_id":         "00103",
    "document_id":       "DOC001",
    "version_series_id": "VS-DOC001-v2",
    "document_name":     "Q2_2026_Balance_Sheet.pdf",
    "document_model":    "sampledocs",
    "document_uri":      "gs://dbdocstore-00103/sampledocs/.../Q2_2026_Balance_Sheet.pdf",
    "chunk_text":        "Total assets for the quarter ended June 30, 2026 were $482.3 million, an increase of 12% year-over-year.",
    "embedding":         [0.0123, -0.0456, ..., 0.9788],
    "chunk_type":        "table",
    "page_no":           3,
    "heading":           "Balance Sheet",
    "section":           "Financial Statements",
    "is_deleted":        false,
    "legal_hold":        false,
    "classification":    null,
    "created_at":        "2026-09-01T00:00:00Z"
  }
}
```

---

## Step 9 — Retrieval fuses results and responds

### Application-side RRF Fusion

Both result sets (up to 40 hits each) are merged using Reciprocal Rank Fusion:

```python
def fuse_results(lexical_hits, knn_hits, rank_constant=20, top_k=8):
    scores = {}
    for rank, hit in enumerate(lexical_hits):
        scores[hit.chunk_id] = scores.get(hit.chunk_id, 0) + 1 / (rank_constant + rank + 1)
    for rank, hit in enumerate(knn_hits):
        scores[hit.chunk_id] = scores.get(hit.chunk_id, 0) + 1 / (rank_constant + rank + 1)
    
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
```

### Defense-in-depth re-check (application-side, LLD §9.4)

After fusion, every hit is re-checked:
```python
# For each hit, verify:
assert hit.tenant_id == "00103"             # ← primary isolation
assert hit.is_deleted == False
assert hit.legal_hold == False
assert hit.document_model in ["sampledocs"] # ← MVP ACL: documentModel
```
Any hit that fails is dropped and logged as a security event.

### Retrieval Service Response to RAG Orchestrator

```json
{
  "requestId": "b7a1e9d0-3c2f-4e91-9c31-2f6a9b7d4e10",
  "tenantId":  "00103",
  "turnId":    "2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1",
  "accessPolicyVersion": "v0-tenant-only",
  "results": [
    {
      "chunkId":         "DOC001#chunk-1",
      "versionSeriesId": "VS-DOC001-v2",
      "documentId":      "DOC001",
      "documentName":    "Q2_2026_Balance_Sheet.pdf",
      "score":           0.0943,
      "scoreBreakdown":  { "lexical": 0.0476, "semantic": 0.0476 },
      "chunkText":       "Total assets for the quarter ended June 30, 2026 were $482.3 million...",
      "chunkType":       "table",
      "pageNo":          3,
      "heading":         "Balance Sheet",
      "section":         "Financial Statements",
      "documentUri":     "gs://dbdocstore-00103/sampledocs/.../Q2_2026_Balance_Sheet.pdf",
      "accessDecision":  "ALLOWED_TENANT_ONLY"
    },
    {
      "chunkId":         "DOC001#chunk-2",
      "documentName":    "Q2_2026_Balance_Sheet.pdf",
      "score":           0.0862,
      "scoreBreakdown":  { "lexical": 0.0432, "semantic": 0.0430 },
      "chunkText":       "Current assets increased by 12% year-over-year, driven by cash and equivalents...",
      "chunkType":       "paragraph",
      "pageNo":          4,
      "accessDecision":  "ALLOWED_TENANT_ONLY"
    }
    // ... 6 more chunks
  ],
  "totalCandidatesConsidered": 67,
  "latencyMs": {
    "embed":  17,
    "search": 25,
    "total":  42
  },
  "warnings": []
}
```

---

## Step 10 — RAG Orchestrator assembles LLM prompt

**File:** `app/services/prompt_builder.py`

The Orchestrator takes:
1. The **8 chunks** from Retrieval (ranked by fused score)
2. The **conversation history** from the Conversation Store (if any)
3. The **system prompt template** from disk (`templates/qa-with-citations/v1.txt`)
4. The **rewritten query** from IQS

And assembles the `messages` array:

### System Message (from `v1.txt` template)

```
You are a financial research assistant for a document retrieval system.

INSTRUCTIONS:
- Answer the user's question using ONLY the provided context below.
- If the context does not contain enough information to answer, say so honestly.
- Reference your sources by number (e.g., [1], [2]) when citing specific facts.
- Be concise and precise. Use financial terminology appropriately.
- Do NOT make up information that is not in the provided context.
```

### User Message (assembled by prompt_builder.py)

```
[1] (Source: Q2_2026_Balance_Sheet.pdf, Page 3)
Total assets for the quarter ended June 30, 2026 were $482.3 million, an increase of 12% year-over-year.

[2] (Source: Q2_2026_Balance_Sheet.pdf, Page 4)
Current assets increased by 12% year-over-year, driven by cash and equivalents...

[3] (Source: Q2_2026_Income_Statement.pdf, Page 2)
Net revenue for Q2 2026 was $124.7 million...

[4] (Source: Q2_2026_Cash_Flow.pdf, Page 3)
Operating cash flow for Q2 2026 was $38.2 million...

... [5]–[8] more chunks ...

Question: What were the total assets in Q2 2026?
```

### Full messages array sent to LLM Gateway

```json
[
  {
    "role": "system",
    "content": "You are a financial research assistant for a document retrieval system.\n\nINSTRUCTIONS:\n- Answer the user's question using ONLY the provided context below.\n- If the context does not contain enough information to answer, say so honestly.\n- Reference your sources by number (e.g., [1], [2]) when citing specific facts.\n- Be concise and precise. Use financial terminology appropriately.\n- Do NOT make up information that is not in the provided context."
  },
  {
    "role": "user",
    "content": "[1] (Source: Q2_2026_Balance_Sheet.pdf, Page 3)\nTotal assets for the quarter ended June 30, 2026 were $482.3 million...\n\n[2] (Source: Q2_2026_Balance_Sheet.pdf, Page 4)\nCurrent assets increased by 12% year-over-year...\n\n... [3]–[8] ...\n\nQuestion: What were the total assets in Q2 2026?"
  }
]
```

---

## Step 11 — RAG Orchestrator calls LLM Gateway (generation)

**Caller:** RAG Orchestration Service  
**Endpoint:** `POST http://llm-gateway:8001/v1/generate`  
**Mode:** `stream: true` (client needs tokens as they arrive)

### Request Payload

```json
{
  "tenantId": "00103",
  "requestId": "2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1",
  "stream": true,
  "messages": [
    { "role": "system", "content": "..." },
    { "role": "user",   "content": "..." }
  ]
}
```

### LLM Gateway internals (what it does with this)

1. Checks if primary model (e.g. `llama3.2` via Ollama) is healthy
2. If healthy → sends to primary
3. If primary fails → falls back to fallback model (e.g. `mistral`)
4. Streams back Server-Sent Events from Ollama / Vertex AI

### LLM Gateway SSE Response Stream

```
event: token
data: {"text": "According"}

event: token
data: {"text": " to"}

event: token
data: {"text": " [1]"}

event: token
data: {"text": " (Source:"}

event: token
data: {"text": " Q2_2026_Balance_Sheet.pdf,"}

event: token
data: {"text": " Page"}

event: token
data: {"text": " 3),"}

event: token
data: {"text": " total"}

event: token
data: {"text": " assets"}

event: token
data: {"text": " for"}

event: token
data: {"text": " the"}

event: token
data: {"text": " quarter"}

event: token
data: {"text": " ended"}

event: token
data: {"text": " June"}

event: token
data: {"text": " 30,"}

event: token
data: {"text": " 2026"}

event: token
data: {"text": " were"}

event: token
data: {"text": " $482.3M."}

event: done
data: {
  "finishReason": "stop",
  "usage": { "inputTokens": 847, "outputTokens": 44 },
  "modelUsed": "primary"
}
```

---

## Step 12 — RAG Orchestrator streams tokens to client

The Orchestrator acts as a **transparent relay** — it reads each `(event_type, data)` tuple from the LLM Gateway stream and immediately re-emits it to the client SSE connection:

```
event: token
data: {"text": "According"}

event: token
data: {"text": " to"}

... (all tokens forwarded as received) ...

event: done
data: {"finishReason": "stop", "usage": {...}, "turnId": "2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1"}
```

> **Note:** The Orchestrator **adds `turnId`** to the `done` event. The LLM Gateway doesn't know the turnId — the Orchestrator injects it so the client can correlate the response to the conversation turn in its own state.

---

## Step 13 — RAG Orchestrator writes final answer to DB

After the `done` event is received from LLM Gateway, the Orchestrator writes back to the Conversation Store. This is the row that IQS created in Step 5 with `NULL` values.

```sql
UPDATE conversation_turns
SET
  final_answer        = 'According to [1] (Source: Q2_2026_Balance_Sheet.pdf, Page 3), total assets for the quarter ended June 30, 2026 were $482.3M.',
  retrieved_chunk_ids = '["DOC001#chunk-1","DOC001#chunk-2","DOC002#chunk-1","DOC002#chunk-2","DOC001#chunk-3","DOC003#chunk-2","DOC004#chunk-2","DOC005#chunk-1"]'
WHERE turn_id = '2c9f5fa0-553c-49d0-8ddf-eb86ea17f4c1';
```

**Why this matters:** The next time the user asks a follow-up question ("What about Q3?"), IQS will fetch this row and use `final_answer` as conversation context for the rewrite.

> **Failure mode (LLD §7):** If this UPDATE fails (DB down, timeout), the Orchestrator logs the error and continues. The client has already received their answer. The only consequence is that the next turn won't have this answer in its conversation history — IQS will see a gap in the history.

---

## Step 14 — RAG Orchestrator sends Citations event

After updating the DB, the Orchestrator emits one final SSE event — the **citations** — derived from all 8 chunks that were used to build the prompt:

```
event: citations
data: {
  "sources": [
    {
      "documentId":   "DOC001",
      "documentName": "Q2_2026_Balance_Sheet.pdf",
      "pageNo":       3,
      "chunkId":      "DOC001#chunk-1"
    },
    {
      "documentId":   "DOC001",
      "documentName": "Q2_2026_Balance_Sheet.pdf",
      "pageNo":       4,
      "chunkId":      "DOC001#chunk-2"
    },
    {
      "documentId":   "DOC002",
      "documentName": "Q2_2026_Income_Statement.pdf",
      "pageNo":       2,
      "chunkId":      "DOC002#chunk-1"
    },
    {
      "documentId":   "DOC003",
      "documentName": "Q2_2026_Cash_Flow.pdf",
      "pageNo":       3,
      "chunkId":      "DOC003#chunk-2"
    },
    {
      "documentId":   "DOC004",
      "documentName": "Annual_Risk_Assessment_2026.pdf",
      "pageNo":       12,
      "chunkId":      "DOC004#chunk-2"
    }
  ]
}
```

The client UI uses this to render clickable source references ("Cited from Q2_2026_Balance_Sheet.pdf, Page 3") below the answer.

**Complete SSE stream from client's perspective:**
```
: ping                           ← keepalive (every 15s)
event: token
data: {"text": "According"}
event: token
data: {"text": " to [1]..."}
... (N token events) ...
event: done
data: {"finishReason": "stop", "turnId": "2c9f5fa0-...", "usage": {...}}
event: citations
data: {"sources": [...]}
```

---

## Database Schemas

### `conversation_turns` (shared between IQS and RAG Orchestrator)

```sql
CREATE TABLE IF NOT EXISTS conversation_turns (
  turn_id             TEXT PRIMARY KEY,           -- UUID, set by IQS
  session_id          TEXT NOT NULL,              -- groups turns into a conversation
  tenant_id           TEXT NOT NULL,              -- hard isolation, never queryable cross-tenant
  raw_query           TEXT NOT NULL,              -- original user message
  rewritten_query     TEXT,                       -- IQS output (what was sent to Retrieval)
  intent              TEXT,                       -- "needs_retrieval" | "short_circuit"
  final_answer        TEXT,                       -- NULL until RAG Orchestrator fills in (Step 13)
  retrieved_chunk_ids TEXT,                       -- JSON array of chunkIds, NULL until Step 13
  turn_index          INTEGER NOT NULL,           -- 1-based position in the session
  created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_turns_session ON conversation_turns(session_id, tenant_id, turn_index);
```

**Who writes what:**

| Column | Written by | When |
|---|---|---|
| All columns except `final_answer`, `retrieved_chunk_ids` | **IQS** | Step 5 (after rewrite) |
| `final_answer` | **RAG Orchestrator** | Step 13 (after LLM generation) |
| `retrieved_chunk_ids` | **RAG Orchestrator** | Step 13 (after LLM generation) |

### Elasticsearch Index Mapping (`rag-chunks-{tenantId}`)

```json
{
  "mappings": {
    "properties": {
      "chunk_id":          { "type": "keyword" },
      "tenant_id":         { "type": "keyword" },
      "document_id":       { "type": "keyword" },
      "version_series_id": { "type": "keyword" },
      "document_name":     { "type": "keyword" },
      "document_model":    { "type": "keyword" },
      "document_uri":      { "type": "keyword" },
      "chunk_text":        { "type": "text"    },
      "embedding": {
        "type":       "dense_vector",
        "dims":       768,
        "index":      true,
        "similarity": "cosine",
        "index_options": { "type": "hnsw", "m": 32, "ef_construction": 100 }
      },
      "chunk_type":    { "type": "keyword" },
      "page_no":       { "type": "integer" },
      "heading":       { "type": "text"    },
      "section":       { "type": "text"    },
      "is_deleted":    { "type": "boolean" },
      "legal_hold":    { "type": "boolean" },
      "classification":{ "type": "keyword" },
      "created_at":    { "type": "date"    }
    }
  }
}
```

---

## Complete Latency Breakdown

For a warm request (embeddings already cached in model, ES index warm):

| Step | Component | Latency |
|---|---|---|
| Client → Orchestrator | Network | ~5ms |
| Orchestrator → IQS | Network | ~2ms |
| IQS DB read (history) | SQLite/Postgres | ~1ms |
| IQS → LLM Gateway (rewrite) | Ollama (local) | ~300ms |
| IQS DB write (turn INSERT) | SQLite/Postgres | ~2ms |
| IQS → Orchestrator | Network | ~2ms |
| Orchestrator → Retrieval | Network | ~2ms |
| Retrieval → Embedding API | Ollama (local) | ~17ms |
| Retrieval → ES (2× parallel) | Elasticsearch | ~25ms |
| Retrieval fusion + re-check | In-process | ~1ms |
| Retrieval → Orchestrator | Network | ~2ms |
| Orchestrator assembles prompt | In-process | ~1ms |
| Orchestrator → LLM Gateway (gen) | Ollama (local) | ~600ms (to first token) |
| LLM tokens streamed | Network | streaming |
| Orchestrator DB write (UPDATE) | SQLite/Postgres | ~2ms |
| **Total (to first token)** | | **~960ms** |
| **Total (full answer)** | | **~2–4s** (model dependent) |

**Production (with Vertex AI, Elasticsearch cloud):**
- Rewrite: ~200ms  
- Embedding: ~40ms  
- ES search: ~100ms  
- Generation first token: ~400ms  
- **Total to first token: ~750ms**
