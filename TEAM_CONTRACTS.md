# Phase 2: Team Contracts & Integration Guide

If these four microservices are to be maintained by four different developers (or teams), strict adherence to API and database contracts is critical. This document outlines the exact boundaries, shared resources, and common pain points to watch out for.

---

## 1. Architecture & Dependency Graph

*   **RAG Orchestrator** (The Front Door): Depends on **IQS**, **Retrieval**, and **LLM Gateway**.
*   **IQS**: Depends on **LLM Gateway** (for rewriting) and the **Conversation Database**.
*   **Retrieval**: Depends on the **Vector Database** and **Document Metadata Database**.
*   **LLM Gateway**: Independent (talks to external providers like Vertex AI/Ollama).

---

## 2. Shared Database Contracts (High Risk)

The biggest integration risk is the **Conversation Store**, which is shared between IQS and the RAG Orchestrator to prevent passing massive conversation histories over the network.

### Database: `conversation_turns` table
*   **Owned by**: IQS Team (they run the migrations).
*   **Read/Written by**: IQS Team AND Orchestrator Team.

| Column | Type | Who Writes It | Who Reads It | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `turn_id` | UUID (PK) | **IQS** | Orchestrator | Unique identifier for the QA turn. |
| `session_id` | String | **IQS** | IQS / Orchestrator | Groups turns into a conversation. |
| `raw_query` | String | **IQS** | Orchestrator | The user's original question. |
| `rewritten_query` | String | **IQS** | Orchestrator | The contextualized question. |
| `final_answer` | String | **Orchestrator** | Orchestrator | The final LLM response. |
| `retrieved_chunk_ids`| JSON Array | **Orchestrator** | - | Stores which chunks were used for citations. |

**🚨 Pain Point for Teams:** 
*   If the **IQS Developer** changes the table schema (e.g., renames `session_id` to `chat_id`), the **Orchestrator** will immediately crash. 
*   The **Orchestrator Developer** must ensure they write the `final_answer` back using the exact `turn_id` provided by IQS in the HTTP response. If they fail to write this, the next turn won't have conversational history.

---

## 3. Service API Contracts

### A. IQS ↔ Orchestrator Contract
**Endpoint:** `POST /internal/rewrite` (Owned by IQS)

**Request (from Orchestrator):**
```json
{
  "sessionId": "sess-1",
  "tenantId": "00103",
  "requestingUser": {"userId": "user@example.com"},
  "rawQuery": "What about Q3?"
}
```

**Response (from IQS):**
```json
{
  "turnId": "uuid-1234",
  "intent": "needs_retrieval",
  "shortCircuit": false,
  "query": {
    "text": "What were total assets in Q3?",
    "topK": 8,
    "searchMode": "hybrid"
  }
}
```
**🚨 Pain Point:** The Orchestrator relies on `query.text`. If IQS changes the structure to `rewrittenQuery: "..."`, the Orchestrator will send empty queries to the Retrieval service.

### B. Retrieval ↔ Orchestrator Contract
**Endpoint:** `POST /internal/retrieve` (Owned by Retrieval)

**Request (from Orchestrator):**
```json
{
  "tenantId": "00103",
  "requestingUser": {"userId": "user@example.com"},
  "query": {
    "text": "What were total assets in Q3?",
    "topK": 8,
    "searchMode": "hybrid"
  }
}
```

**Response (from Retrieval):**
```json
{
  "results": [
    {
      "chunkId": "chunk-001",
      "chunkText": "Total assets in Q3 were $500M...",
      "documentId": "doc-001",
      "documentName": "Q3_Report.pdf",
      "pageNo": 5,
      "score": 0.92
    }
  ]
}
```
**🚨 Pain Point:** The Orchestrator uses `documentName`, `pageNo`, and `chunkId` to build the **Citations**. If the Retrieval developer renames `documentName` to `fileName`, the final UI will show "Unknown Source" for all citations.

### C. LLM Gateway ↔ Orchestrator (and IQS) Contract
**Endpoint:** `POST /v1/generate` (Owned by LLM Gateway)

**Request:**
```json
{
  "tenantId": "00103",
  "requestId": "uuid-123",
  "stream": true, 
  "messages": [{"role": "user", "content": "..."}]
}
```

**Response (Server-Sent Events):**
```text
event: token
data: {"text": "Hello"}

event: done
data: {"finishReason": "stop"}
```
**🚨 Pain Point:** SSE parsing is notoriously fragile. The Orchestrator explicitly looks for `event: token` and `event: done`. If the LLM Gateway developer decides to change `event: token` to `event: message`, the Orchestrator will silently swallow all tokens and the user will see a blank screen.

---

## 4. Operational Pain Points & Risks

If 4 different people are working on this, they need to agree on these operational rules:

1. **Timeout Cascades**: 
   * Orchestrator gives IQS **35 seconds**.
   * Orchestrator gives Retrieval **15 seconds**.
   * Orchestrator gives LLM Gateway **60 seconds**.
   * *Risk*: If the LLM Gateway developer switches to a slower model (e.g., reasoning model) that takes 70 seconds, the Orchestrator will aggressively kill the connection at 60s. The LLM Gateway developer must communicate latency changes.

2. **Graceful Degradation Agreement**:
   * If IQS fails, Orchestrator falls back to the `raw_query`. (IQS developer doesn't need to worry about bringing down the whole app).
   * If Retrieval fails, the Orchestrator throws a fatal error. (Retrieval developer is in the critical path).
   * If the LLM Gateway fails, both IQS and Orchestrator fail. (LLM Gateway developer has the highest uptime responsibility).

3. **Tenant Isolation**:
   * Every service receives `tenantId`. 
   * *Risk*: If the Retrieval developer forgets to apply the `tenantId` filter in Elasticsearch/FAISS, a user from Tenant A could retrieve financial documents from Tenant B. This is a critical security boundary that every team must test independently.
