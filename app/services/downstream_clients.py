"""HTTP clients for calling downstream services (IQS, Retrieval, LLM Gateway)."""

from __future__ import annotations

import json
from typing import AsyncIterator, Dict, List, Optional, Tuple

import httpx
import structlog

logger = structlog.get_logger()


class DownstreamError(Exception):
    """Raised when a downstream service call fails."""
    def __init__(self, message: str, service: str, status_code: Optional[int] = None):
        self.service = service
        self.status_code = status_code
        super().__init__(message)


class IQSClient:
    """Calls IQS POST /internal/rewrite."""

    def __init__(self, base_url: str, timeout_s: int = 35):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s

    async def rewrite(
        self, session_id: str, tenant_id: str, user_id: str, raw_query: str
    ) -> dict:
        """
        Call IQS to rewrite the query. Returns the full JSON response.
        Raises DownstreamError on failure.
        """
        payload = {
            "sessionId": session_id,
            "tenantId": tenant_id,
            "requestingUser": {"userId": user_id},
            "rawQuery": raw_query,
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(f"{self._base_url}/internal/rewrite", json=payload)

            if resp.status_code != 200:
                raise DownstreamError(
                    f"IQS returned {resp.status_code}: {resp.text[:200]}",
                    service="iqs", status_code=resp.status_code,
                )
            return resp.json()
        except httpx.TimeoutException as e:
            raise DownstreamError(f"IQS timed out after {self._timeout}s", service="iqs") from e
        except httpx.HTTPError as e:
            raise DownstreamError(f"IQS HTTP error: {e}", service="iqs") from e


class RetrievalClient:
    """Calls Retrieval Service POST /internal/retrieve — full LLD §3.1 contract."""

    def __init__(self, base_url: str, timeout_s: int = 15):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s

    async def retrieve(
        self,
        tenant_id: str,
        user_id: str,
        query_text: str,
        top_k: int = 8,
        search_mode: str = "hybrid",
        turn_id: Optional[str] = None,
        # Full contract fields
        request_id: Optional[str] = None,
        user_roles: Optional[List[str]] = None,
        user_groups: Optional[List[str]] = None,
        filters: Optional[Dict] = None,
        traceparent: Optional[str] = None,
    ) -> dict:
        """
        Call Retrieval Service with the full LLD §3.1 contract.
        Passes through roles, groups, and all filters so ACL and scoping work.
        """
        requesting_user: Dict = {"userId": user_id}
        if user_roles:
            requesting_user["roles"] = user_roles
        if user_groups:
            requesting_user["groups"] = user_groups

        payload: Dict = {
            "schemaVersion": "1.0",
            "tenantId": tenant_id,
            "requestingUser": requesting_user,
            "query": {
                "text": query_text,
                "topK": top_k,
                "searchMode": search_mode,
                "filters": filters or {
                    # Mandatory safety defaults — LLD §3.2
                    "isDeleted": False,
                    "legalHold": False,
                },
            },
        }
        if request_id:
            payload["requestId"] = request_id
        if turn_id:
            payload["turnId"] = turn_id
        if traceparent:
            payload["traceparent"] = traceparent

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(f"{self._base_url}/internal/retrieve", json=payload)

            if resp.status_code != 200:
                raise DownstreamError(
                    f"Retrieval returned {resp.status_code}: {resp.text[:200]}",
                    service="hybrid_retrieval", status_code=resp.status_code,
                )
            return resp.json()
        except httpx.TimeoutException as e:
            raise DownstreamError(
                f"Retrieval timed out after {self._timeout}s", service="hybrid_retrieval"
            ) from e
        except httpx.HTTPError as e:
            raise DownstreamError(f"Retrieval HTTP error: {e}", service="hybrid_retrieval") from e


class LLMGatewayClient:
    """
    Calls LLM Gateway POST /v1/generate with streaming.

    Consumes the SSE stream and yields token text chunks.
    """

    def __init__(self, base_url: str, timeout_s: int = 60):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s

    async def stream_generate(
        self,
        messages: List[dict],
        tenant_id: str,
        request_id: str,
    ) -> AsyncIterator[Tuple[str, dict]]:
        """
        Stream tokens from LLM Gateway. Yields (event_type, data_dict) tuples.

        Event types: "token", "done", "error"
        """
        payload = {
            "tenantId": tenant_id,
            "requestId": request_id,
            "messages": messages,
            "stream": True,
        }

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                async with client.stream(
                    "POST",
                    f"{self._base_url}/v1/generate",
                    json=payload,
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        raise DownstreamError(
                            f"LLM Gateway returned {resp.status_code}: {body[:200]}",
                            service="llm_gateway", status_code=resp.status_code,
                        )

                    current_event = "token"
                    async for line in resp.aiter_lines():
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("event:"):
                            current_event = line[len("event:"):].strip()
                        elif line.startswith("data:"):
                            data_str = line[len("data:"):].strip()
                            try:
                                data = json.loads(data_str)
                            except json.JSONDecodeError:
                                data = {"text": data_str}
                            yield (current_event, data)

        except httpx.TimeoutException as e:
            raise DownstreamError(
                f"LLM Gateway timed out after {self._timeout}s", service="llm_gateway"
            ) from e
        except httpx.HTTPError as e:
            raise DownstreamError(f"LLM Gateway HTTP error: {e}", service="llm_gateway") from e
