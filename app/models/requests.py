"""Request models — RAG Orchestration LLD §3.1 / Retrieval LLD §3.1."""

from __future__ import annotations

import uuid
from typing import List, Optional

from pydantic import BaseModel, Field


class RequestingUser(BaseModel):
    """
    Identity of the requesting user — resolved upstream (e.g. API gateway)
    before hitting the RAG Orchestration Service.
    """

    user_id: str = Field(..., alias="userId")
    roles: Optional[List[str]] = None
    groups: Optional[List[str]] = None

    model_config = {"populate_by_name": True}


class DateRange(BaseModel):
    """Optional date filter — maps directly to Retrieval Service contract."""

    from_date: Optional[str] = Field(default=None, alias="from")
    to_date: Optional[str] = Field(default=None, alias="to")

    model_config = {"populate_by_name": True}


class QueryFilters(BaseModel):
    """
    Optional search filters passed through to Retrieval Service.
    Server always ANDs tenantId + safety filters regardless of what's here.
    """

    document_model: Optional[List[str]] = Field(
        default=None, alias="documentModel",
        description="Scope search to specific document models (folders). MVP ACL: tenantId + documentModel must match."
    )
    version_series_ids: Optional[List[str]] = Field(default=None, alias="versionSeriesIds")
    legal_hold: bool = Field(default=False, alias="legalHold")
    is_deleted: bool = Field(default=False, alias="isDeleted")
    date_range: Optional[DateRange] = Field(default=None, alias="dateRange")
    classification: Optional[str] = None

    model_config = {"populate_by_name": True}


class ChatRequest(BaseModel):
    """
    POST /v1/chat — the only public-facing endpoint on the RAG Orchestration Service.

    The full request contract flows through to Retrieval Service so that:
      - roles / groups can be used for future per-document ACL checks
      - filters (documentModel, dateRange, etc.) scope the retrieval search
      - requestId / traceparent enable end-to-end distributed tracing
    """

    schema_version: str = Field(default="1.0", alias="schemaVersion")
    request_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        alias="requestId",
        description="Client-provided or auto-generated trace correlation ID.",
    )
    session_id: str = Field(..., alias="sessionId", min_length=1)
    tenant_id: str = Field(..., alias="tenantId", min_length=1)
    raw_query: str = Field(..., alias="rawQuery", min_length=1)
    requesting_user: Optional[RequestingUser] = Field(
        default=None, alias="requestingUser",
        description="Resolved identity from upstream auth. If absent, synthesised from tenantId.",
    )
    filters: Optional[QueryFilters] = Field(
        default=None,
        description="Optional search filters forwarded verbatim to Retrieval Service.",
    )
    traceparent: Optional[str] = Field(
        default=None,
        description="W3C traceparent header value for distributed tracing.",
    )

    model_config = {"populate_by_name": True}
