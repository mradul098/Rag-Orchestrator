"""Response models — LLD §3.2, §3.3."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class CitationSource(BaseModel):
    document_id: str = Field(..., alias="documentId")
    document_name: str = Field(..., alias="documentName")
    page_no: Optional[int] = Field(None, alias="pageNo")
    chunk_id: str = Field(..., alias="chunkId")

    class Config:
        populate_by_name = True


class CitationsEvent(BaseModel):
    sources: List[CitationSource]


class ErrorResponse(BaseModel):
    error: bool = True
    code: str
    stage: str
    message: str
    session_id: Optional[str] = Field(None, alias="sessionId")

    class Config:
        populate_by_name = True
