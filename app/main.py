"""FastAPI entry point — RAG Orchestration Service."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.config import get_settings
from app.routes.chat import router

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(colors=True),
    ],
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    # ── Downstream clients ───────────────────────────────────────────────
    from app.services.downstream_clients import IQSClient, RetrievalClient, LLMGatewayClient

    iqs_client = IQSClient(base_url=settings.iqs_url, timeout_s=settings.iqs_timeout_s)
    retrieval_client = RetrievalClient(base_url=settings.retrieval_url, timeout_s=settings.retrieval_timeout_s)
    llm_client = LLMGatewayClient(base_url=settings.llm_gateway_url, timeout_s=settings.llm_gateway_timeout_s)

    # ── Conversation Store ───────────────────────────────────────────────
    if settings.environment == "local":
        from app.adapters.sqlite_conversation import SqliteConversationStore
        conversation_store = SqliteConversationStore(db_path=settings.sqlite_db_path)
    else:
        from app.adapters.postgres_conversation import PostgresConversationStore
        conversation_store = PostgresConversationStore(dsn=settings.postgres_dsn)

    # ── Orchestrator ─────────────────────────────────────────────────────
    from app.services.orchestrator import Orchestrator

    orchestrator = Orchestrator(
        iqs_client=iqs_client,
        retrieval_client=retrieval_client,
        llm_client=llm_client,
        conversation_store=conversation_store,
        settings=settings,
    )

    app.state.orchestrator = orchestrator

    logger.info(
        "rag_orchestration_started",
        environment=settings.environment,
        iqs_url=settings.iqs_url,
        retrieval_url=settings.retrieval_url,
        llm_gateway_url=settings.llm_gateway_url,
        prompt_version=settings.prompt_template_version,
    )

    yield

    logger.info("rag_orchestration_stopped")


app = FastAPI(
    title="RAG Orchestration Service",
    description="Single front door for the RAG pipeline — orchestrates IQS, Retrieval, and LLM Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
