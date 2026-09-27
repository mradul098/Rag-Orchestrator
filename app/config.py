"""Configuration for the RAG Orchestration Service."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    environment: str = "local"

    # Downstream service URLs
    iqs_url: str = "http://localhost:8003"
    retrieval_url: str = "http://localhost:8002"
    llm_gateway_url: str = "http://localhost:8001"

    # Timeouts (seconds)
    iqs_timeout_s: int = 35
    retrieval_timeout_s: int = 15
    llm_gateway_timeout_s: int = 60

    # Conversation Store
    sqlite_db_path: str = "../iqs-service/data/conversations.db"
    postgres_dsn: str = "postgresql://user:pass@localhost:5432/conversations"
    history_window_size: int = 5

    # Prompt
    prompt_template_version: str = "v1"

    service_name: str = "rag-orchestration-service"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
