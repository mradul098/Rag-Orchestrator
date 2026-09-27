"""Postgres Conversation Store — production adapter."""

from __future__ import annotations

from typing import Dict, List, Optional

import structlog

from app.interfaces.conversation_store import ConversationStore

logger = structlog.get_logger()


class PostgresConversationStore(ConversationStore):

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None
        logger.info("conversation_store_initialized", adapter="postgres")

    async def _get_pool(self):
        if self._pool is None:
            import asyncpg
            self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)
        return self._pool

    async def get_recent_turns(
        self, session_id: str, tenant_id: str, limit: int = 5
    ) -> List[Dict]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT turn_id, raw_query, rewritten_query, final_answer,
                          retrieved_chunk_ids, turn_index
                   FROM conversation_turns
                   WHERE session_id = $1 AND tenant_id = $2
                   ORDER BY turn_index DESC LIMIT $3""",
                session_id, tenant_id, limit,
            )
        turns = []
        for r in reversed(rows):
            turns.append({
                "turn_id": str(r["turn_id"]),
                "raw_query": r["raw_query"],
                "rewritten_query": r["rewritten_query"],
                "final_answer": r["final_answer"],
                "retrieved_chunk_ids": r["retrieved_chunk_ids"],
                "turn_index": r["turn_index"],
            })
        return turns

    async def update_turn_answer(
        self, turn_id: str, final_answer: str,
        retrieved_chunk_ids: Optional[List[str]] = None,
    ) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE conversation_turns SET final_answer = $1, retrieved_chunk_ids = $2 WHERE turn_id = $3",
                final_answer, retrieved_chunk_ids, turn_id,
            )
        logger.info("turn_answer_updated", turn_id=turn_id)
