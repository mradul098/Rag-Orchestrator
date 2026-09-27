"""
SQLite Conversation Store — reads the same DB that IQS writes to.

In local dev both services share a single SQLite file.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

import structlog

from app.interfaces.conversation_store import ConversationStore

logger = structlog.get_logger()


class SqliteConversationStore(ConversationStore):

    def __init__(self, db_path: str):
        self._db_path = db_path
        logger.info("conversation_store_initialized", adapter="sqlite", db_path=db_path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    async def get_recent_turns(
        self, session_id: str, tenant_id: str, limit: int = 5
    ) -> List[Dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT turn_id, raw_query, rewritten_query, final_answer,
                          retrieved_chunk_ids, turn_index
                   FROM conversation_turns
                   WHERE session_id = ? AND tenant_id = ?
                   ORDER BY turn_index DESC LIMIT ?""",
                (session_id, tenant_id, limit),
            ).fetchall()
            turns = []
            for r in reversed(rows):
                chunk_ids = None
                if r["retrieved_chunk_ids"]:
                    chunk_ids = json.loads(r["retrieved_chunk_ids"])
                turns.append({
                    "turn_id": r["turn_id"],
                    "raw_query": r["raw_query"],
                    "rewritten_query": r["rewritten_query"],
                    "final_answer": r["final_answer"],
                    "retrieved_chunk_ids": chunk_ids,
                    "turn_index": r["turn_index"],
                })
            return turns
        finally:
            conn.close()

    async def update_turn_answer(
        self, turn_id: str, final_answer: str,
        retrieved_chunk_ids: Optional[List[str]] = None,
    ) -> None:
        conn = self._conn()
        try:
            chunk_json = json.dumps(retrieved_chunk_ids) if retrieved_chunk_ids else None
            conn.execute(
                "UPDATE conversation_turns SET final_answer = ?, retrieved_chunk_ids = ? WHERE turn_id = ?",
                (final_answer, chunk_json, turn_id),
            )
            conn.commit()
            logger.info("turn_answer_updated", turn_id=turn_id)
        finally:
            conn.close()
