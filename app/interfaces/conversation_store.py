"""
Conversation Store interface — same contract as IQS's store.

RAG Orchestration reads history (for prompt assembly) and writes
final_answer + retrieved_chunk_ids after LLM generation completes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class ConversationStore(ABC):

    @abstractmethod
    async def get_recent_turns(
        self, session_id: str, tenant_id: str, limit: int = 5
    ) -> List[Dict]:
        """Return recent turns as dicts, oldest-first."""
        ...

    @abstractmethod
    async def update_turn_answer(
        self, turn_id: str, final_answer: str,
        retrieved_chunk_ids: Optional[List[str]] = None,
    ) -> None:
        """Write final_answer + chunk_ids to the turn row IQS created."""
        ...
