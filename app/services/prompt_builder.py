"""
Prompt builder — assembles the messages array for LLM Gateway.

Loads versioned prompt templates from disk (LLD §5).
"""

from __future__ import annotations

import os
from typing import List, Optional

import structlog

logger = structlog.get_logger()

# Default system prompt (also saved as templates/qa-with-citations/v1.txt)
DEFAULT_SYSTEM_PROMPT = """You are a financial research assistant for a document retrieval system.

INSTRUCTIONS:
- Answer the user's question using ONLY the provided context below.
- If the context does not contain enough information to answer, say so honestly.
- Reference your sources by number (e.g., [1], [2]) when citing specific facts.
- Be concise and precise. Use financial terminology appropriately.
- Do NOT make up information that is not in the provided context."""


def load_system_prompt(template_dir: str = "app/templates/qa-with-citations", version: str = "v1") -> str:
    """Load a versioned system prompt from disk, or use the default."""
    path = os.path.join(template_dir, f"{version}.txt")
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    logger.warning("prompt_template_not_found", path=path, using="default")
    return DEFAULT_SYSTEM_PROMPT


def build_prompt_messages(
    rewritten_query: str,
    chunks: List[dict],
    history: Optional[List[dict]] = None,
    system_prompt: Optional[str] = None,
) -> List[dict]:
    """
    Assemble the messages array for LLM Gateway (LLD §5.3).

    Format:
      [system] → system instruction
      [user]   → (optional history) + context chunks + question
    """
    if system_prompt is None:
        system_prompt = DEFAULT_SYSTEM_PROMPT

    messages = [{"role": "system", "content": system_prompt}]

    # Build context block from chunks
    context_parts = []
    for i, chunk in enumerate(chunks, 1):
        source_info = f"[{i}] "
        doc_name = chunk.get("documentName", "Unknown")
        page = chunk.get("pageNo", "?")
        source_info += f"(Source: {doc_name}, Page {page})\n"
        source_info += chunk.get("chunkText", "")
        context_parts.append(source_info)

    context_block = "\n\n".join(context_parts)

    # Build conversation history block
    history_block = ""
    if history:
        history_lines = []
        for turn in history:
            history_lines.append(f"User: {turn.get('raw_query', '')}")
            if turn.get("final_answer"):
                answer = turn["final_answer"][:500]
                history_lines.append(f"Assistant: {answer}")
        if history_lines:
            history_block = "Previous conversation:\n" + "\n".join(history_lines) + "\n\n"

    # Assemble user message
    user_content = ""
    if history_block:
        user_content += history_block
    if context_block:
        user_content += f"Context:\n{context_block}\n\n"
    else:
        user_content += "No relevant documents were found.\n\n"

    user_content += f"Question: {rewritten_query}"

    messages.append({"role": "user", "content": user_content})

    return messages
