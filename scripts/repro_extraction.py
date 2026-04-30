"""Reproduce the subject-creation extraction flow for the screenshot scenario.

Runs the same code path the Telegram bot / wizard takes:
  1. Loads Settings (so PCCA_OLLAMA_ENABLED + URLs come from .env).
  2. Builds ModelRouter + PreferenceExtractionService exactly like app.py does.
  3. Constructs a SubjectDraft matching the stored production state
     (title="AI impact over IT jobs market", empty rules).
  4. Calls service.extract(text=<user's actual message>, previous=<draft>).
  5. Prints the result and dumps captured logs.

Usage:
    PCCA_OLLAMA_ENABLED=true python scripts/repro_extraction.py
"""

from __future__ import annotations

import asyncio
import logging
import sys

from pcca.config import Settings
from pcca.repositories.subject_drafts import SubjectDraft
from pcca.services.model_router import ModelRouter
from pcca.services.preference_extraction_service import PreferenceExtractionService

# The user's actual second-attempt message from the 2026-04-30 03:56 screenshot.
USER_MESSAGE = (
    "i would like to be aware of thoughts of SMEs(people with reputation in "
    "the area), how AI impacts IT job market. if thought was share in the "
    "middle of podcast or video interview, i would like to get a short "
    "summary of that thought or a full quote it the thought was short. if "
    "thought didn't contain any novelty, insight, reasoning it could be "
    "better to skip it"
)


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    settings = Settings.from_env()
    print(
        f"Settings: ollama_enabled={settings.ollama_enabled} "
        f"base_url={settings.ollama_base_url} model={settings.ollama_model}"
    )
    print(f"Input text ({len(USER_MESSAGE)} chars):\n  {USER_MESSAGE}\n")

    router = ModelRouter(
        enabled=settings.ollama_enabled,
        ollama_base_url=settings.ollama_base_url,
        ollama_model=settings.ollama_model,
    )
    service = PreferenceExtractionService(model_router=router)

    # Mirror the production stored draft: chat 855127987, empty rules,
    # last_user_message already populated from a prior turn.
    previous = SubjectDraft(
        chat_id=855127987,
        title="AI impact over IT jobs market",
        description_text=USER_MESSAGE,
        include_terms=[],
        exclude_terms=[],
        quality_notes=None,
        last_user_message=USER_MESSAGE,
        updated_at="2026-04-29T19:56:17",
    )
    print("Calling service.extract(text=<user message>, previous=<empty-rules draft>)...\n")
    draft = await service.extract(USER_MESSAGE, previous=previous)
    print("\nResult:")
    print(f"  title:          {draft.title}")
    print(f"  include_terms:  {draft.include_terms}")
    print(f"  exclude_terms:  {draft.exclude_terms}")
    print(f"  quality_notes:  {draft.quality_notes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
