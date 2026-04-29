from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class IntentAction(str, Enum):
    CREATE_SUBJECT = "create_subject"
    LIST_SUBJECTS = "list_subjects"
    ADD_SOURCE = "add_source"
    ADD_SOURCE_URL = "add_source_url"
    REMOVE_SOURCE = "remove_source"
    LIST_SOURCES = "list_sources"
    REFINE_PREFERENCES = "refine_preferences"
    SHOW_PREFERENCES = "show_preferences"
    RUN_READ_CONTENT = "run_read_content"
    RUN_GET_DIGEST = "run_get_digest"
    RUN_REBUILD_DIGEST = "run_rebuild_digest"
    HELP = "help"
    UNKNOWN = "unknown"


@dataclass
class ParsedIntent:
    action: IntentAction
    subject_name: str | None = None
    platform: str | None = None
    source_id: str | None = None
    source_url: str | None = None
    display_name: str | None = None
    include_terms: list[str] | None = None
    exclude_terms: list[str] | None = None
    raw_text: str = ""


@dataclass
class Subject:
    id: int
    name: str
    telegram_thread_id: str | None
    status: str
    created_at: str
    brief_full_text_chars: int = 1800
