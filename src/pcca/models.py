from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class IntentAction(str, Enum):
    CREATE_SUBJECT = "create_subject"
    LIST_SUBJECTS = "list_subjects"
    ADD_SOURCE = "add_source"
    ADD_SOURCE_URL = "add_source_url"
    LIST_SOURCES = "list_sources"
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
    raw_text: str = ""


@dataclass
class Subject:
    id: int
    name: str
    telegram_thread_id: str | None
    status: str
    created_at: str
