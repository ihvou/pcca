from pcca.models import IntentAction
from pcca.services.intent_parser import parse_intent


def test_parse_create_subject_with_colon() -> None:
    parsed = parse_intent("Create subject: Agentic PM")
    assert parsed.action is IntentAction.CREATE_SUBJECT
    assert parsed.subject_name == "Agentic PM"


def test_parse_list_subjects() -> None:
    parsed = parse_intent("List subjects")
    assert parsed.action is IntentAction.LIST_SUBJECTS


def test_parse_add_source() -> None:
    parsed = parse_intent("Add source x:borischerny to Vibe Coding")
    assert parsed.action is IntentAction.ADD_SOURCE
    assert parsed.platform == "x"
    assert parsed.source_id == "borischerny"
    assert parsed.subject_name == "Vibe Coding"


def test_parse_list_sources() -> None:
    parsed = parse_intent("List sources for Vibe Coding")
    assert parsed.action is IntentAction.LIST_SOURCES
    assert parsed.subject_name == "Vibe Coding"


def test_parse_add_source_url() -> None:
    parsed = parse_intent("Add source https://newsletter.substack.com to Vibe Coding")
    assert parsed.action is IntentAction.ADD_SOURCE_URL
    assert parsed.source_url == "https://newsletter.substack.com"
    assert parsed.subject_name == "Vibe Coding"
