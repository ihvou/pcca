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
    parsed = parse_intent("Track https://example.com/feed.xml for Vibe Coding")
    assert parsed.action is IntentAction.ADD_SOURCE_URL
    assert parsed.source_url == "https://example.com/feed.xml"
    assert parsed.subject_name == "Vibe Coding"


def test_parse_add_source_spotify_platform() -> None:
    parsed = parse_intent("Add source spotify:https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL to Vibe Coding")
    assert parsed.action is IntentAction.ADD_SOURCE
    assert parsed.platform == "spotify"


def test_parse_remove_source() -> None:
    parsed = parse_intent("Unsubscribe x:borischerny from Vibe Coding")
    assert parsed.action is IntentAction.REMOVE_SOURCE
    assert parsed.platform == "x"
    assert parsed.source_id == "borischerny"
    assert parsed.subject_name == "Vibe Coding"


def test_parse_refine_preferences() -> None:
    parsed = parse_intent("Refine Agentic PM: include claude code, releases; exclude biography, motivation")
    assert parsed.action is IntentAction.REFINE_PREFERENCES
    assert parsed.subject_name == "Agentic PM"
    assert parsed.include_terms == ["claude code", "releases"]
    assert parsed.exclude_terms == ["biography", "motivation"]


def test_parse_show_preferences() -> None:
    parsed = parse_intent("Show preferences for Agentic PM")
    assert parsed.action is IntentAction.SHOW_PREFERENCES
    assert parsed.subject_name == "Agentic PM"


def test_parse_run_read_content() -> None:
    parsed = parse_intent("read content now")
    assert parsed.action is IntentAction.RUN_READ_CONTENT


def test_parse_run_get_digest() -> None:
    parsed = parse_intent("/get_digest")
    assert parsed.action is IntentAction.RUN_GET_DIGEST
