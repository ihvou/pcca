from __future__ import annotations

import pytest

from pcca.digest_renderer import BriefButtonPayload
from pcca.db import Database
from pcca.repositories.subjects import SubjectRepository
from pcca.services.subject_service import SubjectService
from pcca.services.telegram_service import STALE_BRIEF_EXPAND_MESSAGE, TelegramService


def test_brief_inline_keyboard_omits_global_quick_actions() -> None:
    service = TelegramService.__new__(TelegramService)
    markup = service._brief_inline_keyboard(
        [
            BriefButtonPayload(label="👍", token="fb1", text_macro="more like this", kind="feedback"),
            BriefButtonPayload(label="📖 More", token="more1", text_macro="__expand_brief__", kind="expand"),
        ]
    )

    labels = [button.text for row in markup.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert labels == ["👍", "📖 More"]
    assert callbacks == ["fb:fb1", "more:more1"]
    assert "Read Content Now" not in labels
    assert "Get Briefs" not in labels
    assert "Rebuild Briefs" not in labels


def test_stale_brief_expand_message_is_actionable() -> None:
    assert "earlier delivery" in STALE_BRIEF_EXPAND_MESSAGE
    assert "Update Briefs" in STALE_BRIEF_EXPAND_MESSAGE


def test_t142_quick_actions_are_update_edit_help_only() -> None:
    service = TelegramService.__new__(TelegramService)
    inline = service._quick_actions_inline_keyboard()
    labels = [button.text for row in inline.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in inline.inline_keyboard for button in row]

    assert labels == ["Update Briefs", "Edit Subjects", "Help"]
    assert callbacks == ["run:update", "subject_manage:list", "run:help"]

    reply = service._quick_actions_reply_keyboard()
    assert [[button.text for button in row] for row in reply.keyboard] == [["Update Briefs", "Edit Subjects", "Help"]]


def test_t142_subject_detail_keyboard_has_safe_management_actions() -> None:
    subject = type(
        "Subject",
        (),
        {"id": 7, "status": "active"},
    )()

    markup = TelegramService._subject_detail_keyboard(subject)
    labels = [button.text for row in markup.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]

    assert labels == ["Pause", "Rename", "Edit description", "Adjust relevance floor", "Back to subjects"]
    assert "Delete" not in labels
    assert "subject_manage:toggle:7" in callbacks
    assert "subject_manage:description:7" in callbacks


@pytest.mark.asyncio
async def test_t142_subject_service_pause_rename_and_floor(tmp_path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    service = SubjectService(repository=SubjectRepository(conn=db.conn))
    subject = await service.create_subject(
        "AI Tools",
        include_terms=["claude code"],
        description_text="Track practical AI tooling.",
    )

    paused = await service.set_subject_status(subject.id, "paused")
    assert paused.status == "paused"

    renamed = await service.rename_subject(subject.id, "AI Tools & Tips")
    assert renamed.id == subject.id
    assert renamed.name == "AI Tools & Tips"

    updated = await service.update_subject_description(subject.id, "Updated description")
    assert updated.id == subject.id
    assert await service.repository.get_description_embedding(updated.id, model="fake") is None

    floored = await service.set_subject_min_relevance_threshold(subject.id, 0.72)
    assert floored.min_relevance_threshold == 0.72

    defaulted = await service.set_subject_min_relevance_threshold(subject.id, None)
    assert defaulted.min_relevance_threshold is None

    await db.close()
