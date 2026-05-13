from __future__ import annotations

import asyncio
import logging

import pytest

from pcca.digest_renderer import BriefButtonPayload
from pcca.db import Database
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.source_service import SourceService
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


def test_t142_quick_actions_include_update_and_get_briefs() -> None:
    """Keyboard pairs the slow-path 'Update Briefs' (full collect+score+
    deliver, ~30-70min) with the fast-path 'Get Briefs' (deliver existing
    scored data in <30s). Get Briefs was removed by mistake in T-142's
    initial implementation and restored 2026-05-13.

    Layout: two rows so each pair is visually grouped.
      Row 1: [Update Briefs] [Get Briefs]
      Row 2: [Edit Subjects] [Help]
    """
    service = TelegramService.__new__(TelegramService)
    inline = service._quick_actions_inline_keyboard()
    labels = [button.text for row in inline.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in inline.inline_keyboard for button in row]

    assert labels == ["Update Briefs", "Get Briefs", "Edit Subjects", "Help"]
    assert callbacks == [
        "run:update",
        "run:briefs",
        "subject_manage:list",
        "run:help",
    ]

    reply = service._quick_actions_reply_keyboard()
    assert [[button.text for button in row] for row in reply.keyboard] == [
        ["Update Briefs", "Get Briefs"],
        ["Edit Subjects", "Help"],
    ]


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


@pytest.mark.asyncio
async def test_t143_pause_resume_round_trip_preserves_subject_fields_and_sources(tmp_path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    subject = await service.create_subject(
        "AI Tools",
        include_terms=["claude code"],
        description_text="Track practical AI tooling.",
        min_relevance_threshold=0.73,
    )
    await source_service.add_source_to_subject(
        subject_name=subject.name,
        platform="youtube",
        account_or_channel_id="@openai",
        display_name="OpenAI",
    )

    await service.set_subject_status(subject.id, "paused")
    resumed = await service.set_subject_status(subject.id, "active")
    sources = await source_service.list_sources_for_subject(subject.name)
    description_text = await subject_repo.get_description_text(subject.id)

    assert resumed.status == "active"
    assert resumed.name == subject.name
    assert description_text == "Track practical AI tooling."
    assert resumed.min_relevance_threshold == subject.min_relevance_threshold
    assert [(source.platform, source.account_or_channel_id, source.status) for source in sources] == [
        ("youtube", "@openai", "active")
    ]

    await db.close()


class _FlakyEditMessage:
    def __init__(self, *, failures_before_success: int = 0) -> None:
        self.failures_before_success = failures_before_success
        self.edit_attempts = 0
        self.edited_texts: list[str] = []
        self.replies: list[str] = []
        self.chat_id = 123
        self.message_thread_id = 456

    async def edit_text(self, text: str) -> None:
        self.edit_attempts += 1
        if self.edit_attempts <= self.failures_before_success:
            raise RuntimeError("temporary telegram edit failure")
        self.edited_texts.append(text)

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class _FakeTelegramBot:
    def __init__(self) -> None:
        self.sent_messages: list[dict] = []

    async def send_message(self, **kwargs) -> None:
        self.sent_messages.append(kwargs)


@pytest.mark.asyncio
async def test_t144_edit_message_text_retries_then_succeeds() -> None:
    service = TelegramService.__new__(TelegramService)
    service._edit_retry_delays_seconds = (0.0, 0.0, 0.0)
    message = _FlakyEditMessage(failures_before_success=2)

    ok = await service._edit_message_text(message, "Still updating Briefs...")

    assert ok is True
    assert message.edit_attempts == 3
    assert message.edited_texts == ["Still updating Briefs..."]
    assert message.replies == []


@pytest.mark.asyncio
async def test_t144_final_edit_failure_falls_back_to_new_message() -> None:
    service = TelegramService.__new__(TelegramService)
    service._edit_retry_delays_seconds = (0.0, 0.0, 0.0)
    bot = _FakeTelegramBot()
    service.application = type("FakeApplication", (), {"bot": bot})()
    message = _FlakyEditMessage(failures_before_success=99)

    ok = await service._edit_message_text(message, "Update Briefs finished.", final=True)

    assert ok is True
    assert message.edit_attempts == 4
    assert message.replies == []
    assert bot.sent_messages == [
        {
            "chat_id": 123,
            "text": "Update Briefs finished.",
            "message_thread_id": 456,
            "disable_web_page_preview": True,
        }
    ]


@pytest.mark.asyncio
async def test_t144_drain_progress_tasks_warns_for_leftover_tasks(caplog) -> None:
    service = TelegramService.__new__(TelegramService)
    service._progress_tasks = {asyncio.create_task(asyncio.sleep(10))}

    with caplog.at_level(logging.WARNING, logger="pcca.services.telegram_service"):
        await service._drain_progress_tasks(context="test")

    assert "Telegram progress tasks still running after test count=1" in caplog.text
    for task in service._progress_tasks:
        task.cancel()
    await asyncio.gather(*service._progress_tasks, return_exceptions=True)
