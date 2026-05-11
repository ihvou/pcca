from __future__ import annotations

from pcca.digest_renderer import BriefButtonPayload
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
    assert "Get Briefs" in STALE_BRIEF_EXPAND_MESSAGE
