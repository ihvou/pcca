from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from pcca.models import IntentAction
from pcca.services.feedback_service import FeedbackService
from pcca.services.intent_parser import parse_intent
from pcca.services.preference_service import PreferenceService
from pcca.services.routing_service import RoutingService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService
from pcca.services.voice_transcription_service import VoiceTranscriptionService

logger = logging.getLogger(__name__)


ManualAction = Callable[[], Awaitable[None]]


@dataclass
class TelegramService:
    bot_token: str
    subject_service: SubjectService
    source_service: SourceService
    preference_service: PreferenceService
    feedback_service: FeedbackService
    source_discovery: SourceDiscoveryService
    routing_service: RoutingService
    voice_transcriber: VoiceTranscriptionService
    application: Application | None = None
    read_content_action: ManualAction | None = None
    get_digest_action: ManualAction | None = None
    _manual_action_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def attach_manual_actions(
        self,
        *,
        read_content_action: ManualAction,
        get_digest_action: ManualAction,
    ) -> None:
        self.read_content_action = read_content_action
        self.get_digest_action = get_digest_action

    async def start(self) -> None:
        app = ApplicationBuilder().token(self.bot_token).build()
        app.add_handler(CommandHandler("start", self._on_start))
        app.add_handler(CommandHandler("help", self._on_help))
        app.add_handler(CommandHandler("setup", self._on_setup))
        app.add_handler(CommandHandler("onboard", self._on_setup))
        app.add_handler(CommandHandler("read_content", self._on_read_content_command))
        app.add_handler(CommandHandler("get_digest", self._on_get_digest_command))
        app.add_handler(CallbackQueryHandler(self._on_feedback_callback, pattern=r"^fb:"))
        app.add_handler(CallbackQueryHandler(self._on_run_callback, pattern=r"^run:"))
        app.add_handler(MessageHandler(filters.VOICE, self._on_voice))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text))

        await app.initialize()
        await app.start()
        if app.updater is None:
            raise RuntimeError("Telegram updater is not available.")
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        self.application = app
        logger.info("Telegram service started.")

    async def stop(self) -> None:
        if self.application is None:
            return
        app = self.application
        self.application = None
        if app.updater is not None:
            await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Telegram service stopped.")

    async def send_digest_message(
        self,
        chat_id: int,
        subject_name: str,
        items: list[str],
        item_actions: list[dict] | None = None,
        thread_id: int | None = None,
    ) -> int | None:
        if self.application is None:
            logger.warning("Telegram application not started, skipping digest send.")
            return None
        title = f"📌 {subject_name} — {len(items)} items today"
        body = "\n\n".join(items) if items else "No high-signal items passed your quality bar today."
        keyboard_rows = []
        for action_row in item_actions or []:
            rank = action_row.get("rank")
            tokens = action_row.get("tokens", {})
            keyboard_rows.append(
                [
                    InlineKeyboardButton(f"{rank} 👍", callback_data=f"fb:{tokens.get('up', '')}"),
                    InlineKeyboardButton(f"{rank} 👎", callback_data=f"fb:{tokens.get('down', '')}"),
                    InlineKeyboardButton(f"{rank} 🔖", callback_data=f"fb:{tokens.get('save', '')}"),
                ]
            )
        keyboard_rows.append(
            [
                InlineKeyboardButton("Read Content Now", callback_data="run:read"),
                InlineKeyboardButton("Get Digest Now", callback_data="run:digest"),
            ]
        )
        sent = await self.application.bot.send_message(
            chat_id=chat_id,
            text=f"{title}\n\n{body}",
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
            message_thread_id=thread_id,
            disable_web_page_preview=True,
        )
        return sent.message_id

    async def _on_start(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        new_routes = await self.routing_service.ensure_routes_for_chat(
            chat_id=update.effective_chat.id,
            title=update.effective_chat.title or update.effective_chat.full_name,
        )
        connect_note = (
            f"\nLinked {new_routes} existing subject(s) to this chat for digest delivery."
            if new_routes
            else ""
        )
        await update.message.reply_text(
            "PCCA is connected.\n"
            "Use /setup for guided onboarding, or send free-form commands."
            + connect_note,
            reply_markup=self._quick_actions_reply_keyboard(),
        )
        await self._send_quick_actions(update.message)

    async def _on_setup(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        reauth_note = await self._format_reauth_sources()
        await update.message.reply_text(
            "Scenario 1 setup flow:\n"
            "1. Open the desktop app and use the desktop wizard.\n"
            "2. Set timezone, digest time, and your Telegram bot token.\n"
            "3. Start the local agent, then send `/start` here to verify Telegram.\n"
            "4. Log into platforms in your normal browser, then capture sessions in the desktop app.\n"
            "5. Stage follows/subscriptions, review them, and create your first subject.\n"
            "6. Click Smoke Crawl + Test Digest, or use `/read_content` then `/get_digest` here.\n\n"
            "Connected-account onboarding is available for "
            "X/LinkedIn/YouTube/Substack/Medium/Spotify/Apple Podcasts."
            f"{reauth_note}",
        )

    async def _on_help(self, update: Update, _context: ContextTypes.DEFAULT_TYPE | None) -> None:
        if update.message is None:
            return
        await update.message.reply_text(
            "I can:\n"
            "- show the Scenario 1 setup checklist (`/setup`)\n"
            "- create/list subjects\n"
            "- list/remove imported sources\n"
            "- show/refine preferences per subject\n"
            "- run collection now (`/read_content`)\n"
            "- run digest now (`/get_digest`)\n"
            "- collect feedback (👍 👎 🔖)\n"
            "- accept voice notes (transcription backend pending)",
            reply_markup=self._quick_actions_reply_keyboard(),
        )
        await self._send_quick_actions(update.message)

    async def _on_read_content_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._run_manual_action_from_message(update.message, action_name="read content", action=self.read_content_action)

    async def _on_get_digest_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._run_manual_action_from_message(update.message, action_name="get digest", action=self.get_digest_action)

    async def _on_text(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None or update.message.text is None:
            return
        await self.routing_service.ensure_routes_for_chat(
            chat_id=update.effective_chat.id,
            title=update.effective_chat.title or update.effective_chat.full_name,
        )
        await self._handle_text_intent(update, update.message.text)

    async def _handle_text_intent(self, update: Update, text: str) -> None:
        if update.message is None:
            return
        intent = parse_intent(text)
        thread_id = str(update.message.message_thread_id) if update.message.message_thread_id else None

        if intent.action is IntentAction.CREATE_SUBJECT:
            if not intent.subject_name:
                await update.message.reply_text("Tell me the subject name, for example: Create subject: Vibe Coding")
                return
            subject = await self.subject_service.create_subject(intent.subject_name, telegram_thread_id=thread_id)
            await self.routing_service.link_subject(
                subject_name=subject.name,
                chat_id=update.effective_chat.id,
                thread_id=thread_id,
            )
            await update.message.reply_text(
                f"Subject ready: {subject.name}\n"
                "I will include it in nightly runs and morning digests."
            )
            return

        if intent.action is IntentAction.LIST_SUBJECTS:
            subjects = await self.subject_service.list_subjects()
            if not subjects:
                await update.message.reply_text("No subjects yet. You can create one in free form.")
                return
            lines = [f"- {subject.name}" for subject in subjects]
            await update.message.reply_text("Current subjects:\n" + "\n".join(lines))
            return

        if intent.action is IntentAction.ADD_SOURCE:
            if not intent.subject_name or not intent.platform or not intent.source_id:
                await update.message.reply_text(
                    "Please include all fields, for example:\n"
                    "Add source x:borischerny to Vibe Coding"
                )
                return
            try:
                linked_rows = await self._link_source(
                    subject_name=intent.subject_name,
                    platform=intent.platform,
                    source_value=intent.source_id,
                )
                await self.routing_service.link_subject(
                    subject_name=intent.subject_name,
                    chat_id=update.effective_chat.id,
                    thread_id=thread_id,
                )
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return
            if not linked_rows:
                await update.message.reply_text("Could not resolve that source into a supported format.")
                return
            await update.message.reply_text("Linked sources:\n" + "\n".join(linked_rows))
            return

        if intent.action is IntentAction.ADD_SOURCE_URL:
            if not intent.subject_name or not intent.source_url:
                await update.message.reply_text(
                    "Please connect account follows in the desktop app for Scenario 1 testing."
                )
                return
            discovered = await self.source_discovery.discover(intent.source_url)
            if not discovered:
                await update.message.reply_text(
                    "I could not discover a supported source from that URL yet.\n"
                    "For Scenario 1, use the desktop app to connect accounts and import follows/subscriptions."
                )
                return

            linked_rows: list[str] = []
            for row in discovered:
                await self.source_service.add_source_to_subject(
                    subject_name=intent.subject_name,
                    platform=row.platform,
                    account_or_channel_id=row.source_id,
                    display_name=row.display_name,
                )
                linked_rows.append(f"- [{row.platform}] {row.source_id} ({row.reason})")

            await self.routing_service.link_subject(
                subject_name=intent.subject_name,
                chat_id=update.effective_chat.id,
                thread_id=thread_id,
            )
            await update.message.reply_text("Linked sources:\n" + "\n".join(linked_rows))
            return

        if intent.action is IntentAction.REMOVE_SOURCE:
            if not intent.subject_name or not intent.platform or not intent.source_id:
                await update.message.reply_text(
                    "Please include all fields, for example:\n"
                    "Unsubscribe x:borischerny from Vibe Coding"
                )
                return
            try:
                removed = await self._remove_source(
                    subject_name=intent.subject_name,
                    platform=intent.platform,
                    source_value=intent.source_id,
                )
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return
            if removed:
                await update.message.reply_text(
                    f"Source removed: [{intent.platform}] {intent.source_id} from {intent.subject_name}"
                )
            else:
                await update.message.reply_text("Source was not active for that subject.")
            return

        if intent.action is IntentAction.LIST_SOURCES:
            if not intent.subject_name:
                await update.message.reply_text("Tell me the subject name, for example: List sources for Vibe Coding")
                return
            try:
                sources = await self.source_service.list_sources_for_subject(intent.subject_name)
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return

            if not sources:
                await update.message.reply_text(f"No active sources configured for {intent.subject_name}.")
                return
            lines = [f"- [{source.platform}] {source.account_or_channel_id}" for source in sources]
            await update.message.reply_text("Sources:\n" + "\n".join(lines))
            return

        if intent.action is IntentAction.SHOW_PREFERENCES:
            if not intent.subject_name:
                await update.message.reply_text("Tell me the subject name, for example: Show preferences for Vibe Coding")
                return
            try:
                pref = await self.preference_service.get_preferences_for_subject(intent.subject_name)
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return
            include_topics = pref.include_rules.get("topics", [])
            exclude_topics = pref.exclude_rules.get("topics", [])
            await update.message.reply_text(
                f"Preferences for {intent.subject_name} (v{pref.version}):\n"
                f"- include: {', '.join(include_topics) if include_topics else '(none)'}\n"
                f"- exclude: {', '.join(exclude_topics) if exclude_topics else '(none)'}"
            )
            return

        if intent.action is IntentAction.REFINE_PREFERENCES:
            if not intent.subject_name:
                await update.message.reply_text(
                    "Tell me the subject name, for example:\n"
                    "Refine Vibe Coding: include claude code; exclude biography"
                )
                return
            if not intent.include_terms and not intent.exclude_terms:
                await update.message.reply_text(
                    "Please include include/exclude terms, for example:\n"
                    "Refine Vibe Coding: include releases, practical workflow; exclude motivation"
                )
                return
            try:
                pref = await self.preference_service.refine_subject_rules(
                    subject_name=intent.subject_name,
                    include_terms=intent.include_terms,
                    exclude_terms=intent.exclude_terms,
                )
                await self.feedback_service.add_feedback_by_subject_name(
                    subject_name=intent.subject_name,
                    feedback_type="refine_text",
                    comment_text=text,
                )
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return
            include_topics = pref.include_rules.get("topics", [])
            exclude_topics = pref.exclude_rules.get("topics", [])
            await update.message.reply_text(
                f"Updated preferences for {intent.subject_name} (v{pref.version}).\n"
                f"- include: {', '.join(include_topics) if include_topics else '(none)'}\n"
                f"- exclude: {', '.join(exclude_topics) if exclude_topics else '(none)'}"
            )
            return

        if intent.action is IntentAction.RUN_READ_CONTENT:
            await self._run_manual_action_from_message(
                update.message,
                action_name="read content",
                action=self.read_content_action,
            )
            return

        if intent.action is IntentAction.RUN_GET_DIGEST:
            await self._run_manual_action_from_message(
                update.message,
                action_name="get digest",
                action=self.get_digest_action,
            )
            return

        if intent.action is IntentAction.HELP:
            await self._on_help(update, None)
            return

        await update.message.reply_text(
            "I can handle setup/refinement/actions in free form.\n"
            "Try: /setup"
        )

    async def _on_voice(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None or update.message.voice is None:
            return
        tg_file = await context.bot.get_file(update.message.voice.file_id)
        file_bytes = await tg_file.download_as_bytearray()
        transcript = await self.voice_transcriber.transcribe_telegram_voice(bytes(file_bytes))
        if not transcript:
            await update.message.reply_text(
                "Voice note received. Voice transcription backend is not wired yet.\n"
                "Please send the same instruction as text for now."
            )
            return
        await self._handle_text_intent(update, transcript)

    async def _on_feedback_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        token = (update.callback_query.data or "fb:").split(":", 1)[1]
        button = await self.feedback_service.get_digest_button(token)
        if button is None:
            await update.callback_query.answer("This feedback button is no longer available.")
            return
        await self.feedback_service.add_feedback_by_subject_id(
            subject_id=button.subject_id,
            item_id=button.item_id,
            feedback_type=f"button_{button.action}",
        )
        await update.callback_query.answer("Feedback saved.")

    async def _on_run_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        data = update.callback_query.data or ""
        await update.callback_query.answer()
        message = update.callback_query.message
        if message is None:
            return
        if data == "run:read":
            await self._run_manual_action_from_message(message, action_name="read content", action=self.read_content_action)
            return
        if data == "run:digest":
            await self._run_manual_action_from_message(message, action_name="get digest", action=self.get_digest_action)
            return

    async def _run_manual_action_from_message(
        self,
        message,
        *,
        action_name: str,
        action: ManualAction | None,
    ) -> None:
        if action is None:
            await message.reply_text(
                f"`{action_name}` action is not available yet in this runtime.",
                parse_mode="Markdown",
            )
            return
        if self._manual_action_lock.locked():
            await message.reply_text("Another manual run is in progress. Please wait a bit and try again.")
            return
        await message.reply_text(f"Running `{action_name}` now...", parse_mode="Markdown")
        try:
            async with self._manual_action_lock:
                await action()
            reauth_note = ""
            if action_name == "read content":
                reauth_note = await self._format_reauth_sources()
            await message.reply_text(f"{action_name} completed.{reauth_note}")
        except Exception:
            logger.exception("Manual action failed: %s", action_name)
            await message.reply_text(f"`{action_name}` failed. Check logs and try again.", parse_mode="Markdown")

    async def _format_reauth_sources(self) -> str:
        sources = await self.source_service.list_sources_needing_reauth()
        if not sources:
            return ""
        lines = [
            f"- [{source.platform}] {source.display_name} ({source.account_or_channel_id})"
            for source in sources
        ]
        return "\n\nNeeds re-login before next collection:\n" + "\n".join(lines)

    async def _send_quick_actions(self, message) -> None:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Read Content Now", callback_data="run:read"),
                    InlineKeyboardButton("Get Digest Now", callback_data="run:digest"),
                ]
            ]
        )
        await message.reply_text("Quick actions:", reply_markup=keyboard)

    @staticmethod
    def _quick_actions_reply_keyboard() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            [["Read Content", "Get Digest"], ["List subjects", "Help"]],
            resize_keyboard=True,
            one_time_keyboard=False,
        )

    async def _link_source(self, *, subject_name: str, platform: str, source_value: str) -> list[str]:
        candidate = source_value.strip()
        if not candidate:
            return []
        rows: list[str] = []
        if candidate.startswith(("http://", "https://")):
            discovered = await self.source_discovery.discover(candidate)
            matched = [d for d in discovered if d.platform == platform]
            for row in matched:
                await self.source_service.add_source_to_subject(
                    subject_name=subject_name,
                    platform=row.platform,
                    account_or_channel_id=row.source_id,
                    display_name=row.display_name,
                )
                rows.append(f"- [{row.platform}] {row.source_id} ({row.reason})")
            if rows:
                return rows

        await self.source_service.add_source_to_subject(
            subject_name=subject_name,
            platform=platform,
            account_or_channel_id=candidate,
        )
        rows.append(f"- [{platform}] {candidate} (explicit platform:id input)")
        return rows

    async def _remove_source(self, *, subject_name: str, platform: str, source_value: str) -> bool:
        candidate = source_value.strip()
        if not candidate:
            return False
        if candidate.startswith(("http://", "https://")):
            discovered = await self.source_discovery.discover(candidate)
            matched = [d for d in discovered if d.platform == platform]
            any_removed = False
            for row in matched:
                removed = await self.source_service.remove_source_from_subject(
                    subject_name=subject_name,
                    platform=row.platform,
                    account_or_channel_id=row.source_id,
                )
                any_removed = any_removed or removed
            if matched:
                return any_removed
        return await self.source_service.remove_source_from_subject(
            subject_name=subject_name,
            platform=platform,
            account_or_channel_id=candidate,
        )
