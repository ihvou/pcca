from __future__ import annotations

import logging
from dataclasses import dataclass

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
from pcca.services.intent_parser import parse_intent
from pcca.services.routing_service import RoutingService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService
from pcca.services.voice_transcription_service import VoiceTranscriptionService

logger = logging.getLogger(__name__)


@dataclass
class TelegramService:
    bot_token: str
    subject_service: SubjectService
    source_service: SourceService
    source_discovery: SourceDiscoveryService
    routing_service: RoutingService
    voice_transcriber: VoiceTranscriptionService
    application: Application | None = None

    async def start(self) -> None:
        app = ApplicationBuilder().token(self.bot_token).build()
        app.add_handler(CommandHandler("start", self._on_start))
        app.add_handler(CommandHandler("help", self._on_help))
        app.add_handler(CallbackQueryHandler(self._on_feedback_callback, pattern=r"^fb:"))
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

    async def send_digest_message(self, chat_id: int, subject_name: str, items: list[str], thread_id: int | None = None) -> None:
        if self.application is None:
            logger.warning("Telegram application not started, skipping digest send.")
            return
        title = f"📌 {subject_name} — {len(items)} items today"
        body = "\n\n".join(items) if items else "No high-signal items passed your quality bar today."
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("👍", callback_data="fb:up"),
                    InlineKeyboardButton("👎", callback_data="fb:down"),
                    InlineKeyboardButton("🔖", callback_data="fb:save"),
                ]
            ]
        )
        await self.application.bot.send_message(
            chat_id=chat_id,
            text=f"{title}\n\n{body}",
            reply_markup=keyboard,
            message_thread_id=thread_id,
            disable_web_page_preview=True,
        )

    async def _on_start(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self.routing_service.register_chat(
            chat_id=update.effective_chat.id,
            title=update.effective_chat.title or update.effective_chat.full_name,
        )
        await update.message.reply_text(
            "PCCA is connected. Send messages in free form, for example:\n"
            "- Create subject: Vibe Coding\n"
            "- I want a new topic for Agentic PM\n"
            "- Add source x:borischerny to Vibe Coding\n"
            "- Add source https://newsletter.substack.com to Vibe Coding\n"
            "- List sources for Vibe Coding\n"
            "- List subjects"
        )

    async def _on_help(self, update: Update, _context: ContextTypes.DEFAULT_TYPE | None) -> None:
        if update.message is None:
            return
        await update.message.reply_text(
            "I can currently:\n"
            "- create subjects from free-form text\n"
            "- link and list sources per subject\n"
            "- discover sources from URLs (Substack/Medium/podcast/blog links)\n"
            "- list configured subjects\n"
            "- accept voice notes (transcription backend is pending)"
        )

    async def _on_text(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None or update.message.text is None:
            return
        await self.routing_service.register_chat(
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
                await self.source_service.add_source_to_subject(
                    subject_name=intent.subject_name,
                    platform=intent.platform,
                    account_or_channel_id=intent.source_id,
                )
                await self.routing_service.link_subject(
                    subject_name=intent.subject_name,
                    chat_id=update.effective_chat.id,
                    thread_id=thread_id,
                )
            except ValueError as exc:
                await update.message.reply_text(str(exc))
                return

            await update.message.reply_text(
                f"Source linked: [{intent.platform}] {intent.source_id} -> {intent.subject_name}"
            )
            return

        if intent.action is IntentAction.ADD_SOURCE_URL:
            if not intent.subject_name or not intent.source_url:
                await update.message.reply_text(
                    "Please include URL and subject, for example:\n"
                    "Add source https://newsletter.substack.com to Vibe Coding"
                )
                return
            discovered = await self.source_discovery.discover(intent.source_url)
            if not discovered:
                await update.message.reply_text(
                    "I could not discover a supported source from that URL yet.\n"
                    "Try another URL or add source manually (for example: add source rss:https://... to ...)."
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
            await update.message.reply_text(
                "Linked sources:\n" + "\n".join(linked_rows)
            )
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
                await update.message.reply_text(f"No sources configured for {intent.subject_name}.")
                return
            lines = [f"- [{source.platform}] {source.account_or_channel_id}" for source in sources]
            await update.message.reply_text("Sources:\n" + "\n".join(lines))
            return

        if intent.action is IntentAction.HELP:
            await self._on_help(update, None)
            return

        await update.message.reply_text(
            "I got that. For now I support free-form subject setup and listing.\n"
            "Try: Create subject: Agentic PM"
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

        # Reuse text flow once transcription is available.
        await self._handle_text_intent(update, transcript)

    async def _on_feedback_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        await update.callback_query.answer("Feedback logged (placeholder).")
