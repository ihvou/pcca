from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

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

from pcca.digest_renderer import BriefPayload, EXPAND_BRIEF_ACTION, escape_markdown_v2
from pcca.models import IntentAction
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.subject_drafts import DESKTOP_SUBJECT_DRAFT_CHAT_ID, SubjectDraft, SubjectDraftRepository
from pcca.services.feedback_service import FeedbackService
from pcca.services.intent_parser import parse_intent
from pcca.services.preference_extraction_service import (
    ExtractedSubjectDraft,
    PreferenceExtractionService,
    draft_has_actionable_rules,
)
from pcca.services.preference_service import PreferenceService
from pcca.services.routing_service import RoutingService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService
from pcca.services.voice_transcription_service import VoiceTranscriptionService

logger = logging.getLogger(__name__)
STALE_BRIEF_EXPAND_MESSAGE = "This Brief is from an earlier delivery. Tap 'Update Briefs' to refresh and try again."


ManualAction = Callable[[], Awaitable[Any]]
SubjectScopedAction = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class PendingSubjectEdit:
    subject_id: int
    action: str


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
    subject_draft_repo: SubjectDraftRepository | None = None
    preference_extractor: PreferenceExtractionService | None = None
    item_score_repo: ItemScoreRepository | None = None
    application: Application | None = None
    read_content_action: ManualAction | None = None
    get_digest_action: SubjectScopedAction | None = None
    rebuild_digest_action: SubjectScopedAction | None = None
    update_briefs_action: SubjectScopedAction | None = None
    cancel_update_action: ManualAction | None = None
    _manual_action_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _pending_subject_edits: dict[str, PendingSubjectEdit] = field(default_factory=dict, init=False)
    _progress_tasks: set[asyncio.Task] = field(default_factory=set, init=False)

    def attach_manual_actions(
        self,
        *,
        read_content_action: ManualAction,
        get_digest_action: SubjectScopedAction,
        rebuild_digest_action: SubjectScopedAction | None = None,
        update_briefs_action: SubjectScopedAction | None = None,
        cancel_update_action: ManualAction | None = None,
    ) -> None:
        self.read_content_action = read_content_action
        self.get_digest_action = get_digest_action
        self.rebuild_digest_action = rebuild_digest_action
        self.update_briefs_action = update_briefs_action
        self.cancel_update_action = cancel_update_action

    async def start(self) -> None:
        started_at = time.monotonic()
        logger.info("Telegram service starting.")
        app = ApplicationBuilder().token(self.bot_token).build()
        app.add_handler(CommandHandler("start", self._on_start))
        app.add_handler(CommandHandler("help", self._on_help))
        app.add_handler(CommandHandler("settings", self._on_settings))
        app.add_handler(CommandHandler("setup", self._on_setup))
        app.add_handler(CommandHandler("onboard", self._on_setup))
        app.add_handler(CommandHandler("read_content", self._on_read_content_command))
        app.add_handler(CommandHandler("briefs", self._on_briefs_command))
        app.add_handler(CommandHandler("update_briefs", self._on_update_briefs_command))
        app.add_handler(CommandHandler("rebuild_briefs", self._on_rebuild_briefs_command))
        app.add_handler(CommandHandler("get_digest", self._on_get_digest_command))
        app.add_handler(CommandHandler("rebuild_digest", self._on_rebuild_digest_command))
        app.add_handler(CommandHandler("cancel", self._on_cancel_command))
        app.add_handler(CallbackQueryHandler(self._on_feedback_callback, pattern=r"^fb:"))
        app.add_handler(CallbackQueryHandler(self._on_more_callback, pattern=r"^more:"))
        app.add_handler(CallbackQueryHandler(self._on_route_subject_callback, pattern=r"^route:"))
        app.add_handler(CallbackQueryHandler(self._on_run_subject_callback, pattern=r"^run_subject:"))
        app.add_handler(CallbackQueryHandler(self._on_subject_manage_callback, pattern=r"^subject_manage:"))
        app.add_handler(CallbackQueryHandler(self._on_run_callback, pattern=r"^run:"))
        app.add_handler(MessageHandler(filters.VOICE, self._on_voice))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text))

        await app.initialize()
        await app.start()
        if app.updater is None:
            raise RuntimeError("Telegram updater is not available.")
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        self.application = app
        logger.info("Telegram service started duration_ms=%d", int((time.monotonic() - started_at) * 1000))

    async def stop(self) -> None:
        if self.application is None:
            return
        started_at = time.monotonic()
        logger.info("Telegram service stopping.")
        app = self.application
        self.application = None
        if app.updater is not None:
            await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Telegram service stopped duration_ms=%d", int((time.monotonic() - started_at) * 1000))

    async def send_brief_message(
        self,
        chat_id: int,
        subject_name: str,
        brief: BriefPayload,
        footer: str | None = None,
        thread_id: int | None = None,
    ) -> int | None:
        if self.application is None:
            logger.warning("Telegram application not started, skipping Brief send.")
            return None
        started_at = time.monotonic()
        logger.info(
            "Telegram Brief send started chat_id=%s subject=%s item_id=%s rank=%s thread_id=%s",
            chat_id,
            subject_name,
            brief.item_id,
            brief.rank,
            thread_id,
        )
        text = brief.short_text
        if footer:
            text = f"{text}\n\n{escape_markdown_v2(footer)}"
        try:
            sent = await self.application.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=self._brief_inline_keyboard(brief.buttons),
                message_thread_id=thread_id,
                disable_web_page_preview=True,
                parse_mode="MarkdownV2",
            )
            logger.info(
                "Telegram Brief send finished chat_id=%s subject=%s item_id=%s message_id=%s duration_ms=%d",
                chat_id,
                subject_name,
                brief.item_id,
                sent.message_id,
                int((time.monotonic() - started_at) * 1000),
            )
            return sent.message_id
        except Exception:
            logger.exception(
                "Telegram Brief send failed chat_id=%s subject=%s item_id=%s duration_ms=%d",
                chat_id,
                subject_name,
                brief.item_id,
                int((time.monotonic() - started_at) * 1000),
            )
            raise

    async def send_no_briefs_message(
        self,
        chat_id: int,
        subject_name: str,
        footer: str | None = None,
        thread_id: int | None = None,
    ) -> int | None:
        if self.application is None:
            logger.warning("Telegram application not started, skipping empty Brief notice.")
            return None
        body = "No high-signal briefs passed your quality bar today."
        if footer:
            body = f"{body}\n\n{footer}"
        sent = await self.application.bot.send_message(
            chat_id=chat_id,
            text=f"📌 {subject_name}\n\n{body}",
            reply_markup=self._quick_actions_inline_keyboard(),
            message_thread_id=thread_id,
            disable_web_page_preview=True,
        )
        return sent.message_id

    async def _on_start(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        logger.info(
            "Telegram /start received chat_id=%s user_id=%s",
            update.effective_chat.id if update.effective_chat else None,
            update.effective_user.id if update.effective_user else None,
        )
        if update.effective_chat is None:
            return
        chat_id = update.effective_chat.id
        thread_id = self._message_thread_id(update.message)
        title = update.effective_chat.title or update.effective_chat.full_name
        await self.routing_service.register_chat(chat_id=chat_id, title=title)
        routes = await self.routing_service.list_routes_for_chat(chat_id=chat_id, thread_id=thread_id)
        subjects = await self.subject_service.list_subjects()
        logger.info(
            "Telegram /start routing state chat_id=%s thread_id=%s routes=%d subjects=%d",
            chat_id,
            thread_id,
            len(routes),
            len(subjects),
        )
        if routes:
            route_names = ", ".join(route.subject_name for route in routes)
            await update.message.reply_text(
                "PCCA is connected.\n"
                f"This chat is linked to: {route_names}\n"
                "Use /setup for guided onboarding, or send free-form commands.",
                reply_markup=self._quick_actions_reply_keyboard(),
            )
            await self._send_quick_actions(update.message)
            return

        if not subjects:
            await update.message.reply_text(
                "PCCA is connected.\n"
                "No subjects exist yet. Create your first subject in the desktop wizard or send a free-form subject request here.",
                reply_markup=self._quick_actions_reply_keyboard(),
            )
            await self._send_quick_actions(update.message)
            return

        if len(subjects) == 1:
            subject = await self.routing_service.link_subject_id(
                subject_id=subjects[0].id,
                chat_id=chat_id,
                thread_id=thread_id,
            )
            logger.info(
                "Telegram /start auto-linked single subject chat_id=%s thread_id=%s subject_id=%s",
                chat_id,
                thread_id,
                subject.id,
            )
            await update.message.reply_text(
                "PCCA is connected.\n"
                f"This chat is linked to {subject.name} for Brief delivery.\n"
                "Use /setup for guided onboarding, or send free-form commands.",
                reply_markup=self._quick_actions_reply_keyboard(),
            )
            await self._send_quick_actions(update.message)
            return

        await update.message.reply_text(
            "PCCA is connected.\n"
            "Choose which subject this chat should receive Briefs for:",
            reply_markup=self._quick_actions_reply_keyboard(),
        )
        await update.message.reply_text(
            "Subject route:",
            reply_markup=self._subject_picker_keyboard(subjects),
        )

    async def _on_setup(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        reauth_note = await self._format_reauth_sources()
        await update.message.reply_text(
            "Scenario 1 setup flow:\n"
            "1. Open the desktop app and use the desktop wizard.\n"
            "2. Set timezone, Brief time, and your Telegram bot token.\n"
            "3. Start the local agent, then send `/start` here to verify Telegram.\n"
            "4. Log into platforms in your normal browser, then capture sessions in the desktop app.\n"
            "5. Stage follows/subscriptions, review them, and create your first subject.\n"
            "6. Click Smoke Crawl + Test Briefs, or tap Update Briefs here.\n\n"
            "Connected-account onboarding is available for "
            "X/LinkedIn/YouTube/Substack/Medium/Spotify/Apple Podcasts."
            f"{reauth_note}",
        )

    async def _on_help(self, update: Update, _context: ContextTypes.DEFAULT_TYPE | None) -> None:
        if update.message is None:
            return
        await update.message.reply_text(
            self._help_text(),
            reply_markup=self._quick_actions_reply_keyboard(),
        )
        await self._send_quick_actions(update.message)

    async def _on_settings(self, update: Update, _context: ContextTypes.DEFAULT_TYPE | None) -> None:
        if update.message is None:
            return
        await update.message.reply_text(
            "Brief settings:\n"
            "- PCCA uses a minimum relevance floor before sending Briefs.\n"
            "- Default: `PCCA_MIN_BRIEF_RELEVANCE=0.55`.\n"
            "- If a subject has no strongly relevant candidates, PCCA sends a no-Briefs notice instead of noisy items.\n"
            "- Set the env var lower to allow weaker matches, or to `0` to disable the floor globally.",
            reply_markup=self._quick_actions_reply_keyboard(),
        )

    async def _on_read_content_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._run_manual_action_from_message(update.message, action_name="read content", action=self.read_content_action)

    async def _on_briefs_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._run_briefs_from_message(update.message)

    async def _on_update_briefs_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._run_update_briefs_from_message(update.message)

    async def _on_rebuild_briefs_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await update.message.reply_text("`/rebuild_briefs` is now `/update_briefs`. I will update Briefs now.", parse_mode="Markdown")
        await self._run_update_briefs_from_message(update.message)

    async def _on_get_digest_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await update.message.reply_text("`/get_digest` is now `/briefs`. I will run `/briefs` now.", parse_mode="Markdown")
        await self._run_briefs_from_message(update.message)

    async def _on_rebuild_digest_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await update.message.reply_text(
            "`/rebuild_digest` is now `/update_briefs`. I will update Briefs now.",
            parse_mode="Markdown",
        )
        await self._run_update_briefs_from_message(update.message)

    async def _on_cancel_command(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None:
            return
        await self._cancel_update_from_message(update.message)

    async def _on_text(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None or update.message.text is None:
            return
        logger.debug(
            "Telegram text received chat_id=%s user_id=%s chars=%d",
            update.effective_chat.id if update.effective_chat else None,
            update.effective_user.id if update.effective_user else None,
            len(update.message.text),
        )
        if update.effective_chat is not None:
            await self.routing_service.register_chat(
                chat_id=update.effective_chat.id,
                title=update.effective_chat.title or update.effective_chat.full_name,
            )
        if await self._record_brief_reply_feedback(update, update.message.text):
            return
        if await self._handle_pending_subject_edit(update, update.message.text):
            return
        await self._handle_text_intent(update, update.message.text)

    async def _record_brief_reply_feedback(self, update: Update, text: str) -> bool:
        if update.message is None or update.effective_chat is None:
            return False
        replied_to = update.message.reply_to_message
        if replied_to is None:
            return False
        delivery = await self.feedback_service.find_digest_item_by_message(
            chat_id=update.effective_chat.id,
            message_id=replied_to.message_id,
        )
        if delivery is None:
            return False
        await self.feedback_service.add_feedback_by_subject_id(
            subject_id=delivery.subject_id,
            item_id=delivery.item_id,
            feedback_type="reply_text",
            comment_text=text,
        )
        await update.message.reply_text("Feedback saved for this Brief.")
        logger.info(
            "Telegram Brief reply feedback saved chat_id=%s reply_to=%s subject_id=%s item_id=%s chars=%d",
            update.effective_chat.id,
            replied_to.message_id,
            delivery.subject_id,
            delivery.item_id,
            len(text),
        )
        return True

    async def _handle_text_intent(self, update: Update, text: str) -> None:
        if update.message is None:
            return
        intent = parse_intent(text)
        logger.info(
            "Telegram intent parsed action=%s subject=%s platform=%s source_present=%s",
            intent.action.value,
            intent.subject_name,
            intent.platform,
            bool(intent.source_id or intent.source_url),
        )
        thread_id = str(update.message.message_thread_id) if update.message.message_thread_id else None
        if await self._handle_pending_subject_draft(update, text=text, intent=intent, thread_id=thread_id):
            return

        if intent.action is IntentAction.CREATE_SUBJECT:
            if not intent.subject_name:
                if self.subject_draft_repo is None or self.preference_extractor is None:
                    await update.message.reply_text(
                        "Subject creation requires preference extraction. Restart the local agent from the Wizard, "
                        "then describe what to include and avoid."
                    )
                    return
            if self.subject_draft_repo is not None and self.preference_extractor is not None:
                draft = await self.preference_extractor.extract(text)
                if intent.subject_name:
                    draft.title = intent.subject_name
                await self._save_subject_draft(update, draft=draft, last_user_message=text)
                await update.message.reply_text(self._format_subject_draft(draft))
                return
            await update.message.reply_text(
                "Subject creation requires preference extraction. Restart the local agent from the Wizard, "
                "then describe what to include and avoid."
            )
            return

        if intent.action is IntentAction.LIST_SUBJECTS:
            await self._send_subject_management(update.message)
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
                    description_append=text,
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
            await self._run_briefs_from_message(update.message)
            return

        if intent.action is IntentAction.RUN_UPDATE_BRIEFS:
            await self._run_update_briefs_from_message(update.message)
            return

        if intent.action is IntentAction.RUN_REBUILD_DIGEST:
            await self._run_update_briefs_from_message(update.message)
            return

        if intent.action is IntentAction.HELP:
            await self._on_help(update, None)
            return

        await update.message.reply_text(
            "I can handle setup/refinement/actions in free form.\n"
            "Try: /setup"
        )

    async def _handle_pending_subject_draft(
        self,
        update: Update,
        *,
        text: str,
        intent,
        thread_id: str | None,
    ) -> bool:
        if (
            update.message is None
            or update.effective_chat is None
            or self.subject_draft_repo is None
            or self.preference_extractor is None
        ):
            return False
        chat_id = update.effective_chat.id
        draft = await self.subject_draft_repo.get(chat_id)
        if draft is None:
            draft = await self.subject_draft_repo.get(DESKTOP_SUBJECT_DRAFT_CHAT_ID)
        if draft is None:
            return False

        lowered = text.strip().lower()
        if lowered in {"cancel", "cancel subject", "discard subject", "abandon subject"}:
            await self.subject_draft_repo.delete(draft.chat_id)
            await update.message.reply_text("Subject draft discarded.")
            return True

        if lowered in {"save", "save it", "save subject", "confirm", "confirm subject", "yes", "looks good"}:
            if not draft_has_actionable_rules(draft):
                await update.message.reply_text(self._format_subject_draft(draft))
                return True
            subject = await self.subject_service.create_subject(
                draft.title,
                telegram_thread_id=thread_id,
                include_terms=draft.include_terms,
                exclude_terms=draft.exclude_terms,
            )
            await self.routing_service.link_subject(
                subject_name=subject.name,
                chat_id=chat_id,
                thread_id=thread_id,
            )
            await self.subject_draft_repo.delete(draft.chat_id)
            await update.message.reply_text(
                f"Subject saved: {subject.name}\n"
                "Future updates for this subject will use these preferences."
            )
            return True

        # Only treat the message as a draft refinement when the intent parser
        # could not classify it (UNKNOWN) and the user did not type a slash
        # command. Any explicit intent (LIST_SUBJECTS, ADD_SOURCE, CREATE_SUBJECT,
        # REFINE_PREFERENCES, etc.) belongs to its own handler downstream — the
        # previous bypass list was too narrow and ate commands like "List
        # subjects" while a draft was pending.
        if text.startswith("/") or intent.action is not IntentAction.UNKNOWN:
            return False

        updated = await self.preference_extractor.extract(text, previous=draft)
        await self._save_subject_draft(update, draft=updated, last_user_message=text)
        await update.message.reply_text(self._format_subject_draft(updated))
        return True

    async def _save_subject_draft(
        self,
        update: Update,
        *,
        draft: ExtractedSubjectDraft,
        last_user_message: str,
    ) -> SubjectDraft:
        if update.effective_chat is None or self.subject_draft_repo is None:
            raise RuntimeError("Subject draft storage is unavailable.")
        return await self.subject_draft_repo.upsert(
            chat_id=update.effective_chat.id,
            title=draft.title,
            description_text=draft.description_text,
            include_terms=draft.include_terms,
            exclude_terms=draft.exclude_terms,
            quality_notes=draft.quality_notes,
            last_user_message=last_user_message,
        )

    def _format_subject_draft(self, draft: ExtractedSubjectDraft) -> str:
        include = ", ".join(draft.include_terms) if draft.include_terms else "(none yet)"
        exclude = ", ".join(draft.exclude_terms) if draft.exclude_terms else "(none yet)"
        quality = f"\nGood looks like: {draft.quality_notes}" if draft.quality_notes else ""
        warning = f"\n\n⚠️ {draft.extraction_warning}" if draft.extraction_warning else ""
        if not draft_has_actionable_rules(draft):
            return (
                f"I'll call this: {draft.title}\n"
                f"Include: {include}\n"
                f"Avoid: {exclude}"
                f"{quality}\n\n"
                "Tell me a bit more before I save it: what should I include, what should I avoid, "
                "or what would be an example of a high-quality update?"
                f"{warning}"
            )
        return (
            f"I'll call this: {draft.title}\n"
            f"Include: {include}\n"
            f"Avoid: {exclude}"
            f"{quality}\n\n"
            "Reply `save subject` to create it, send corrections, or `cancel subject`."
            f"{warning}"
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
        if await self._record_brief_reply_feedback(update, transcript):
            return
        await self._handle_text_intent(update, transcript)

    async def _on_feedback_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        token = (update.callback_query.data or "fb:").split(":", 1)[1]
        button = await self.feedback_service.get_digest_button(token)
        if button is None or button.kind != "feedback":
            await update.callback_query.answer("This feedback button is no longer available.")
            return
        await self.feedback_service.add_feedback_by_subject_id(
            subject_id=button.subject_id,
            item_id=button.item_id,
            feedback_type="button_macro",
            comment_text=button.action,
        )
        await update.callback_query.answer("Feedback saved.")

    async def _on_more_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        token = (update.callback_query.data or "more:").split(":", 1)[1]
        button = await self.feedback_service.get_digest_button(token)
        if button is None or button.kind != "expand" or button.action != EXPAND_BRIEF_ACTION:
            await update.callback_query.answer(STALE_BRIEF_EXPAND_MESSAGE)
            return
        view = await self.feedback_service.get_digest_brief_view(
            digest_id=button.digest_id,
            item_id=button.item_id,
        )
        latest_view = await self.feedback_service.get_latest_digest_brief_view_for_item(
            subject_id=button.subject_id,
            item_id=button.item_id,
        )
        if latest_view is not None and latest_view.digest_id != button.digest_id:
            view = latest_view
        if view is None:
            await update.callback_query.answer(STALE_BRIEF_EXPAND_MESSAGE)
            return
        buttons = await self.feedback_service.list_digest_buttons_for_item(
            digest_id=view.digest_id,
            item_id=button.item_id,
        )
        await update.callback_query.edit_message_text(
            text=view.full_text,
            reply_markup=self._brief_inline_keyboard_from_button_rows(
                [row for row in buttons if row.kind == "feedback"]
            ),
            disable_web_page_preview=True,
            parse_mode="MarkdownV2",
        )
        await update.callback_query.answer("Expanded.")

    async def _on_route_subject_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        message = update.callback_query.message
        if message is None:
            await update.callback_query.answer("This route picker is no longer available.")
            return
        subject_id = self._parse_callback_subject_id(update.callback_query.data or "", expected_parts=2)
        if subject_id is None:
            await update.callback_query.answer("This route picker is invalid.")
            return
        chat_id = self._message_chat_id(message)
        if chat_id is None:
            await update.callback_query.answer("Could not detect this chat.")
            return
        chat_title = self._message_chat_title(message)
        thread_id = self._message_thread_id(message)
        await self.routing_service.register_chat(chat_id=chat_id, title=chat_title)
        try:
            subject = await self.routing_service.link_subject_id(
                subject_id=subject_id,
                chat_id=chat_id,
                thread_id=thread_id,
            )
        except ValueError:
            await update.callback_query.answer("That subject no longer exists.")
            return
        logger.info(
            "Telegram subject route selected chat_id=%s thread_id=%s subject_id=%s subject=%s",
            chat_id,
            thread_id,
            subject.id,
            subject.name,
        )
        await update.callback_query.answer(f"Linked {subject.name}.")
        await update.callback_query.edit_message_text(
            f"This chat is now linked to {subject.name} for Brief delivery.",
            reply_markup=self._quick_actions_inline_keyboard(),
        )

    async def _on_run_subject_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        message = update.callback_query.message
        if message is None:
            await update.callback_query.answer("This action is no longer available.")
            return
        parts = (update.callback_query.data or "").split(":")
        if len(parts) != 3:
            await update.callback_query.answer("This action is invalid.")
            return
        action_name = parts[1]
        try:
            subject_id = int(parts[2])
        except ValueError:
            await update.callback_query.answer("This subject selection is invalid.")
            return
        chat_id = self._message_chat_id(message)
        if chat_id is None:
            await update.callback_query.answer("Could not detect this chat.")
            return
        thread_id = self._message_thread_id(message)
        await self.routing_service.register_chat(chat_id=chat_id, title=self._message_chat_title(message))
        try:
            subject = await self.routing_service.link_subject_id(
                subject_id=subject_id,
                chat_id=chat_id,
                thread_id=thread_id,
            )
        except ValueError:
            await update.callback_query.answer("That subject no longer exists.")
            return
        await update.callback_query.answer(f"Selected {subject.name}.")
        logger.info(
            "Telegram subject-scoped action selected action=%s chat_id=%s thread_id=%s subject_id=%s",
            action_name,
            chat_id,
            thread_id,
            subject.id,
        )
        if action_name == "update":
            await self._run_update_briefs_from_message(message, subject_ids={subject.id}, subject_name=subject.name)
            return
        if action_name == "briefs":
            await self._run_briefs_from_message(message, subject_ids={subject.id}, subject_name=subject.name)
            return
        if action_name == "rebuild":
            await self._run_update_briefs_from_message(message, subject_ids={subject.id}, subject_name=subject.name)
            return
        await message.reply_text("That subject action is not supported yet.")

    async def _on_subject_manage_callback(self, update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query is None:
            return
        query = update.callback_query
        message = query.message
        if message is None:
            await query.answer("This subject editor is no longer available.")
            return
        parts = (query.data or "").split(":")
        action = parts[1] if len(parts) >= 2 else ""
        if action == "list":
            await query.answer()
            await self._send_subject_management(message, edit_existing=True)
            return
        if len(parts) < 3:
            await query.answer("This subject action is invalid.")
            return
        try:
            subject_id = int(parts[2])
        except ValueError:
            await query.answer("This subject selection is invalid.")
            return
        try:
            subject = await self.subject_service.repository.get_by_id(subject_id)
        except ValueError:
            await query.answer("That subject no longer exists.")
            return

        if action == "open":
            await query.answer(subject.name)
            await self._edit_subject_detail_message(message, subject_id)
            return
        if action == "toggle":
            next_status = "active" if subject.status == "paused" else "paused"
            subject = await self.subject_service.set_subject_status(subject_id, next_status)
            await query.answer(f"{subject.name} is now {subject.status}.")
            await self._edit_subject_detail_message(message, subject_id)
            return
        if action in {"rename", "description", "floor"}:
            key = self._message_scope_key(message)
            self._pending_subject_edits[key] = PendingSubjectEdit(subject_id=subject_id, action=action)
            prompts = {
                "rename": f"Send the new name for {subject.name}.",
                "description": f"Send the new description for {subject.name}. This will invalidate the subject embedding.",
                "floor": (
                    f"Send a relevance floor for {subject.name}: a number from 0.0 to 1.0, "
                    "or `default` to use the global setting."
                ),
            }
            await query.answer("Waiting for your reply.")
            await message.reply_text(prompts[action])
            return
        await query.answer("That subject action is not supported yet.")

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
        if data == "run:update":
            await self._run_update_briefs_from_message(message)
            return
        if data in {"run:briefs", "run:digest"}:
            await self._run_briefs_from_message(message)
            return
        if data in {"run:rebuild", "run:rebuild_briefs"}:
            await self._run_update_briefs_from_message(message)
            return
        if data == "run:help":
            await message.reply_text(self._help_text(), reply_markup=self._quick_actions_reply_keyboard())
            return

    async def _run_briefs_from_message(
        self,
        message,
        subject_ids: set[int] | None = None,
        subject_name: str | None = None,
    ) -> None:
        if self.get_digest_action is None:
            await message.reply_text(
                "`get briefs` action is not available yet in this runtime.",
                parse_mode="Markdown",
            )
            return
        if subject_ids is None:
            resolved = await self._resolve_subject_ids_for_message(
                message,
                action_label="Briefs",
                callback_action="briefs",
            )
            if resolved is None:
                return
            subject_ids, subject_name = resolved

        async def action() -> Any:
            return await self.get_digest_action(subject_ids=subject_ids)

        action_name = f"get briefs for {subject_name}" if subject_name else "get briefs"
        await self._run_manual_action_from_message(message, action_name=action_name, action=action)

    async def _run_update_briefs_from_message(
        self,
        message,
        subject_ids: set[int] | None = None,
        subject_name: str | None = None,
    ) -> None:
        if self.update_briefs_action is None:
            await message.reply_text(
                "`update briefs` action is not available yet in this runtime.",
                parse_mode="Markdown",
            )
            return
        if subject_ids is None:
            resolved = await self._resolve_subject_ids_for_message(
                message,
                action_label="Update Briefs",
                callback_action="update",
            )
            if resolved is None:
                return
            subject_ids, subject_name = resolved
        if self._manual_action_lock.locked():
            await message.reply_text("Another manual run is in progress. Please wait a bit and try again.")
            return

        status_message = await message.reply_text(
            "Updating Briefs. This usually takes 30-70 min depending on content volume.\n"
            "Send /cancel to stop after the current source finishes."
        )
        last_edit_at = 0.0

        def progress(event: dict[str, Any]) -> None:
            nonlocal last_edit_at
            now = time.monotonic()
            # Telegram edit limits are easy to hit during source loops; keep
            # progress visible without trying to animate every single event.
            if now - last_edit_at < 2.0 and event.get("kind") not in {"delivery", "finished"}:
                return
            last_edit_at = now
            text = self._format_update_progress(event)
            if not text:
                return
            self._track_task(self._edit_message_text(status_message, text))

        started_at = time.monotonic()
        try:
            async with self._manual_action_lock:
                stats = await self.update_briefs_action(subject_ids=subject_ids, progress_callback=progress)
            if isinstance(stats, dict) and stats.get("cancelled"):
                await self._edit_message_text(status_message, "Update Briefs cancelled. No partial Briefs were delivered.")
                return
            if isinstance(stats, dict) and stats.get("skipped_already_running"):
                await self._edit_message_text(status_message, "Another collection run is already in progress. Try again later.")
                return
            summary = self._format_update_summary(stats if isinstance(stats, dict) else {})
            await self._edit_message_text(status_message, summary)
            logger.info(
                "Telegram Update Briefs finished subject_ids=%s duration_ms=%d stats=%s",
                sorted(subject_ids) if subject_ids is not None else None,
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
        except Exception:
            logger.exception(
                "Telegram Update Briefs failed subject_ids=%s duration_ms=%d",
                sorted(subject_ids) if subject_ids is not None else None,
                int((time.monotonic() - started_at) * 1000),
            )
            await self._edit_message_text(status_message, "`update briefs` failed. Check logs and try again.")

    async def _cancel_update_from_message(self, message) -> None:
        if self.cancel_update_action is None:
            await message.reply_text("No cancellable update action is available in this runtime.")
            return
        result = await self.cancel_update_action()
        if isinstance(result, dict) and result.get("cancel_requested"):
            await message.reply_text("Cancel requested. The current source will finish, then the update will stop.")
            return
        await message.reply_text("No Update Briefs run is active right now.")

    def _track_task(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._progress_tasks.add(task)
        task.add_done_callback(self._progress_tasks.discard)

    async def _edit_message_text(self, message, text: str) -> None:
        try:
            await message.edit_text(text)
        except Exception:
            logger.debug("Telegram progress edit failed; sending progress as reply fallback.", exc_info=True)
            try:
                await message.reply_text(text)
            except Exception:
                logger.exception("Telegram progress reply fallback failed.")

    @staticmethod
    def _format_update_progress(event: dict[str, Any]) -> str | None:
        kind = str(event.get("kind") or "")
        if kind == "collection":
            return (
                f"Reading {event.get('platform')} "
                f"({event.get('source_index')}/{event.get('source_total')})...\n"
                f"{event.get('display_name') or event.get('source_id')}"
            )
        if kind == "collection_done":
            return (
                f"Read {event.get('platform')} "
                f"({event.get('source_index')}/{event.get('source_total')}). "
                f"New/raw items from source: {event.get('item_count', 0)}."
            )
        if kind == "auto_backfill":
            return f"Preparing embeddings ({event.get('processed', 0)}/{event.get('total', 0)})..."
        if kind == "embedding_item_backfill":
            return f"Embedding items ({event.get('processed', 0)}/{event.get('total', 0)})..."
        if kind == "embedding_segment_backfill":
            return f"Embedding segments ({event.get('processed', 0)}/{event.get('total', 0)})..."
        if kind == "embedding_not_warmed":
            return f"Cold embedding cache for {event.get('subject_name')}; using keyword fallback while warming catches up."
        if kind == "scoring":
            return (
                f"Scoring subject {event.get('subject_index')}/{event.get('subject_total')}: "
                f"{event.get('subject_name')}..."
            )
        if kind == "delivery":
            return "Delivering Briefs..."
        return None

    @staticmethod
    def _format_update_summary(stats: dict[str, Any]) -> str:
        collection = stats.get("collection") if isinstance(stats.get("collection"), dict) else {}
        briefs = stats.get("briefs") if isinstance(stats.get("briefs"), dict) else {}
        delivered = int(briefs.get("briefs_sent") or 0)
        subjects = int(briefs.get("subjects_with_routes") or 0)
        collected = int(collection.get("items_collected") or 0)
        inserted = int(collection.get("items_inserted") or 0)
        updated = int(collection.get("items_updated") or 0)
        return (
            f"Update Briefs completed.\n"
            f"Collected {collected} item(s): {inserted} new, {updated} updated.\n"
            f"Delivered {delivered} Brief(s) for {subjects} subject(s)."
        )

    async def _run_rebuild_digest_from_message(
        self,
        message,
        subject_ids: set[int] | None = None,
        subject_name: str | None = None,
    ) -> None:
        if self.rebuild_digest_action is None:
            await message.reply_text(
                "`rebuild briefs` action is not available yet in this runtime.",
                parse_mode="Markdown",
            )
            return
        if subject_ids is None:
            resolved = await self._resolve_subject_ids_for_message(
                message,
                action_label="rebuild Briefs",
                callback_action="rebuild",
            )
            if resolved is None:
                return
            subject_ids, subject_name = resolved

        async def action() -> Any:
            return await self.rebuild_digest_action(subject_ids=subject_ids)

        action_name = f"rebuild briefs for {subject_name}" if subject_name else "rebuild briefs"
        await self._run_manual_action_from_message(message, action_name=action_name, action=action)

    async def _resolve_subject_ids_for_message(
        self,
        message,
        *,
        action_label: str,
        callback_action: str,
    ) -> tuple[set[int], str | None] | None:
        chat_id = self._message_chat_id(message)
        if chat_id is None:
            await message.reply_text(f"I could not detect the chat for {action_label}.")
            return None
        thread_id = self._message_thread_id(message)
        await self.routing_service.register_chat(chat_id=chat_id, title=self._message_chat_title(message))
        routes = await self.routing_service.list_routes_for_chat(chat_id=chat_id, thread_id=thread_id)
        if len(routes) == 1:
            route = routes[0]
            return {route.subject_id}, route.subject_name
        if len(routes) > 1:
            await message.reply_text(
                f"This chat is linked to multiple subjects. Choose which subject should run {action_label}:",
                reply_markup=self._subject_action_keyboard_from_routes(routes, callback_action),
            )
            return None

        subjects = await self.subject_service.list_subjects()
        if not subjects:
            await message.reply_text(
                "No subjects exist yet. Create your first subject in the desktop wizard or send a free-form subject request here."
            )
            return None
        if len(subjects) == 1:
            subject = await self.routing_service.link_subject_id(
                subject_id=subjects[0].id,
                chat_id=chat_id,
                thread_id=thread_id,
            )
            logger.info(
                "Telegram action auto-linked single subject action=%s chat_id=%s thread_id=%s subject_id=%s",
                callback_action,
                chat_id,
                thread_id,
                subject.id,
            )
            return {subject.id}, subject.name

        await message.reply_text(
            f"This chat is not linked to a subject yet. Choose one and I will link it before running {action_label}:",
            reply_markup=self._subject_action_keyboard(subjects, callback_action),
        )
        return None

    async def _send_subject_management(self, message, *, edit_existing: bool = False) -> None:
        subjects = await self.subject_service.list_subjects()
        if not subjects:
            text = "No subjects yet. You can create one in free form."
            if edit_existing and hasattr(message, "edit_text"):
                await message.edit_text(text)
            else:
                await message.reply_text(text)
            return
        rows = []
        for subject in subjects:
            rows.append(await self._format_subject_manage_row(subject))
        text = "Edit Subjects:\n" + "\n".join(rows) + "\n\nTip: open the wizard to manage sources for a subject."
        markup = self._subject_management_keyboard(subjects)
        if edit_existing and hasattr(message, "edit_text"):
            await message.edit_text(text, reply_markup=markup)
        else:
            await message.reply_text(text, reply_markup=markup)

    async def _edit_subject_detail_message(self, message, subject_id: int) -> None:
        subject = await self.subject_service.repository.get_by_id(subject_id)
        text = await self._format_subject_detail(subject)
        await message.edit_text(text, reply_markup=self._subject_detail_keyboard(subject))

    async def _format_subject_manage_row(self, subject) -> str:
        source_count = await self._source_count_for_subject(subject)
        top_score = await self._top_score_for_subject(subject.id)
        status = "paused" if subject.status == "paused" else "active"
        floor = (
            f", floor {subject.min_relevance_threshold:.2f}"
            if subject.min_relevance_threshold is not None
            else ""
        )
        score = f", top {top_score:.2f}" if top_score is not None else ""
        return f"- {subject.name} ({status}) — {source_count} sources{score}{floor}"

    async def _format_subject_detail(self, subject) -> str:
        source_count = await self._source_count_for_subject(subject)
        description = await self.subject_service.repository.get_description_text(subject.id)
        top_score = await self._top_score_for_subject(subject.id)
        floor = (
            f"{subject.min_relevance_threshold:.2f}"
            if subject.min_relevance_threshold is not None
            else "global default"
        )
        score = f"{top_score:.2f}" if top_score is not None else "not scored yet"
        return (
            f"{subject.name}\n"
            f"Status: {subject.status}\n"
            f"Sources: {source_count}\n"
            f"Top score: {score}\n"
            f"Relevance floor: {floor}\n\n"
            f"Description:\n{description or '(empty)'}\n\n"
            "Tip: open the wizard to manage sources for this subject."
        )

    async def _source_count_for_subject(self, subject) -> int:
        try:
            return len(await self.source_service.list_sources_for_subject(subject.name))
        except Exception:
            logger.debug("Could not count sources for subject_id=%s", getattr(subject, "id", None), exc_info=True)
            return 0

    async def _top_score_for_subject(self, subject_id: int) -> float | None:
        if self.item_score_repo is None:
            return None
        try:
            candidates = await self.item_score_repo.top_candidates(subject_id=subject_id, limit=1)
        except Exception:
            logger.debug("Could not read top score for subject_id=%s", subject_id, exc_info=True)
            return None
        if not candidates:
            return None
        return float(candidates[0].final_score or 0.0)

    async def _handle_pending_subject_edit(self, update: Update, text: str) -> bool:
        if update.message is None:
            return False
        key = self._message_scope_key(update.message)
        pending = self._pending_subject_edits.get(key)
        if pending is None:
            return False
        lowered = text.strip().lower()
        if lowered in {"cancel", "/cancel", "never mind", "stop"}:
            self._pending_subject_edits.pop(key, None)
            await update.message.reply_text("Subject edit cancelled.")
            return True
        try:
            if pending.action == "rename":
                subject = await self.subject_service.rename_subject(pending.subject_id, text)
                await update.message.reply_text(f"Renamed subject to {subject.name}.")
            elif pending.action == "description":
                subject = await self.subject_service.update_subject_description(pending.subject_id, text)
                await update.message.reply_text(
                    f"Updated description for {subject.name}. The subject embedding will be rebuilt on the next score run."
                )
            elif pending.action == "floor":
                threshold = None if lowered in {"default", "global", "none", "unset"} else float(text.strip())
                if threshold is not None and not 0.0 <= threshold <= 1.0:
                    await update.message.reply_text("Please send a number from 0.0 to 1.0, or `default`.")
                    return True
                subject = await self.subject_service.set_subject_min_relevance_threshold(
                    pending.subject_id,
                    threshold,
                )
                label = "global default" if subject.min_relevance_threshold is None else f"{subject.min_relevance_threshold:.2f}"
                await update.message.reply_text(f"Set relevance floor for {subject.name} to {label}.")
            else:
                return False
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return True
        self._pending_subject_edits.pop(key, None)
        return True

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
        started_at = time.monotonic()
        try:
            async with self._manual_action_lock:
                await action()
            reauth_note = ""
            if action_name == "read content":
                reauth_note = await self._format_reauth_sources()
            await message.reply_text(f"{action_name} completed.{reauth_note}")
            logger.info(
                "Telegram manual action finished action=%s duration_ms=%d",
                action_name,
                int((time.monotonic() - started_at) * 1000),
            )
        except Exception:
            logger.exception(
                "Manual action failed: %s duration_ms=%d",
                action_name,
                int((time.monotonic() - started_at) * 1000),
            )
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
        await message.reply_text("Quick actions:", reply_markup=self._quick_actions_inline_keyboard())

    def _brief_inline_keyboard(self, buttons) -> InlineKeyboardMarkup:
        feedback_buttons = [button for button in buttons if button.kind == "feedback"]
        expand_buttons = [button for button in buttons if button.kind == "expand"]
        rows: list[list[InlineKeyboardButton]] = []
        if feedback_buttons:
            rows.append(
                [
                    InlineKeyboardButton(button.label, callback_data=f"fb:{button.token}")
                    for button in feedback_buttons[:4]
                ]
            )
        if expand_buttons:
            rows.append(
                [
                    InlineKeyboardButton(
                        expand_buttons[0].label,
                        callback_data=f"more:{expand_buttons[0].token}",
                    )
                ]
            )
        return InlineKeyboardMarkup(rows)

    def _brief_inline_keyboard_from_button_rows(self, buttons) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        if buttons:
            rows.append(
                [
                    InlineKeyboardButton(button.label, callback_data=f"fb:{button.token}")
                    for button in buttons[:4]
                ]
            )
        return InlineKeyboardMarkup(rows)

    def _quick_actions_inline_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(self._quick_action_rows())

    @staticmethod
    def _quick_action_rows() -> list[list[InlineKeyboardButton]]:
        return [
            [
                InlineKeyboardButton("Update Briefs", callback_data="run:update"),
                InlineKeyboardButton("Edit Subjects", callback_data="subject_manage:list"),
                InlineKeyboardButton("Help", callback_data="run:help"),
            ]
        ]

    @staticmethod
    def _quick_actions_reply_keyboard() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            [["Update Briefs", "Edit Subjects", "Help"]],
            resize_keyboard=True,
            one_time_keyboard=False,
        )

    @staticmethod
    def _subject_picker_keyboard(subjects) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton(subject.name, callback_data=f"route:{subject.id}")] for subject in subjects]
        )

    @staticmethod
    def _subject_action_keyboard(subjects, action: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(subject.name, callback_data=f"run_subject:{action}:{subject.id}")]
                for subject in subjects
            ]
        )

    @staticmethod
    def _subject_action_keyboard_from_routes(routes, action: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(route.subject_name, callback_data=f"run_subject:{action}:{route.subject_id}")]
                for route in routes
            ]
        )

    @staticmethod
    def _subject_management_keyboard(subjects) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton(subject.name, callback_data=f"subject_manage:open:{subject.id}")] for subject in subjects]
        )

    @staticmethod
    def _subject_detail_keyboard(subject) -> InlineKeyboardMarkup:
        pause_label = "Resume" if subject.status == "paused" else "Pause"
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(pause_label, callback_data=f"subject_manage:toggle:{subject.id}")],
                [
                    InlineKeyboardButton("Rename", callback_data=f"subject_manage:rename:{subject.id}"),
                    InlineKeyboardButton("Edit description", callback_data=f"subject_manage:description:{subject.id}"),
                ],
                [InlineKeyboardButton("Adjust relevance floor", callback_data=f"subject_manage:floor:{subject.id}")],
                [InlineKeyboardButton("Back to subjects", callback_data="subject_manage:list")],
            ]
        )

    @staticmethod
    def _help_text() -> str:
        return (
            "I can:\n"
            "- show the Scenario 1 setup checklist (`/setup`)\n"
            "- create/edit subjects\n"
            "- list/remove imported sources\n"
            "- show/refine preferences per subject\n"
            "- update Briefs now (`/update_briefs`)\n"
            "- show delivery settings (`/settings`)\n"
            "- collect per-Brief feedback (buttons or replies)\n"
            "- accept voice notes (transcription backend pending)"
        )

    @staticmethod
    def _parse_callback_subject_id(data: str, *, expected_parts: int) -> int | None:
        parts = data.split(":")
        if len(parts) != expected_parts:
            return None
        try:
            return int(parts[-1])
        except ValueError:
            return None

    @staticmethod
    def _message_chat_id(message) -> int | None:
        chat = getattr(message, "chat", None)
        chat_id = getattr(message, "chat_id", None) or getattr(chat, "id", None)
        return int(chat_id) if chat_id is not None else None

    @staticmethod
    def _message_chat_title(message) -> str | None:
        chat = getattr(message, "chat", None)
        if chat is None:
            return None
        return getattr(chat, "title", None) or getattr(chat, "full_name", None)

    @staticmethod
    def _message_thread_id(message) -> str | None:
        thread_id = getattr(message, "message_thread_id", None)
        return str(thread_id) if thread_id else None

    @classmethod
    def _message_scope_key(cls, message) -> str:
        chat_id = cls._message_chat_id(message)
        thread_id = cls._message_thread_id(message) or ""
        return f"{chat_id or 0}:{thread_id}"

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
