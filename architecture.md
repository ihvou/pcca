# PCCA — Architecture

This document is the authoritative design reference. It describes what the system does, how the modules fit together, and where extension points live. Interaction flows belong in [scenarios.md](./scenarios.md); the refactoring backlog belongs in [tasks.md](./tasks.md).

---

## 0) Problem Framing

Native platform recommendation algorithms optimize for engagement and session time, not for personal signal quality. For users tracking niche technical topics, this produces:
- high noise volume
- weak novelty filtering
- poor alignment to nuanced personal standards

**Competitive goal:** outperform native platform recommendations per subject by delivering fewer but higher-value items. Success criteria:
- "glad I did not miss this" events at higher frequency than native feeds
- lower noise rate for the same topic window
- better practical relevance (implementation detail over generic commentary)

---

## 1) Product Summary

PCCA is a local desktop agent (Mac / Windows / Linux) that:
- Runs nightly on the user's machine.
- Opens logged-in browser sessions to collect new content from selected sources.
- Analyzes content with a local-first model pipeline (free-first), with an optional premium reranking lane.
- Delivers concise morning digests to Telegram, one per subject.
- Learns preferences over time, per subject, via inline buttons and free-form conversation.

Core value:
- Surface high-signal, practical, timely content.
- Suppress generic, repetitive, and low-trust noise.
- Support nuanced filtering — segment-level relevance, not only topic-level relevance.
- Free-form text and voice feedback loops per subject.

---

## 2) Product Decisions

1. **Social ingestion v1**: X via browser only (no API). LinkedIn via browser. Both are subject to platform UI / policy changes.
2. **YouTube transcripts v1**: captions only. Whisper audio fallback is deferred.
3. **Model strategy**: local-first (Ollama) for bulk analysis. Optional premium reranker for top candidates. Fallback path keeps the system operational if premium is unavailable.
4. **Telegram UX**: each subject is its own thread context with independent preference memory.
5. **Interaction mode**: free-form by default. Voice supported for feedback.
6. **Deployment**: local desktop + local DB. No cloud backend for core functionality.

---

## 3) System Overview

Four logical layers run on a daily cycle. Feedback runs continuously.

```
[ Ingestion ]  →  [ Curation ]  →  [ Delivery ]  →  [ Feedback ]
                       ↑                                    |
                       └────────── Taste Model ─────────────┘
```

Layer responsibilities:

- **Ingestion** — collect raw items from logged-in browser sessions and RSS-like feeds; normalize into canonical `items` + `item_segments`.
- **Curation** — score items against subject preferences through a two-pass pipeline (Pass-1 cheap screening → shortlist → Pass-2 deep analysis).
- **Delivery** — compose per-subject digests and send to mapped Telegram threads with inline feedback controls.
- **Feedback** — capture inline reactions and free-form conversational signals; update subject preferences and source trust.

The **taste model** (subject preferences + feedback-derived signals) sits across the curation and feedback layers and is the moat.

---

## 4) Modules

Each module is one directory under `src/pcca/`. Dependencies flow downward: collectors depend on nothing in this list; `app.py` depends on everything.

### 4.1 Desktop Shell (`desktop_shell.py`, `desktop_web/`)
Installer-adjacent first-run wizard, logs viewer, and local controls. The shell is a PyWebView native window backed by a token-protected web UI served on `127.0.0.1:<random-port>`. Business actions run through `DesktopCommandService` so CLI and desktop flows share the same implementation.

### 4.2 Agent Core (`app.py`, `__main__.py`, `__init__.py`)
Main process runtime, lifecycle management, dependency wiring. `PCCAApp` is the composition root.

### 4.3 Scheduler (`scheduler.py`)
Nightly crawl, morning digest, weekly reflection, discovery jobs. Wake-aware and catch-up on resume (task T-18). Uses APScheduler `AsyncIOScheduler`.

### 4.4 Browser Session Manager (`browser/session_manager.py`)
Playwright persistent profiles per platform. Session cookies are captured from the user's normal browser and injected into these profiles; PCCA does not drive anti-bot-protected login/OAuth flows. Browser-channel stealth/orphan cleanup remains useful for scraping, not login. Session health checks. `needs_reauth` flagging (task T-1).

### 4.4.1 Session Capture Service (`services/session_capture_service.py`)
Reads platform auth cookies from supported local browser stores with explicit user action, then injects them into the matching PCCA Playwright profile. T-37A/T-37C implement macOS Chromium-family browser capture (Chrome/Arc/Brave/Edge) for X, LinkedIn, YouTube, Spotify, Substack, Medium, and best-effort Apple Podcasts. Raw cookie values are never logged or written into PCCA's SQLite DB.

### 4.5 Source Collectors (`collectors/`)
One file per platform:
- `x_collector.py` — browser scrape of profile timeline
- `linkedin_collector.py` — browser scrape of recent-activity / company posts
- `youtube_collector.py` — browser listing + transcript via `YouTubeTranscriptService`
- `reddit_collector.py` — HTTP JSON from `/new.json`
- `spotify_collector.py` — browser scrape of show page
- `rss_collector.py` — `feedparser`; aliased as `substack`, `medium`, `apple_podcasts`

All implement the `Collector` Protocol in `base.py`.

#### 4.5.1 Platform Matrix

How each supported platform is reached for two distinct operations: importing the user's existing follows / subscriptions, and collecting new content from those follows.

| Platform | Follows / subscriptions import | Content collection |
|---|---|---|
| X (Twitter) | Session capture from user's browser, then browser scrape — logged-in `/{handle}/following` (`FollowImportService.import_x_follows`) | Browser scrape — logged-in `/{handle}` profile timeline (`XCollector`) |
| LinkedIn | Session capture from user's browser, then browser scrape — logged-in `/feed/following/` (`FollowImportService.import_linkedin_follows`) | Browser scrape — logged-in `/in/{user}/recent-activity/all/` or `/company/{slug}/posts/` (`LinkedInCollector`) |
| YouTube | Session capture from user's browser, then browser scrape — logged-in `/feed/channels` (`FollowImportService.import_youtube_subscriptions`) | Browser scrape — public `/{handle}/videos` list (`YouTubeCollector`) + `youtube-transcript-api` HTTP for captions (`YouTubeTranscriptService`) |
| Reddit | N/A — user supplies subreddits / users by name | Public JSON API — `https://www.reddit.com/r/{sub}/new.json` and `/user/{u}/submitted.json`, no auth (`RedditCollector` via `httpx`) |
| Spotify (podcasts) | Session capture from user's browser, then browser scrape — logged-in `/collection/podcasts` (`FollowImportService.import_spotify_podcast_follows`) | Browser scrape — `open.spotify.com/show/{id}` (`SpotifyCollector`) |
| Apple Podcasts | Best-effort session capture from user's browser, then browser scrape — logged-in `podcasts.apple.com/library/shows` (`FollowImportService.import_apple_podcast_subscriptions`) | RSS — feed URL discovered via iTunes lookup API (`SourceDiscoveryService._lookup_apple_podcast_feed`), parsed by `feedparser` (`RSSCollector` aliased as `apple_podcasts`) |
| Substack | Session capture from user's browser, then browser scrape — logged-in `substack.com/settings` (`FollowImportService.import_substack_subscriptions`) | RSS — `{publication}.substack.com/feed`, parsed by `feedparser` (`RSSCollector` aliased as `substack`) |
| Medium | Session capture from user's browser, then browser scrape — logged-in `medium.com/me/following` (`FollowImportService.import_medium_following`) | RSS — `medium.com/feed/{handle-or-publication}`, parsed by `feedparser` (`RSSCollector` aliased as `medium`) |
| Generic RSS / Atom | N/A — user supplies feed URLs directly | RSS — `feedparser` invoked via `asyncio.to_thread` (`RSSCollector`) |

Notes:
- Browser scraping uses persistent Playwright Chromium profiles per platform (see §4.4). Sessions should be imported through `Session Capture Service` where supported; headful login windows are developer escape hatches only.
- Login state is detected by URL pattern in `is_*_login_url()` helpers per browser collector; on detection the collector raises `SessionChallengedError`, the orchestrator marks `sources.follow_state='needs_reauth'`, and the user is prompted in `/setup`.
- For RSS-bridged platforms (Apple Podcasts, Substack, Medium) login is required only for the follows import pass; subsequent content collection is unauthenticated RSS.
- Reddit and Generic RSS are the only platforms with no logged-in dependency in either column; they are the safest first sources for Scenario 1 smoke tests.
- Adding a platform requires the five coordinated changes in [§13](#13-extension-points).

### 4.6 Extraction Layer
Currently inline in each collector. Deterministic DOM extractors first; structured fallback deferred. Normalizes into `CollectedItem` (see [§5](#5-data-model)).

### 4.7 Model Router (`services/model_router.py`)
Routes scoring tasks. Default local (Ollama). Optional premium lane for shortlisted items. Enforces cost/time caps and fallback behavior (task T-16).

### 4.8 Curation Engine (`pipeline/curation.py`)
Pass-1 keyword-based screening. Pass-2 deep analysis. Ranking and rationale emission. Pass-2 currently runs inline on every item; must be shortlisted (task T-3).

### 4.9 Subject Service (`services/subject_service.py`)
Subject creation, lifecycle, routing, isolation.

### 4.10 Preference Engine (`services/preference_service.py`)
Subject-specific profile state. Versioned rule updates. Rollback capable.

### 4.11 Telegram Service (`services/telegram_service.py`)
Digest delivery, button callbacks, conversational control. Free-form intent dispatch via `intent_parser.py`. Voice via `voice_transcription_service.py` (placeholder, task T-23).

### 4.12 Storage Service (`db.py`, `repositories/`)
SQLite access, migrations, repositories, event logs. Single shared `aiosqlite.Connection` in v1; split reader/writer planned (task T-9).

### 4.13 Observability
Today: `run_logs.stats_json` blob. Target: per-collector extraction success %, model latency, digest delivery success, session-challenge counters (task T-20).

### 4.14 Discovery Service (`services/source_discovery_service.py`)
Canonical source normalization for imported follows/subscriptions and platform-specific source identities. Passive + active account discovery is planned.

### 4.15 Voice Transcription Service (`services/voice_transcription_service.py`)
Converts Telegram voice notes to text for intent parsing. v1 placeholder (always returns `None`). Local-first path planned (task T-23).

---

## 5) Data Model

Subject-centric. Subject isolation at the preference layer; items are global; scores are per-(item, subject).

### 5.1 Core Tables

```
subjects(id, name UNIQUE, telegram_thread_id, status, created_at)

subject_preferences(id, subject_id, version, include_rules_json,
                    exclude_rules_json, source_weights_json,
                    quality_rules_json, updated_at)

sources(id, platform, account_or_channel_id, display_name,
        follow_state, last_crawled_at,
        UNIQUE(platform, account_or_channel_id))

discovered_sources(id, platform, account_or_channel_id, display_name,
                   discovery_type, evidence_json, confidence_score,
                   status, created_at)

subject_sources(subject_id, source_id, priority, status,
                PRIMARY KEY(subject_id, source_id))

items(id, platform, external_id, canonical_url, author, published_at,
      raw_text, transcript_text, metadata_json,
      UNIQUE(platform, external_id))

item_segments(id, item_id, start_offset, end_offset,
              segment_text, segment_type)

item_scores(id, item_id, subject_id, pass1_score, pass2_score,
            practicality_score, novelty_score, trust_score,
            noise_penalty, final_score, rationale_json,
            UNIQUE(item_id, subject_id))

digests(id, subject_id, run_date, sent_at, status)

digest_items(digest_id, item_id, rank, reason_selected,
             PRIMARY KEY(digest_id, item_id))

feedback_events(id, subject_id, item_id, feedback_type,
                comment_text, created_at)

run_logs(id, run_type, started_at, ended_at, status, stats_json)

telegram_chats(chat_id PRIMARY KEY, title, last_seen_at)

subject_routes(id, subject_id, chat_id, thread_id, created_at,
               UNIQUE(subject_id, chat_id, thread_id))
```

### 5.2 Design Rules

- **Subject isolation** is mandatory at the preference layer. A feedback on subject A must not change scoring for subject B unless explicitly opted in.
- **Item storage is global**; scoring is per-subject. The same item discovered on two platforms should collapse at the dedupe step (task T-10).
- **Preference edits are versioned and append-only.** Rollback by reading an older version.
- **Run IDs** for ingestion are mandatory; **dedupe keys** for digests (`UNIQUE(subject_id, run_date)`) are mandatory (task T-5).

---

## 6) Analysis Logic (Practicality-First)

### 6.1 Pass-1 — Cheap Screening
Inputs: titles, snippets, first transcript chunks.
Outputs: relevance estimate, novelty estimate, likely-noise flag, keep/drop decision.
Today: keyword list in `curation.py`. Target: keep for pre-filter only (task T-16).

### 6.2 Pass-2 — Deep Curation
Inputs: full text / transcript segments for **shortlisted** items.
Outputs: actionable insights extracted, practical-value score, trust assessment, subject-specific explanation.
Today: same keyword pass as Pass-1 plus optional Ollama delta. Target: Ollama (or premium) as primary scorer on top-K only (tasks T-3, T-16).

### 6.3 Segment-Level Filtering
For long-form content (transcripts, long articles):
- Chunk by paragraph / ~300-token window
- Score per segment
- Include only chunks with practical signal; drop bio / generic commentary

Today: `item_segments` table exists but is unpopulated (task T-11).

### 6.4 Ranking Formula (Configurable)

```
final_score = w1·relevance + w2·practicality + w3·novelty
            + w4·trust − w5·noise
```

Weights live in per-subject `quality_rules_json` so they can evolve.

---

## 7) Scheduling

Default daily schedule (local time, configurable via `PCCA_NIGHTLY_CRON` / `PCCA_MORNING_CRON`):

- `01:00` — start crawl
- `01:00–03:00` — collection + normalization
- `03:00–04:00` — Pass-1 + Pass-2
- `04:00` — finalize candidates
- `08:30` — send digest

Weekly:
- Preference reflection summary (not yet implemented — task T-27)
- Source health summary
- Discovery suggestion run

**Wake behavior:**
- Use OS wake timers for scheduled runs where available.
- Cannot wake a fully powered-off machine.
- On resume, detect missed jobs and run catch-up policy (task T-18).

---

## 8) Reliability and Safety Controls

1. **Crawl throttling** — randomized delays, per-source/per-account limits, circuit breaker on repeated failures (tasks T-7, T-8).
2. **Session handling** — detect login redirects / challenges, mark `sources.follow_state = 'needs_reauth'`, continue other sources (task T-1).
3. **Power-state awareness** — heavy jobs run only when awake and preferably charging. Avoid wake loops on low battery.
4. **Idempotency** — run IDs for ingestion; dedupe keys for digest delivery (task T-5).
5. **Degradation policy** — partial digest allowed if some sources fail; explicit status note when confidence is reduced.
6. **Budget controls** — caps on deep-analysis item count; provider timeout and failover thresholds (part of task T-16).

---

## 9) Security and Privacy

- Data stays local by default.
- Session profiles stored under `data_dir/browser_profiles/`. Sensitive config encrypted at rest (planned — not in v1 scope).
- Outbound data to model providers only when the optional premium lane is enabled.
- Export / delete controls for user data (CLI-level today).

---

## 10) Compliance and Risk Notes

- Browser automation against social platforms may conflict with TOS and is subject to countermeasures.
- v1 is designed for personal local use. It is not guaranteed for large-scale or commercial operation.
- Any public distribution must include clear user disclosures about platform dependency and account risk.

---

## 11) Out of Scope (v1)

- Multi-user cloud backend
- Full mobile app
- Audio transcription fallback (Whisper) when captions are unavailable
- Autonomous account-follow/unfollow actions on social platforms
- Real-time push alerts — digest-first model
- Web dashboard

---

## 12) Source Discovery

**Goal:** discover valuable new accounts / blogs / channels the user is not tracking yet.

**Passive discovery:** observe repeated references from trusted sources (mentions, reposts, citations). Track co-occurrence across platforms.

**Active discovery:** weekly query expansion from subject keywords + trusted entities. Evaluate candidates for novelty, practical signal likelihood, and trust.

**User flow:**
1. Bot sends "Suggested sources" per subject once per week.
2. User replies free-form: "Add 1 and 3", "Ignore this source type", "Only suggest practitioners".
3. App updates source lists and discovery preferences.

**Module interaction:**
`DiscoveryService` → `CurationEngine.rank_suggestions` → `TelegramService.send` → `FeedbackParser` → `SourceRegistry`.

Full implementation is post-v1.

---

## 13) Extension Points

This section defines where new code should attach. Anything outside these seams should be discussed first.

### Add a new source platform
Five coordinated changes — all required, in this order:
1. Implement a collector under `src/pcca/collectors/` matching the `Collector` Protocol. Register it in `app.py::PCCAApp.start.collectors`.
2. Add source normalization in `SourceDiscoveryService`.
3. Add the platform tag to `intent_parser.PLATFORMS`.
4. If follow-import is feasible, extend `FollowImportService.supported_platforms()` and add the scraping method.
5. Update `/help`, `/setup`, and `README.md` copy.

Missing any step produces a silent platform mismatch (user can add a source but can't import follows, or vice versa).

### Add a new Telegram intent
1. Add the enum value in `models.py::IntentAction`.
2. Add the pattern and extraction logic in `intent_parser.py`.
3. Add the handler branch in `telegram_service.py::_handle_text_intent`.
4. Update `/help` copy.
5. Add unit tests in `tests/test_intent_parser.py`.

### Add a new scoring dimension
1. Extend `ScoredItem` in `pipeline/curation.py` and `item_scores` schema via a new migration in `db.py::MIGRATIONS`.
2. Update `final_score` weighting and `rationale_json` emission.
3. Expose the new dimension in `quality_rules_json` so subjects can tune its weight.

### Add a new background job
1. Add the coroutine to `JobRunner` in `scheduler.py`.
2. Register the trigger in `AgentScheduler.start` with a cron expression bound to a `Settings` field.
3. Add an env var default in `config.py` if operator-tunable.

### Replace the curation engine
`CurationEngine` is injected into `PipelineOrchestrator`. Swap by implementing the same `score()` signature. The Ollama reranker path (`ModelRouter.rerank`) is the sanctioned extension point for LLM-based scoring.

---

## 14) Technology Stack

| Concern | Choice |
|---|---|
| Language | Python 3.11+ |
| Async runtime | `asyncio` |
| DB | SQLite via `aiosqlite` |
| Scheduler | APScheduler (`AsyncIOScheduler`) |
| Browser | Playwright (Chromium, persistent context) |
| Telegram | `python-telegram-bot` v20+ |
| RSS | `feedparser` |
| YouTube transcripts | `youtube-transcript-api` |
| Local LLM | Ollama (optional) |
| HTTP | `httpx` |
| Tests | `pytest`, `pytest-asyncio` |

**Free-stack first.** All core paths run without paid services. Only the optional premium rerank lane makes outbound calls to paid APIs, and is off by default.
