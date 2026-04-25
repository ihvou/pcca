# PCCA — Personal Content Curation Agent

Local-first content curation agent. Runs nightly on the user's machine, collects content from logged-in browser sessions + RSS + Reddit, scores it against per-subject preferences, and delivers a concise morning digest to Telegram. Conversation with the agent happens in Telegram (free-form text, voice planned).

Single-user, local deployment. SQLite for storage. Telegram as the only UI. Optional Ollama reranker. No cloud backend.

---

## Repository Map

Start with the document that matches your question:

- **[architecture.md](./architecture.md)** — system design, module responsibilities, data model, analysis logic, scheduling, reliability, security, extension points. Read first when changing how something works.
- **[scenarios.md](./scenarios.md)** — normative user + system interaction flows (install, new subject, nightly collection, morning digest, feedback loop). These drive acceptance criteria.
- **[tasks.md](./tasks.md)** — prioritized refactoring backlog (P0 → P3) with problem / solution / acceptance / impact per item. **Any non-trivial change should map to a task here; if it doesn't, add one before starting.**
- **[README.md](./README.md)** — user-facing quick start, CLI commands, Telegram usage examples.

---

## Status

Phase-1 foundation is in place:
- SQLite schema + migrations
- Subject, source, preference, feedback, routing services
- Collectors for X, LinkedIn, YouTube, Reddit, Spotify, Apple Podcasts, Substack, Medium, generic RSS
- Pipeline orchestrator + keyword curation engine + optional Ollama reranker
- Telegram bot with free-form intent parsing, digest delivery, inline feedback buttons
- APScheduler-based nightly/morning cron
- Browser session manager with persistent Chromium profiles

Known spec-compliance and reliability gaps are tracked in [tasks.md](./tasks.md). **Do not add new features while P0 items are open.**

---

## Core Conventions

- **Async everywhere.** All IO is `async`. Single-process `asyncio` loop hosts APScheduler, Telegram bot, and pipeline.
- **Layered**: `collectors → repositories → services → pipeline → scheduler → app`. Cross-layer calls go downward only. Nothing depends on `app.py`.
- **Collectors** implement the `Collector` Protocol in `collectors/base.py`. One method: `async collect_from_source(source_id: str) -> list[CollectedItem]`. Each collector normalizes its own source-id format.
- **Repositories** wrap a shared `aiosqlite.Connection`. All SQL is parameterized. Migrations are additive — add a new `(version, sql)` tuple to `MIGRATIONS` in `db.py`. Never edit an applied migration.
- **Services** are stateless — they hold repositories and other services only. State lives in the DB.
- **Browser sessions** are persistent Chromium profiles at `data_dir/browser_profiles/<platform>/`, managed by `BrowserSessionManager`. Login is headful (`pcca login`); scraping defaults to headless (see task T-13).
- **Free-form Telegram input** is parsed by `intent_parser.py` (regex patterns today; LLM fallback planned — see task T-19). No command syntax required from the user.

---

## Key Invariants

- `items` are globally unique by `(platform, external_id)` and shared across subjects.
- `item_scores` are per `(item_id, subject_id)` — scoring is subject-specific, storage is not.
- `sources.follow_state` ∈ `{active, needs_reauth, inactive}`. Collectors must respect it and update it on session challenge (see task T-1).
- `subject_preferences` are append-only and versioned. Never rewrite in place.
- Every pipeline run has a `run_logs.id`. Every digest should be idempotent per `(subject_id, run_date)` (see task T-5).
- Subject isolation is mandatory at the preference layer. Feedback on subject A must not affect subject B unless the user explicitly says so.

---

## Working on This Repo

**Adding a new collector** requires four coordinated changes, in this order:
1. Implement the collector under `src/pcca/collectors/` and register it in `app.py::PCCAApp.start`'s `collectors` dict.
2. Add source normalization and account-import rules where the platform supports them.
3. Add the `platform:id` alias to `intent_parser.PLATFORMS`.
4. If follow-import is feasible, extend `FollowImportService.supported_platforms()` and add the import method.
5. Update `/help` and `/setup` copy in `telegram_service.py` and the platform list in `README.md`.

Skipping any of these creates the silent platform-mismatch class of bug that already exists for `reddit` and `rss` — see task T-26.

**Adding a new Telegram command or intent** requires updates to:
- `intent_parser.py` (pattern + `IntentAction` enum + `models.py`)
- `telegram_service.py` (handler branch + `/help` copy)
- Tests under `tests/test_intent_parser.py`

**Schema changes** require a new migration tuple in `db.py::MIGRATIONS`. Tests in `test_db_bootstrap.py` must still pass on a fresh DB.

**Tests** live in `tests/`. Async tests use `pytest-asyncio`. Prefer integration tests against an on-disk tmpfile SQLite for repositories; mock only external IO (HTTP, Playwright page).

**Before a PR**: `pytest` from the repo root must pass without setting `PYTHONPATH`. Don't create commits unless the user explicitly asks.

---

## Scope

**In scope (v1):** Telegram UI, browser + RSS ingestion on the listed platforms, local heuristic curation + optional Ollama reranker, per-subject preference refinement, inline feedback.

**Out of scope (v1):** Whisper audio transcription fallback, autonomous follow/unfollow on social platforms, real-time push alerts, multi-user cloud backend, mobile app, web dashboard.

See [architecture.md §11](./architecture.md#11-out-of-scope-v1) for the authoritative list.
