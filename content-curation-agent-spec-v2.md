# Personal Content Curation Agent - Full Spec v2

Status: Draft for implementation
Date: 2026-04-24
Scope: Single-user local desktop agent (Mac/Windows), Telegram as primary UI

## 0) Problem Statement and Competitive Goal

Native recommendation algorithms optimize for engagement and session time, not for practical value and personal signal quality. For users tracking niche technical topics, this creates three failures:
- high noise volume
- weak novelty filtering
- poor alignment to nuanced personal standards

This product exists to build a personal curation layer that competes directly with native feeds.

Primary competitive goal:
- outperform native platform recommendations for each subject by delivering fewer but higher-value items.

Competitive success criteria:
- user reports "glad I did not miss this" events at higher frequency than native feeds
- lower noise rate than native feeds for the same topic window
- better practical relevance (implementation details over generic commentary)

## 1) Product Summary

This system is a local content-curation agent that:
- Runs nightly on the user's machine.
- Opens logged-in browser sessions to collect new content from selected sources.
- Analyzes content with a local-first model pipeline (free-first), with optional premium reranking lane.
- Delivers concise morning digests to Telegram.
- Learns preferences over time, per subject/topic.

Core value:
- Surface high-signal, practical, timely content.
- Suppress generic, repetitive, and low-trust noise.
- Support nuanced filtering (segment-level relevance, not only topic-level relevance).
- Allow free-form text and voice feedback loops per subject.

## 2) Product Decisions Locked In

1. Social ingestion for v1:
- X: browser-only (no X API).
- LinkedIn: browser ingestion supported.
- Risk note: both platforms can change UI and policies; anti-automation measures are possible.

2. YouTube transcript policy for v1:
- Captions/transcripts only.
- Audio-to-text fallback (Whisper) is deferred to later version.

3. Model strategy:
- Local-first LLM path (Ollama) for bulk analysis.
- Optional premium reranking/summarization lane for top candidates only.
- Full fallback path keeps system operational if premium lane is unavailable.

4. Telegram UX:
- Multi-subject experience is implemented as separate subject threads.
- Each subject has its own digest and preference memory.

5. Interaction mode:
- User actions should work in free form by default (not command syntax dependent).
- Voice messages are supported for preference refinement and quick feedback.

6. Deployment:
- Local desktop app + local database.
- No cloud backend required for core functionality.

## 3) Main Scenarios (User + System Interactions)

This section is normative and drives implementation priority.

---

## Scenario 1: User Installation / Launch / Initial Configuration

### Goal
User installs app, connects Telegram, logs into sources once, imports follows/subscriptions, and receives first digest.

### User Journey
1. User installs desktop app and launches it.
2. App opens first-run wizard.
3. User sets timezone and digest send window (default: 08:30 local time).
4. User connects Telegram bot.
5. User logs in to each source via embedded/automated browser windows (X, LinkedIn, YouTube if used).
6. App reads follows/subscriptions from connected accounts where available.
7. User reviews imported sources, removes obvious noise, and confirms source list.
8. User creates first subject (example: "Vibe Coding").
9. App runs a smoke crawl and sends a test digest.

### Module Interaction (Under the Hood)
1. `Desktop Shell` starts `Agent Core`.
2. `Config Service` creates local config and encryption key material.
3. `Storage Service` initializes SQLite schema.
4. `Telegram Service` performs bot handshake and stores `chat_id` and subject-thread mapping.
5. `Browser Session Manager` creates persistent Playwright profiles per platform.
6. User login completes in each platform profile; session state is persisted.
7. `Follow Import Service` pulls follow/channel/subscription lists from connected accounts when feasible.
8. `Source Normalizer` maps imported follows into canonical source entities.
9. `Subject Service` creates default subject profile and rule set.
10. `Scheduler` registers nightly crawl + morning digest jobs.
11. `Pipeline Orchestrator` runs dry-run crawl for sanity check.
12. `Digest Composer` generates test digest; `Telegram Service` sends it.

### Acceptance Criteria
- First-run wizard completes in under 10 minutes.
- At least one source returns content in smoke crawl.
- Test digest delivered to Telegram successfully.

### Failure Handling
- If login challenge occurs: mark source `NEEDS_REAUTH`, continue setup.
- If Telegram handshake fails: setup pauses with actionable retry.
- If follow import is limited/unavailable on a platform: fallback to manual source add flow.

---

## Scenario 2: User Starts New Subject and Sets Preferences

### Goal
User creates a new subject-specific curation channel with independent preferences and refinement loop.

### User Journey
1. User sends a free-form Telegram message such as:
- "I want a new subject for Agentic PM."
- "Track AI coding workflows in a separate thread."
- "Create a topic for Ukraine OSINT."
2. Bot asks 3 quick onboarding prompts:
- What should be included?
- What should be excluded?
- What examples are "high quality" for this subject?
3. User answers in natural language.
4. User can answer by text or voice message.
5. Bot confirms created subject and starts separate digest stream for it.

### Multi-Subject UX Model
- Each subject is represented as its own Telegram thread context.
- Subject context contains:
- Topic intent
- Inclusion/exclusion rules
- Source priorities
- Quality bar
- Feedback history

### Module Interaction (Under the Hood)
1. `Telegram Service` receives user intent.
2. If input is voice, `Voice Transcription Service` converts voice note to text.
3. `Intent Parser` classifies action: `CREATE_SUBJECT`.
4. `Subject Service` creates `subject_id`.
5. `Preference Engine` seeds initial profile from user prompts.
6. `Routing Registry` maps Telegram thread -> `subject_id`.
7. `Scheduler` includes new subject in nightly run.
8. `Digest Composer` sets per-subject max items and formatting style.
9. Bot confirms setup and expected first digest time.

### Acceptance Criteria
- Subject creation is fully conversational.
- New subject receives independent digest.
- Preferences in one subject do not affect others by default.

### Failure Handling
- If subject name duplicates existing one: bot asks to merge or keep separate.
- If preference prompt incomplete: create subject with defaults and mark profile confidence low.

---

## Scenario 3: System Collects, Processes, and Stores Content Nightly

### Goal
Run a reliable nightly pipeline from crawl to ranked candidate storage.

### Nightly Job Window
- Default: 01:00-05:00 local time.

### Pipeline Steps
1. `Scheduler` triggers `Pipeline Orchestrator`.
2. For each subject, `Source Planner` builds crawl plan from followed accounts/channels/blogs.
3. `Browser Collector` opens platform profiles one by one and captures new items.
4. `Normalizer` converts raw platform items into canonical `Item` schema.
5. `Deduper` removes duplicates by canonical URL hash + near-text fingerprint.
6. `Segmenter` splits long items/transcripts into analyzable chunks.
7. `Pass-1 Analyzer` (local model) scores relevance/noise/novelty cheaply.
8. `Candidate Selector` keeps top-N for deep analysis.
9. `Pass-2 Analyzer`:
- Default: local deep analysis.
- Optional premium lane for final reranking/summarization on top-K only.
10. `Ranker` computes final score with weighted features.
11. `Store` writes:
- raw ingestion artifacts
- normalized items
- per-subject scores
- rationale snippets
- run diagnostics
12. `Run Reporter` posts internal status summary (success/partial/fail).

### Module Interaction (Under the Hood)
1. `Scheduler -> Orchestrator`
2. `Orchestrator -> Session Manager` (ensure sessions valid)
3. `Orchestrator -> Collectors` (X, LinkedIn, YouTube, Reddit, RSS)
4. `Collectors -> Storage (raw_items)`
5. `Storage -> Normalizer -> Deduper -> Segmenter`
6. `Segmenter -> Model Router (pass-1)`
7. `Model Router -> Ollama Provider`
8. `Candidate Selector -> Model Router (pass-2)`
9. `Model Router -> [Ollama or Optional Premium Provider]`
10. `Scores -> Ranker -> Storage (curated_candidates)`
11. `Orchestrator -> Telemetry/Reporter`

### Quality Logic (Important)
- Relevance is not enough; each candidate also needs:
- Practicality score: concrete tactics, tools, implementation details.
- Novelty score: likely new to user, not repeated boilerplate.
- Trustworthiness score: source trust + corroboration signals.
- Noise penalty: hype, generic advice, biography chatter, promotional fluff.

### Acceptance Criteria
- Pipeline completes within nightly window for configured source volume.
- Per-subject candidate list exists by morning cutoff.
- Partial failures in one source do not cancel other sources.

### Failure Handling
- Session expired: source marked `NEEDS_REAUTH`, continue others.
- DOM extraction failed: fallback parser and retry with capped attempts.
- Model unavailable: route to fallback provider; if none, use pass-1 shortlist only.

---

## Scenario 4: User Receives Morning Content Updates

### Goal
Deliver compact, actionable digest per subject in Telegram.

### User Journey
1. At configured time, user receives digest in each active subject thread.
2. Each item includes:
- title/link/source
- concise "why this matters for this subject"
- optional segment references (for long-form/video)
- feedback buttons: thumbs up / thumbs down / save
3. On zero-gem days, user gets a short "no high-signal items today" note.

### Module Interaction (Under the Hood)
1. `Scheduler` triggers `Digest Job`.
2. `Digest Composer` fetches top items per subject from `curated_candidates`.
3. `Explanation Builder` formats personalized rationale.
4. `Telegram Service` sends message in mapped subject thread.
5. `Interaction Logger` records delivery status and message IDs for feedback mapping.

### Digest Constraints
- Max read time target: under 3 minutes per subject.
- Hard cap on number of items (configurable, default 3-5).
- Priority to practical and novel items over topical but generic items.

### Acceptance Criteria
- Digest delivered at configured time +/- 10 minutes.
- Every delivered item has an explicit reason-selected field.
- Delivery is idempotent (no duplicate sends for same run).

---

## Scenario 5: User Refines Preferences Per Subject

### Goal
User can continuously refine curation behavior with low friction.

### User Journey
1. User reacts to an item (up/down/save) or sends natural-language feedback.
2. User can send feedback as text or voice message.
3. Bot confirms interpreted change.
4. Preferences update immediately for that subject only.
5. Future digests reflect the refinement.

### Example Feedback
- "More practical Claude Code release details, less commentary."
- "Ignore Skills hype unless there is a new official capability."
- "Only include long podcasts if at least one high-value segment exists."

### Module Interaction (Under the Hood)
1. `Telegram Service` receives reaction/message.
2. If input is voice, `Voice Transcription Service` converts voice note to text.
3. `Feedback Parser` extracts structured intent:
- target subject
- inclusion/exclusion signals
- quality constraints
- source trust adjustments
4. `Preference Engine` updates subject profile version.
5. `Learning Service` updates feature weights from explicit feedback.
6. `Audit Log` stores before/after diff for explainability.
7. `Bot` returns concise confirmation.

### Acceptance Criteria
- Feedback affects only current subject unless user explicitly says cross-subject.
- Preference changes are versioned and reversible.
- Negative feedback reduces recurrence of similar items within 3 digest cycles.

---

## 4) System Modules and Responsibilities

## 4.1 Desktop Shell
- Installer, tray/menu control, first-run wizard, logs viewer.

## 4.2 Agent Core
- Main process runtime, lifecycle management, dependency wiring.

## 4.3 Scheduler
- Nightly crawl jobs, morning digest jobs, weekly reflection jobs.
- Discovery jobs.
- Wake-aware job orchestration and catch-up runs after resume.

## 4.4 Browser Session Manager
- Playwright persistent profiles.
- Session health checks and reauth flags.

## 4.5 Source Collectors
- X collector (browser).
- LinkedIn collector (browser).
- YouTube collector (channel pages + transcript retrieval where available).
- Reddit collector (subreddits and selected user feeds).
- RSS collector (blogs, podcasts, Substack feeds).

## 4.6 Extraction Layer
- Deterministic DOM extractors first.
- Structured fallback extraction.
- Canonical item normalization.

## 4.7 Model Router
- Routes tasks to local model by default.
- Optional premium lane for selected high-value tasks.
- Enforces cost/time caps and fallback behavior.

## 4.8 Curation Engine
- Pass-1 filter/scoring.
- Pass-2 deep analysis.
- Final ranking and confidence scores.

## 4.9 Subject Service
- Subject creation, lifecycle, routing, and isolation.

## 4.10 Preference Engine
- Subject-specific profile state.
- Rule updates from feedback.
- Versioning and rollback.

## 4.11 Telegram Service
- Digest delivery, button callbacks, conversational controls.

## 4.12 Storage Service
- SQLite access, migrations, repositories, event logs.

## 4.13 Observability
- Run reports, extraction success rate, model latency, digest delivery success.

## 4.14 Discovery Service
- Passive discovery from trusted-source interactions (mentions/reposts/references).
- Active discovery from query expansion and community sources.
- Candidate source suggestions and confidence scores.

## 4.15 Voice Transcription Service
- Converts Telegram voice notes to text for intent and feedback parsing.
- Supports local-first transcription path with configurable provider fallback.

## 5) Data Model (Subject-Centric)

## 5.1 Core Tables

`subjects`
- id
- name
- telegram_thread_id
- status
- created_at

`subject_preferences`
- id
- subject_id
- version
- include_rules_json
- exclude_rules_json
- source_weights_json
- quality_rules_json
- updated_at

`sources`
- id
- platform
- account_or_channel_id
- display_name
- follow_state
- last_crawled_at

`discovered_sources`
- id
- platform
- account_or_channel_id
- display_name
- discovery_type
- evidence_json
- confidence_score
- status
- created_at

`subject_sources`
- subject_id
- source_id
- priority
- status

`items`
- id
- platform
- external_id
- canonical_url
- author
- published_at
- raw_text
- transcript_text
- metadata_json

`item_segments`
- id
- item_id
- start_offset
- end_offset
- segment_text
- segment_type

`item_scores`
- id
- item_id
- subject_id
- pass1_score
- pass2_score
- practicality_score
- novelty_score
- trust_score
- noise_penalty
- final_score
- rationale_json

`digests`
- id
- subject_id
- run_date
- sent_at
- status

`digest_items`
- digest_id
- item_id
- rank
- reason_selected

`feedback_events`
- id
- subject_id
- item_id
- feedback_type
- comment_text
- created_at

`run_logs`
- id
- run_type
- started_at
- ended_at
- status
- stats_json

## 5.2 Design Rules
- Subject isolation is mandatory at preference layer.
- Item storage is global; scoring is per subject.
- All preference edits are versioned.

## 6) Analysis Logic (Practicality-First)

## 6.1 Pass-1 (Cheap Screening)
- Inputs: titles, snippets, first transcript chunks.
- Outputs:
- relevance estimate
- novelty estimate
- likely-noise flag
- keep/drop decision

## 6.2 Pass-2 (Deep Curation)
- Inputs: full text/transcript segments for shortlisted items.
- Outputs:
- actionable insights extracted
- practical-value score
- trust assessment
- subject-specific explanation

## 6.3 Segment-Level Filtering
- For long-form content:
- analyze by segment/chunk
- include only chunks with practical signal
- ignore low-value sections (bio, generic commentary)

## 6.4 Ranking Formula (Configurable)
`final_score = w1*relevance + w2*practicality + w3*novelty + w4*trust - w5*noise`

## 7) Scheduling

Default schedule per day:
- 01:00 start crawl
- 01:00-03:00 collection + normalization
- 03:00-04:00 pass-1 + pass-2
- 04:00 finalize candidates
- 08:30 send digest

Weekly:
- Preference reflection summary
- Source health summary
- Discovery suggestion run

Wake behavior:
- App attempts to use OS-supported wake timers for scheduled runs.
- App cannot wake a fully powered-off machine.
- If run is missed due to sleep/offline, execute catch-up run on next resume.

## 8) Reliability and Safety Controls

1. Crawl throttling
- randomized delays
- per-source/per-account limits
- single-source circuit breaker on repeated failures

2. Session handling
- detect login redirects/challenges
- mark `NEEDS_REAUTH` and continue others

3. Power-state awareness
- run heavy jobs only when machine is awake and preferably charging
- on wake/resume, evaluate missed jobs and run catch-up policy
- avoid repeated wake loops when battery is low

4. Idempotency
- run IDs and dedupe keys for ingestion and delivery

5. Degradation policy
- partial digest allowed if some sources fail
- explicit status note when confidence reduced

6. Budget controls
- caps on deep-analysis item count
- provider timeout and failover thresholds

## 9) Security and Privacy

- Data stays local by default.
- Session profiles stored locally; sensitive config encrypted at rest.
- Minimal outbound data to model providers (only when optional premium lane enabled).
- Export/delete controls for user data.

## 10) Compliance and Risk Notes

- Browser automation against social platforms may face terms/policy constraints and technical countermeasures.
- This v1 is designed for personal local use, not guaranteed for large-scale commercial operation.
- Any public distribution should include clear user disclosures about platform dependency and account risk.

## 11) Out of Scope (v1)

- Multi-user cloud backend.
- Full mobile app.
- Audio transcription fallback (Whisper) when captions are unavailable.
- Autonomous account-follow/unfollow actions on social platforms.
- Real-time push alerts (digest-first model).

## 12) Source Discovery Feature

Goal:
- discover valuable new accounts/blogs/channels that user is not tracking yet.

Passive discovery:
- observe repeated references from trusted sources
- track mention frequency, co-occurrence patterns, and cross-platform citations

Active discovery:
- run weekly query expansions from subject keywords and trusted entities
- evaluate candidates for novelty, practical signal likelihood, and source trust

User flow:
1. Bot sends "Suggested sources" per subject once per week.
2. User replies in free form:
- "Add 1 and 3"
- "Ignore this source type"
- "Only suggest practitioners"
3. App updates source lists and discovery preferences accordingly.

Module interaction:
1. `Discovery Service` creates candidate sources.
2. `Curation Engine` ranks suggestions for each subject.
3. `Telegram Service` delivers suggestions.
4. `Feedback Parser` interprets user actions.
5. `Source Registry` applies approved changes.

## 13) MVP Acceptance Criteria

1. Setup and first digest
- New user completes setup and gets first digest same day.

2. Subject isolation
- At least 2 subjects can run with independent preferences and outputs.

3. Quality improvement loop
- Negative feedback measurably reduces similar low-value items within one week.

4. Stability
- Nightly runs succeed (full or partial) on at least 80% of days over two weeks.

5. User value
- User rates at least one item per week as "glad I did not miss this."

## 14) Suggested Implementation Phases

Phase 1:
- Core app shell, SQLite schema, Telegram integration, one subject flow.

Phase 2:
- Browser session manager + X/LinkedIn collectors + basic extractors.

Phase 3:
- Curation pipeline pass-1/pass-2 with local model router.

Phase 4:
- Multi-subject threads, preference engine versioning, weekly reflection.

Phase 5:
- Optional premium reranking lane + advanced reliability tuning.

---

This spec is implementation-ready for a local MVP and aligned with constraints discussed so far:
- browser-first social ingestion
- free-first tooling
- local-first analysis
- nuanced per-subject preference refinement
