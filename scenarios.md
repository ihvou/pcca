# PCCA — Scenarios

This document is **normative** and drives implementation priority. Each scenario describes a complete user + system interaction with goals, journey, module interactions, acceptance criteria, and failure handling.

Related documents:
- [architecture.md](./architecture.md) — system design
- [tasks.md](./tasks.md) — refactoring backlog
- [CLAUDE.md](./CLAUDE.md) — entry point

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
