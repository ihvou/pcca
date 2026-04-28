# PCCA

Personal Content Curation Agent (local-first).

[scenarios.md](./scenarios.md) is the product-level source of truth for user/stakeholder intent. This README describes the current implementation and how to run/test it locally.

## What Is Implemented

Phase-1 functional foundation is in place:
- SQLite schema + migrations
- subject creation/listing
- global monitored sources plus legacy per-subject source overrides
- source linking/removal and connected-account follow import
- subject preference refinement (include/exclude topics, versioned)
- conversational subject drafts in Telegram (`save subject` / corrections / cancel)
- feedback event logging from per-Brief buttons and replies
- detailed run/browser logging to `.pcca/logs/pcca.log` for local debugging
- nightly collection pipeline + shared-source collection + per-subject scoring + persistence
- top-K model rerank shortlisting
- swappable internal renderer + Telegram Brief delivery wiring
- auto-refresh of captured browser cookies before follow import and collection
- forced rebuild of today's Briefs from current scores
- browser-session login and follow import for X/LinkedIn/YouTube/Substack/Medium/Spotify/Apple Podcasts
- unified source flow for X/LinkedIn/YouTube/Substack/reddit/Spotify/Apple Podcasts/Medium
- collectors: X, LinkedIn, YouTube, Substack, Reddit, Spotify, Apple Podcasts, Medium, RSS
- free-form Telegram commands (+ on-demand run controls)
- PyWebView desktop onboarding wizard for non-terminal setup/control

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -e ".[dev]"
playwright install chromium
cp .env.example .env
```

Install Google Chrome if it is not already installed, or set `PCCA_BROWSER_CHANNEL=bundled` to use Playwright Chromium for PCCA's own scraping profile. PCCA can capture X sessions from Chrome/Arc/Brave/Edge browser stores, then inject cookies into its local Playwright profile; it should not drive X/Google/LinkedIn login flows itself.

Launch the desktop wizard. It initializes the local DB and starts the local agent automatically:

```bash
pcca run-desktop
```

Use the desktop wizard to:
- save timezone, Brief time, and Telegram bot token
- capture sessions from your normal browser
- let PCCA auto-refresh captured sessions before future imports and reads
- stage follows/subscriptions for review
- confirm which staged sources should be monitored
- create the first subject separately from source monitoring
- trigger read/Brief runs

CLI one-shot jobs are available for developer/debug use:

```bash
pcca run-nightly-once
pcca run-briefs-once
pcca rebuild-briefs-once
```

Run the long-lived agent:

```bash
pcca run-agent
```

Run desktop wizard (UI):

```bash
pcca run-desktop
# or
pcca-desktop
```

## Current Implementation Test Flow

These steps are for testing what the current code can do today. They are not the source of truth for future product behavior; use [scenarios.md](./scenarios.md) for that.

1. Launch desktop onboarding:

```bash
pcca run-desktop
```

2. In the desktop wizard:
- local storage initializes automatically
- the local agent starts automatically and stops when the wizard closes
- paste your Telegram bot token and save runtime settings
- saving/changing the token restarts the agent so Telegram is picked up

3. In Telegram:
- open your bot chat and send `/start`
- optionally send `/setup` to see the guided checklist

4. Back in the desktop wizard:
- capture sessions from the browser where you are already logged in
- after first capture, PCCA re-syncs fresh cookies automatically before import/read runs
- stage follows/subscriptions
- review staged sources
- confirm which sources should be monitored
- create the first subject with include/exclude/high-quality notes
- click `Smoke Crawl + Test Briefs`

5. In Telegram:
- confirm the Brief messages arrive
- use the per-Brief buttons or reply to a Brief message with free-form feedback

## Current First-Run Test Walkthrough

Goal: test the current implementation's first-run setup flow, including connected-account follow import and first Brief delivery.
This walkthrough is a dogfood checklist, not a future-version product spec.

1. Install and launch

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -e ".[dev]"
playwright install chromium
cp .env.example .env
```

2. Launch the desktop onboarding wizard

```bash
pcca run-desktop
```

3. Complete the desktop wizard
- Local storage and the agent are initialized automatically when the wizard opens.
- Set timezone and Brief time.
- Paste your individual Telegram bot token and click `Save Runtime Settings`.
- If the agent was already running, saving settings restarts it with the new token.
- Open your bot chat in Telegram and send `/start`.

4. Connect account sessions
- In the desktop wizard, choose a platform.
- Log into that platform in your normal browser first.
- Leave browser capture on `Auto`, or choose the exact browser where you are logged in.
- Click `Capture Session`.
- Repeat for each platform you want included in the smoke test.
- After first capture, normal `Stage Follows`, `Read Content`, and nightly runs re-read fresh cookies from your normal browser automatically on a cooldown.

5. Stage and review follows/subscriptions
- For each connected platform, click `Stage Follows`.
- Click `Refresh Sources` if the review list does not update immediately.
- Remove obvious noise with the `Remove` buttons if needed.
- Click `Monitor These Sources`. Sources are monitored globally and will feed every subject through that subject's own preferences.

6. Create the first subject
- Enter the subject name, include terms, exclude terms, and high-quality examples.
- Click `Create Subject`.

7. Trigger first read + first Brief delivery immediately
- Click `Smoke Crawl + Test Briefs`.
- Or use Telegram:
  - `/read_content`
  - `/briefs`
- If you added sources after already sending today's Briefs, use `Rebuild Today's Briefs` in the wizard or `/rebuild_briefs` in Telegram.

8. Validate current implementation behavior
- You can list subjects and sources in Telegram:
  - `List subjects`
  - `List sources for Agentic PM`
- You receive one Telegram message per Brief, each with action buttons.

## Session Capture + Follow Import

PCCA does not ask you to log into social platforms through an automated browser. Instead:
- log into the platform in your normal browser
- run capture from the desktop wizard or CLI
- PCCA injects the captured cookies into `.pcca/browser_profiles/<platform>`
- follow import and scraping use that local PCCA profile afterward

Current capture support:
- X, LinkedIn, YouTube, Spotify, Substack, Medium, and Apple Podcasts from Chrome / Arc / Brave / Edge on macOS Chromium cookie stores
- Apple Podcasts capture is best-effort because Apple web auth cookies vary more by region/account state than the other platforms
- cookies are copied into the PCCA browser profile; raw cookie values are not printed or stored in the PCCA SQLite DB
- Safari / Firefox are tracked in T-38; Windows Chromium cookie stores are tracked in T-37D

```bash
pcca capture-session --platform x
pcca capture-session --platform linkedin
pcca capture-session --platform youtube
pcca capture-session --platform spotify
pcca capture-session --platform substack
pcca capture-session --platform medium
pcca capture-session --platform apple_podcasts

# Arc/Brave/Edge are also supported on macOS Chromium cookie stores:
pcca capture-session --platform x --browser arc

pcca import-follows --subject "Vibe Coding" --platform x --limit 150
pcca import-follows --subject "Vibe Coding" --platform linkedin --limit 150
pcca import-follows --subject "Vibe Coding" --platform youtube --limit 150
pcca import-follows --subject "Vibe Coding" --platform substack --limit 150
pcca import-follows --subject "Vibe Coding" --platform medium --limit 150
pcca import-follows --subject "Vibe Coding" --platform spotify --limit 150
pcca import-follows --subject "Vibe Coding" --platform apple_podcasts --limit 150
```

For local debugging, set detailed logs in `.env`:

```bash
PCCA_LOG_LEVEL=DEBUG
PCCA_BROWSER_HEADFUL_PLATFORMS=x,linkedin
```

Logs are written to `.pcca/logs/pcca.log` by default. Set `PCCA_LOG_FILE=/path/to/pcca.log` to choose another file, or `PCCA_LOG_FILE=off` to disable file logging.

Failed browser extraction/import attempts save a local screenshot plus JSON breadcrumbs under `.pcca/debug/browser/`. These may include visible logged-in page content, so treat them as local private debugging artifacts.

Create a redacted support bundle:

```bash
pcca debug-bundle
```

The bundle includes redacted logs, DB summaries, and debug artifacts. It does not include raw browser profiles or raw cookie stores.

`PCCA_BROWSER_HEADFUL_PLATFORMS` keeps selected browser collectors visible even when the rest run headless.

## Telegram Commands / Actions

- `/setup` guided onboarding checklist
- `/read_content` manual on-demand collection run
- `/briefs` smart on-demand Brief delivery
- `/rebuild_briefs` delete today's existing internal batch rows and send a fresh composition
- free-form examples:
  - `Create subject: Agentic PM`
  - `I want practical AI-in-HR case studies, no hype`
  - `save subject`
  - `Unsubscribe x:borischerny from Vibe Coding`
  - `Refine Vibe Coding: include release notes; exclude motivation`
  - `Show preferences for Vibe Coding`

### Brief Delivery: On Demand By Default

In v1 the daily Brief delivery is **on-demand only**: click `Get Briefs` in the
Telegram bot whenever you want today's Briefs. The nightly content
collection still runs on schedule (`PCCA_NIGHTLY_CRON`, default `0 1 * * *`)
so the DB has fresh items in the morning. To re-enable the auto-send
morning cron, set `PCCA_DIGEST_AUTO_SEND=true` in `.env` — the morning
cron will fire at `PCCA_MORNING_CRON` (default `30 8 * * *`).

`Read Content` is incremental and de-duplicated: each run accumulates
new items in `items`, refuses to overwrite existing rows whose
`content_hash` matches, and respects `last_crawled_at` per source.
Running it multiple times is safe.

### Session lifetime

`Capture Session` reads cookies from your real browser and injects them
into PCCA's Playwright profile. Cookie lifetimes vary by platform:
- **X (`auth_token`)** — ~30 days, sliding window if you keep using X.
- **LinkedIn (`li_at`)** — ~1 year.
- **Spotify / Substack / Medium** — long-lived.
- **YouTube / Google (SID family)** — rotates aggressively; can require
  periodic refresh. As long as you stay logged into Google in your normal
  browser, PCCA auto-refreshes its Playwright profile before follow import
  and collection so manual re-capture should be a one-time setup/repair action.

Session refresh is enabled by default:

```bash
PCCA_SESSION_REFRESH_ENABLED=true
PCCA_SESSION_REFRESH_COOLDOWN_SECONDS=1800
# Optional: chrome, arc, brave, or edge. Empty means auto-detect.
PCCA_SESSION_REFRESH_BROWSER=
```

When PCCA detects a logged-out scrape (401 / login redirect), it marks
the source `follow_state='needs_reauth'` and the wizard surfaces it
under `Sources needing re-auth`.

## Multilingual Analytics (EN/UK/RU)

Heuristic scoring includes Cyrillic-aware tokenization and practical/noise terms.

Optional local-model rerank lane:

```bash
# .env
PCCA_OLLAMA_ENABLED=true
PCCA_OLLAMA_MODEL=qwen2.5:7b
PCCA_OLLAMA_BASE_URL=http://localhost:11434
```

If Ollama rerank is enabled, make sure the model exists locally.

## Notes

- X and LinkedIn collection require logged-in browser sessions.
- YouTube collector fetches list pages and tries transcript extraction.
- Voice note handler exists, but transcription backend is still placeholder.
- `pcca run-desktop` uses PyWebView + a token-protected localhost web UI on macOS/Windows.
- Linux desktop UI is intentionally deferred to T-35; use the CLI commands directly there for now.
