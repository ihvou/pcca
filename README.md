# PCCA

Personal Content Curation Agent (local-first), based on `architecture.md`, `scenarios.md`, and `tasks.md`.

## What Is Implemented

Phase-1 functional foundation is in place:
- SQLite schema + migrations
- subject creation/listing
- source linking/removal and connected-account follow import
- subject preference refinement (include/exclude topics, versioned)
- feedback event logging from digest buttons
- detailed run/browser logging to `.pcca/logs/pcca.log` for local debugging
- nightly collection pipeline + scoring + persistence
- morning digest assembly and Telegram delivery wiring
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

Initialize DB and launch the desktop wizard:

```bash
pcca init-db
pcca run-desktop
```

Use the desktop wizard to:
- save timezone, digest time, and Telegram bot token
- start the local agent
- capture sessions from your normal browser
- stage follows/subscriptions for review
- create the first subject from staged sources
- trigger read/digest runs

CLI one-shot jobs are available for developer/debug use:

```bash
pcca run-nightly-once
pcca run-digest-once
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

## Real Scenario Testing (User Flow)

1. Launch desktop onboarding:

```bash
pcca run-desktop
```

2. In the desktop wizard:
- paste your Telegram bot token and save runtime settings
- click `Init DB`
- click `Start Agent`

3. In Telegram:
- open your bot chat and send `/start`
- optionally send `/setup` to see the guided checklist

4. Back in the desktop wizard:
- capture sessions from the browser where you are already logged in
- stage follows/subscriptions
- review staged sources
- create the first subject with include/exclude/high-quality notes
- click `Smoke Crawl + Test Digest`

5. In Telegram:
- confirm the digest arrives
- use `👍 / 👎 / 🔖` on digest messages

## Scenario 1 Walkthrough (Install / Launch / Initial Config)

Goal: complete first-run setup in one flow, including connected-account follow import and first digest.

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
- Set timezone and digest time.
- Paste your individual Telegram bot token and click `Save Runtime Settings`.
- Click `Init DB`, then `Start Agent`.
- Open your bot chat in Telegram and send `/start`.

4. Connect account sessions
- In the desktop wizard, choose a platform.
- Log into that platform in your normal browser first.
- Leave browser capture on `Auto`, or choose the exact browser where you are logged in.
- Click `Capture Session`.
- Repeat for each platform you want included in the smoke test.

5. Stage and review follows/subscriptions
- For each connected platform, click `Stage Follows`.
- Click `Refresh Sources` if the review list does not update immediately.
- Remove obvious noise with the `Remove` buttons if needed.

6. Create the first subject
- Enter the subject name, include terms, exclude terms, and high-quality examples.
- Click `Create Subject + Confirm Sources`.

7. Trigger first read + first digest immediately
- Click `Smoke Crawl + Test Digest`.
- Or use Telegram:
  - `/read_content`
  - `/get_digest`

8. Validate Scenario 1 success criteria
- You can list subjects and sources in Telegram:
  - `List subjects`
  - `List sources for Agentic PM`
- You receive digest in Telegram with action buttons.

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

`PCCA_BROWSER_HEADFUL_PLATFORMS` keeps selected browser collectors visible even when the rest run headless.

## Telegram Commands / Actions

- `/setup` guided onboarding checklist
- `/read_content` manual on-demand collection run
- `/get_digest` manual on-demand digest run
- free-form examples:
  - `Create subject: Agentic PM`
  - `Unsubscribe x:borischerny from Vibe Coding`
  - `Refine Vibe Coding: include release notes; exclude motivation`
  - `Show preferences for Vibe Coding`

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
