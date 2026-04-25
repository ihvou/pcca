# PCCA

Personal Content Curation Agent (local-first), based on `architecture.md`, `scenarios.md`, and `tasks.md`.

## What Is Implemented

Phase-1 functional foundation is in place:
- SQLite schema + migrations
- subject creation/listing
- source linking/removal and connected-account follow import
- subject preference refinement (include/exclude topics, versioned)
- feedback event logging from digest buttons
- detailed run/browser logging for local debugging
- nightly collection pipeline + scoring + persistence
- morning digest assembly and Telegram delivery wiring
- browser-session login and follow import for X/LinkedIn/YouTube/Substack/Medium/Spotify/Apple Podcasts
- unified source flow for X/LinkedIn/YouTube/Substack/reddit/Spotify/Apple Podcasts/Medium
- collectors: X, LinkedIn, YouTube, Substack, Reddit, Spotify, Apple Podcasts, Medium, RSS
- free-form Telegram commands (+ on-demand run controls)
- minimal desktop shell for non-terminal onboarding/control

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -e ".[dev]"
playwright install chromium
cp .env.example .env
```

Initialize DB and launch the desktop shell:

```bash
pcca init-db
pcca run-desktop
```

Use the desktop shell to:
- save timezone, digest time, and Telegram bot token
- start the local agent
- open browser login windows
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

Run desktop shell (UI):

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

2. In the Onboarding tab:
- paste your Telegram bot token and save runtime settings
- click `Init DB`
- click `Start Agent`

3. In Telegram:
- open your bot chat and send `/start`
- optionally send `/setup` or `/onboard` to see the guided checklist

4. Back in the Onboarding tab:
- open login windows for the platforms you want to test
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
# set PCCA_TELEGRAM_BOT_TOKEN in .env
```

2. Launch the desktop onboarding shell

```bash
pcca run-desktop
```

3. Complete the Onboarding tab
- Set timezone and digest time.
- Paste your individual Telegram bot token and click `Save Runtime Settings`.
- Click `Init DB`, then `Start Agent`.
- Open your bot chat in Telegram and send `/start`.

4. Connect account sessions
- In the Onboarding tab, choose a platform.
- Click `Open Login Window`.
- Complete login in the browser window, then close that browser window.
- Repeat for each platform you want included in the smoke test.

5. Stage and review follows/subscriptions
- For each connected platform, click `Stage Follows`.
- Click `List Staged Sources`.
- Remove obvious noise by staged source id if needed.

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

## Browser Login + Follow Import

```bash
pcca login --platform x
pcca login --platform linkedin
pcca login --platform youtube
pcca login --platform substack
pcca login --platform medium
pcca login --platform spotify
pcca login --platform apple_podcasts

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
- `pcca run-desktop` requires `tkinter` support in your local Python installation.
