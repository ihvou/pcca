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
- start the agent
- open browser login flows
- import follows/subscriptions into a subject
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

1. Start agent:

```bash
# in .env first
PCCA_TELEGRAM_BOT_TOKEN=<your_bot_token>
pcca run-agent
```

2. In Telegram create the subject:
- `/setup`
- `Create subject: Agentic PM`
- `Refine Agentic PM: include claude code, releases; exclude biography`
- `Show preferences for Agentic PM`

3. In the desktop shell:
- open login flow for X, LinkedIn, YouTube, or another platform you want to test
- complete login in the browser window
- import follows/subscriptions into `Agentic PM`

4. Back in Telegram:
- `/read_content` (same as nightly read now)
- `/get_digest` (same as scheduled digest now)
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

2. Initialize and start runtime

```bash
pcca init-db
pcca run-agent
```

3. Connect Telegram and create first subject
- Open your bot chat in Telegram and send:
  - `/start`
  - `Create subject: Agentic PM`

4. Connect account sessions in the desktop shell
- Open the `Actions` tab.
- Choose a platform (for example `x`, `linkedin`, or `youtube`).
- Click `Open Login Flow`.
- Complete login in the browser window.
- Repeat for each platform you want included in the smoke test.

5. Import follows/subscriptions from connected accounts
- In the `Actions` tab, enter subject `Agentic PM`.
- Choose the platform you just logged into.
- Click `Import Follows`.
- Repeat for each connected platform.

6. Review imported sources
- Open the `Sources` tab.
- List sources for `Agentic PM`.
- Remove obvious noise if needed.

7. Trigger first read + first digest immediately
- In Telegram:
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
