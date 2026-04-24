# PCCA

Personal Content Curation Agent (local-first), based on `content-curation-agent-spec-v2.md`.

## What Is Implemented

Phase-1 functional foundation is in place:
- SQLite schema + migrations
- subject creation/listing
- source linking/removal and URL-based source discovery
- subject preference refinement (include/exclude topics, versioned)
- feedback event logging from digest buttons
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

Initialize DB and create your first subject:

```bash
pcca init-db
pcca create-subject --name "Vibe Coding"
```

Add sources:

```bash
# direct platform source
pcca add-source --subject "Vibe Coding" --platform x --source-id borischerny
pcca add-source --subject "Vibe Coding" --platform youtube --source-id UCxxxx
pcca add-source --subject "Vibe Coding" --platform spotify --source-id https://open.spotify.com/show/2MAi0BvDc6GTFvKFPXnkCL
pcca add-source --subject "Vibe Coding" --platform apple_podcasts --source-id https://podcasts.apple.com/us/podcast/example/id123456789

# URL discovery/import
pcca add-source-url --subject "Vibe Coding" --url "https://newsletter.substack.com"
pcca add-source-url --subject "Vibe Coding" --url "https://medium.com/@openai"
```

Run one-shot jobs:

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

2. In Telegram test real usage:
- `/setup`
- `Create subject: Agentic PM`
- `Create subject: Vibe Coding`
- `Add source https://newsletter.substack.com to Agentic PM`
- `Add source x:borischerny to Vibe Coding`
- `Refine Agentic PM: include claude code, releases; exclude biography`
- `Show preferences for Agentic PM`
- `/read_content` (same as nightly read now)
- `/get_digest` (same as scheduled digest now)
- Use `👍 / 👎 / 🔖` on digest messages

3. Optional desktop-driven setup instead of terminal:
- run `pcca run-desktop`
- use tabs to init DB, create subjects, add sources, login/import follows, run on-demand actions

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

4. Connect account sessions (browser login once per platform)

```bash
pcca login --platform x
pcca login --platform linkedin
pcca login --platform youtube
pcca login --platform substack
pcca login --platform medium
pcca login --platform spotify
pcca login --platform apple_podcasts
```

5. Import follows/subscriptions from connected accounts

```bash
pcca import-follows --subject "Agentic PM" --platform x --limit 150
pcca import-follows --subject "Agentic PM" --platform linkedin --limit 150
pcca import-follows --subject "Agentic PM" --platform youtube --limit 150
pcca import-follows --subject "Agentic PM" --platform substack --limit 150
pcca import-follows --subject "Agentic PM" --platform medium --limit 150
pcca import-follows --subject "Agentic PM" --platform spotify --limit 150
pcca import-follows --subject "Agentic PM" --platform apple_podcasts --limit 150
```

6. Add extra sources by URL (optional but recommended)
- In Telegram (or CLI), add sources directly:
  - `Add source https://newsletter.substack.com to Agentic PM`
  - `Add source https://medium.com/@openai to Agentic PM`
  - `Add source https://open.spotify.com/show/<show_id> to Agentic PM`

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

## Telegram Commands / Actions

- `/setup` guided onboarding checklist
- `/read_content` manual on-demand collection run
- `/get_digest` manual on-demand digest run
- free-form examples:
  - `Create subject: Agentic PM`
  - `Add source https://newsletter.substack.com to Agentic PM`
  - `Unsubscribe x:borischerny from Vibe Coding`
  - `Refine Vibe Coding: include release notes; exclude motivation`
  - `Show preferences for Vibe Coding`

## URL Discovery Coverage

`add-source-url` currently supports:
- Substack publication URLs -> Substack source (feed-backed)
- Medium URLs -> Medium source (feed-backed)
- Apple Podcasts URLs -> Apple Podcasts source via iTunes feed lookup
- Spotify show URLs -> Spotify source
- Google Podcasts feed URLs -> decoded RSS feed URL
- any page exposing RSS/Atom `<link rel="alternate">`

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
