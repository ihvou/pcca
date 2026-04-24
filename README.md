# PCCA

Personal Content Curation Agent (local-first), based on `content-curation-agent-spec-v2.md`.

## What Is Implemented

Phase-1 functional foundation is in place:
- SQLite schema + migrations
- subject creation/listing
- source linking and URL-based source discovery
- nightly collection pipeline + scoring + persistence
- morning digest assembly and Telegram delivery wiring
- browser-session login and follow import for X/LinkedIn/YouTube
- collectors: X, LinkedIn, YouTube, Reddit, RSS
- free-form Telegram commands (+ voice-note hook placeholder)

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

## Can I Test It Now?

Yes. Fastest smoke test (no Telegram needed):

```bash
pcca init-db
pcca create-subject --name "Agentic PM"
pcca add-source-url --subject "Agentic PM" --url "https://newsletter.substack.com"
pcca run-nightly-once
```

Then verify:

```bash
pcca list-sources --subject "Agentic PM"
```

If you want Telegram delivery testing:

```bash
# in .env
PCCA_TELEGRAM_BOT_TOKEN=<your_bot_token>

# then
pcca run-agent
```

## Browser Login + Follow Import

```bash
pcca login --platform x
pcca login --platform linkedin
pcca login --platform youtube

pcca import-follows --subject "Vibe Coding" --platform x --limit 150
pcca import-follows --subject "Vibe Coding" --platform linkedin --limit 150
pcca import-follows --subject "Vibe Coding" --platform youtube --limit 150
```

## URL Discovery Coverage

`add-source-url` currently supports:
- Substack publication URLs -> RSS `/feed`
- Medium URLs -> Medium feed mapping
- Apple Podcasts URLs -> feed URL via iTunes lookup
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
