# PCCA — Personal Content Curation Agent

Local-first agent that watches the sources you choose, scores everything against
the subjects you care about, and sends concise **Briefs** to Telegram.

[scenarios.md](./scenarios.md) is the product source of truth. This README is
how to run the current implementation today.

---

## Quick Start (≈ 5 minutes)

You need: macOS or Windows, Python 3.10+, a Telegram bot token from
[@BotFather](https://t.me/BotFather), and Google Chrome (or any Chromium-family
browser already logged in to the platforms you want to follow).

```bash
# 1. clone, isolate, install
git clone <this repo> pcca && cd pcca
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -e ".[dev]"
playwright install chromium

# 2. seed config
cp .env.example .env
# open .env, paste your Telegram bot token after PCCA_TELEGRAM_BOT_TOKEN=

# 3. launch the wizard
pcca run-desktop
```

The wizard handles everything else through four tabs:

1. **Config** — paste the Telegram bot token, set timezone and Brief time.
   Leaving the token field blank later preserves the saved token.
2. **Use** — start the local agent, send `/start` to your Telegram bot, then
   describe your first subject in free-form English. Thin one-liners become
   drafts; the wizard asks for more detail before saving.
3. **Sources** — choose a platform and click **Get Sources**. PCCA imports
   follows/subscriptions from your already logged-in normal browser session and
   asks for inline session repair only if needed.
4. **Sources** — prune the list if needed, click **Monitor Pending Sources**,
   then click **Get Content** to collect fresh items.
5. **Use** — click **Get Brief** next to a subject, or send `/briefs` in
   Telegram. Get Brief automatically rebuilds when new content or changed
   preferences require it.

That's it. Briefs arrive as separate Telegram messages with 👍 / 👎 / 🔖 / 🚫 /
📖 More buttons on each.

---

## What you do day to day

In Telegram, with the bot:

| You want to… | Do |
|---|---|
| Get today's Briefs | `/briefs` |
| React to a Brief | Tap 👍 / 👎 / 🔖 / 🚫 on the Brief message |
| Expand a Brief | Tap 📖 More |
| Give specific feedback | Reply to the Brief with text — *"less hype like this"*, *"no cursor content"*, etc. |
| Create another subject | Describe it in free form: *"I want a separate stream for Ukrainian Sole Proprietor regulations."* |
| Refine a subject | *"Refine Vibe Coding: include release notes; exclude motivation"* |
| List subjects/sources | *"List subjects"*, *"List sources for Vibe Coding"* |
| See setup checklist | `/setup` |

If a session expires (you logged out somewhere), PCCA marks the source as
`needs_reauth` and the wizard surfaces it. As long as you stay logged in to
the platform in your normal browser, PCCA auto-refreshes its cookies before
each scrape. Use **Get Sources** again to trigger inline session repair when
needed.

---

## When something goes wrong

- **Bot stopped responding.** Check `.env` for `PCCA_TELEGRAM_BOT_TOKEN`. If
  it's empty, paste the token back and restart the agent. Logs at
  `.pcca/logs/pcca.log` will say `Telegram service will be disabled` if the
  token is missing.
- **No items collected.** Run `/read_content`, then check the wizard's Logs
  tab. Sources flagged `needs_reauth` need session repair from the Sources tab.
- **Briefs feel stale after preference change.** Use `/briefs`; it now rebuilds
  automatically when preferences changed since the last delivered Brief.

For deeper debugging, run `pcca debug-bundle` — it writes a redacted zip with
logs and DB summaries (no raw cookies).

---

## Reference

### CLI commands

The wizard wraps these; you only need them for headless / debug use.

```bash
pcca run-desktop              # PyWebView wizard (default entry point)
pcca run-agent                # long-lived agent (Telegram bot + scheduler)
pcca run-nightly-once         # one-shot collection
pcca run-briefs-once          # one-shot Brief delivery
pcca rebuild-briefs-once      # force-recompute today's Briefs
pcca capture-session --platform x [--browser auto|chrome|arc|brave|edge]
pcca import-follows --subject "Subject Name" --platform x [--limit 150]
pcca debug-bundle             # redacted local support bundle
```

`pcca capture-session` and `pcca import-follows` accept any of these platforms:
`x`, `linkedin`, `youtube`, `spotify`, `substack`, `medium`, `apple_podcasts`.

### Configuration (`.env`)

```bash
# Telegram bot — required
PCCA_TELEGRAM_BOT_TOKEN=          # from @BotFather

# Scheduling
PCCA_TIMEZONE=UTC
PCCA_NIGHTLY_CRON=0 1 * * *       # nightly content collection
PCCA_MORNING_CRON=30 8 * * *      # only used when DIGEST_AUTO_SEND=true
PCCA_DIGEST_AUTO_SEND=false       # default off — Briefs are on-demand via /briefs

# Browser
PCCA_BROWSER_CHANNEL=chrome       # or 'bundled' for Playwright Chromium
PCCA_BROWSER_HEADFUL_PLATFORMS=x,linkedin

# Session refresh (auto re-read cookies before scrape)
PCCA_SESSION_REFRESH_ENABLED=true
PCCA_SESSION_REFRESH_COOLDOWN_SECONDS=1800
PCCA_SESSION_REFRESH_BROWSER=     # chrome|arc|brave|edge; empty = auto

# Optional local LLM rerank
PCCA_OLLAMA_ENABLED=false
PCCA_OLLAMA_MODEL=qwen2.5:7b
PCCA_OLLAMA_BASE_URL=http://localhost:11434

# Logging
PCCA_LOG_LEVEL=INFO               # DEBUG for verbose
PCCA_LOG_FILE=                    # default .pcca/logs/pcca.log; "off" to disable
```

### Where things live

```
.pcca/pcca.db                       SQLite database (subjects, items, scores, …)
.pcca/logs/pcca.log                 rotating app log
.pcca/browser_profiles/<platform>/  Playwright session profile per platform
.pcca/debug/browser/                screenshots + JSON breadcrumbs from failed scrapes
.pcca/debug/pcca-debug-*.zip        redacted support bundles from `pcca debug-bundle`
.env                                runtime configuration (NOT committed)
```

### Session capture details

PCCA does not drive logins for X / LinkedIn / Google / etc. Instead it reads
your real browser's session cookies and injects them into its own Playwright
profile. Cookie lifetimes vary:

| Platform | Lifetime |
|---|---|
| X (`auth_token`) | ~30 days, sliding while you keep using X |
| LinkedIn (`li_at`) | ~1 year |
| Spotify / Substack / Medium | long-lived (months+) |
| YouTube / Google (SID family) | rotates aggressively; auto-refresh handles it |
| Apple Podcasts | best-effort (varies by region/account state) |

Supported on macOS today: Chrome, Arc, Brave, Microsoft Edge. Safari and
Firefox tracked in [tasks.md](./tasks.md#refactoring-tasks) (T-38). Windows
Chromium tracked in T-37D.

Failed browser scrapes save a screenshot + JSON metadata under
`.pcca/debug/browser/`. Treat these as private debug artifacts — they may
contain logged-in page content. `pcca debug-bundle` redacts them on export.

### Multilingual scoring

Heuristic scoring is Cyrillic-aware (English / Ukrainian / Russian). The
optional Ollama rerank lane is language-agnostic — set `PCCA_OLLAMA_ENABLED=true`
and pull a multilingual model:

```bash
ollama pull qwen2.5:7b
```

### Linux

The PyWebView desktop wizard is intentionally not yet shipped on Linux
(tracked in T-35). On Linux, drive PCCA through the CLI commands above —
they work identically.

---

## Status / what's not yet done

Phase-1 foundation is in place: collectors for nine platforms, session capture
+ auto-refresh, conversational subject creation, per-Brief Telegram delivery,
PyWebView wizard. Known gaps and follow-up work live in
[tasks.md](./tasks.md). Notable: a pluggable Learning Strategy that reads
button reactions and refinement replies (T-17), full rich-rule preference
extraction including author-level conditionals (T-59), and Telegram as a
source platform (T-60–T-65) are open.
