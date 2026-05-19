# LLM A/B test harnesses for PCCA's Pass-2 `summarize_batch`

Three standalone scripts that exercise the exact production prompt from
`pcca.services.model_router.summarize_batch` against multiple LLM providers
on a fixed set of candidates, then report:

- Schema compliance — did every item come back with required fields?
- Dual-output rate — for non-low-content items, are BOTH `brief_summary` and
  `detailed_summary` populated? (The bottom-line production failure for
  `llama3.1:8b` is that it drops one half ~80% of the time.)
- Subject-aware accuracy — does the model's accept/reject call match the
  honest pre-tag for each (subject, item)?
- Latency per batch.

## Scripts

| Script | Items | Subjects | Models | Purpose |
|---|---|---|---|---|
| `clean_set.py` | 8 mixed (clearly accept / clearly reject) | AI Tools & Tips, AI PM Success Stories | llama3.1:8b, qwen2.5:7b, gemini-2.5-flash, gpt-4o-mini | Sanity check — does each model produce well-formed output at all? |
| `prod_failures.py` | 8 items pulled from a real digest where `llama3.1:8b` produced partial-schema output | AI impact over IT jobs market, AI PM Success Stories | (same four) | The real test — does the new model fix the production failure modes? |
| `multilingual.py` | 4 Ukrainian-Cyrillic items (Portnikov / STERNENKO) | Ukraine War News | llama3.1:8b, gemini-2.5-flash | Verify the candidate model handles non-English content. |

## Setup

1. `cd <repo root>` then run any script directly via `python -m`:
   ```bash
   .venv/bin/python scripts/llm_ab_test/prod_failures.py
   ```
   (The scripts find `.env` relative to the repo root, so they work from any
   cwd — the `python -m` form is just convention.)

2. Required env vars in `<repo root>/.env`:
   - `PCCA_GEMINI_API_KEY` — get from https://aistudio.google.com/apikey (free tier ample for the test).
   - `PCCA_OPENAI_API_KEY` — needs paid credit on OpenAI; total test cost ~$0.01.
   - Local Ollama models must be pulled separately:
     ```bash
     ollama pull llama3.1:8b
     ollama pull qwen2.5:7b
     ```

3. If a key isn't set, the matching model is skipped and the rest still run.

## Output

Each script prints per-item side-by-side judgments plus an aggregate table at
the end. Sample (from `prod_failures.py`, 2026-05-19):

```
AGGREGATE (16 total judgments per model across 2 subjects)
  model              batches  schema-ok  avg-lat  subj-acc  BOTH%  reject%
  llama3.1:8b              2          2   137.6s       25%   100%       0%
  qwen2.5:7b               2          0    96.1s       19%    62%       0%
  gemini-2.5-flash         2          2    15.7s       75%   100%      50%
  gpt-4o-mini              2          2    11.4s       44%   100%      31%
```

These results motivated T-164 (Gemini 2.5 Flash as the new default Pass-2
model). See `tasks.md` row T-164 for the full evidence package.

## Adding a new candidate model

Edit the `MODELS` list at the top of any script. Each entry is `{"id": str, "provider": str}` where provider is `ollama`, `openai`, or `gemini`. To
test a model from a different provider:

1. Add a `call_<provider>(model, prompt)` function alongside the existing
   `call_ollama` / `call_openai` / `call_gemini`. Must return
   `{"raw": <json string>, "duration": <seconds>}`.
2. Add the provider branch to `run_batch`.
3. Add the new entry to `MODELS`.

The prompt and `SUMMARY_SCHEMA` are fixed across all providers — they mirror
what `pcca.services.model_router.summarize_batch` sends in production. **Do
not modify them** unless production also changes, or the comparison stops
being apples-to-apples.

## Refreshing the test fixtures

The candidate items are pulled from the user's DB at the time the script
was written; the text is then inlined verbatim so the test stays reproducible.
To regenerate against a fresh corpus:

1. Pull substantive items (≥400 chars) for the target subjects via:
   ```bash
   sqlite3 .pcca/pcca.db \
     "SELECT i.id, i.author, i.canonical_url, i.published_at, SUBSTR(i.raw_text, 1, 1200) \
      FROM items i WHERE LENGTH(i.raw_text) > 400 AND ..."
   ```
2. Paste them into the `CANDIDATES` list following the same shape.
3. Update `EXPECTED` per subject with your honest accept/reject judgment.

## Production cost reference (Gemini 2.5 Flash)

- Free tier: 250 RPD / 250K TPM / 10 RPM
- PCCA's load: ~13 batches/night × 1 call = ~13 RPD. Well under cap.
- Paid tier (if you blow the free quota): ~$0.18/night = ~$5.40/month at
  PCCA's current 5-subject load.

## Related code

- `src/pcca/services/model_router.py` — the production `summarize_batch` that
  these tests mirror. If the prompt there changes, update the
  `build_prompt()` function in each script to match.
- `tasks.md` T-164 — full evidence package + decision to move to Gemini.
