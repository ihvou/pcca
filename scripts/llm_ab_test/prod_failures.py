"""A/B test (v3): four models against EIGHT items that produced partial-schema
output in today's production run (run 112, 2026-05-19).

Every candidate in this test:
  - Got only brief_summary OR only detailed_summary from llama3.1:8b in production
  - Is substantive content (not a low-content one-liner)
  - Made it into a digest (= scored above 0.55 relevance floor)

This test exists because the "clean test set" version (ab_test_llms_v2) hit
100% dual-output rate for all four models — but production data clearly shows
llama3.1:8b drops one half of the summary pair ~80% of the time. We rerun on
the actual production failures to settle whether API models fix that.

Subjects tested are the EXACT production subjects where these items failed
(Subject 1 = AI impact on IT jobs market, Subject 2 = AI PM Success Stories).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import httpx


OLLAMA_BASE = "http://localhost:11434"
TIMEOUT = 600.0

MODELS = [
    {"id": "llama3.1:8b",      "provider": "ollama"},
    {"id": "qwen2.5:7b",       "provider": "ollama"},
    {"id": "gemini-2.5-flash", "provider": "gemini"},
    {"id": "gpt-4o-mini",      "provider": "openai"},
]


REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts/llm_ab_test/x.py -> repo root


def load_env_keys() -> dict[str, str]:
    env_path = REPO_ROOT / ".env"
    keys: dict[str, str] = {}
    if not env_path.exists():
        return keys
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        keys[k.strip()] = v.strip().strip("'").strip('"')
    return keys


ENV = load_env_keys()
OPENAI_API_KEY = ENV.get("PCCA_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
GEMINI_API_KEY = ENV.get("PCCA_GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY")


# ---- Subjects ----
# Subject 1 description is the LIVE production one. Subject 2 description was
# restored on 2026-05-19 after T-157 (it had been corrupted with feedback macros).
SUBJECTS = {
    "AI impact over IT jobs market": (
        "Track the impact of AI on the IT job market. Surface SME perspectives, "
        "podcast discussions, and video interviews. Prioritize novel insights and "
        "concrete signals over generic opinions or repetitive takes."
    ),
    "AI PM Success Stories": (
        "Track concrete AI Product Manager success stories — case studies where "
        "AI changed how PMs work, ship, or make decisions. Prioritize specific "
        "outcomes (features shipped, metrics moved, decisions accelerated) over "
        "generic AI commentary. Include practitioner threads, podcast clips, and "
        "workflow demos that show AI in real PM contexts, not just AI news."
    ),
}


# ---- The 8 production-failure items ----
# Each item was in either digest 91 (Subject 2) or digest 92 (Subject 1) today
# and had only ONE of brief/detailed populated in production (the v3 schema
# requires BOTH to be eligible). All items have substantive content.
CANDIDATES = [
    {
        "item_id": 2138,
        "author": "Melissa Cote",
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:7457493093705994242",
        "published_at": "2026-05-06T09:11:00",
        "text": (
            "Why did OpenAI launch a consulting firm? Why is Anthropic partnering "
            "with megafunds to launch an Enterprise AI services firm? We've moved "
            "from AI bottleneck to AI bottleneck in a frenzied pace over the last "
            "three years: First, the models. Can they reason, code, and stop "
            "hallucinating. Then compute. Can we get enough GPUs. Then "
            "semiconductors. Can Nvidia & TSMC keep up. Now power and data "
            "centers. Can the grid, land, cooling, permitting, and capex support "
            "AI infrastructure. What's the next frontier? Deployment. Engineers "
            "build, salespeople sell, but who owns and implements? Who's helping "
            "companies rewire entire workflows? Who's helping employees rethink "
            "how they work? Kiss goodbye to all those theoretical EBITDA "
            "improvements if change management fails. Failure modes are plenty: "
            "passive resistance to change, internal politics over who owns the AI "
            "transformation, budget fights, middle managers resisting because AI "
            "threatens headcount."
        ),
    },
    {
        "item_id": 2169,
        "author": "Eduardo Serodio Schefer",
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:7444464846168981505",
        "published_at": "2026-04-13T09:12:14",
        "text": (
            "Some really uncomfortable truths coming: The way we 'cost and measure' "
            "software development is basically toast. Story points are dead. Sprint "
            "velocity: dead. And if we're being honest, most of the Agile/SAFe/SDLC "
            "apparatus is basically meaningless (with 'Waterfall' having mercifully "
            "died long ago). I was at a CTO conference this past week and this topic "
            "came up. The entire way we've estimated and costed software for the "
            "last 20 years assumed a roughly stable relationship between human "
            "effort and output. AI completely broke that relationship. Now? A "
            "senior engineer with the right AI tooling can mass-produce a feature "
            "in two hours that would have been a two-week sprint six months ago. "
            "However, an architectural decision still takes the same amount of "
            "judgment time it always did. The unit of work has decoupled from the "
            "unit of output, and nobody's measurement system has caught up."
        ),
    },
    {
        "item_id": 2390,
        "author": "Elena Verna",
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:7460731175851032576",
        "published_at": "2026-05-14T14:38:57",
        "text": (
            "Lovable x PMMs 🤘 We're putting on our very first vibe coding for "
            "PMMs talk, and Vikas Bhagat and I are hosting!! WHAT DO YOU WANNA "
            "HEAR ABOUT? Some ideas 💅 — How we've AI-pilled PMM at Lovable — "
            "Best use cases for PMMs on Lovable — How to become a mega power user "
            "of Lovable. Drop what you want to see in the comments and register here."
        ),
    },
    {
        "item_id": 2408,
        "author": "Mykola Zomchak",
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:6792713351005814786/",
        "published_at": "2021-05-16T09:40:44",
        "text": (
            "We are excited to release meeting summaries features. This is the "
            "start of a long journey of building AI knowledge keeper for companies "
            "and we thrilled to start it with this feature."
        ),
    },
    {
        "item_id": 2416,
        "author": "gdb",
        "url": "https://x.com/gdb/status/2055034165968384099",
        "published_at": "2026-05-14T21:15:00",
        "text": (
            "You can now use Codex, wherever you have it running, from the ChatGPT "
            "app. Huge step forward for universal usage of agents."
        ),
    },
    {
        "item_id": 2431,
        "author": "twistartups",
        "url": "https://x.com/twistartups/status/2055038076226097228",
        "published_at": "2026-05-14T21:30:32",
        "text": (
            "Use AI first or this isn't the place for you. If you don't have the "
            "time to learn AI, you don't have a job anymore. Ask AI how to do your "
            "job better. Let it do your repetitive tasks. Keep your job."
        ),
    },
    {
        "item_id": 2460,
        "author": "Elena Verna",
        "url": "https://www.linkedin.com/feed/update/urn:li:activity:7461061222982135808",
        "published_at": "2026-05-15T15:11:07",
        "text": (
            "Being an IC is becoming the new career flex. I call it Hi-C "
            "(High-impact IC). And I'm so here for it (and testing it myself). "
            "The traditional career path has always been kind of dumb: Get really "
            "good at your craft so you can earn a promotion into... no longer "
            "doing your craft. Instead, you become a professional meeting attender, "
            "a full-time cross-functional coordinator, all while routing info up "
            "and down the chain. But now AI gives you the abilities of an average "
            "marketer, designer, PM, engineer, analyst, etc. Combined with actual "
            "domain expertise, one person can now do work that used to require "
            "entire teams. Increasing your impact no longer means you need a team "
            "to get things done. I became an IC at Lovable few months back and I "
            "love it. I think I was always a mediocre manager anyway. But in "
            "today's environment, it's a career upgrade. You get to spend your "
            "time doing what you're actually good at. And what you love."
        ),
    },
    {
        "item_id": 1524,
        "author": "lexfridman",
        "url": "https://x.com/lexfridman/status/2039841897066414291",
        "published_at": "2026-04-02T23:06:21",
        "text": (
            "Same, I have a similar setup. A mix of Obsidian, Cursor (for md), "
            "and vibe-coded web terminals as front-end. Since I do a podcast, "
            "the number/diversity of research interests is very large. But the "
            "knowledge-base approach has been working great. For answers, I often "
            "have it query across all my notes."
        ),
    },
]


# Honest pre-judgment for each (subject, item).
# "accept" = should produce a real brief_summary + detailed_summary
# "reject" = should mark is_low_content=true (off-topic for the subject or vacuous)
EXPECTED = {
    "AI impact over IT jobs market": {
        2138: "accept",  # Melissa Cote on AI deployment & change-mgmt impact — directly job-impact
        2169: "accept",  # Eduardo Schefer on Story Points / sprint velocity dead — direct hit
        2390: "reject",  # Elena Verna PMM talk announcement — not insight
        2408: "reject",  # Mykola Zomchak 2021 product launch — old, unrelated to current jobs impact
        2416: "reject",  # gdb Codex feature news — product, not jobs
        2431: "accept",  # twistartups "use AI or no job" — direct take on AI-or-jobless
        2460: "accept",  # Elena Verna Hi-C IC career flex — direct take on AI changing career path
        1524: "reject",  # lexfridman personal Obsidian/Cursor setup — workflow, not jobs
    },
    "AI PM Success Stories": {
        2138: "reject",  # Industry commentary, not a PM success story
        2169: "reject",  # Engineering methodology, PM-adjacent at best
        2390: "reject",  # PMM (not PM) talk announcement, not a success story
        2408: "reject",  # Product launch PR (also 2021), not a story
        2416: "reject",  # Product feature news
        2431: "reject",  # Workplace declaration, not PM-specific
        2460: "reject",  # IC career philosophy mentions PM tangentially
        1524: "reject",  # Personal workflow, not PM
    },
}


# ---- Schemas ----

SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "summaries": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer"},
                    "brief_summary": {"type": "string"},
                    "detailed_summary": {"type": "string"},
                    "is_low_content": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
                "required": ["item_id", "brief_summary", "detailed_summary", "is_low_content"],
            },
        }
    },
    "required": ["summaries"],
}


def _openai_schema() -> dict:
    return {
        "name": "summarize_batch",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "summaries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "item_id": {"type": "integer"},
                            "brief_summary": {"type": "string"},
                            "detailed_summary": {"type": "string"},
                            "is_low_content": {"type": "boolean"},
                            "reason": {"type": "string"},
                        },
                        "required": ["item_id", "brief_summary", "detailed_summary", "is_low_content", "reason"],
                    },
                }
            },
            "required": ["summaries"],
        },
    }


def _gemini_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "summaries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "item_id": {"type": "integer"},
                        "brief_summary": {"type": "string"},
                        "detailed_summary": {"type": "string"},
                        "is_low_content": {"type": "boolean"},
                        "reason": {"type": "string"},
                    },
                    "required": ["item_id", "brief_summary", "detailed_summary", "is_low_content"],
                },
            }
        },
        "required": ["summaries"],
    }


def build_prompt(subject_name: str, subject_description: str, candidates: list[dict]) -> str:
    compact = [
        {
            "item_id": c["item_id"],
            "author": c["author"],
            "published_at": c.get("published_at"),
            "url": c.get("url"),
            "text": c["text"][:1000],
        }
        for c in candidates[:20]
    ]
    return (
        "You prepare candidate content for one user's Brief.\n"
        "Each candidate already passed a relevance filter. Your job is NOT to score it; your job is "
        "to produce clean user-facing summaries or reject low-content candidates.\n\n"
        "Return JSON only with field summaries: an array of objects with item_id, brief_summary, "
        "detailed_summary, is_low_content, and optional reason. Include every candidate item exactly once.\n\n"
        "Content rules:\n"
        "- Use only content present in the candidate text. Do not introduce names, claims, products, companies, or context from the subject description.\n"
        "- brief_summary must be one complete sentence, 15-30 words, no direct quotes, summarizing the speaker's specific point.\n"
        "- detailed_summary must be 3-5 concise sentences paraphrasing literal claims from the candidate text, with speaker/source context when clear.\n"
        "- If the candidate is ambiguous, say it briefly mentions the topic without elaborating; do not fill gaps.\n"
        "- Name the speaker/source when clear from author or title; avoid generic 'the author' or 'the speaker'.\n"
        "- If the candidate is filler, ad read, transition, greeting, or low-content, set is_low_content=true and return empty summaries.\n"
        "- For every non-low-content candidate, return BOTH brief_summary and detailed_summary. Do not return only one of them.\n"
        "- Do not include biography, hype, or internal scoring details.\n\n"
        f"SUBJECT TITLE: {subject_name}\n"
        f"FULL SUBJECT DESCRIPTION:\n{subject_description[:4000]}\n\n"
        f"CANDIDATES:\n{json.dumps(compact, ensure_ascii=False)}"
    )


def call_ollama(model: str, prompt: str) -> dict:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": SUMMARY_SCHEMA,
        "options": {"temperature": 0.1},
    }
    started = time.monotonic()
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.post(f"{OLLAMA_BASE}/api/generate", json=payload)
        r.raise_for_status()
    duration = time.monotonic() - started
    return {"raw": r.json().get("response", ""), "duration": duration}


def call_openai(model: str, prompt: str) -> dict:
    if not OPENAI_API_KEY:
        return {"raw": "", "duration": 0.0, "error": "no PCCA_OPENAI_API_KEY"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_schema", "json_schema": _openai_schema()},
        "temperature": 0.1,
    }
    started = time.monotonic()
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json=payload,
        )
        r.raise_for_status()
    duration = time.monotonic() - started
    body = r.json()
    return {"raw": body["choices"][0]["message"]["content"], "duration": duration}


def call_gemini(model: str, prompt: str) -> dict:
    if not GEMINI_API_KEY:
        return {"raw": "", "duration": 0.0, "error": "no PCCA_GEMINI_API_KEY"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": _gemini_schema(),
            "temperature": 0.1,
        },
    }
    started = time.monotonic()
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}",
            json=payload,
        )
        r.raise_for_status()
    duration = time.monotonic() - started
    body = r.json()
    try:
        raw = body["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        raw = json.dumps(body)
    return {"raw": raw, "duration": duration}


def run_batch(model_spec: dict, subject_name: str, subject_description: str, candidates: list[dict]) -> dict:
    prompt = build_prompt(subject_name, subject_description, candidates)
    provider = model_spec["provider"]
    model = model_spec["id"]
    try:
        if provider == "ollama":
            result = call_ollama(model, prompt)
        elif provider == "openai":
            result = call_openai(model, prompt)
        elif provider == "gemini":
            result = call_gemini(model, prompt)
        else:
            return {"summaries": [], "duration": 0.0, "error": f"unknown provider: {provider}"}
    except httpx.HTTPStatusError as e:
        return {"summaries": [], "duration": 0.0, "error": f"http {e.response.status_code}: {e.response.text[:300]}"}
    except Exception as e:
        return {"summaries": [], "duration": 0.0, "error": f"{type(e).__name__}: {e}"}
    if "error" in result:
        return {"summaries": [], "duration": result.get("duration", 0.0), "error": result["error"]}
    raw = result["raw"]
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return {"summaries": [], "duration": result["duration"], "error": f"json_decode: {e}", "raw_preview": raw[:300]}
    return {"summaries": parsed.get("summaries", []) or [], "duration": result["duration"], "raw": raw}


def classify(s: dict) -> str:
    if bool(s.get("is_low_content")):
        return "low_content"
    brief = (s.get("brief_summary") or "").strip()
    detailed = (s.get("detailed_summary") or "").strip()
    if brief and detailed:
        return "both"
    if brief and not detailed:
        return "brief_only"
    if detailed and not brief:
        return "detailed_only"
    return "empty"


def main() -> int:
    agg = {m["id"]: {"schema_complete": 0, "items_total": 0, "both": 0, "brief_only": 0,
                     "detailed_only": 0, "low_content": 0, "empty": 0, "correct": 0,
                     "duration_total": 0.0, "batches": 0, "errors": 0}
           for m in MODELS}

    for subject_name, subject_description in SUBJECTS.items():
        print(f"\n{'='*80}\nSUBJECT: {subject_name}\n{'='*80}")
        per_model_summaries: dict[str, dict] = {}
        for m in MODELS:
            mid = m["id"]
            print(f"\n--- {mid} ({m['provider']}) ---")
            result = run_batch(m, subject_name, subject_description, CANDIDATES)
            agg[mid]["batches"] += 1
            if "error" in result:
                print(f"  ERROR: {result['error']}")
                if "raw_preview" in result:
                    print(f"  RAW (first 300): {result['raw_preview']}")
                agg[mid]["errors"] += 1
                per_model_summaries[mid] = {}
                continue
            agg[mid]["duration_total"] += result["duration"]
            by_id = {int(s.get("item_id", -1)): s for s in result["summaries"]}
            print(f"  duration: {result['duration']:.1f}s | returned: {len(by_id)}/{len(CANDIDATES)}")
            if len(by_id) == len(CANDIDATES):
                agg[mid]["schema_complete"] += 1
            per_model_summaries[mid] = by_id

            for c in CANDIDATES:
                s = by_id.get(c["item_id"])
                agg[mid]["items_total"] += 1
                if s is None:
                    agg[mid]["empty"] += 1
                    continue
                cat = classify(s)
                agg[mid][cat] += 1
                expected = EXPECTED[subject_name][c["item_id"]]
                model_call = "reject" if cat == "low_content" else "accept"
                if model_call == expected:
                    agg[mid]["correct"] += 1

        print("\nPer-item:")
        for c in CANDIDATES:
            expected = EXPECTED[subject_name][c["item_id"]]
            print(f"\n  item {c['item_id']} ({c['author']}) | expected: {expected}")
            preview = (c['text'][:80] + "...") if len(c['text']) > 80 else c['text']
            print(f"    text: {preview}")
            for m in MODELS:
                mid = m["id"]
                s = per_model_summaries.get(mid, {}).get(c["item_id"])
                if s is None:
                    print(f"    [{mid:18s}]: MISSING")
                    continue
                cat = classify(s)
                if cat == "low_content":
                    reason = (s.get("reason") or "")[:80]
                    print(f"    [{mid:18s}]: REJECT  (reason: {reason})")
                else:
                    brief = (s.get("brief_summary") or "").strip()
                    detailed = (s.get("detailed_summary") or "").strip()
                    flags = ("Y" if brief else "N") + "/" + ("Y" if detailed else "N")
                    print(f"    [{mid:18s}]: {cat:13s} brief/detailed={flags}")
                    if brief:
                        print(f"      brief: {brief[:110]}")
                    if detailed:
                        print(f"      detail: {detailed[:160]}")

    print(f"\n\n{'='*80}\nAGGREGATE (16 total judgments per model across 2 subjects)\n{'='*80}")
    print(f"  {'model':<20} {'batches':>9} {'schema-ok':>11} {'avg-lat':>10} {'subj-acc':>10} {'BOTH%':>7}  {'reject%':>9}  {'br-only':>9}  {'det-only':>10}  {'empty':>6}")
    for m in MODELS:
        mid = m["id"]
        a = agg[mid]
        n = a["items_total"] or 1
        non_low = max(1, a["both"] + a["brief_only"] + a["detailed_only"] + a["empty"])
        lat = a["duration_total"] / max(1, a["batches"])
        print(f"  {mid:<20} {a['batches']:>9} {a['schema_complete']:>11} {lat:>9.1f}s "
              f"{a['correct']/n*100:>9.0f}% {a['both']/non_low*100:>6.0f}% "
              f"{a['low_content']/n*100:>8.0f}% {a['brief_only']:>9} {a['detailed_only']:>10} {a['empty']:>6}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
