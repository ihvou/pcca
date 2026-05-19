"""A/B test (v2): four models on PCCA's summarize_batch.

Models tested (in order):
  1. llama3.1:8b   (Ollama, baseline)
  2. qwen2.5:7b    (Ollama, local alt)
  3. gemini-2.0-flash (Google API, free tier)
  4. gpt-4o-mini   (OpenAI API)

Same EXACT production prompt as model_router.py::summarize_batch.
Same 8 candidates × 2 subjects (AI Tools & Tips, AI PM Success Stories).
Same JSON schema requirement (brief_summary + detailed_summary + is_low_content).

Output: per-item side-by-side + aggregate metrics:
  - Schema compliance (8/8 returned?)
  - Dual-output rate (BOTH brief+detailed for non-low-content)
  - Subject-aware correctness (matches honest pre-judgment)
  - Latency per batch
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import httpx


# ---------- Provider config ----------

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
    """Read PCCA_*_API_KEY values from /Users/bobdean/Projects/ccas/pcca/.env."""
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


# ---------- Test set ----------

SUBJECTS = {
    "AI Tools & Tips": (
        "Track practical AI tools and tips. Cover leading AI companies, "
        "vibe coding, AI agents, and IT applications with real-world "
        "implementations. Avoid personal LinkedIn posts, skills.md hype, "
        "prompt collections, engagement spam, and generic top-N tool "
        "listicles unless they discuss specific application cases."
    ),
    "AI PM Success Stories": (
        "Track concrete AI Product Manager success stories — case studies "
        "where AI changed how PMs work, ship, or make decisions. Prioritize "
        "specific outcomes (features shipped, metrics moved, decisions "
        "accelerated) over generic AI commentary. Include practitioner "
        "threads, podcast clips, and workflow demos that show AI in real "
        "PM contexts, not just AI news."
    ),
}

CANDIDATES = [
    {
        "item_id": 2585,
        "author": "Claude",
        "url": "https://www.youtube.com/watch?v=eW3oTyfeWZ0",
        "published_at": "2026-05-18T18:00:00",
        "text": (
            "Context Management in Claude Code\n\nContext is Claude's working memory. "
            "Every file it reads, every command it runs, every message you send, it all "
            "takes up space in the context window. Think of the context window as the "
            "amount of space that Claude can hold in its memory. Whenever you approach "
            "this limit, the context window is automatically compacted. Compaction will "
            "summarize important details and remove the unnecessary tool call results "
            "and free up a lot of space in your context window. Do note though that this "
            "could potentially lose details in your previous conversation. You can run "
            "the compaction manually as well with the /compact command. This will compact "
            "everything that you've done up to that point, which could be handy if you "
            "want to clear up context space, but also have a memory of what you "
            "previously worked on. If you want to completely start from scratch without "
            "memory of what was previously worked on, you can also run /clear."
        ),
    },
    {
        "item_id": 2584,
        "author": "Claude",
        "url": "https://www.youtube.com/watch?v=dDg7vhvtbEE",
        "published_at": "2026-05-18T18:00:00",
        "text": (
            "Claude Cowork for sales\n\nHey, I'm Brittany. I'm a growth account "
            "executive and I manage a portfolio of our most strategic startup accounts. "
            "Before I meet with a customer for the first time, I need to get smart on "
            "them fast. Who are they? What are they building? How are they using Claude "
            "today? What's their spend look like today on us? And what are the risks and "
            "growth trends of this account? Now all of this information exists today, "
            "but it's scattered across Salesforce, our data warehouse, call recordings, "
            "Slack, email, and the web. Pulling this together used to mean hours of "
            "manual research. But with Claude Cowork, I have a skill that does it for me "
            "in minutes. To actually build the skill, I open up a Cowork session and "
            "describe what I'd want to know walking into that meeting. Claude drafted a "
            "skill file from that conversation, which is basically a text file that "
            "tells Claude how to approach the task."
        ),
    },
    {
        "item_id": 2582,
        "author": "Claude",
        "url": "https://www.youtube.com/watch?v=EPUg9pmfPk0",
        "published_at": "2026-05-18T18:00:00",
        "text": (
            "Claude Cowork for legal teams\n\nHi, I'm Mark. I'm an in-house product "
            "lawyer at Anthropic. This morning, a product manager sent me a Slack asking "
            "a quick question on a feature that launched a few months ago. Now, I've "
            "only got a few minutes for my next meeting and none of the context I had "
            "when I wrote the original memo about it. The old me would have spent the "
            "first hour of the day rereading my own work, pulling up the old brief, "
            "figuring out what actually changed, and then start thinking about the "
            "question. Here's how I set it up in Claude Cowork so I don't have to do "
            "that anymore. With Claude Cowork, I can schedule a task to run first thing "
            "in the morning. It's like having a personalized chief of staff that "
            "delivers a memo for me each day about what's on my plate, what's new, and "
            "what's urgent. The skill I lean on most is /brief, which is what I run when "
            "I need to come up to speed on a specific product fast."
        ),
    },
    {
        "item_id": 2580,
        "author": "twistartups",
        "url": "https://x.com/twistartups/status/2056422324099035141",
        "published_at": "2026-05-18T22:00:00",
        "text": "Gen Z rage against the (AI) machine",
    },
    {
        "item_id": 2579,
        "author": "theallinpod",
        "url": "https://x.com/theallinpod/status/2056414027748634819",
        "published_at": "2026-05-18T21:00:00",
        "text": (
            "Marc Benioff hilariously recaps the last year in AI: \"Sex bots off, coding "
            "agents on!\" @Benioff: \"Every company has kind of chosen a slightly different "
            "path. You had Elon, he went out, he had Grok, and he kind of started "
            "building these companions and sex bots, and all this stuff. Then on the "
            "complete opposite side, you had Anthropic, who said: we're going to bet "
            "everything on coding agents. And then everyone else has been kind of in the "
            "middle. The Anthropic bet looks like the right one for the enterprise.\""
        ),
    },
    {
        "item_id": 2578,
        "author": "navalpodcast",
        "url": "https://x.com/navalpodcast/status/2056010598618648737",
        "published_at": "2026-05-18T15:00:00",
        "text": "\"A taste of freedom can make you unemployable.\" — @naval",
    },
    {
        "item_id": 2577,
        "author": "lilianweng",
        "url": "https://x.com/lilianweng/status/2056177479782658363",
        "published_at": "2026-05-18T16:00:00",
        "text": "I only recently read more about the concept of system accidents by Charles Perrow, very insightful and relatable.",
    },
    {
        "item_id": 2576,
        "author": "lennysan",
        "url": "https://x.com/lennysan/status/2056407342242267163",
        "published_at": "2026-05-18T20:00:00",
        "text": "Full conversation",
    },
]

# Honest pre-judgment per (subject, item).
EXPECTED = {
    "AI Tools & Tips": {
        2585: "accept", 2584: "accept", 2582: "accept", 2580: "reject",
        2579: "accept", 2578: "reject", 2577: "reject", 2576: "reject",
    },
    "AI PM Success Stories": {
        2585: "reject", 2584: "reject", 2582: "reject", 2580: "reject",
        2579: "reject", 2578: "reject", 2577: "reject", 2576: "reject",
    },
}


# ---------- Schema ----------

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

# OpenAI strict-schema variant (requires additionalProperties: false everywhere)
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


# ---------- Prompt builder (matches model_router.py::summarize_batch) ----------

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


# ---------- Provider clients ----------

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
    raw = r.json().get("response", "")
    return {"raw": raw, "duration": duration}


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
    raw = body["choices"][0]["message"]["content"]
    return {"raw": raw, "duration": duration}


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


def _gemini_schema() -> dict:
    # Gemini doesn't accept `additionalProperties` or string `required` array
    # the way OpenAI does; this is the canonical shape it accepts.
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
