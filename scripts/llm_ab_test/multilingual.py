"""Ukrainian-content sanity test: Gemini 2.5 Flash vs llama3.1:8b on 4 items
for Subject 4 (Ukraine War News).

Goal: confirm Gemini handles Ukrainian/Cyrillic input cleanly before we ship.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import httpx


OLLAMA_BASE = "http://localhost:11434"
TIMEOUT = 600.0

MODELS = [
    {"id": "llama3.1:8b",      "provider": "ollama"},
    {"id": "gemini-2.5-flash", "provider": "gemini"},
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
GEMINI_API_KEY = ENV.get("PCCA_GEMINI_API_KEY")


SUBJECT_NAME = "Ukraine War News"
SUBJECT_DESCRIPTION = (
    "Track meaningful Ukraine War News: frontline updates, strategic developments, "
    "political analysis from credible Ukrainian commentators, drone strikes, peace "
    "talks, Russian war crimes, NATO/Western support, sanctions, refugees. Surface "
    "novel insights, expert analysis, and consequential events. Avoid generic news "
    "recap, social media noise, unverified rumors."
)


CANDIDATES = [
    {
        "item_id": 2587,
        "author": "Vitaly Portnikov",
        "url": "https://www.youtube.com/watch?v=pR95Z6OOjN0",
        "published_at": "2026-05-18T19:22:00",
        "text": (
            "Путін пропонує перемовини | Віталій Портников. "
            "Прес-секретар президента Російської Федерації Дмитрій Пісков наголосив, "
            "що Кремль зацікавлений у відновленні мирних перемовин з Україною і "
            "розраховує тут на сприяння Сполучених Штатів. Нагадаю, що раніше "
            "російські чиновники, серед яких були помічник президента Російської "
            "Федерації з питань зовнішньої політики Юрій Ушаков та очільник "
            "російського зовнішньополітичного відомства Сергій Лавров підкреслили, "
            "що Російська Федерація відмовляється від подальших перемовин з "
            "Україною до того моменту, поки українські Збройні сили не будуть "
            "виведені з території Донецької області. Президент Росії Путін також "
            "підкреслював, що військові дії на російсько-українському фронті "
            "можуть завершитися у будь-який момент, якщо президент України "
            "Володимир Зеленський ухвалить потрібні рішення."
        ),
    },
    {
        "item_id": 2586,
        "author": "Vitaly Portnikov",
        "url": "https://www.youtube.com/watch?v=5XQqadvmY1s",
        "published_at": "2026-05-18T16:53:00",
        "text": (
            "Навіщо Путіна викликали до Пекіна | Віталій Портников. "
            "Президент Російської Федерації Володимир Путін вже 19 травня "
            "відправиться із візитом до Китаю. Буквально за кілька днів після того, "
            "як з Пекіну відбула американська делегація на чолі із президентом "
            "Дональдом Трампом. І це буде перший раз, коли голова Китайської "
            "Народної Республіки Сідзенпін прийматиме у Пекіні і президента "
            "Сполучених Штатів, і президента Російської Федерації протягом одного "
            "місяця. Саме тому легко знайти відповідь на питання: а чого очікувати "
            "від візиту президента Російської Федерації до Китайської Народної "
            "Республіки? Відповідь тут є достатньо простою. Результати візиту "
            "президента Російської Федерації будуть нагадувати результати візиту "
            "президента Сполучених Штатів."
        ),
    },
    {
        "item_id": 2581,
        "author": "STERNENKO",
        "url": "https://www.youtube.com/watch?v=TCcELqmivJ8",
        "published_at": "2026-05-18T15:00:00",
        "text": (
            "Шок і страх у Москві – атаки ніколи не було, але росіяни просять ще. "
            "Маємо прекрасні наслідки після учорашньої атаки на Москву та "
            "Московську область українських безпілотників. Та сама нафтоналивна "
            "станція Солнечногорська у населеному пункті Дурикіно — я не жартую — "
            "майже повністю вигоріла. За даними OSINT-ерів, із чотирьох резервуарів "
            "лишився тільки один, але я впевнений, це тимчасово. Можна іще говорити "
            "про кінетичні наслідки українських атак на Москву та Московську "
            "область, але набагато приємніше дивитися на інші наслідки — "
            "когнітивні, точніше на те, як нують москвичі. Ми з вами вже бачили, "
            "як росіяни не хотіли їхати з Криму. Бачили, як росіяни не хотіли "
            "їхати з Туапсе, а тепер бачимо, як росіяни не хочуть їхати із Москви."
        ),
    },
    {
        "item_id": 2529,
        "author": "Vitaly Portnikov",
        "url": "https://www.youtube.com/watch?v=QElodfQOBQg",
        "published_at": "2026-05-15T18:00:00",
        "text": (
            "Війна на знищення | Віталій Портников. "
            "Всім доброго вечора. Сьогодні в нас вечір про подію цікаву, важливу, "
            "яка відбулася буквально поруч біля церкви. Це проект 'Війна на "
            "знищення', в якому ми говоримо і показуємо весь процес за можливою "
            "геноцидальною частиною Російської імперії не тільки стосунком до "
            "України, майже за 900 років. Я думаю, що почнемо ми, як годиться, з "
            "хвилини мовчання, пам'ять про загиблих. Очевидно, що почнемо вечір "
            "ми із більш детального розповідь про сам проект."
        ),
    },
]


EXPECTED = {
    2587: "accept",  # Portnikov on Putin peace talks — strategic political analysis from credible Ukrainian commentator
    2586: "accept",  # Portnikov on Putin Beijing visit — strategic political analysis
    2581: "accept",  # STERNENKO on Ukrainian drone attack on Moscow fuel station — frontline/strategic
    2529: "accept",  # Portnikov on "War of annihilation" — historical/strategic essay at event
}


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


def _gemini_schema() -> dict:
    return SUMMARY_SCHEMA


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
    return {"raw": r.json().get("response", ""), "duration": time.monotonic() - started}


def call_gemini(model: str, prompt: str) -> dict:
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
    print(f"SUBJECT: {SUBJECT_NAME}")
    print(f"Items: {len(CANDIDATES)} (all Ukrainian Cyrillic, all expected: accept)\n")

    per_model: dict[str, dict] = {}
    for m in MODELS:
        mid = m["id"]
        print(f"--- {mid} ---")
        prompt = build_prompt(SUBJECT_NAME, SUBJECT_DESCRIPTION, CANDIDATES)
        try:
            if m["provider"] == "ollama":
                result = call_ollama(mid, prompt)
            else:
                result = call_gemini(mid, prompt)
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            per_model[mid] = {}
            continue
        try:
            parsed = json.loads(result["raw"])
            summaries = parsed.get("summaries", []) or []
        except json.JSONDecodeError as e:
            print(f"  JSON ERROR: {e}")
            print(f"  RAW: {result['raw'][:300]}")
            per_model[mid] = {}
            continue
        by_id = {int(s.get("item_id", -1)): s for s in summaries}
        print(f"  duration: {result['duration']:.1f}s | returned: {len(by_id)}/{len(CANDIDATES)}")
        per_model[mid] = by_id

    print("\nPer-item:")
    for c in CANDIDATES:
        expected = EXPECTED[c["item_id"]]
        title_line = c["text"].split("\n")[0][:70] if c["text"] else ""
        print(f"\n  item {c['item_id']} ({c['author']}) | expected: {expected}")
        print(f"    title: {title_line}")
        for m in MODELS:
            mid = m["id"]
            s = per_model.get(mid, {}).get(c["item_id"])
            if s is None:
                print(f"    [{mid:18s}]: MISSING")
                continue
            cat = classify(s)
            if cat == "low_content":
                reason = (s.get("reason") or "")[:120]
                print(f"    [{mid:18s}]: REJECT  ({reason})")
            else:
                brief = (s.get("brief_summary") or "").strip()
                detailed = (s.get("detailed_summary") or "").strip()
                flags = ("Y" if brief else "N") + "/" + ("Y" if detailed else "N")
                print(f"    [{mid:18s}]: {cat:13s} br/det={flags}")
                if brief:
                    print(f"      brief:  {brief[:140]}")
                if detailed:
                    print(f"      detail: {detailed[:200]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
