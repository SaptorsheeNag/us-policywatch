# app/ai_summarizer.py
import os, time
import asyncio
import json
import httpx
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Load .env here too, just in case this module is used outside FastAPI
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")
CF_AI_BASE = "https://api.cloudflare.com/client/v4/accounts/{acct}/ai/run"
CF_GENERIC_MODEL = "@cf/meta/llama-3.1-8b-instruct"

PROVIDER = os.getenv("AI_PROVIDER", "hf").lower()     # hf | none
HF_MODEL = os.getenv("HF_MODEL", "sshleifer/distilbart-cnn-12-6")
HF_TOKEN = os.getenv("HF_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

_openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

AI_TIMEOUT = float(os.getenv("AI_TIMEOUT_SEC", "12"))
DAILY_CALL_BUDGET = int(os.getenv("AI_DAILY_CALL_BUDGET", "200"))

# simple in-memory daily counter (resets when process restarts)
_calls_used = {"day": None, "count": 0}

_lock = asyncio.Lock()

print(
    "ai_summarizer init:",
    "AI_PROVIDER=", PROVIDER,
    "HF_TOKEN set=", bool(HF_TOKEN),
    "HF_MODEL=", HF_MODEL,
)

def _day_key():
    return time.strftime("%Y-%m-%d", time.gmtime())

async def _within_budget_async() -> bool:
    # ✅ unlimited mode
    if DAILY_CALL_BUDGET <= 0:
        return True

    async with _lock:
        day = _day_key()
        if _calls_used["day"] != day:
            _calls_used["day"] = day
            _calls_used["count"] = 0
        return _calls_used["count"] < DAILY_CALL_BUDGET

async def _bump_budget_async():
    async with _lock:
        day = _day_key()
        if _calls_used["day"] != day:
            _calls_used["day"] = day
            _calls_used["count"] = 0
        _calls_used["count"] += 1


async def _hf_polish(draft: str, title: str, url: str) -> str:
    """
    Hugging Face free Inference API (rate-limited but $0).
    IMPORTANT: seq2seq summarizers expect raw text, not instructions.
    """
    if not HF_TOKEN:
        print("HF polish: NO HF_TOKEN, skipping")
        return draft

    # ----- build plain input (NO instructions) -----
    draft_clean = (draft or "").strip()
    if title:
        input_text = f"{title}. {draft_clean}"
    else:
        input_text = draft_clean

    print("HF polish: calling model", HF_MODEL, "for URL", url)

    payload = {
        "inputs": input_text,
        "parameters": {
            "min_length": 40,
            "max_length": 120,
            "do_sample": False,
            "repetition_penalty": 1.05,
        },
        "options": {
            "wait_for_model": True,
            "use_cache": True,
        },
    }

    headers = {
        "Authorization": f"Bearer {HF_TOKEN}",
        "Content-Type": "application/json",
    }
    url_api = f"https://router.huggingface.co/hf-inference/models/{HF_MODEL}"

    try:
        async with httpx.AsyncClient(timeout=AI_TIMEOUT) as cx:
            r = await cx.post(url_api, headers=headers, json=payload)
            print("HF polish: status", r.status_code)
            if r.status_code >= 400:
                print("HF polish error body:", r.text[:300])
                return draft

        data = r.json()

        if isinstance(data, list) and data:
            if "summary_text" in data[0]:
                out = (data[0]["summary_text"] or "").strip()
                return out or draft
            if "generated_text" in data[0]:
                out = (data[0]["generated_text"] or "").strip()
                return out or draft

        return draft
    except Exception as e:
        print("HF polish: exception", repr(e))
        return draft
    
async def _openai_polish(draft: str, title: str, url: str) -> str:
    if not _openai_client:
        return draft

    try:
        print("OpenAI polish: CALL", "model=", OPENAI_MODEL, "url=", url)

        resp = await _openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {
                    "role": "system",
                    "content": (
                        "Rewrite the draft into a clear, neutral summary for policy analysts. "
                        "Preserve facts. Use 2–4 sentences. No bullets. No opinions."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"TITLE: {title}\n"
                        f"URL: {url}\n\n"
                        f"DRAFT SUMMARY:\n{draft[:2000]}"
                    ),
                },
            ],
            temperature=0.2,
            max_output_tokens=120,
        )

        # ✅ prove it actually succeeded
        rid = getattr(resp, "id", None)
        usage = getattr(resp, "usage", None)
        print("OpenAI polish: OK", "id=", rid, "usage=", usage)

        out = (resp.output_text or "").strip()
        return out or draft

    except Exception as e:
        print("OpenAI polish: ERROR", repr(e), "url=", url, "model=", OPENAI_MODEL)
        return draft


async def ai_polish_summary(draft: str, title: str = "", url: str = "") -> str:
    """
    Provider-agnostic polish step.
    Providers:
      - openai (GPT-4.1-mini)
      - hf (legacy / fallback)
      - none (skip)
    """
    if not draft:
        return draft

    if not await _within_budget_async():
        print("AI polish: SKIP (budget exceeded)", "url=", url)
        return draft
    
    print(
        "AI polish: decision",
        "provider=", PROVIDER,
        "openai_key=", bool(OPENAI_API_KEY),
        "hf_token=", bool(HF_TOKEN),
        "url=", url,
    )

    # 🔹 OpenAI (PRIMARY)
    if PROVIDER in ("openai", "gpt") and OPENAI_API_KEY:
        print("AI polish: USING OPENAI", "model=", OPENAI_MODEL, "url=", url)
        out = await _openai_polish(draft, title, url)
        await _bump_budget_async()
        return out or draft


    # 🔹 HuggingFace fallback
    if PROVIDER == "hf" and HF_TOKEN:
        print("AI polish: USING HF", "model=", HF_MODEL, "url=", url)
        out = await _hf_polish(draft, title, url)
        await _bump_budget_async()
        return out or draft

    print("AI polish: SKIP (no provider configured)", "provider=", PROVIDER, "url=", url)
    return draft

async def ai_extract_flgov_date(page_text: str, url: str) -> datetime | None:
    """
    Use Cloudflare Workers AI to extract the publication date from a Florida
    Governor press release page (plain text, not HTML).

    We expect a single line: YYYY-MM-DD or 'unknown'.
    """
    # if CF credentials are missing, just bail out gracefully
    if not (CF_ACCOUNT_ID and CF_API_TOKEN):
        print("AI date-extract: SKIP (no CF creds)", "url=", url)
        return None

    system = (
        "You are a strict parser. "
        "Given the plain text of a Florida Governor's press release page, "
        "you must extract the *publication date* of the press release.\n\n"
        "Return exactly ONE line in the format YYYY-MM-DD, or the word 'unknown' "
        "if you cannot determine it. Do not add extra text."
    )

    user = (
        f"URL: {url}\n\n"
        "Text:\n"
        f"{(page_text or '')[:8000]}"  # safety limit if pages are huge
    )

    url_api = f"{CF_AI_BASE.format(acct=CF_ACCOUNT_ID)}/{CF_GENERIC_MODEL}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}

    try:
        async with httpx.AsyncClient(timeout=AI_TIMEOUT) as cx:
            print("AI date-extract: USING CLOUDFLARE", "model=", CF_GENERIC_MODEL, "url=", url)
            r = await cx.post(
                url_api,
                json={
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ]
                },
                headers=headers,
            )
            r.raise_for_status()
            data = r.json()
            # Cloudflare Workers AI returns {"result": {"response": "..."}}
            raw = (data.get("result", {}).get("response") or "").strip()
    except Exception:
        return None

    if not raw:
        return None

    # in case model adds extra lines, just take the first
    first_line = raw.splitlines()[0].strip()

    if first_line.lower() == "unknown":
        return None

    try:
        # expect YYYY-MM-DD
        dt = datetime.strptime(first_line, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None
    
print("AI_PROVIDER:", PROVIDER, "HF_TOKEN set:", bool(HF_TOKEN), "CF set:", bool(CF_ACCOUNT_ID))
print("OPENAI key set:", bool(OPENAI_API_KEY), "OPENAI model:", OPENAI_MODEL)

