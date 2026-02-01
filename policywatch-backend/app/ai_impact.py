# app/ai_impact.py
from __future__ import annotations

import os
import json
import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from openai import AsyncOpenAI

# Load env
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT_SEC", "12"))  # (kept for consistency)

_openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def _strip_code_fences(s: str) -> str:
    if not s:
        return s
    t = s.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json\n"):
            t = t[5:]
    return t.strip()


def _safe_json_loads(s: str) -> Dict[str, Any]:
    t = _strip_code_fences(s)
    return json.loads(t)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass
class ImpactResult:
    score: float
    impact: Dict[str, Any]
    model: str


# ✅ CHANGE 1: ai_summary -> summary_text
def _impact_prompt(title: str, url: str, summary_text: str) -> str:
    return f"""
Return STRICT JSON only (no markdown, no extra text).

You are a policy impact analyst. Based ONLY on the given title + summary, estimate which industries may be affected and whether the impact is positive or negative.

JSON schema:
{{
  "score": number,  // in [-1, 1], negative=likely negative economic/industry impact, positive=likely positive
  "industries": [
    {{
      "name": string,
      "direction": "positive" | "negative" | "mixed",
      "magnitude": number,   // 0..1
      "confidence": number,  // 0..1
      "why": string          // <= 220 chars
    }}
  ],
  "tags": [string],         // 0..8 short tags
  "overall_why": string     // <= 260 chars
}}

Rules:
- Use 0–5 industries max.
- If unclear, set score=0 and industries=[].
- Do not invent facts. Be conservative.

TITLE: {title}
URL: {url}

SUMMARY:
{summary_text}
""".strip()


# ✅ CHANGE 2: ai_summary -> summary_text

async def score_item_impact(title: str, url: str, summary_text: str) -> Optional[ImpactResult]:
    if not summary_text or not summary_text.strip():
        return None

    if not _openai_client or not OPENAI_API_KEY:
        return None

    prompt = _impact_prompt(title or "", url or "", summary_text.strip()[:2500])

    try:
        print("AI impact: CALL", "model=", OPENAI_MODEL, "url=", url)

        resp = await _openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You output only strict JSON. "
                        "No markdown. No extra keys beyond the schema. "
                        "Be conservative and avoid speculation."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_output_tokens=380,
        )

        out_text = (resp.output_text or "").strip()
        if not out_text:
            raise RuntimeError("empty model output")

        data = _safe_json_loads(out_text)

        score = _clamp(float(data.get("score", 0.0)), -1.0, 1.0)

        data["industries"] = (data.get("industries") or [])[:5]
        data["tags"] = (data.get("tags") or [])[:8]
        data["overall_why"] = (data.get("overall_why") or "")[:260]

        return ImpactResult(score=score, impact=data, model=OPENAI_MODEL)

    except Exception as e:
        print("AI impact: ERROR", repr(e), "url=", url)
        return ImpactResult(
            score=0.0,
            impact={
                "score": 0.0,
                "industries": [],
                "tags": [],
                "overall_why": "error generating impact",
            },
            model=OPENAI_MODEL,
        )
