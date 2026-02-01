# app/summarize.py
# --- Zero-dependency extractive summarizer for items ---

import re
from html import unescape
from urllib.parse import urlsplit
from typing import List, Tuple, Optional
import httpx
from .db import connection
# summarize.py
from .ai_summarizer import ai_polish_summary


# Reuse the same "HTML-friendly" headers you used elsewhere
BROWSER_UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://www.whitehouse.gov/",
}

# --- White House Executive Order helpers ---

# classic preamble: "By the authority vested in me ... it is hereby ordered:"
_WH_EO_PREAMBLE_PAT = re.compile(
    r'^\s*by the authority vested in me.*?it is hereby ordered:?',
    re.IGNORECASE | re.DOTALL
)

_KEY_VERBS = (
    "directs", "orders", "establishes", "requires",
    "designates", "amends", "revokes", "implements"
)

def _looks_like_eo(url: str) -> bool:
    p = urlsplit(url).path or ""
    return "/presidential-actions/executive-orders/" in p

def _eo_trim_preamble(text: str) -> str:
    # Remove classic EO preamble if present at top
    return _WH_EO_PREAMBLE_PAT.sub("", text).strip()

# very common breadcrumbs / noise on WH pages
_WH_BREADCRUMB_PAT = re.compile(
    r'(?im)^(briefings\s*&\s*statements|fact\s*sheets|news|the white house|articles)\b.*$', 
    re.UNICODE
)

def _remove_breadcrumb_lines(text: str) -> str:
    lines = [ln.strip() for ln in text.split("\n")]
    kept = [ln for ln in lines if ln and not _WH_BREADCRUMB_PAT.match(ln)]
    return "\n".join(kept)



# ----------------------------
# Basic HTML -> clean text
# ----------------------------

_ARTICLE_PAT = re.compile(r'(?is)<article[^>]*>(.*?)</article>')
_MAIN_PAT    = re.compile(r'(?is)<main[^>]*>(.*?)</main>')

def _extract_main_html(html_str: str) -> str:
    if not html_str:
        return ""
    m = _ARTICLE_PAT.search(html_str)
    if m:
        return m.group(1)
    m = _MAIN_PAT.search(html_str)
    if m:
        return m.group(1)
    return html_str  # fallback

def _strip_html_to_text(html_str: str) -> str:
    """Crude but effective: drop scripts/styles/nav, keep text and paragraph breaks."""
    if not html_str:
        return ""
    html_str = _extract_main_html(html_str)   # <<< add this
    s = re.sub(r"(?is)<(script|style|noscript|nav|header|footer|aside)[\s\S]*?</\1>", " ", html_str)
    s = re.sub(r"(?is)<br\s*/?>", "\n", s)
    s = re.sub(r"(?is)</(p|div|h\d)>", "\n", s)
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = unescape(s)
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n\s+", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# ----------------------------
# Token helpers (very light)
# ----------------------------
_STOP = {
    # tiny stoplist; good enough for sentence similarity
    "the","a","an","and","or","but","if","while","of","to","in","on","for",
    "is","are","was","were","be","been","it","that","this","with","as","by",
    "at","from","we","our","their","his","her","they","them","you","your",
}

def _sent_split(text: str) -> List[str]:
    # Split on sentence enders; keep short lines out
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-Z0-9])", text.strip())
    # fall back to linewise if no punctuation present
    if len(parts) <= 1:
        parts = re.split(r"\n+", text.strip())
    # keep reasonably sized candidates
    return [s.strip() for s in parts if len(s.strip()) >= 25]

_EMOJI_RE = re.compile(r"[\u2600-\u27BF\uE000-\uF8FF\U0001F300-\U0001FAFF]")

def _is_bulletish(s: str) -> bool:
    s2 = s.lstrip()
    return s2.startswith(("•","-","–","—","✅","✔","▪","►","○","●"))

def _is_promo_boilerplate(s: str) -> bool:
    s2 = s.strip()
    # CA newsroom common promo/openers & headings we don’t want to summarize
    if s2.upper().startswith(("ICYMI", "WHAT YOU NEED TO KNOW")):
        return True
    # brag/context lines that aren’t about the specific action
    if re.search(r"\b(in\s20\d\d,\s*voters approved)\b", s2, re.I):
        return True
    if re.search(r"\b(he has signed into law|has signed into law)\b", s2, re.I):
        return True
    # lines that are mostly emoji bullets / decorative
    if _EMOJI_RE.search(s2) and len(s2) < 220:
        return True
    return False

def _has_numbers_or_money(s: str) -> bool:
    return bool(re.search(r"(\$[\d,]+|\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b|\b\d+%|\b(million|billion|thousand)\b)", s, re.I))


def _looks_like_quote(s: str) -> bool:
    s2 = s.strip()
    # leading curly or straight quote, or attribution-y sentences
    if s2.startswith(("“", '"', "'")):
        return True
    # common press-release pattern: “…,” said <Name> / according to …
    if re.search(r'\b(said|according to|stated|noted|added)\b', s2, re.I) and "“" in s2:
        return True
    # pull-quote style: ends with a closing quote
    if s2.endswith(("”", '"', "'")) and "“" in s2:
        return True
    return False


def summarize_extractive(title: str, url: str, html: str, max_sentences: int = 2, max_chars: int = 700) -> str:
    text = _strip_html_to_text(html)
    text = _remove_breadcrumb_lines(text)
    if _looks_like_eo(url):
        text = _eo_trim_preamble(text)

    sents = _sent_split(text)
    # drop quotes, promo/brag openers, and bullet/emoji lines
    sents = [s for s in sents if not _looks_like_quote(s) and not _is_bulletish(s) and not _is_promo_boilerplate(s)]
    if not sents:
        return ""

    def score(idx_sent: tuple[int, str]) -> tuple[int, int, int, int, int]:
        idx, s = idx_sent
        s_lower = s.lower()
        has_kw = any(kw in s_lower for kw in _KEY_VERBS)
        is_quote = _looks_like_quote(s)
        has_num = _has_numbers_or_money(s)
        # prefer: policy verbs, concreteness (numbers/$), earlier, longer
        # penalize: quotes (should already be filtered, but keep safe)
        quote_penalty = -1 if is_quote else 0
        return (1 if has_kw else 0, 1 if has_num else 0, quote_penalty, len(s), -idx)

    best = sorted(enumerate(sents), key=score, reverse=True)[:max_sentences]
    best = sorted(best, key=lambda t: t[0])
    out = " ".join(s for _, s in best)
    out = re.sub(r"\s+", " ", out).strip()
    if len(out) > max_chars:
        out = out[:max_chars].rsplit(" ", 1)[0].rstrip(" .,;:") + "…"
    return out


def _tokens(s: str) -> List[str]:
    return [w.lower() for w in re.findall(r"[a-zA-Z0-9']+", s) if w.lower() not in _STOP]

def _cosine(a: List[str], b: List[str]) -> float:
    if not a or not b:
        return 0.0
    # bag of words cosine without numpy
    ca, cb = {}, {}
    for t in a: ca[t] = ca.get(t, 0) + 1
    for t in b: cb[t] = cb.get(t, 0) + 1
    # dot
    dot = sum(ca[t] * cb.get(t, 0) for t in ca)
    # norms
    na = sum(v*v for v in ca.values()) ** 0.5
    nb = sum(v*v for v in cb.values()) ** 0.5
    return (dot / (na * nb)) if na and nb else 0.0

# ----------------------------
# Tiny TextRank (power iteration)
# ----------------------------
def _textrank(sentences: List[str], iters: int = 20, damping: float = 0.85) -> List[float]:
    n = len(sentences)
    if n == 0:
        return []
    # pre-tokenize & similarity matrix (symmetric, no self-sim)
    toks = [_tokens(s) for s in sentences]
    sim = [[0.0]*n for _ in range(n)]
    for i in range(n):
        for j in range(i+1, n):
            c = _cosine(toks[i], toks[j])
            sim[i][j] = c
            sim[j][i] = c
    # row-normalize
    for i in range(n):
        s = sum(sim[i])
        if s > 0:
            sim[i] = [x/s for x in sim[i]]
    # rank vector
    r = [1.0/n]*n
    base = (1.0 - damping)/n
    for _ in range(iters):
        new = [base + damping*sum(sim[j][i]*r[j] for j in range(n)) for i in range(n)]
        r = new
    return r

_ACRONYM_OK = {"US", "USA", "U.S.", "U.S.A.", "DHS", "HHS", "EPA", "FBI", "CIA", "NATO", "AI"}

def _soft_normalize_caps(text: str) -> str:
    """
    Lowercases long ALL-CAPS runs that look like headings,
    but keeps common acronyms. Conservative on short words.
    """
    def fix_token(tok: str) -> str:
        if tok in _ACRONYM_OK: 
            return tok
        # long ALL-CAPS tokens (8+), convert to Title Case
        if len(tok) >= 8 and tok.isupper():
            return tok.title()
        return tok

    # Convert runs of ALL-CAPS words (with spaces/punctuation between) if long enough
    def repl(m: re.Match) -> str:
        chunk = m.group(0)
        toks = re.findall(r"[A-Z][A-Z0-9\-']+|[A-Z]", chunk)
        fixed = " ".join(fix_token(t) for t in toks)
        # keep punctuation and spacing roughly intact
        return fixed

    # Only touch sequences that are mostly caps and at least ~12 chars
    return re.sub(r"(?:[A-Z0-9][A-Z0-9 \-'/]{11,})", repl, text)

def summarize_text(text: str, max_sentences: int = 3, max_chars: int = 700) -> str:
    text = _remove_breadcrumb_lines(text)      # <<< add this line
    sents = _sent_split(text)
    if not sents:
        return ""
    sents = [s for s in sents if not _looks_like_quote(s) and not _is_bulletish(s) and not _is_promo_boilerplate(s)] or sents
    ranks = _textrank(sents)
    order = sorted(range(len(sents)), key=lambda i: ranks[i], reverse=True)
    # pick top N but keep original order for readability
    top_idx = sorted(order[:max_sentences])
    chosen = [sents[i] for i in top_idx]
    out = " ".join(chosen).strip()
    # ensure we respect max_chars
    if len(out) > max_chars:
        out = out[:max_chars].rsplit(" ", 1)[0].rstrip(" .,;:") + "…"
    return out

async def _fetch_url_text(url: str) -> str:
    async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
        r = await cx.get(url)
        if r.status_code >= 400:
            return ""
        return _strip_html_to_text(r.text)

# ----------------------------
# DB workflow
# ----------------------------
async def summarize_items_needing_help(
    source_name: Optional[str] = None,
    limit: int = 40,
    max_chars: int = 480,
) -> int:
    """
    Summarize items whose summary is NULL or longer than 500 chars.
    If source_name is given, constrain to that source.
    """
    where_src = "s.name = $1" if source_name else "TRUE"
    params = [source_name] if source_name else []

    q = f"""
        select i.external_id, i.title, i.summary, i.url
        from items i
        join sources s on s.id = i.source_id
        where {where_src}
          and (
            i.summary is null
            or btrim(i.summary) = ''
            or length(i.summary) < 140
            or length(i.summary) > 700
            or i.summary ~* '^\s*(by the authority vested)'
          )
        order by i.id desc
        limit {limit}
    """

    updated = 0
    async with connection() as conn:
        rows = await conn.fetch(q, *params)
        if not rows:
            return 0

        updates: List[Tuple[str, str]] = []   # (summary, external_id)
        for r in rows:
            url = r["url"]
            # fetch full HTML (not just plain text), because summarize_extractive needs HTML
            async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
                resp = await cx.get(url)
                html = resp.text if resp.status_code < 400 else ""
            if not html:
                continue

            if _looks_like_eo(url):
                # EO-aware extractive (skips boilerplate)
                summ = summarize_extractive(r["title"] or "", url, html, max_sentences=2, max_chars=700)
            else:
                # Your existing TextRank on plain text
                text = _strip_html_to_text(html)
                summ = summarize_text(text, max_sentences=3, max_chars=700)

            if not summ:
                continue

            # normalize shouty caps before polishing
            summ = _soft_normalize_caps(summ)

            # 🔽 AI polish (2–3 lines)
            summ = await ai_polish_summary(summ, r["title"], url)

            updates.append((summ, r["external_id"]))

        if updates:
            await conn.executemany(
                "update items set summary = $1 where external_id = $2",
                updates,
            )
            updated = len(updates)

    return updated

async def resummarize_white_house_batch(conn, limit: int = 50) -> int:
    """
    Re-summarize recent White House items whose summaries are empty or
    start with the EO boilerplate. Uses summarize_extractive (EO-aware).
    """
    rows = await conn.fetch("""
        select i.external_id, i.url, i.title
        from items i
        join sources s on s.id = i.source_id
        where s.name = 'White House — News & Actions'
        and (
            i.summary ~* '^\\s*(by the authority vested)'             -- EO boilerplate
            or i.summary ~* '^\\s*(news|articles|briefings\\s*&\\s*statements|fact\\s*sheets)\\b'
            or i.summary is null
            or i.summary = ''
        )
        order by i.published_at desc
        limit $1
    """, limit)


    if not rows:
        return 0

    updates: List[Tuple[str, str]] = []
    async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
        for r in rows:
            url = r["url"]
            title = r["title"] or ""
            try:
                resp = await cx.get(url)
                if resp.status_code >= 400 or not resp.text:
                    continue
                html = resp.text
                summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                # after computing `summary`
                if not summary:
                    # fallback: first reasonably long paragraph
                    paras = [p.strip() for p in re.split(r'\n+', _strip_html_to_text(html)) if len(p.strip()) > 60]
                    if paras:
                        summary = paras[0]
                if not summary:
                    continue

                # normalize caps before polishing
                summary = _soft_normalize_caps(summary)
                
                # 🔽 AI polish (2–3 lines)
                summary = await ai_polish_summary(summary, r["title"], url)


                updates.append((summary, r["external_id"]))
            except Exception:
                continue

    if not updates:
        return 0

    await conn.executemany(
        "update items set summary = $1 where external_id = $2",
        updates,
    )
    return len(updates)

async def force_repolish_white_house_batch(conn, limit: int = 50) -> int:
    """
    Force AI re-polish of recent White House summaries,
    even if they already exist and look fine.
    """
    rows = await conn.fetch("""
        select i.external_id, i.url, i.title, i.summary
        from items i
        join sources s on s.id = i.source_id
        where s.name = 'White House — News & Actions'
        order by i.published_at desc
        limit $1
    """, limit)

    if not rows:
        return 0

    updates: list[tuple[str, str]] = []
    for r in rows:
        draft = r["summary"] or ""
        # if summary missing, regenerate extractive first
        if not draft:
            async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
                resp = await cx.get(r["url"])
                if resp.status_code >= 400 or not resp.text:
                    continue
                draft = summarize_extractive(r["title"] or "", r["url"], resp.text, max_sentences=2, max_chars=700)

        if not draft:
            continue

        # normalize caps before polishing
        draft = _soft_normalize_caps(draft)

        # Always polish with AI (force overwrite)
        polished = await ai_polish_summary(draft, r["title"], r["url"])
        if polished:
            updates.append((polished, r["external_id"]))

    if not updates:
        return 0

    await conn.executemany(
        "update items set summary = $1 where external_id = $2",
        updates,
    )
    return len(updates)

