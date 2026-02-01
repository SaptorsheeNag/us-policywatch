# app/ingest_states.py
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)
import time
import os
from typing import Dict, List, Tuple
import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from datetime import datetime, timezone, timedelta
import asyncio
import httpx
from playwright.async_api import async_playwright
from .db import connection
from .ingest_rss import fetch_rss, map_rss_to_rows, upsert_items_from_rows
from .ingest_federal_register import get_or_create_source  # reuse helper
from .ai_summarizer import ai_polish_summary, ai_extract_flgov_date
# add to existing imports from .summarize
# ADD this one line instead:
from .summarize import (
    summarize_extractive,
    summarize_text,            # <-- needed for PDFs
    _soft_normalize_caps,
    BROWSER_UA_HEADERS,
    _strip_html_to_text,
)
from email.utils import parsedate_to_datetime  # stdlib
import json
import html as html_lib
try:
    # optional dependency; see requirements note below
    from pdfminer.high_level import extract_text as _pdf_extract_text
except Exception:
    _pdf_extract_text = None

def _extract_pdf_text_from_bytes(data: bytes) -> str:
    """
    Best-effort PDF -> text. Returns "" if pdfminer isn't available or fails.
    """
    if not data or _pdf_extract_text is None:
        return ""
    try:
        # pdfminer works with file-like objects
        import io
        return (_pdf_extract_text(io.BytesIO(data)) or "").strip()
    except Exception:
        return ""
    
# Safe wrappers so HF timeouts / errors don't kill the ingest
async def _safe_ai_polish(summary: str, title: str, url: str) -> str:
    if not summary:
        return ""
    try:
        return await ai_polish_summary(summary, title, url)
    except Exception as e:
        print("AI polish error:", e)
        return summary

async def _pw_get_html(url: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=60000)
        html = await page.content()
        await browser.close()
        return html or ""

async def _safe_ai_extract_flgov_date(text: str, url: str):
    try:
        return await ai_extract_flgov_date(text, url)
    except Exception as e:
        print("AI date-extract error:", e)
        return None
    
def _nz(s: str | None) -> str:
    """Return a safe, stripped string with NULs removed (Postgres-safe)."""
    if not s:
        return ""
    return s.replace("\x00", "").strip()

def _set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, doseq=True), parts.fragment))


# ---------- RSS-based EO feeds for states that actually provide them ----------
STATE_EO_FEEDS: Dict[str, str] = {
    # keep others; California uses Newsroom scraping (see below)
    "Texas":        "https://gov.texas.gov/news/rss",
    "New York":     "https://www.governor.ny.gov/news/rss.xml",
    "Florida":      "https://www.flgov.com/feed/",
    #"Pennsylvania": "https://www.governor.pa.gov/feed/",
    "Washington":   "https://www.governor.wa.gov/rss",
}

_IL_DATE_PATTERNS = [
    r'<meta[^>]+property=["\']og:updated_time["\'][^>]+content=["\']([^"\']+)["\']',
    r'<meta[^>]+itemprop=["\']datePublished["\'][^>]+content=["\']([^"\']+)["\']',
    r'<meta[^>]+name=["\']publish[-_ ]?date["\'][^>]+content=["\']([^"\']+)["\']',
    r'<time[^>]+datetime=["\']([^"\']+)["\']',                           # <time datetime="...">
    r'(?i)\b(Released|Published):?\s*([A-Z][a-z]+ \d{1,2}, \d{4})',      # Released: August 22, 2025
    r'(?i)\b([A-Z][a-z]+ \d{1,2}, \d{4})\b',                             # August 22, 2025
    r'News\s*[–-]\s*(?:[A-Za-z]+,\s*)?([A-Z][a-z]+ \d{1,2}, \d{4})'
]

# near other helpers
_IL_PDF_DATE_PAT = re.compile(r'(20\d{2})[-_]?(\d{2})[-_]?(\d{2})')  # 2025-08-22 or 20250822

# Only treat real detail pages as "article" URLs, not listing/pagination URLs
_FL_ARTICLE_HREF_RE = re.compile(
    r'href=["\'](?P<u>(?:https?://www\.flgov\.com)?/eog/news/'
    r'(?:press|executive-orders)/[^"\']+)["\']',
    re.I,
)

MASS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.mass.gov/",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

# ---------- Washington (governor.wa.gov) Drupal Views AJAX ----------

WA_LIST_URL = "https://governor.wa.gov/news/news-releases"
WA_AJAX_URL = "https://governor.wa.gov/views/ajax"

_WA_VIEW_DOM_ID_RE = re.compile(
    r'data-view-dom-id="(?P<id>[a-f0-9]{32,})"|'
    r'"view_dom_id"\s*:\s*"(?P<id2>[a-f0-9]{32,})"',
    re.I
)

# article links on governor.wa.gov are usually under /news/...
_WA_ARTICLE_HREF_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://governor\.wa\.gov)?/(?:news|news-releases)/[^\'"#?]+)[\'"]',
    re.I
)


_WA_HUMAN_DATE_RE = re.compile(
    r'\b('
    r'January|February|March|April|May|June|July|August|September|October|November|December'
    r')\s+\d{1,2},\s+\d{4}\b'
)

_WA_DRUPAL_SETTINGS_RE = re.compile(
    r"drupalSettings\s*=\s*(\{.*?\});",
    re.I | re.S
)

_WA_AJAXVIEWS_RE = re.compile(
    r'ajaxViews\s*:\s*(\{.*?\})\s*,\s*ajaxTrustedUrl',
    re.I | re.S
)

_WA_DRUPAL_SETTINGS_JSON_RE = re.compile(
    r'(?is)<script[^>]+data-drupal-selector=["\']drupal-settings-json["\'][^>]*>(?P<json>\{.*?\})</script>'
)

# ---------- Illinois type/status helpers ----------

IL_ALLOWED_LABELS = {
    "news": "News",
    "news release": "News Release",
    "press release": "Press Release",
    "director's letters": "Director's Letters",
    "directors letters": "Director's Letters",
    "announcement": "Announcement",
}

IL_STATUS_BY_LABEL = {
    "News": "news",
    "News Release": "news_release",
    "Press Release": "press_release",
    "Director's Letters": "directors_letters",
    "Announcement": "announcement",
}

# Generic/non-unique titles commonly used as <h1> on IL pages
_IL_GENERIC_TITLES = {
    "news", "news release", "press release",
    "director's letters", "directors letters",
    "announcement", "notice",
    "release",  # ✅ ADD
}

_IL_LISTING_LABEL_RE = re.compile(
    r'(?i)\b(News Release|Press Release|Director\'s Letters|Directors Letters|Announcement|News)\b\s*[-–]',
)

def _il_pick_category_label(text: str) -> str | None:
    """
    Parse listing/byline like 'IDOR, News - Wednesday, ...' and return canonical label.
    """
    if not text:
        return None
    m = _IL_LISTING_LABEL_RE.search(text)
    if not m:
        return None
    raw = (m.group(1) or "").strip().lower()
    return IL_ALLOWED_LABELS.get(raw)

def _il_desc_is_generic(desc: str) -> bool:
    """
    AppSearch sometimes returns a generic statewide boilerplate (e.g., traveler info).
    If we see those patterns, never use it as a summary fallback.
    """
    d = (desc or "").strip().lower()
    if not d:
        return True

    # The repeated bad one you showed:
    if (
        "department of transportation" in d
        and "traveler information" in d
        and "road conditions" in d
    ):
        return True

    # Other common boilerplate-ish patterns
    if "javascript" in d and "enable" in d:
        return True

    # Too short to be useful
    if len(d) < 40:
        return True

    return False


def _il_extract_title(html: str, fallback: str = "") -> str:
    """
    Illinois pages sometimes use a generic H1 like 'release' while the real headline
    is an H2 or a different header block. Prefer OG/Twitter, then best H1/H2, then <title>.
    """
    if not html:
        return fallback

    def _clean(s: str) -> str:
        s = re.sub(r"(?is)<[^>]+>", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _is_generic(t: str) -> bool:
        tl = (t or "").strip().lower()
        if not tl:
            return True
        if tl in _IL_GENERIC_TITLES:
            return True
        # super short single-word section labels commonly occur
        if len(tl) <= 8 and " " not in tl:
            return True
        return False

    # 1) og:title
    m = re.search(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html)
    if m:
        t = re.sub(r"\s+", " ", m.group(1)).strip()
        if t and not _is_generic(t):
            return t

    # 2) twitter:title
    m = re.search(r'(?is)<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\'](.*?)["\']', html)
    if m:
        t = re.sub(r"\s+", " ", m.group(1)).strip()
        if t and not _is_generic(t):
            return t

    # 3) Collect H1 and H2 candidates (pick longest non-generic)
    cands: list[str] = []
    for tag in ("h1", "h2"):
        for mh in re.finditer(rf'(?is)<{tag}[^>]*>(.*?)</{tag}>', html):
            t = _clean(mh.group(1) or "")
            if t and not _is_generic(t):
                cands.append(t)

    if cands:
        cands.sort(key=len, reverse=True)
        return cands[0]

    # 4) title tag fallback (often "Some Headline - Illinois.gov")
    m = re.search(r'(?is)<title[^>]*>(.*?)</title>', html)
    if m:
        t = _clean(m.group(1) or "")
        # strip common suffixes
        t = re.sub(r'(?i)\s*[-|]\s*illinois(\.gov)?\s*$', "", t).strip()
        if t and not _is_generic(t):
            return t

    return fallback

def _wa_get_ajax_page_state(html: str) -> tuple[str, str] | None:
    """
    Returns (theme, libraries) from Drupal settings JSON.
    Needed for WA /views/ajax calls.
    """
    if not html:
        return None

    m = _WA_DRUPAL_SETTINGS_JSON_RE.search(html)
    if not m:
        return None

    try:
        settings = json.loads(m.group("json"))
    except Exception:
        return None

    aps = settings.get("ajaxPageState") or settings.get("ajax_page_state") or {}
    theme = (aps.get("theme") or "").strip()
    libraries = (aps.get("libraries") or "").strip()

    if not theme or not libraries:
        return None

    return theme, libraries


def _wa_js_object_to_json(s: str) -> str:
    """
    Best-effort transform of a JS object literal into JSON.
    Works for drupalSettings/views/ajaxViews blobs (simple objects).
    """
    if not s:
        return s

    # Remove JS comments
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)
    s = re.sub(r"//[^\n]*", "", s)

    # Quote unquoted keys: {foo: 1} -> {"foo": 1}
    s = re.sub(r'([{,]\s*)([A-Za-z0-9_]+)\s*:', r'\1"\2":', s)

    # Convert single quotes to double quotes (good enough for these blobs)
    s = re.sub(r"'", r'"', s)

    # Remove trailing commas: {"a":1,} -> {"a":1}
    s = re.sub(r",\s*([}\]])", r"\1", s)

    return s

def _wa_get_ajax_view_settings(html: str, view_dom_id: str) -> dict | None:
    """
    Return the ajaxViews entry matching view_dom_id.
    Drupal often uses ajaxViews keys == dom_id, and the object is JS (not JSON),
    so we parse only the ajaxViews blob with a tolerant converter.
    """
    if not html or not view_dom_id:
        return None

    m = _WA_AJAXVIEWS_RE.search(html)
    if not m:
        return None

    blob = m.group(1)
    try:
        ajax_views = json.loads(_wa_js_object_to_json(blob))
    except Exception:
        ajax_views = None

    if isinstance(ajax_views, dict):
        # 1) Most common: key == dom_id
        if view_dom_id in ajax_views and isinstance(ajax_views[view_dom_id], dict):
            return ajax_views[view_dom_id]

        # 2) Sometimes key contains dom_id
        for k, v in ajax_views.items():
            if isinstance(k, str) and view_dom_id in k and isinstance(v, dict):
                return v

        # 3) Sometimes value contains view_dom_id
        for k, v in ajax_views.items():
            if isinstance(v, dict):
                vd = (v.get("view_dom_id") or v.get("viewDomId") or "").strip()
                if vd == view_dom_id:
                    return v

    # Fallback: regex-pick required fields near the dom_id
    # (This avoids being totally blocked if parsing fails.)
    chunk = ""
    pos = html.find(view_dom_id)
    if pos != -1:
        chunk = html[max(0, pos - 2000): pos + 4000]

    def _pick(name: str) -> str:
        mm = re.search(rf'{name}\s*:\s*["\']([^"\']+)["\']', chunk, re.I)
        return (mm.group(1) if mm else "") or ""

    view_name = _pick("view_name")
    view_display_id = _pick("view_display_id")
    view_path = _pick("view_path")
    view_base_path = _pick("view_base_path")
    view_args = _pick("view_args")
    pager_element = ""
    mm = re.search(r'pager_element\s*:\s*(\d+)', chunk, re.I)
    if mm:
        pager_element = mm.group(1)

    if view_name and view_display_id:
        return {
            "view_name": view_name,
            "view_display_id": view_display_id,
            "view_args": view_args,
            "view_path": view_path,
            "view_base_path": view_base_path,
            "pager_element": pager_element or "0",
        }

    return None

def _abs_wagov(u: str) -> str:
    if not u:
        return u
    if u.startswith("http"):
        return u.split("?")[0].split("#")[0]
    return ("https://governor.wa.gov" + u).split("?")[0].split("#")[0]

def _wa_get_view_dom_id(html: str) -> str | None:
    if not html:
        return None
    m = _WA_VIEW_DOM_ID_RE.search(html)
    if not m:
        return None
    return (m.group("id") or m.group("id2") or "").strip() or None

def _wa_extract_html_from_drupal_ajax(payload: object) -> str:
    """
    Drupal views/ajax returns JSON: a list of commands.
    Each command may include {"data": "<html...>"} fragments.
    """
    if not isinstance(payload, list):
        return ""
    bits: list[str] = []
    for cmd in payload:
        if isinstance(cmd, dict):
            data = cmd.get("data")
            if isinstance(data, str) and data.strip():
                bits.append(data)
    return "\n".join(bits)

async def _wa_fetch_ajax_page(
    cx: httpx.AsyncClient,
    view_dom_id: str,
    page: int,
    theme: str,
    libraries: str,
) -> str:
    params = {
        "_wrapper_format": "drupal_ajax",
        "view_name": "news",
        "view_display_id": "news_releases",
        "view_args": "",
        "view_path": "/node/12328",
        "view_base_path": "rss/news.xml",
        "view_dom_id": view_dom_id,
        "pager_element": "0",
        "page": str(page),
        "_drupal_ajax": "1",
        "ajax_page_state[theme]": theme,
        "ajax_page_state[theme_token]": "",
        "ajax_page_state[libraries]": libraries,
    }

    r = await cx.get(
        WA_AJAX_URL,
        params=params,
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": WA_LIST_URL,
        },
        timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
    )

    if r.status_code >= 400 or not r.text:
        print("WA ajax error:", r.status_code, (r.text or "")[:200])
        return ""

    try:
        j = r.json()
    except Exception:
        print("WA ajax non-json response:", r.status_code, (r.text or "")[:200])
        return ""

    return _wa_extract_html_from_drupal_ajax(j)


def _wa_parse_listing_fragment_for_urls(fragment_html: str) -> list[str]:
    if not fragment_html:
        return []
    urls: list[str] = []
    seen: set[str] = set()
    for m in _WA_ARTICLE_HREF_RE.finditer(fragment_html):
        u = _abs_wagov((m.group("u") or "").strip())
        if u and u not in seen:
            seen.add(u)
            urls.append(u)
    return urls

async def _collect_wa_news_urls(
    cx: httpx.AsyncClient,
    max_pages: int = 50,
    limit: int = 500,
) -> list[str]:

    r = await cx.get(
        WA_LIST_URL,
        headers={**BROWSER_UA_HEADERS, "Referer": "https://governor.wa.gov/"},
        timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
    )
    if r.status_code >= 400 or not r.text:
        return []

    view_dom_id = _wa_get_view_dom_id(r.text)
    if not view_dom_id:
        print("WA: could not find view_dom_id on listing page.")
        return []

    aps = _wa_get_ajax_page_state(r.text)
    if not aps:
        print("WA: could not find ajaxPageState (theme/libraries) on listing page.")
        return []
    theme, libraries = aps


    urls: list[str] = []
    seen: set[str] = set()

    for p in range(0, max_pages):
        frag = await _wa_fetch_ajax_page(
            cx,
            view_dom_id=view_dom_id,
            page=p,
            theme=theme,
            libraries=libraries,
        )

        page_urls = _wa_parse_listing_fragment_for_urls(frag)

        if not page_urls:
            print("WA: stopping at page", p, "(no URLs)")
            break

        page_new = 0
        for u in page_urls:
            if u not in seen:
                seen.add(u)
                urls.append(u)
                page_new += 1
                if len(urls) >= limit:
                    return urls[:limit]

        print(f"WA: page={p} parsed={len(page_urls)} new={page_new} total={len(urls)}")

        if page_new == 0:
            break

        await asyncio.sleep(0.25)

    return urls[:limit]

# ----------------------------
# Washington Executive Orders (table -> PDFs)
# ----------------------------

WA_EO_CURRENT_URL = "https://governor.wa.gov/office-governor/office/official-actions/executive-orders"
WA_EO_PREV_URL = "https://governor.wa.gov/office-governor/office/official-actions/executive-orders?combine=&governor=71&field_executive_order_status_target_id=All"


# ----------------------------
# Washington Proclamations (listing -> PDFs)
# ----------------------------

WA_PROC_URL = "https://governor.wa.gov/office-governor/office/official-actions/proclamations"
WA_PROC_STOP_AT_PDF = "https://governor.wa.gov/sites/default/files/proclamations/24-01%20-%20December%20Storm%20Damage.pdf"

# only pick proclamation PDFs from this subtree
_WA_PROC_PDF_RE = re.compile(
    r'href=["\'](?P<u>(?:https?://governor\.wa\.gov)?/sites/default/files/proclamations/[^"\']+\.pdf)["\']',
    re.I
)

def _wa_norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u = u.split("#")[0]
    return u  # keep queryless; PDFs won't have pagination queries anyway


# Stop once we reach this EO number (inclusive). Adjust if you want older.
WA_EO_STOP_AT_NUMBER = "24-01"


def _wa_abs(href: str) -> str:
    """
    Make EO PDF links absolute for governor.wa.gov.
    """
    if not href:
        return ""
    href = html_lib.unescape(href.strip())
    href = href.split("#")[0]
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://governor.wa.gov" + href
    return "https://governor.wa.gov/" + href


def _wa_with_page(url: str, page: int) -> str:
    """
    Adds/replaces ?page=N on a URL.
    """
    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs["page"] = str(page)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs, doseq=True), parts.fragment))


def _wa_extract_eo_rows_from_html(html: str) -> list[tuple[str, str, str, datetime | None]]:
    """
    Parse the WA EO table.
    Returns list of: (eo_number, eo_title, pdf_url, issued_dt_from_table_or_none)

    Expected row pattern (typical):
      <tr>
        <td>24-01</td>
        <td>01/30/2024</td>
        <td><a href="/sites/default/files/...pdf">Title ...</a></td>
      </tr>
    """
    out: list[tuple[str, str, str, datetime | None]] = []
    if not html:
        return out

    for tr in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", html):
        tds = re.findall(r"(?is)<td[^>]*>(.*?)</td>", tr)
        if len(tds) < 3:
            continue

        eo_number = _strip_html_to_text(tds[0]).strip()
        issued_raw = _strip_html_to_text(tds[1]).strip()
        title_cell = tds[2]

        eo_title = _strip_html_to_text(title_cell).strip()

        m = re.search(r'(?is)href=["\']([^"\']+\.pdf[^"\']*)["\']', title_cell)
        if not m:
            continue

        pdf_url = _wa_abs(m.group(1))

        issued_dt: datetime | None = None
        try:
            issued_dt = datetime.strptime(issued_raw, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except Exception:
            issued_dt = None

        if eo_number and pdf_url:
            out.append((eo_number, eo_title, pdf_url, issued_dt))

    return out

# ----------------------------
# Washington EO "Signed and sealed..." PDF date
# ----------------------------

_WA_EO_SIGNED_RE = re.compile(
    r'(?is)'
    r'signed\s+and\s+sealed.*?'
    r'on\s+this\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+'
    r'(january|february|march|april|may|june|july|august|september|october|november|december)'
    r'\s*,?\s*ad\s*,?\s*'
    r'('
    r'(?:\d{4})'
    r'|'
    r'(?:two\s+thousand(?:\s+and)?(?:\s+[a-z\- ]+?)?)'
    r')'
    r'(?=\s*(?:,|\sat\b))'   # ✅ stop year phrase before comma or "at ..."
)

_WA_NUM_WORDS_0_19 = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
    "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
}
_WA_TENS = {
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}

def _wa_parse_0_99_words(s: str) -> int | None:
    """
    Parse 'twenty five' / 'twenty-five' / 'nineteen' -> 0..99.
    Robust to trailing junk like 'twenty-four at olympia washington' by
    stopping once tokens stop being number words.
    """
    s = (s or "").strip().lower()
    if not s:
        return 0

    s = s.replace("-", " ")
    toks_all = [t for t in re.split(r"\s+", s) if t and t != "and"]

    # keep only the leading numeric words; stop at first unknown word
    toks: list[str] = []
    for t in toks_all:
        if t in _WA_NUM_WORDS_0_19 or t in _WA_TENS:
            toks.append(t)
            continue
        break

    if not toks:
        return 0

    if len(toks) == 1:
        if toks[0] in _WA_NUM_WORDS_0_19:
            return _WA_NUM_WORDS_0_19[toks[0]]
        if toks[0] in _WA_TENS:
            return _WA_TENS[toks[0]]
        return None

    # e.g. "twenty four"
    if toks[0] in _WA_TENS and toks[1] in _WA_NUM_WORDS_0_19:
        return _WA_TENS[toks[0]] + _WA_NUM_WORDS_0_19[toks[1]]

    return None

def _wa_year_from_words(year_phrase: str) -> int | None:
    """
    Handles:
      - 'Two Thousand and Twenty-Five'
      - 'two thousand and twenty-four'
      - 'two thousand twenty five'
    Assumes 2000..2099.
    """
    yp = (year_phrase or "").strip().lower()
    yp = re.sub(r"[^a-z0-9\s\-]", " ", yp)
    yp = re.sub(r"\s+", " ", yp).strip()

    # numeric year
    if re.fullmatch(r"\d{4}", yp):
        try:
            return int(yp)
        except Exception:
            return None

    if "two thousand" not in yp:
        return None

    tail = yp.split("two thousand", 1)[1].strip()
    tail = tail.lstrip()  # may start with 'and ...'
    tail = re.sub(r"^\s*and\s+", "", tail)

    # If tail empty => 2000
    n = _wa_parse_0_99_words(tail)
    if n is None:
        return None
    return 2000 + n

def _wa_date_from_signed_and_sealed(pdf_text: str) -> datetime | None:
    """
    Extracts WA EO date from the signature block:
    'Signed and sealed ... on this 18th day of December, AD, Two Thousand and Twenty-Five ...'
    """
    if not pdf_text:
        return None

    txt = re.sub(r"\s+", " ", pdf_text).strip()
    m = _WA_EO_SIGNED_RE.search(txt)
    if not m:
        return None

    try:
        day = int(m.group(1))
        month_name = (m.group(2) or "").strip().title()
        year_raw = (m.group(3) or "").strip()
        year = _wa_year_from_words(year_raw)
        if not year:
            return None

        month = datetime.strptime(month_name, "%B").month
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None



# Very safe PDF date parse: just look for "Month DD, YYYY" anywhere in PDF text
_WA_PDF_HUMAN_DATE_RE = re.compile(
    r"\b("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+\d{1,2},\s+\d{4}\b"
)

def _wa_date_from_pdf_text(pdf_text: str) -> datetime | None:
    """
    WA EO date priority:
      1) Signed-and-sealed signature block (best)
      2) Fallback: any 'Month DD, YYYY' in PDF text
    """
    if not pdf_text:
        return None

    # ✅ 1) Best: signature block
    dt_sig = _wa_date_from_signed_and_sealed(pdf_text)
    if dt_sig:
        return dt_sig

    # ✅ 2) Fallback: human date
    txt = re.sub(r"\s+", " ", pdf_text)
    m = _WA_PDF_HUMAN_DATE_RE.search(txt)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    

# ----------------------------
# Washington Proclamation PDF date ("Signed and sealed...")
# ----------------------------

_WA_PROC_SIGNED_RE = re.compile(
    r'(?is)'
    r'signed\s+and\s+sealed.*?'
    r'(?:on\s+this\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+'
    r'(january|february|march|april|may|june|july|august|september|october|november|december)'
    r'.{0,80}?'                              # allow "A.D.," and spacing
    r'(?:a\.?\s*d\.?\.?,?\s*)'               # A.D. (optional punctuation)
    r'('
    r'(?:\d{4})'
    r'|'
    r'(?:two\s+thousand(?:\s+and)?(?:\s+[a-z\- ]+?)?)'
    r')'
    r'(?=\s*(?:,|\sat\b))'                   # stop before comma or "at Olympia..."
)

def _wa_date_from_proc_pdf_text(pdf_text: str) -> datetime | None:
    """
    Extract proclamation date from:
      'Signed and sealed ... this 16th day of December A.D., two thousand and twenty-five, at Seattle...'
      '... this 24th day of January A.D., Two Thousand and Twenty-Four at Olympia...'
    """
    if not pdf_text:
        return None

    txt = re.sub(r"\s+", " ", pdf_text).strip()
    m = _WA_PROC_SIGNED_RE.search(txt)
    if not m:
        return None

    try:
        day = int(m.group(1))
        month_name = (m.group(2) or "").strip().title()
        year_raw = (m.group(3) or "").strip()

        year = _wa_year_from_words(year_raw)
        if not year:
            # numeric year fallback
            if re.fullmatch(r"\d{4}", year_raw):
                year = int(year_raw)
            else:
                return None

        month = datetime.strptime(month_name, "%B").month
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None


async def _collect_wa_executive_orders(
    cx: httpx.AsyncClient,
    max_pages_each: int = 50,
    limit_each: int = 2000,
) -> list[tuple[str, str, str, datetime | None]]:
    """
    Collect EOs from:
      1) current governor page (WA_EO_CURRENT_URL)
      2) previous governor page (WA_EO_PREV_URL) paginated with ?page=N

    Stops once WA_EO_STOP_AT_NUMBER is encountered (inclusive).
    """
    max_pages_each = max(1, int(max_pages_each or 50))
    limit_each = max(1, int(limit_each or 2000))

    collected: list[tuple[str, str, str, datetime | None]] = []
    seen_pdf: set[str] = set()

    # 1) current page
    r0 = await _get(cx, WA_EO_CURRENT_URL, headers={**BROWSER_UA_HEADERS, "Referer": "https://governor.wa.gov/"})
    if r0.status_code < 400 and r0.text:
        rows = _wa_extract_eo_rows_from_html(r0.text)
        for eo_number, eo_title, pdf_url, issued_dt in rows:
            if pdf_url in seen_pdf:
                continue
            seen_pdf.add(pdf_url)
            collected.append((eo_number, eo_title, pdf_url, issued_dt))
            if len(collected) >= limit_each:
                return collected

            if (eo_number or "").strip() == WA_EO_STOP_AT_NUMBER:
                return collected

    # 2) previous governor page, paginated
    for page in range(0, max_pages_each):
        page_url = _wa_with_page(WA_EO_PREV_URL, page)
        r = await _get(cx, page_url, headers={**BROWSER_UA_HEADERS, "Referer": WA_EO_CURRENT_URL})
        if r.status_code >= 400 or not r.text:
            break

        rows = _wa_extract_eo_rows_from_html(r.text)
        if not rows:
            break

        for eo_number, eo_title, pdf_url, issued_dt in rows:
            if pdf_url in seen_pdf:
                continue
            seen_pdf.add(pdf_url)
            collected.append((eo_number, eo_title, pdf_url, issued_dt))

            # stop at 24-01 inclusive
            if (eo_number or "").strip() == WA_EO_STOP_AT_NUMBER:
                return collected

            if len(collected) >= limit_each:
                return collected

        await asyncio.sleep(0.15)

    return collected

async def _collect_wa_proclamation_pdfs(
    cx: httpx.AsyncClient,
    max_pages: int = 50,
    limit: int = 500,
    stop_at_pdf: str = WA_PROC_STOP_AT_PDF,
) -> list[tuple[str, str]]:
    """
    Returns list of (pdf_url, title_guess) newest -> older.
    Stops once stop_at_pdf is encountered (inclusive).
    """
    max_pages = max(1, int(max_pages or 50))
    limit = max(1, int(limit or 500))

    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    stop_at_pdf = _wa_norm_url(stop_at_pdf)

    for page in range(0, max_pages):
        page_url = _wa_with_page(WA_PROC_URL, page)
        r = await _get(cx, page_url, headers={**BROWSER_UA_HEADERS, "Referer": WA_PROC_URL})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text or ""
        hits = list(_WA_PROC_PDF_RE.finditer(html))
        if not hits:
            break

        page_new = 0

        for m in hits:
            raw = (m.group("u") or "").strip()
            pdf_url = _wa_abs(raw)  # reuse your WA absolute helper
            pdf_url = _wa_norm_url(pdf_url)
            if not pdf_url or pdf_url in seen:
                continue

            # Title guess: use filename (decoded-ish) OR try anchor text nearby
            title_guess = pdf_url.rsplit("/", 1)[-1]
            title_guess = re.sub(r"(?i)\.pdf$", "", title_guess).replace("%20", " ").strip()

            # Try to capture anchor text for this exact href (often cleaner than filename)
            # (best-effort; safe if fails)
            try:
                anchor_pat = r'(?is)<a[^>]+href=["\']%s["\'][^>]*>(?P<t>.*?)</a>' % re.escape(raw)
                ma = re.search(anchor_pat, html)
                if ma:
                    t = re.sub(r"(?is)<[^>]+>", " ", ma.group("t") or "")
                    t = re.sub(r"\s+", " ", t).strip()
                    if t and len(t) >= 3:
                        title_guess = t
            except Exception:
                pass

            seen.add(pdf_url)
            out.append((pdf_url, title_guess))
            page_new += 1

            # stop at your target PDF (inclusive)
            if stop_at_pdf and pdf_url == stop_at_pdf:
                return out

            if len(out) >= limit:
                return out

        if page_new == 0:
            break

        await asyncio.sleep(0.15)

    return out



_FL_LISTING_ROOTS = [
    "https://www.flgov.com/eog/news/press",
    "https://www.flgov.com/eog/news/executive-orders",
]


TEXAS_MIN_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

# New York newsroom: only keep items published on/after 2025-06-01.
NY_NEWS_MIN_DATE = datetime(2025, 6, 1, tzinfo=timezone.utc)

# California: only ingest down to the first 2025 item (inclusive)
CA_MIN_DATE = datetime(2025, 1, 2, tzinfo=timezone.utc)

async def ingest_state_executive_orders(states: List[str] | None = None) -> dict:
    targets = states or list(STATE_EO_FEEDS.keys())
    out = {}
    async with connection() as conn:
        for state in targets:
            url = STATE_EO_FEEDS.get(state)
            if not url:
                out[state] = {"upserted": 0, "skipped": "no_feed_configured"}
                continue

            src = await conn.fetchrow(
                """
                insert into sources(name, kind, base_url)
                values($1,$2,$3)
                on conflict (name) do update
                    set kind = excluded.kind, base_url = excluded.base_url
                returning id
                """,
                f"{state} Governor – Executive Orders (RSS)",
                "state_governor_eo_rss",
                url
            )
            source_id = src["id"]
            feed = await fetch_rss(url)
            rows = map_rss_to_rows(
                feed,
                source_id=source_id,
                jurisdiction=state.lower(),
                agency=f"{state} Governor",
                default_status="notice",
            )
            count = await upsert_items_from_rows(conn, rows)
            out[state] = {"upserted": count}
    return out

# ---------- Newsroom scraping for states without a usable EO feed ----------
STATE_NEWSROOM_SITES: Dict[str, str] = {
    "California": "https://www.gov.ca.gov/newsroom/",
    "Florida":    "https://www.flgov.com/eog/news",          # <- add
    "Texas":      "https://gov.texas.gov/news/",
    "New York":   "https://www.governor.ny.gov/news",   # 👈 add this
    "Illinois":   "https://www.illinois.gov/search-results.html?contentType=news&q=",
    "Pennsylvania": "https://www.pa.gov/governor/newsroom",
    "Massachusetts": "https://www.mass.gov/press-releases/recent?page=0",
    "Washington": "https://governor.wa.gov/news/news-releases",
}

# Also crawl these listing pages for CA (they point to the same dated posts)
STATE_EXTRA_LISTS: Dict[str, List[str]] = {
    "California": [
        "https://www.gov.ca.gov/category/press-releases/",
        "https://www.gov.ca.gov/category/executive-orders/",
        "https://www.gov.ca.gov/category/featured/",
        "https://www.gov.ca.gov/category/first-partner/",
        "https://www.gov.ca.gov/category/media-advisories/",
        "https://www.gov.ca.gov/category/proclamations/",
        # keep your tag page if you still want it
        "https://www.gov.ca.gov/tag/legislation/",
    ],

    "Florida": [                                            # <- add
        "https://www.flgov.com/eog/news/press",
        "https://www.flgov.com/eog/news/executive-orders",
    ],
     "Texas": [   # <- add legislative category
        "https://gov.texas.gov/news/category/press-release",
        "https://gov.texas.gov/news/category/appointment",
        "https://gov.texas.gov/news/category/proclamation",
        "https://gov.texas.gov/news/category/legislative",
    ],
    "New York": [   # 👈 add this
        "https://www.governor.ny.gov/executiveorders"
    ],
}

# find article URLs like https://www.gov.ca.gov/2025/08/22/some-slug/
_CA_NEWS_URL_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.gov\.ca\.gov)?/\d{4}/\d{2}/\d{2}/[^\'"#]+/)[\'"]',
    re.I
)


# pagination via rel="next" (works even if the “Older” label changes)
_CA_PAGER_NEXT_RE = re.compile(
    r'<a[^>]+rel=[\'"]?next[\'"]?[^>]*href=[\'"](?P<u>[^\'"]+)[\'"]',
    re.I
)

# AFTER (allow optional domain + relative paths)
_FL_NEWS_URL_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.flgov\.com)?/eog/news/(?!executive-orders)[^\'"#]+)[\'"]',
    re.I
)
_FL_EO_PDF_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.flgov\.com)?'
    r'(?:/eog)?/sites/default/files/executive-orders/'
    r'\d{4}/EO(?:%20|%2520|[- ])\d{2}-\d+\.pdf)[\'"]',
    re.I
)

# one match = one <tr> block; we'll parse inside it
_FL_EO_ROW_RE = re.compile(
    r'<tr[^>]*>(?P<row>.*?)</tr>',
    re.I | re.S,
)

_FL_EO_DETAIL_RE = re.compile(
    r'href=["\'](?P<u>(?:https?://www\.flgov\.com)?/eog/news/executive-orders/[^"\']+)["\']',
    re.I,
)


# Florida newsroom: dates like "November 21, 2025" OR
# split across lines: "November\n21\n2025"
_FL_HUMAN_DATE_RE = re.compile(
    r'\b([A-Z][a-z]+)\s+(\d{1,2})\s*,?\s*(\d{4})\b',
    re.S,
)

# NEW: explicit "DATE: Friday, December 5, 2025" line on press pages
_FL_PRESS_DATE_LINE_RE = re.compile(
    r'DATE:\s*(?:[A-Za-z]+,\s*)?([A-Z][a-z]+ \d{1,2}, \d{4})',
    re.I
)

# ---------- Texas helpers ----------

_TX_HUMAN_DATE_RE = re.compile(
    r'\b('
    r'January|February|March|April|May|June|July|August|September|October|November|December'
    r')\s+\d{1,2},\s+\d{4}\b'
)

_TX_CATEGORY_RE = re.compile(
    r'(?:January|February|March|April|May|June|July|August|September|October|November|December)'
    r'\s+\d{1,2},\s+\d{4}\s*\|\s*[^|]+\|\s*'
    r'(Press Release|Appointment|Proclamation|Legislative Statement)',
    re.I
)

# ---------- New York helpers ----------

_NY_CATEGORY_RE = re.compile(
    r'>\s*(Statement|Press Release)\s*<',
    re.I,
)

# e.g. "November 24, 2025" at top of the article
_NY_HUMAN_DATE_RE = re.compile(
    r'\b('
    r'January|February|March|April|May|June|July|August|September|October|November|December'
    r')\s+\d{1,2},\s+\d{4}\b'
)

# ---------- Pennsylvania (pa.gov) helpers ----------

PA_NEWSROOM_LISTING = "https://www.pa.gov/governor/newsroom#sortCriteria=%40copapwpeffectivedate%20descending"

# You MUST set this once after capturing it in DevTools → Network (Fetch/XHR).
# Put the full URL here (or set env var PA_GOV_NEWSROOM_API).
PA_GOV_NEWSROOM_API = os.getenv("PA_GOV_NEWSROOM_API", "").strip()

PA_GOV_NEWSROOM_METHOD = os.getenv("PA_GOV_NEWSROOM_METHOD", "POST").strip().upper()

# If the XHR requires extra headers (often does), store JSON in env:
# export PA_GOV_NEWSROOM_HEADERS='{"Accept":"application/json","Content-Type":"application/json"}'
PA_GOV_NEWSROOM_HEADERS = os.getenv("PA_GOV_NEWSROOM_HEADERS", "").strip()

IL_APPSEARCH_URL = os.getenv("IL_APPSEARCH_URL", "").strip()
IL_APPSEARCH_TOKEN = os.getenv("IL_APPSEARCH_TOKEN", "").strip()

def _il_appsearch_payload(page_current: int, page_size: int = 10) -> dict:
    return {
        "query": "",
        "page": {"size": page_size, "current": page_current},
        "result_fields": {
            "url": {"raw": {}},
            "title": {"raw": {}},
            "description": {"raw": {}},
            "articledate": {"raw": {}},
            "lastmodified": {"raw": {}},
            "contenttype": {"raw": {}},
            "path": {"raw": {}},
            "sitelink": {"raw": {}},
        },
        "filters": {"contenttype": ["news"]},
        "sort": {"articledate": "desc"},
    }

def _try_parse_isoish_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

async def _il_appsearch_fetch(cx: httpx.AsyncClient, page_current: int, page_size: int = 10) -> dict | None:
    if not IL_APPSEARCH_URL or not IL_APPSEARCH_TOKEN:
        print("IL AppSearch: missing IL_APPSEARCH_URL or IL_APPSEARCH_TOKEN")
        return None

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {IL_APPSEARCH_TOKEN}",
        "Origin": "https://www.illinois.gov",
        "Referer": "https://www.illinois.gov/",
        **BROWSER_UA_HEADERS,
    }

    try:
        r = await cx.post(
            IL_APPSEARCH_URL,
            json=_il_appsearch_payload(page_current, page_size),
            headers=headers,
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        )
        if r.status_code >= 400:
            print("IL AppSearch error:", r.status_code, r.text[:300])
            return None
        return r.json()
    except Exception as e:
        print("IL AppSearch exception:", repr(e))
        return None

def _il_pick_url(rec: dict) -> str:
    # AppSearch fields look like {"raw": "..."}
    for k in ("url", "sitelink", "path"):
        v = rec.get(k)
        if isinstance(v, dict):
            raw = v.get("raw")
            if isinstance(raw, str) and raw.strip():
                u = raw.strip().split("#")[0]
                if u.startswith("/"):
                    u = "https://www.illinois.gov" + u
                return u
    return ""

async def _collect_florida_eo_pdfs(
    cx: httpx.AsyncClient,
    years: list[int] = [2026, 2025, 2024],
    max_pages_per_year: int = 200,
) -> list[str]:
    """
    Florida EO PDFs: crawl server-rendered listing with GET params.

    Key detail:
      field_date_value MUST be the <option value> (e.g., "2"), not the year (e.g., "2025").
    """
    # Load base EO page once to extract the dropdown map (year -> option value)
    r0 = await _get(
        cx,
        FL_EO_LIST_URL,
        headers={**BROWSER_UA_HEADERS, "Referer": "https://www.flgov.com/"},
    )
    if r0.status_code >= 400 or not r0.text:
        return []

    year_map = _fl_extract_year_value_map(r0.text)  # {2026:"1", 2025:"2", ...}
    if not year_map:
        print("FL EO: could not parse year_map from base page")
        return []

    urls: list[str] = []
    seen: set[str] = set()

    # crawl years newest->oldest as passed
    for year in years:
        year_value = year_map.get(year)
        if not year_value:
            print("FL EO: year not present in dropdown:", year)
            continue

        for page in range(0, max_pages_per_year):
            params = {
                # keep the EO category filter (matches the listing behavior)
                "field_eo_category_value": "1",
                # IMPORTANT: option value, not the literal year
                "field_date_value": year_value,
                "page": str(page),
            }

            r = await _get(
                cx,
                FL_EO_LIST_URL,
                params=params,
                headers={**BROWSER_UA_HEADERS, "Referer": FL_EO_LIST_URL},
            )
            if r.status_code >= 400 or not r.text:
                break

            page_new = 0

            # extract PDFs directly from HTML
            for m in _FL_EO_PDF_RE.finditer(r.text):
                raw = (m.group("u") or "").strip()
                u = _abs_flgov(raw)
                if u and u not in seen:
                    seen.add(u)
                    urls.append(u)
                    page_new += 1

            print(f"FL EO GET year={year} val={year_value} page={page} new={page_new} total={len(urls)}")

            # stop paging that year once this page contributes nothing new
            if page_new == 0:
                break

            await asyncio.sleep(0.15)

    return urls


def _il_record_meta(rec: dict) -> tuple[str, str, datetime | None]:
    title = ""
    desc = ""
    pub_dt = None

    t = rec.get("title")
    if isinstance(t, dict) and isinstance(t.get("raw"), str):
        title = t["raw"].strip()

    d = rec.get("description")
    if isinstance(d, dict) and isinstance(d.get("raw"), str):
        desc = d["raw"].strip()

    # prefer articledate; fallback lastmodified
    ad = rec.get("articledate")
    lm = rec.get("lastmodified")
    ad_raw = ad.get("raw") if isinstance(ad, dict) else None
    lm_raw = lm.get("raw") if isinstance(lm, dict) else None
    pub_dt = _try_parse_isoish_date(ad_raw) or _try_parse_isoish_date(lm_raw)

    return title, desc, pub_dt

async def _collect_il_from_appsearch(
    cx: httpx.AsyncClient,
    max_pages: int = 100,
    page_size: int = 10,
) -> tuple[list[str], dict[str, tuple[str, str, datetime | None]]]:
    """
    Returns:
      urls
      meta_by_url[url] = (title_from_search, desc_from_search, pub_dt_from_search)
    """
    urls: list[str] = []
    seen: set[str] = set()
    meta_by_url: dict[str, tuple[str, str, datetime | None]] = {}

    for p in range(1, max_pages + 1):
        data = await _il_appsearch_fetch(cx, page_current=p, page_size=page_size)
        if not data:
            break

        results = data.get("results") or []
        if not isinstance(results, list) or not results:
            break

        new = 0
        for rec in results:
            if not isinstance(rec, dict):
                continue
            u = _il_pick_url(rec)
            if not u:
                continue

            # optional filter: only keep real “article-like” pages + PDFs
            if not _is_il_article_like(u):
                continue

            if u not in seen:
                seen.add(u)
                urls.append(u)
                title, desc, pub_dt = _il_record_meta(rec)
                meta_by_url[u] = (title, desc, pub_dt)
                new += 1

        print(f"IL AppSearch page {p}: new={new} total={len(urls)}")
        if new == 0:
            break

    return urls, meta_by_url


def _abs_pa(u: str) -> str:
    if not u:
        return u
    if u.startswith("http"):
        return u.split("?")[0].split("#")[0]
    # PA listing may return relative paths
    if not u.startswith("/"):
        u = "/" + u
    return ("https://www.pa.gov" + u).split("?")[0].split("#")[0]

def _date_guard_not_future(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    # allow a tiny buffer for timezone/CDN weirdness
    if dt > now + timedelta(days=2):
        return None
    return dt

def _date_from_pa_article(html: str, url: str):
    dt = _date_from_html_or_url(html, url) or _date_from_json_ld(html)
    dt = _date_guard_not_future(dt)
    if dt:
        return dt

    text = _strip_html_to_text(html)
    m = re.search(
        r'\b('
        r'January|February|March|April|May|June|July|August|September|October|November|December'
        r')\s+\d{1,2},\s+\d{4}\b',
        text,
    )
    if not m:
        return None
    try:
        dt2 = datetime.strptime(m.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
        return _date_guard_not_future(dt2)
    except Exception:
        return None

# ---------- Pennsylvania (pa.gov) token helpers ----------

PA_GOV_COVEO_TOKEN = os.getenv("PA_GOV_COVEO_TOKEN", "").strip()

_PA_TOKEN_CACHE: str | None = None
_PA_TOKEN_CACHE_TS: float = 0.0

def _date_from_wa_html(html: str) -> datetime | None:
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _WA_HUMAN_DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _looks_like_coveo_token(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", (s or "").strip()))


async def _pa_discover_coveo_token(cx: httpx.AsyncClient) -> str | None:
    """
    Best-effort token discovery from pa.gov HTML (fallback only).
    If PA stops embedding it, this returns None and you rely on .env.
    """
    try:
        r = await cx.get(
            "https://www.pa.gov/governor/newsroom",
            headers={**BROWSER_UA_HEADERS, "Referer": "https://www.pa.gov/"},
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
            follow_redirects=True,
        )
        if r.status_code >= 400 or not r.text:
            return None

        html = r.text

        # Common embedding patterns
        pats = [
            r'authorization["\']?\s*:\s*["\']Bearer\s+([0-9a-fA-F\-]{36})["\']',
            r'["\']accessToken["\']\s*:\s*["\']([0-9a-fA-F\-]{36})["\']',
            r'Bearer\s+([0-9a-fA-F\-]{36})',
        ]
        for pat in pats:
            m = re.search(pat, html, re.I)
            if m:
                tok = m.group(1).strip()
                if _looks_like_coveo_token(tok):
                    return tok

        # last resort: any UUID on the page
        m2 = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", html)
        if m2 and _looks_like_coveo_token(m2.group(1)):
            return m2.group(1)

        return None
    except Exception:
        return None


async def _pa_get_coveo_token(cx: httpx.AsyncClient, force_refresh: bool = False) -> str | None:
    """
    Token order:
      1) cached token (this process)
      2) env token
      3) best-effort discovery from pa.gov
    """
    global _PA_TOKEN_CACHE, _PA_TOKEN_CACHE_TS

    if not force_refresh and _PA_TOKEN_CACHE and (time.time() - _PA_TOKEN_CACHE_TS) < 3600:
        return _PA_TOKEN_CACHE

    if PA_GOV_COVEO_TOKEN and not force_refresh:
        _PA_TOKEN_CACHE = PA_GOV_COVEO_TOKEN
        _PA_TOKEN_CACHE_TS = time.time()
        return _PA_TOKEN_CACHE

    tok = await _pa_discover_coveo_token(cx)
    if tok:
        _PA_TOKEN_CACHE = tok
        _PA_TOKEN_CACHE_TS = time.time()
        return tok

    return None


async def _pa_fetch_page(cx: httpx.AsyncClient, offset: int, limit: int) -> dict | None:
    if not PA_GOV_NEWSROOM_API:
        print("PA: missing PA_GOV_NEWSROOM_API.")
        return None

    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://www.pa.gov",
        "Referer": "https://www.pa.gov/governor/newsroom",
    }


    tok = await _pa_get_coveo_token(cx, force_refresh=False)
    if not tok:
        print("PA: missing/expired PA_GOV_COVEO_TOKEN (set it in .env)")
        return None
    headers["Authorization"] = f"Bearer {tok}"

    payload = {
        "locale": "en",
        "debug": False,
        "tab": "default",
        "referrer": "default",
        "timezone": "America/New_York",
        "q": "",
        "enableQuerySyntax": False,
        "searchHub": "Gov-News",
        "sortCriteria": "@copapwpeffectivedate descending",
        "numberOfResults": limit,
        "firstResult": offset,
        # Keep fields small; Coveo still returns clickUri/title, but raw fields help.
        "fieldsToInclude": [
            "date",
            "urihash",
            "permanentid",
            "source",
            "collection",
            "copapwptitle",
            "copapwpeffectivedate",
            "copapwparticledate",
            "copapwpcontenttype",
            "copapwpcategory",
            "copapwppagetitle",
        ],
    }

    try:
        api = PA_GOV_NEWSROOM_API
        if "organizationId=" not in api:
            api = api.rstrip("?") + "?organizationId=commonwealthofpennsylvaniaproductiono8jd9ckm"

        r = await cx.post(
            api,
            json=payload,
            headers=headers,
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        )

        # ✅ ADD THIS BLOCK RIGHT HERE (immediately after the first POST)
        if r.status_code in (401, 403):
            fresh = await _pa_get_coveo_token(cx, force_refresh=True)
            if fresh:
                headers["Authorization"] = f"Bearer {fresh}"
                r = await cx.post(
                    api,
                    json=payload,
                    headers=headers,
                    timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
                )

        # 🔎 DEBUG: inspect Coveo response shape (THIS is the right spot)
        print("PA API status:", r.status_code)
        if r.status_code < 400:
            try:
                j = r.json()
                print("PA keys:", list(j.keys())[:20])
                print("PA results count:", len(j.get("results", [])))
            except Exception as e:
                print("PA JSON parse error:", repr(e))

        if r.status_code >= 400:
            print("PA API error body:", r.text[:300])
            return None

        return r.json()

    except Exception as e:
        print("PA API exception:", repr(e))
        return None

def _pa_extract_items(data: dict) -> list[dict]:
    """
    Extract the list of records from the response.
    You must adjust this once you see the actual JSON shape.
    """
    if not isinstance(data, dict):
        return []

    # common shapes:
    #  - { "results": [...] }
    #  - { "items": [...] }
    #  - { "data": { "results": [...] } }
    for key_path in (("results",), ("items",), ("data", "results"), ("data", "items")):
        cur = data
        ok = True
        for k in key_path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, list):
            return cur

    return []

def _pa_pick_url(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""

    # Coveo standard fields
    for k in ("clickUri", "uri"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return _abs_pa(v)

    raw = rec.get("raw")
    if isinstance(raw, dict):
        for k in ("clickuri", "clickUri", "uri", "permanentid", "permanentId"):
            v = raw.get(k)
            if isinstance(v, str) and v.strip():
                return _abs_pa(v)

    # fallback (your older logic)
    u = rec.get("url") or rec.get("link") or rec.get("path")
    if isinstance(u, str) and u.strip():
        return _abs_pa(u)

    return ""

def _pa_status_from_url(url: str) -> str:
    u = (url or "").lower()
    if "press-releases" in u:
        return "press_release"
    return "notice"


async def _collect_pa_gov_newsroom_urls(
    cx: httpx.AsyncClient,
    want: int = 350,
    page_size: int = 50,
) -> list[str]:
    """
    Collect first N item URLs from PA listing via its JSON endpoint.
    """
    urls: list[str] = []
    seen: set[str] = set()

    offset = 0
    while len(urls) < want:
        data = await _pa_fetch_page(cx, offset=offset, limit=page_size)
        if not data:
            break

        records = _pa_extract_items(data)
        if not records:
            print("PA: could not find records. Top-level keys:", list(data.keys())[:30])
            break

        page_new = 0
        for rec in records:
            u = _pa_pick_url(rec)
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
                page_new += 1
                if len(urls) >= want:
                    break

        print(f"PA: offset={offset} got={len(records)} new={page_new} total={len(urls)}")

        if page_new == 0:
            break

        offset += page_size

    return urls[:want]

# ---------- Massachusetts (mass.gov) helpers ----------
# Matches one teaser-ish chunk: link + nearby date/agency text


_MA_LISTING_ITEM_RE = re.compile(
    r'(?is)'
    r'<a[^>]+href="(?P<href>/(?:info-details|news|press-releases)/[^"#?]+)"[^>]*>'
    r'(?P<title>.*?)</a>'
    r'(?P<trail>.{0,1200})'
)

_MA_TIME_DATETIME_RE = re.compile(r'(?is)<time[^>]+datetime="(?P<dt>\d{4}-\d{2}-\d{2})')
_MA_HUMAN_DATE_RE = re.compile(
    r'\b('
    r'January|February|March|April|May|June|July|August|September|October|November|December'
    r')\s+\d{1,2},\s+\d{4}\b'
)
_MA_NUM_DATE_RE = re.compile(r'\b\d{1,2}/\d{1,2}/\d{4}\b')


# Detail pages often have "FOR IMMEDIATE RELEASE:" then a date, like:
# FOR IMMEDIATE RELEASE: 11/20/2025
_MA_IMMEDIATE_RELEASE_RE = re.compile(
    r'(?i)\b(?:for\s+immediate\s+release|immediate\s+release|release\s+date)\s*:?\s*'
    r'(\d{1,2}/\d{1,2}/\d{4})'
)

# ---------- Massachusetts Executive Orders (mass.gov) helpers ----------

MA_EO_LANDING = "https://www.mass.gov/massachusetts-executive-orders"

# Range box links like:
#   /law-library/massachusetts-executive-orders-600-699
_MA_EO_RANGE_RE = re.compile(
    r'href=["\'](?P<u>(?:https?://www\.mass\.gov)?/law-library/massachusetts-executive-orders-(?P<start>\d+)-(?P<end>\d+)[^"\']*)["\']',
    re.I,
)

# EO detail links like:
#   /executive-orders/no-649-reestablishing-the-seaport-economic-council
_MA_EO_ITEM_RE = re.compile(
    r'href=["\'](?P<u>/executive-orders/no-(?P<num>\d+)[^"\']*)["\']',
    re.I,
)

def _date_from_ma_eo_detail(html: str) -> datetime | None:
    """
    EO pages have a field like:
      Date: 01/06/2023
    Parse that specifically (most reliable).
    """
    if not html:
        return None

    text = _strip_html_to_text(html)

    m = re.search(r'(?i)\bdate\s*:\s*(\d{1,2}/\d{1,2}/\d{4})\b', text)
    if m:
        return _try_parse_us_date(m.group(1))

    # fallback: any US short date on page
    m2 = _US_SHORT_DATE_RE.search(text)
    if m2:
        return _try_parse_us_date(m2.group(0))

    return None


async def _collect_ma_executive_order_urls(
    cx: httpx.AsyncClient,
    min_eo_number: int = 604,     # stop once we go older than this
    limit: int = 2000,
) -> list[str]:
    """
    Future-proof MA EO collector:
      - Discover all EO range pages from MA_EO_LANDING.
      - Extract EO detail URLs from each range page.
      - Sort by EO number desc (newest first).
      - Keep only EO numbers >= min_eo_number (so this auto-includes future 700+).
    """
    html0 = await _pw_get_html(MA_EO_LANDING)

    # 🔍 DEBUG (copy/paste)
    print("MA EO landing len:", len(html0 or ""))
    print("MA EO landing sample:", (html0 or "")[:1200])

    if not html0:
        print("MA EO landing fetch failed (empty HTML)")
        return []

    # collect ranges
    ranges: list[tuple[int, int, str]] = []
    seen_range: set[str] = set()

    for m in _MA_EO_RANGE_RE.finditer(html0):
        href = (m.group("u") or "").strip()
        if not href:
            continue
        if href in seen_range:
            continue
        seen_range.add(href)

        try:
            start_n = int(m.group("start"))
            end_n = int(m.group("end"))
        except Exception:
            continue

        ranges.append((start_n, end_n, _abs_mass(href)))

    if not ranges:
        print("MA EO: no range pages found on landing.")
        return []

    # newest range first (e.g., 700-799, then 600-699, then 500-599...)
    ranges.sort(key=lambda t: t[0], reverse=True)

    # If a whole range is older than our minimum, we can stop early.
    eo_hits: dict[int, str] = {}

    for start_n, end_n, range_url in ranges:
        if end_n < min_eo_number:
            break  # everything below this range is too old

        range_html = await _pw_get_html(range_url)
        if not range_html:
            continue

        for mm in _MA_EO_ITEM_RE.finditer(range_html):
            href = (mm.group("u") or "").strip()
            if not href:
                continue
            try:
                num = int(mm.group("num"))
            except Exception:
                continue

            if num < min_eo_number:
                continue

            u = _abs_mass(href)
            # keep the first seen (should be identical anyway)
            if num not in eo_hits:
                eo_hits[num] = u

        # soft politeness
        await asyncio.sleep(0.15)

    if not eo_hits:
        return []

    # Sort newest → oldest by EO number, cap to limit
    nums_sorted = sorted(eo_hits.keys(), reverse=True)
    urls = [eo_hits[n] for n in nums_sorted][:limit]

    # Debug
    print("MA EO collected:", len(urls), "latest:", urls[0], "oldest:", urls[-1])

    return urls


def _abs_mass(u: str) -> str:
    if not u:
        return u
    if u.startswith("http"):
        return u.split("?")[0].split("#")[0]
    # mass.gov uses relative /news/...
    if not u.startswith("/"):
        u = "/" + u
    return ("https://www.mass.gov" + u).split("?")[0].split("#")[0]

def _date_from_mass_detail(html: str) -> datetime | None:
    if not html:
        return None
    text = _strip_html_to_text(html)

    m = _MA_IMMEDIATE_RELEASE_RE.search(text)
    if m:
        dt = _try_parse_us_date(m.group(1))
        if dt:
            return dt

    # fallback: any MM/DD/YYYY in the page (first one usually is release date)
    m2 = _US_SHORT_DATE_RE.search(text)
    if m2:
        dt2 = _try_parse_us_date(m2.group(0))
        if dt2:
            return dt2
    
    # Also allow "Month DD, YYYY" in the body
    mh = _MA_HUMAN_DATE_RE.search(text)
    if mh:
        try:
            return datetime.strptime(mh.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None

async def _collect_mass_recent_press(
    cx: httpx.AsyncClient,
    max_pages: int = 14,
    limit: int = 500,
) -> tuple[list[str], dict[str, tuple[str, datetime | None, str]]]:
    """
    Returns:
      urls (deduped)
      meta[url] = (title_from_listing, pub_dt_from_listing, agency_from_listing)
    """
    max_pages = max(1, min(int(max_pages or 14), 14))  # pages 0..13
    urls: list[str] = []
    seen: set[str] = set()
    meta: dict[str, tuple[str, datetime | None, str]] = {}

    for p in range(0, max_pages):
        page_url = f"https://www.mass.gov/press-releases/recent?page={p}"
        print("MA page:", page_url)

        r = await _get(cx, page_url)
        await asyncio.sleep(0.2)
        # 🔍 DEBUG — ADD THIS
        if p == 0:
            print("MA page 0 status:", r.status_code, "len:", len(r.text or ""))
            print("MA page 0 sample:", (r.text or "")[:800])
        if r.status_code >= 400 or not r.text:
            break

        page_new = 0
        for m in _MA_LISTING_ITEM_RE.finditer(r.text):
            href = (m.group("href") or "").strip()
            u = _abs_mass(href)
            if not u:
                continue

            # Title cleanup
            raw_title = re.sub(r"(?is)<[^>]+>", " ", (m.group("title") or ""))
            title = re.sub(r"\s+", " ", raw_title).strip()

            trail = m.group("trail") or ""
            trail_text = re.sub(r"(?is)<[^>]+>", " ", trail)
            trail_text = re.sub(r"\s+", " ", trail_text).strip()

            # Date priority:
            # 1) <time datetime="YYYY-MM-DD">
            pub_dt = None
            mt = _MA_TIME_DATETIME_RE.search(trail)
            if mt:
                try:
                    pub_dt = datetime.fromisoformat(mt.group("dt")).replace(tzinfo=timezone.utc)
                except Exception:
                    pub_dt = None

            # 2) "Month DD, YYYY"
            if not pub_dt:
                mh = _MA_HUMAN_DATE_RE.search(trail_text)
                if mh:
                    try:
                        pub_dt = datetime.strptime(mh.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
                    except Exception:
                        pub_dt = None

            # 3) MM/DD/YYYY
            if not pub_dt:
                mn = _MA_NUM_DATE_RE.search(trail_text)
                if mn:
                    pub_dt = _try_parse_us_date(mn.group(0))

            # Agency: usually appears near the date, often separated by "|"
            agency = ""
            if "|" in trail_text:
                # take text after the first pipe as “agency-ish”
                agency = trail_text.split("|", 1)[1].strip()
                # chop if it keeps going
                agency = agency.split(" Learn", 1)[0].strip()
                agency = agency[:140].strip()

            if u not in seen:
                seen.add(u)
                urls.append(u)
                meta[u] = (title, pub_dt, agency)
                page_new += 1
                if len(urls) >= limit:
                    return urls, meta

        if page_new == 0:
            # no new items on this page => stop early
            break

    return urls, meta


def _category_from_nygov_html(html: str) -> str | None:
    """
    Look for the yellow category pill on NY governor pages, e.g. 'Statement' or 'Press Release'.
    Returns one of: 'statement', 'press_release'.
    """
    if not html:
        return None
    m = _NY_CATEGORY_RE.search(html)
    if not m:
        return None
    label = m.group(1).strip().lower()
    if "statement" in label:
        return "statement"
    if "press release" in label:
        return "press_release"
    return None

def _date_from_nygov_html(html: str):
    """
    Extract 'Month DD, YYYY' from the header of a NY governor article.
    We take the first full date in the text, which is the page header date.
    """
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _NY_HUMAN_DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _date_from_texas_html(html: str):
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _TX_HUMAN_DATE_RE.search(text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _category_from_texas_html(html: str) -> str | None:
    """
    Parse 'December 5, 2025 | Austin, Texas | Press Release' line.
    Returns one of: press_release, appointment, proclamation, legislative_statement
    """
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _TX_CATEGORY_RE.search(text)
    if not m:
        return None
    label = m.group(1).strip().lower()
    if "press release" in label:
        return "press_release"
    if "appointment" in label:
        return "appointment"
    if "proclamation" in label:
        return "proclamation"
    if "legislative statement" in label:
        return "legislative_statement"
    return None


_TX_NEWS_URL_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://gov\.texas\.gov)?/news/post/[^\'"#]+)[\'"]',
    re.I
)

_NY_NEWS_URL_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.governor\.ny\.gov)?/news/[^\'"#]+)[\'"]',
    re.I
)

_NY_EO_PDF_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.governor\.ny\.gov)?/sites/default/files/[^\'"#]+\.pdf)[\'"]',
    re.I
)

_NY_EO_PAGE_RE = re.compile(
    r'href=[\'"](?P<u>(?:https?://www\.governor\.ny\.gov)?/executive-order/[^\'"#]+)[\'"]',
    re.I
)

# ---- Illinois article-like path allow list ----
_IL_ARTICLE_PATH_RE = re.compile(
    r'^/(?:'
    r'news(?:/\d{4})?/'                     # /news/ or /news/2025/
    r'|newsroom(?:/\d{4})?/'                # /newsroom/ or /newsroom/2025/
    r'|research/news/'                      # tax.illinois.gov/research/news/...
    r'|resource-center/(?:news|communications)/'  # dph.illinois.gov/resource-center/news/... or communications/...
    r'|announcements/'                      # ipa/icdd announcements
    r'|commission-updates/'                 # ilac commission updates
    r')',
    re.I
)

def _is_il_article_like(u: str) -> bool:
    """
    True for IL newsroom/article pages and allowed PDFs.
    - Allow PDFs anywhere.
    - From /content/dam/... allow only PDFs.
    - For HTML, require paths that look like real news/article sections
      (news/YYYY/, newsroom/YYYY/, research/news/, resource-center/news|communications/).
    """
    try:
        parts = urlsplit(u)
        path = (parts.path or "").lower()

        # PDFs are always allowed
        if path.endswith(".pdf"):
            return True

        # /content/dam/ subtree is asset storage → only allow PDFs from there
        if "/content/dam/" in path:
            return path.endswith(".pdf")

        # For HTML pages, only allow known “article-like” sections
        return bool(_IL_ARTICLE_PATH_RE.match(path))
    except Exception:
        return False

_US_SHORT_DATE_RE = re.compile(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b')

def _parse_fl_eo_rows(listing_html: str) -> List[tuple[str, str, str]]:
    """
    Robust EO parser for FL Drupal Views markup (NOT a table).
    Returns list of (pdf_url, title_text, date_str).
    """
    out: List[tuple[str, str, str]] = []
    if not listing_html:
        return out

    seen: set[str] = set()

    for m in _FL_EO_PDF_RE.finditer(listing_html):
        raw_href = (m.group("u") or "").strip()
        pdf_url = _abs_flgov(raw_href)
        if not pdf_url or pdf_url in seen:
            continue

        # Look around the match to find title + date
        start = max(0, m.start() - 1200)
        end = min(len(listing_html), m.end() + 1200)
        chunk = listing_html[start:end]

        # Title: anchor text for THIS exact href (best effort)
        title = ""
        ma = re.search(
            r'(?is)<a[^>]+href=["\']%s["\'][^>]*>(?P<t>.*?)</a>' % re.escape(raw_href),
            chunk,
        )
        if ma:
            raw_title = ma.group("t") or ""
            title = re.sub(r"(?is)<[^>]+>", " ", raw_title)
            title = re.sub(r"\s+", " ", title).strip()

        # Date: find the closest MM/DD/YYYY in the chunk
        date_str = ""
        md = _US_SHORT_DATE_RE.search(chunk)
        if md:
            date_str = md.group(0)

        out.append((pdf_url, title, date_str))
        seen.add(pdf_url)

    return out

def _abs_flgov(u: str) -> str:
    if not u:
        return u
    if u.startswith("http"):
        return u.split("?")[0].split("#")[0]
    # make absolute; strip query/fragment
    return ("https://www.flgov.com" + u).split("?")[0].split("#")[0]

def _fl_norm_pdf_url(href: str) -> str:
    return _abs_flgov(href).split("#", 1)[0]


def _abs_texas(u: str) -> str:
    if not u:
        return u
    if u.startswith("http"):
        return u.split("?")[0].split("#")[0]
    return ("https://gov.texas.gov" + u).split("?")[0].split("#")[0]

def _abs_nygov(u: str) -> str:
    if not u: return u
    if u.startswith("http"):  # strip query/fragment
        return u.split("?")[0].split("#")[0]
    return ("https://www.governor.ny.gov" + u).split("?")[0].split("#")[0]

def _abs_with_page_origin(page_url: str, href: str) -> str:
    """
    Make href absolute using the scheme+host of page_url.
    Preserve query strings (e.g., ?page=2) so pagination works.
    Only strip fragments (#...).
    """
    if not href:
        return href

    # strip fragment only, keep ?page=...
    href = href.split("#")[0].strip()

    if href.startswith("//"):
        return "https:" + href
    if href.startswith("http"):
        return href

    parts = urlsplit(page_url)
    origin = f"{parts.scheme}://{parts.netloc}"

    if href.startswith("/"):
        return origin + href

    return origin + "/" + href

def _try_parse_us_date(date_str: str):
    try:
        m, d, y = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str).groups()
        return datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
    except Exception:
        # fallback ISO
        try:
            return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        except Exception:
            return None

def _date_from_dated_url(url: str):
    try:
        parts = urlsplit(url).path.strip("/").split("/")
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        return datetime(y, m, d, tzinfo=timezone.utc)
    except Exception:
        return None
    
def _date_from_html_or_url(html: str, url: str):
    m = re.search(
        r'property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
        html, re.I
    )
    if m:
        try:
            return datetime.fromisoformat(m.group(1).replace('Z', '+00:00')).astimezone(timezone.utc)
        except Exception:
            pass
    return _date_from_dated_url(url)

def _date_from_json_ld(html: str):
    if not html:
        return None
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.I|re.S):
        try:
            blob = m.group(1).strip()
            data = json.loads(blob)
            # handle dict or list of dicts
            candidates = data if isinstance(data, list) else [data]
            for node in candidates:
                if not isinstance(node, dict): 
                    continue
                # datePublished is most common
                dp = node.get("datePublished") or node.get("dateCreated") or node.get("dateModified")
                if dp:
                    try:
                        return datetime.fromisoformat(dp.replace('Z', '+00:00')).astimezone(timezone.utc)
                    except Exception:
                        pass
        except Exception:
            continue
    return None


def _extract_h1(html: str) -> str:
    # 1) Prefer og:title (NY pages usually have the real article title here)
    m = re.search(
        r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
        html
    )
    if m:
        t = re.sub(r'\s+', ' ', m.group(1)).strip()
        if t:
            return t

    # 2) Otherwise, collect all h1s and pick the best one (usually the longest)
    h1s: list[str] = []
    for mh in re.finditer(r'(?is)<h1[^>]*>(.*?)</h1>', html):
        t = re.sub(r'(?is)<[^>]+>', ' ', mh.group(1))
        t = re.sub(r'\s+', ' ', t).strip()
        if t:
            h1s.append(t)

    if h1s:
        h1s.sort(key=len, reverse=True)
        return h1s[0]

    # 3) title tag fallback
    m = re.search(r'(?is)<title[^>]*>(.*?)</title>', html)
    if m:
        t = re.sub(r'(?is)<[^>]+>', ' ', m.group(1))
        t = re.sub(r'\s+', ' ', t).strip()
        if t:
            return t

    return ""


def _try_parse_date_str(s: str):
    from datetime import datetime, timezone
    s = s.strip()
    # ISO-ish?
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc)
    except Exception:
        pass
    # "August 22, 2025"
    try:
        return datetime.strptime(s, "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _date_from_il_html(html: str):
    if not html:
        return None
    for pat in _IL_DATE_PATTERNS:
        m = re.search(pat, html)
        if not m:
            continue
        # Cases:
        #  - meta/time patterns → group(1)
        #  - "Released|Published: Month Day, Year" → group(2)
        #  - "News – [weekday,] Month Day, Year" → group(1)
        candidate = None
        txt = (m.group(0) or "").lower()
        if m.lastindex and m.lastindex >= 2 and ("released" in txt or "published" in txt):
            candidate = m.group(2)
        else:
            candidate = m.group(1) if m.lastindex and m.lastindex >= 1 else None
        if candidate:
            dt = _try_parse_date_str(candidate)
            if dt:
                return dt
    return None

def _date_from_il_pdf_filename(url: str):
    m = _IL_PDF_DATE_PAT.search(urlsplit(url).path or "")
    if not m:
        return None
    y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return datetime(y, mth, d, tzinfo=timezone.utc)


def _date_from_flgov_html(html: str, url: str | None = None):
    if not html:
        return None

    text = _strip_html_to_text(html)

    # 1) Try explicit "DATE:" line
    m_line = _FL_PRESS_DATE_LINE_RE.search(text)
    if m_line:
        try:
            return datetime.strptime(m_line.group(1), "%B %d, %Y").replace(tzinfo=timezone.utc)
        except Exception:
            pass

    now = datetime.now(timezone.utc)
    ordered_dates: list[datetime] = []

    # 2) Month + day + year, with flexible whitespace
    for m in _FL_HUMAN_DATE_RE.finditer(text):
        month_name, day_str, year_str = m.groups()
        try:
            dt = datetime.strptime(
                f"{month_name} {int(day_str)} {int(year_str)}",
                "%B %d %Y",
            ).replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if dt.year < 2000:
            continue
        if dt > now + timedelta(days=2):
            continue

        ordered_dates.append(dt)

    if not ordered_dates:
        return None

    year_hint = None
    if url:
        m_url = re.search(r'/eog/news/(?:press|executive-orders)/(\d{4})/', url)
        if m_url:
            try:
                year_hint = int(m_url.group(1))
            except Exception:
                year_hint = None

    if year_hint is not None:
        for dt in ordered_dates:
            if dt.year == year_hint:
                return dt

    return ordered_dates[0]

# --- Florida EO PDF date ("this 2nd day of January, 2025") ---
_FL_EO_TESTIMONY_DATE_RE = re.compile(
    r'(?is)\bthis\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+'
    r'(january|february|march|april|may|june|july|august|september|october|november|december)'
    r'\s*,\s*(\d{4})\b'
)

def _date_from_florida_eo_pdf_text(pdf_text: str):
    """
    Extracts date from Florida EO PDFs that contain:
      '... affixed at Tallahassee, this 2nd day of January, 2025.'
    Returns a timezone-aware UTC datetime or None.
    """
    if not pdf_text:
        return None

    m = _FL_EO_TESTIMONY_DATE_RE.search(pdf_text)
    if not m:
        return None

    try:
        day = int(m.group(1))
        month_name = m.group(2).strip().title()
        year = int(m.group(3))

        month = datetime.strptime(month_name, "%B").month
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None

async def _get(
    cx: httpx.AsyncClient,
    url: str,
    tries: int = 3,
    read_timeout: float = 45.0,
    params: dict | None = None,
    headers: dict | None = None,
) -> httpx.Response:
    """
    GET with retries + per-attempt timeouts.
    Never raises; on failure returns a 599 Response.
    """
    last_exc = None

    for attempt in range(1, tries + 1):
        try:
            req_headers = headers or {}

            # 🗽 New York hardening: avoid long-lived keep-alive issues
            if "governor.ny.gov" in url:
                req_headers = {**req_headers, "Connection": "close"}

            r = await cx.get(
                url,
                params=params,
                headers=req_headers,
                timeout=httpx.Timeout(
                    connect=15.0,
                    read=read_timeout,
                    write=15.0,
                    pool=None,
                ),
            )

            # Accept anything except server errors / rate limits
            if r.status_code < 500 and r.status_code != 429:
                return r

        except (
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.ReadError,          # ✅ THIS IS THE MISSING PIECE
        ) as e:
            last_exc = e
            print(
                f"[GET retry {attempt}/{tries}] {url} "
                f"error={type(e).__name__}: {e}"
            )

        # exponential backoff with cap
        await asyncio.sleep(min(1.5 * (2 ** (attempt - 1)), 6.0))

    # Final failure: return sentinel response instead of crashing
    return httpx.Response(
        599,
        request=httpx.Request("GET", url),
        content=b"",
        headers={
            "X-Error": str(last_exc) if last_exc else "unknown",
            "X-Retries": str(tries),
        },
    )
async def _collect_listing_urls(cx: httpx.AsyncClient, start_url: str, max_pages: int) -> List[str]:
    """
    Collect dated article URLs from a listing page with rel=next pagination.
    Handles CA newsroom lists and FL newsroom/EO lists.
    """
    urls: List[str] = []
    seen: set[str] = set()  # track unique URLs within this listing crawl
    next_url = start_url

    def pick_pattern(page_url: str):
        if "www.gov.ca.gov" in page_url:
            return _CA_NEWS_URL_RE

        if "www.flgov.com" in page_url and "/eog/news/executive-orders" in page_url:
            return _FL_EO_PDF_RE
        if "www.flgov.com" in page_url:
            return _FL_NEWS_URL_RE

        if "gov.texas.gov" in page_url:
            return _TX_NEWS_URL_RE

        if "governor.ny.gov" in page_url:
            # prefer EO detail pages (HTML) so we can summarize, but keep PDFs as fallback
            if "/executiveorders" in page_url:
                return _NY_EO_PAGE_RE
            return _NY_NEWS_URL_RE

        # default fallback
        return _CA_NEWS_URL_RE

    for page in range(max_pages):
        # 🗽 NY NEWSROOM HARD CUTOFF:
        # /news?page=97 is where June 2025 lives right now.
        # Pages beyond 97 are April 2024 and older → skip them entirely.
        if "governor.ny.gov" in start_url and "/news" in start_url and page > 97:
            print("NY newsroom reached cutoff page", page, "stopping")
            break

        # 🗽 New York: synthesize ?page=N instead of parsing “Next page” links
        if "governor.ny.gov" in start_url:
            base = start_url.split("?", 1)[0]
            next_url = base if page == 0 else f"{base}?page={page}"
            print("NY listing page:", next_url)

        # 🔹 Texas: hard cutoff at /news/P1560
        if "gov.texas.gov/news/P" in next_url:
            m_page = re.search(r'/news/P(\d+)', next_url)
            if m_page:
                page_num = int(m_page.group(1))
                if page_num > 1560:
                    print("TX reached cutoff page, stopping at:", next_url)
                    break

        # 👇 Texas-specific progress log
        if "gov.texas.gov" in next_url:
            print("TX page:", next_url)

        r = await _get(cx, next_url)
        if r.status_code >= 400 or not r.text:
            break

        pat = pick_pattern(next_url)
        page_new = 0  # how many *new* URLs did we get on this page?

        ca_hit_cutoff = False

        # --- CA cutoff tracking (per page) ---
        ca_page_max_dt = None   # newest date found on this page
        ca_page_any_dt = False  # did we parse any dated URLs at all?


        for m in pat.finditer(r.text):
            u = m.group("u").strip()
            if "flgov.com" in next_url:
                u = _abs_flgov(u)
            elif "www.gov.ca.gov" in next_url:
                u = _abs_with_page_origin(next_url, u)
            elif "gov.texas.gov" in next_url:
                u = _abs_texas(u)
            elif "governor.ny.gov" in next_url:
                u = _abs_nygov(u)
            
            # ---------- California cutoff ----------
            if "www.gov.ca.gov" in next_url:
                dt = _date_from_dated_url(u)
                if dt is not None:
                    ca_page_any_dt = True
                    if (ca_page_max_dt is None) or (dt > ca_page_max_dt):
                        ca_page_max_dt = dt

                # Never include items older than cutoff
                if dt is not None and dt < CA_MIN_DATE:
                    continue

            if u and u not in seen:
                seen.add(u)
                urls.append(u)
                page_new += 1

        # 🗽 New York: if this page didn't add anything new, we've gone past the last real page.
        if "governor.ny.gov" in start_url and page > 0 and page_new == 0:
            print("NY listing reached end – no new URLs on page", page)
            break

        # 🗽 For New York, we *don’t* parse pagination links – the for-loop advances ?page=N.
        if "governor.ny.gov" in start_url:
            continue

        # ✅ California: stop paging only when the *newest* item on this page is older than cutoff
        if "www.gov.ca.gov" in next_url and ca_page_any_dt and ca_page_max_dt and ca_page_max_dt < CA_MIN_DATE:
            print("CA reached cutoff page (max_dt < 2025-01-02), stopping at:", next_url, "max_dt:", ca_page_max_dt)
            break

        # pagination (non-NY paths)
        m = _CA_PAGER_NEXT_RE.search(r.text)
        if m:
            href = m.group("u")
            next_url = _abs_with_page_origin(next_url, href)
            continue

        # Generic rel="next"
        m2 = re.search(
            r'<a[^>]*rel=["\']?next["\']?[^>]*href=["\']([^"\']+)["\']',
            r.text,
            re.I,
        )
        if m2:
            next_url = _abs_with_page_origin(next_url, m2.group(1))
            continue

        # ---------- California Divi pagination (robust) ----------
        if "www.gov.ca.gov" in next_url:
            # 1) Prefer an actual "next" button if present (common Divi markup)
            m_next = re.search(
                r'<a[^>]+class=["\'][^"\']*\bnext\b[^"\']*["\'][^>]*href=["\'](?P<u>[^"\']+)["\']',
                r.text,
                re.I,
            )
            if m_next and "/newsroom/page/" in (m_next.group("u") or ""):
                href = m_next.group("u")
                next_url = _abs_with_page_origin(next_url, href)
                # If Divi expects the XHR flavor, ensure ?et_blog is present
                if "/newsroom/page/" in next_url and "et_blog" not in next_url:
                    next_url = _set_query_param(next_url, "et_blog", "")
                continue

            # 2) Fallback: any newsroom/page/N link (relative or absolute), with or without ?et_blog
            m_ca = re.search(
                r'href=["\'](?P<u>(?:https?://www\.gov\.ca\.gov)?/newsroom/page/\d+/?(?:\?et_blog)?)["\']',
                r.text,
                re.I,
            )
            if m_ca:
                href = m_ca.group("u")
                next_url = _abs_with_page_origin(next_url, href)
                if "/newsroom/page/" in next_url and "et_blog" not in next_url:
                    next_url = _set_query_param(next_url, "et_blog", "")
                continue


        # Florida: any link whose href points to another newsroom page with /page/N or ?page=N
        m_fl = re.search(
            r'<a[^>]+href=["\'](?P<u>(?:https?://www\.flgov\.com)?/eog/news[^"\']*(?:/page/\d+|\?page=\d+)[^"\']*)["\'][^>]*>',
            r.text,
            re.I,
        )
        if m_fl:
            next_url = _abs_with_page_origin(next_url, m_fl.group("u"))
            continue

        # Texas: "Next Page" link that goes to /news/P8, /news/P16, or category/P8 etc.
        m_tx = re.search(
            r'<a[^>]+href=["\'](?P<u>(?:https?://gov\.texas\.gov)?/news[^"\']*)["\'][^>]*>\s*Next Page\s*</a>',
            r.text,
            re.I,
        )
        if m_tx:
            next_url = _abs_with_page_origin(next_url, m_tx.group("u"))
            continue

        # no pagination link found
        break

    return urls

# --- Florida EO Drupal Views AJAX helpers ---

_FL_EO_YEAR_OPTION_RE = re.compile(
    r'<select[^>]+name=["\']field_date_value[^"\']*["\'][^>]*>(?P<body>.*?)</select>',
    re.I | re.S,
)

_FL_OPTION_RE = re.compile(
    r'<option[^>]+value=["\'](?P<val>\d+)["\'][^>]*>\s*(?P<label>\d{4})\s*</option>',
    re.I | re.S,
)

_FL_VIEW_DOM_ID_RE = re.compile(r'data-view-dom-id=["\'](?P<id>[a-f0-9]{32,})["\']', re.I)

_FL_DRUPAL_SETTINGS_JSON_RE2 = re.compile(
    r'(?is)<script[^>]+data-drupal-selector=["\']drupal-settings-json["\'][^>]*>(?P<json>\{.*?\})</script>'
)

def _fl_extract_year_value_map(html: str) -> dict[int, str]:
    out: dict[int, str] = {}
    if not html:
        return out

    m = _FL_EO_YEAR_OPTION_RE.search(html)
    if not m:
        return out

    body = m.group("body") or ""
    for om in _FL_OPTION_RE.finditer(body):
        try:
            year = int(om.group("label"))     # 2025
            val = om.group("val").strip()     # "2"
            out[year] = val                   # ✅ correct
        except Exception:
            continue
    return out

def _fl_extract_ajax_libraries(html: str) -> str:
    if not html:
        return ""

    m = _FL_DRUPAL_SETTINGS_JSON_RE2.search(html)
    if not m:
        return ""

    try:
        settings = json.loads(m.group("json"))
        aps = settings.get("ajaxPageState") or {}
        libs = aps.get("libraries")
        return libs if isinstance(libs, str) else ""
    except Exception:
        return ""

def _fl_extract_view_dom_id(html: str) -> str:
    if not html:
        return ""

    # 1) Try drupalSettings JSON (most reliable)
    m = _FL_DRUPAL_SETTINGS_JSON_RE2.search(html)
    if m:
        try:
            settings = json.loads(m.group("json"))
            views = settings.get("views") or {}
            ajax_views = views.get("ajaxViews") or {}

            # Find the executive_orders view entry and return its view_dom_id
            for _k, v in ajax_views.items():
                if not isinstance(v, dict):
                    continue
                # typical fields: view_name, view_display_id, view_dom_id
                if v.get("view_name") == "pages" and v.get("view_display_id") == "executive_orders":
                    dom_id = v.get("view_dom_id")
                    if isinstance(dom_id, str) and dom_id:
                        return dom_id
        except Exception:
            pass

    # 2) Fallback: data-view-dom-id="..."
    m2 = _FL_VIEW_DOM_ID_RE.search(html)
    if m2:
        return (m2.group("id") or "").strip()

    # 3) Fallback: id="views-dom-id-<hash>"
    m3 = re.search(r'id=["\']views-dom-id-([a-f0-9]{32,})["\']', html, re.I)
    if m3:
        return (m3.group(1) or "").strip()

    return ""

def _fl_extract_view_dom_id_for(html: str, view_name: str, view_display_id: str) -> str:
    """Extract view_dom_id for a specific Drupal view (more reliable than generic extraction)."""
    if not html:
        return ""

    m = _FL_DRUPAL_SETTINGS_JSON_RE2.search(html)
    if m:
        try:
            settings = json.loads(m.group("json"))
            views = settings.get("views") or {}
            ajax_views = views.get("ajaxViews") or {}

            for _k, v in ajax_views.items():
                if not isinstance(v, dict):
                    continue
                if v.get("view_name") == view_name and v.get("view_display_id") == view_display_id:
                    dom_id = v.get("view_dom_id")
                    if isinstance(dom_id, str) and dom_id:
                        return dom_id
        except Exception:
            pass

    return ""

async def _fl_fetch_press_urls_via_ajax(
    cx: httpx.AsyncClient,
    view_dom_id: str,
    libraries: str,
    page: int = 0,
) -> list[str]:
    """
    Florida press releases are Drupal Views.
    Newest (incl 2026) show up only when field_year_value=All via /eog/views/ajax.
    """

    async def _call(view_path: str, view_base_path: str) -> list[str]:
        params = {
            "_wrapper_format": "drupal_ajax",
            "view_name": "news_releases",
            "view_display_id": "press_releases",
            "view_args": "",
            "view_path": view_path,
            "view_base_path": view_base_path,
            "view_dom_id": view_dom_id,
            "pager_element": "0",

            # show all years (incl 2026+)
            "field_year_value": "All",
            "field_month_value": "All",
            "field_city_target_id_selective": "All",
            "combine": "",

            # paging
            "page": str(page),
            "_drupal_ajax": "1",

            "ajax_page_state[theme]": FL_THEME,
            "ajax_page_state[theme_token]": "",
            "ajax_page_state[libraries]": libraries,
        }

        r = await _get(
            cx,
            FL_EO_AJAX_URL,
            params=params,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": "https://www.flgov.com/eog/news/press",
                **BROWSER_UA_HEADERS,
            },
            read_timeout=45.0,
        )
        if r.status_code >= 400 or not r.text:
            return []

        try:
            j = r.json()
        except Exception:
            return []

        fragment_html = _fl_extract_view_html_from_ajax(j) or ""

        print("FL PRESS ajax fragment len:", len(fragment_html))
        print("FL PRESS ajax fragment sample:", fragment_html[:500])

        if not fragment_html:
            return []

        out: list[str] = []
        for m in _FL_NEWS_URL_RE.finditer(fragment_html):
            u = _abs_flgov((m.group("u") or "").strip())
            if u and "/eog/news/press/" in u.lower():
                out.append(u)

        # dedupe keep order
        seen = set()
        deduped = []
        for u in out:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped

    # ✅ correct for Florida site
    urls = await _call("/eog/news/press", "eog/news/press")

    # fallback (older capture sometimes lacks /eog)
    if not urls:
        urls = await _call("/news/press", "news/press")

    return urls


def _fl_parse_eo_detail_rows(listing_html: str) -> list[tuple[str, str, str]]:
    """
    Parse EO listing table rows:
      - detail_url like /eog/news/executive-orders/...
      - title text
      - date like 01/16/2026
    """
    out: list[tuple[str, str, str]] = []
    if not listing_html:
        return out

    for rm in _FL_EO_ROW_RE.finditer(listing_html):
        row = rm.group("row") or ""

        m_detail = _FL_EO_DETAIL_RE.search(row)
        if not m_detail:
            continue
        detail_url = _abs_flgov((m_detail.group("u") or "").strip())

        # title = anchor text inside the row (best effort)
        title = ""
        ma = re.search(r'(?is)<a[^>]+href=["\'][^"\']+["\'][^>]*>(?P<t>.*?)</a>', row)
        if ma:
            raw_title = ma.group("t") or ""
            title = re.sub(r"(?is)<[^>]+>", " ", raw_title)
            title = re.sub(r"\s+", " ", title).strip()

        # date column is MM/DD/YYYY
        date_str = ""
        md = _US_SHORT_DATE_RE.search(row)
        if md:
            date_str = md.group(0)

        if detail_url:
            out.append((detail_url, title, date_str))

    return out


def _fl_extract_pdf_from_eo_detail(detail_html: str) -> str:
    """Find the first EO PDF link on an EO detail page."""
    if not detail_html:
        return ""
    m = _FL_EO_PDF_RE.search(detail_html)
    if not m:
        return ""
    return _abs_flgov((m.group("u") or "").strip())


def _fl_extract_view_html_from_ajax(resp_json) -> str:
    if not isinstance(resp_json, list):
        return ""

    chunks: list[str] = []
    for cmd in resp_json:
        if not isinstance(cmd, dict):
            continue
        data = cmd.get("data")
        if isinstance(data, str) and "<" in data:
            chunks.append(data)

    return "\n".join(chunks)

FL_EO_LIST_URL = "https://www.flgov.com/eog/news/executive-orders"
FL_EO_AJAX_URL  = "https://www.flgov.com/eog/views/ajax"
FL_THEME = "bootstrap_barrio_subtheme"

async def _fl_fetch_eo_rows_via_ajax(
    cx: httpx.AsyncClient,
    year_value: str,
    view_dom_id: str,
    libraries: str,
    page: int = 0,
) -> list[tuple[str, str, str]]:
    params = {
        "_wrapper_format": "drupal_ajax",
        "view_name": "pages",
        "view_display_id": "executive_orders",
        "view_args": "",
        "view_path": "/eog/news/executive-orders",
        "view_base_path": "eog/news/executive-orders",
        "view_dom_id": view_dom_id,
        "pager_element": "0",

        "field_eo_category_value": "1",
        "field_date_value": year_value,   # ✅ like "2" for 2025
        "keys": "",

        "page": str(page),                # ✅ pagination
        "_drupal_ajax": "1",

        "ajax_page_state[theme]": FL_THEME,
        "ajax_page_state[theme_token]": "",
        "ajax_page_state[libraries]": libraries,
    }

    r = await _get(
        cx,
        FL_EO_AJAX_URL,
        params=params,
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": FL_EO_LIST_URL,
            **BROWSER_UA_HEADERS,
        },
        read_timeout=45.0,
    )
    if r.status_code >= 400 or not r.text:
        return []

    try:
        j = r.json()
    except Exception:
        return []

    fragment_html = _fl_extract_view_html_from_ajax(j)

    # ✅ ADD THESE 2 PRINTS RIGHT HERE
    print("FL EO ajax fragment len:", len(fragment_html or ""))
    print("FL EO ajax fragment sample:", (fragment_html or "")[:400])

    if not fragment_html:
        return []

    # reuse your row parser (it works fine on fragments too)
    # Try to parse detail rows if the fragment contains detail links + dates
    detail_rows = _fl_parse_eo_detail_rows(fragment_html)
    if detail_rows:
        # Convert detail rows -> pdf rows by fetching detail pages (slower but reliable)
        out: list[tuple[str, str, str]] = []
        for detail_url, title, date_str in detail_rows:
            dr = await _get(cx, detail_url)
            if dr.status_code >= 400 or not dr.text:
                continue
            pdf_url = _fl_extract_pdf_from_eo_detail(dr.text)
            if pdf_url:
                out.append((pdf_url, title, date_str))
        return out

    # fallback: direct PDFs in fragment
    return _parse_fl_eo_rows(fragment_html)


def _fl_find_last_page(html: str) -> int | None:
    """
    Best-effort: look at the pager and find the highest ?page=N.
    Returns None if not found.
    """
    if not html:
        return None

    nums: list[int] = []
    for m in re.finditer(r'\?page=(\d+)', html, re.I):
        try:
            nums.append(int(m.group(1)))
        except Exception:
            continue

    return max(nums) if nums else None

def _fl_parse_eo_listing_rows_generic(listing_html: str) -> list[tuple[str, str, str]]:
    """
    Parse FL EO listing page even if Drupal changes table markup.
    Returns list of (detail_url, title, date_str).
    """
    out: list[tuple[str, str, str]] = []
    if not listing_html:
        return out

    # Find EO detail links
    for m in _FL_EO_DETAIL_RE.finditer(listing_html):
        detail_url = _abs_flgov((m.group("u") or "").strip())
        if not detail_url:
            continue

        # Look around link for title + date
        start = max(0, m.start() - 800)
        end = min(len(listing_html), m.end() + 800)
        chunk = listing_html[start:end]

        # Title: anchor text near the link
        title = ""
        ma = re.search(
            r'(?is)<a[^>]+href=["\']%s["\'][^>]*>(?P<t>.*?)</a>' % re.escape(m.group("u")),
            chunk,
        )
        if ma:
            raw_title = ma.group("t") or ""
            title = re.sub(r"(?is)<[^>]+>", " ", raw_title)
            title = re.sub(r"\s+", " ", title).strip()

        # Date: nearest MM/DD/YYYY near the link
        date_str = ""
        md = _US_SHORT_DATE_RE.search(chunk)
        if md:
            date_str = md.group(0)

        out.append((detail_url, title, date_str))

    # Dedup (detail_url)
    dedup: dict[str, tuple[str, str, str]] = {}
    for u, t, d in out:
        dedup[u] = (u, t, d)
    return list(dedup.values())

def _fl_parse_eo_pdf_rows_generic(listing_html: str) -> list[tuple[str, str, str]]:
    """
    Parse EO listing page for direct PDF links:
      /eog/sites/default/files/executive-orders/2026/EO%2026-09.pdf
    Returns list of (pdf_url, title, date_str).
    """
    out: list[tuple[str, str, str]] = []
    if not listing_html:
        return out

    for m in _FL_EO_PDF_RE.finditer(listing_html):
        pdf_url = _abs_flgov((m.group("u") or "").strip())
        if not pdf_url:
            continue

        # Look around the link to find a nearby date/title
        start = max(0, m.start() - 900)
        end = min(len(listing_html), m.end() + 900)
        chunk = listing_html[start:end]

        # Title: try anchor text for this exact href
        title = ""
        ma = re.search(
            r'(?is)<a[^>]+href=["\']%s["\'][^>]*>(?P<t>.*?)</a>' % re.escape(m.group("u")),
            chunk,
        )
        if ma:
            raw_title = ma.group("t") or ""
            title = re.sub(r"(?is)<[^>]+>", " ", raw_title)
            title = re.sub(r"\s+", " ", title).strip()

        if not title:
            # fallback: filename
            title = pdf_url.rsplit("/", 1)[-1]

        # Date near the link (MM/DD/YYYY)
        date_str = ""
        md = _US_SHORT_DATE_RE.search(chunk)
        if md:
            date_str = md.group(0)

        out.append((pdf_url, title, date_str))

    # Dedup by pdf_url (keep last/most complete)
    dedup: dict[str, tuple[str, str, str]] = {}
    for u, t, d in out:
        dedup[u] = (u, t, d)
    return list(dedup.values())


async def _collect_florida_urls(
    cx: httpx.AsyncClient,
    max_pages: int,
    limit: int,
) -> tuple[list[str], list[tuple[str, str, str]]]:
    """
    Florida-only helper.

    PRESS:
      - Must use Drupal Views AJAX with field_year_value=All to see 2026+.

    EO:
      - Listing page is detail links + dates; PDFs live on detail pages.
      - We fetch listing by year, then fetch detail pages to extract PDFs.
    """
    if max_pages is None or max_pages <= 0:
        max_pages = 20
    if limit is None or limit <= 0:
        limit = 10_000

    item_urls: list[str] = []
    eo_rows: list[tuple[str, str, str]] = []
    seen_items: set[str] = set()

    roots = _FL_LISTING_ROOTS  # press + executive-orders

    for root in roots:
        print("FL page:", root)
        r = await _get(cx, root)
        if r.status_code >= 400 or not r.text:
            continue

        html = r.text or ""
        is_press = "/press" in root

        # ---------------- PRESS (NO AJAX: server-rendered listing) ----------------
        if is_press:
            for page in range(0, max_pages):
                params = {
                    # make sure we don't get stuck on a single year
                    "field_year_value": "All",
                    "field_month_value": "All",
                    # Drupal pager
                    "page": str(page),
                }

                rr = await _get(
                    cx,
                    root,
                    params=params,
                    headers={**BROWSER_UA_HEADERS, "Referer": root},
                )
                if rr.status_code >= 400 or not rr.text:
                    break

                page_new = 0
                for m in _FL_NEWS_URL_RE.finditer(rr.text):
                    u = _abs_flgov((m.group("u") or "").strip())
                    if u and "/eog/news/press/" in u.lower() and u not in seen_items:
                        seen_items.add(u)
                        item_urls.append(u)
                        page_new += 1
                        if len(item_urls) >= limit:
                            return item_urls, eo_rows

                print(f"FL PRESS page={page} new={page_new} total={len(item_urls)}")
                if page_new == 0:
                    break

                await asyncio.sleep(0.15)

            continue

        # ---------------- EO (listing GET -> detail -> PDF) ----------------
        base_html = html
        year_map = _fl_extract_year_value_map(base_html)

        hit_cutoff = False

        if not year_map:
            print("FL EO: could not parse year_map; falling back to base HTML only")
            detail_rows = _fl_parse_eo_listing_rows_generic(base_html)
            for detail_url, title, date_str in detail_rows:
                dr = await _get(cx, detail_url)
                if dr.status_code >= 400 or not dr.text:
                    continue
                pdf_url = _fl_extract_pdf_from_eo_detail(dr.text)
                if not pdf_url:
                    continue
                eo_rows.append((pdf_url, title, date_str))
                if pdf_url not in seen_items:
                    seen_items.add(pdf_url)
                    item_urls.append(pdf_url)
            continue

        years_sorted = sorted(year_map.keys(), reverse=True)
        preferred = [y for y in years_sorted if y in (2026, 2025, 2024)]
        target_years = preferred if preferred else years_sorted

        for y in target_years:
            if hit_cutoff:
                break

            y_val = year_map.get(y)
            if not y_val:
                continue

            print(f"FL EO GET year={y} (field_date_value={y_val})")

            for page in range(0, max_pages):
                params = {
                    "field_eo_category_value": "1",
                    "field_date_value": y_val,   # important: option value, not literal year
                    "page": str(page),
                }

                rr = await _get(
                    cx,
                    FL_EO_LIST_URL,
                    params=params,
                    headers={**BROWSER_UA_HEADERS, "Referer": FL_EO_LIST_URL},
                )
                if rr.status_code >= 400 or not rr.text:
                    break

                # ✅ FIRST: parse direct PDF links (current Florida site behavior)
                before = len(seen_items)
                page_new = 0

                pdf_rows = _fl_parse_eo_pdf_rows_generic(rr.text)
                if pdf_rows:
                    for pdf_url, title, date_str in pdf_rows:
                        pdf_url = _abs_flgov(pdf_url)

                        # cutoff: stop once we reach EO 24-01
                        if re.search(r'\bEO(?:%20|%2520|[- ])24-0?1\.pdf\b', pdf_url, re.I):
                            eo_rows.append((pdf_url, title, date_str))
                            if pdf_url not in seen_items:
                                seen_items.add(pdf_url)
                                item_urls.append(pdf_url)
                            hit_cutoff = True
                            break

                        eo_rows.append((pdf_url, title, date_str))
                        if pdf_url not in seen_items:
                            seen_items.add(pdf_url)
                            item_urls.append(pdf_url)
                            page_new += 1
                            if len(item_urls) >= limit:
                                return item_urls, eo_rows

                    print(f"FL EO year={y} page={page} pdf_new={page_new} total={len(item_urls)}")

                    if hit_cutoff or page_new == 0:
                        break

                    await asyncio.sleep(0.15)
                    continue  # ✅ done with this page (no need for detail fallback)

                # ---- FALLBACK: older behavior (detail pages -> PDF) ----
                detail_rows = _fl_parse_eo_listing_rows_generic(rr.text)
                if not detail_rows:
                    break

                for detail_url, title, date_str in detail_rows:
                    dr = await _get(cx, detail_url)
                    if dr.status_code >= 400 or not dr.text:
                        continue

                    pdf_url = _fl_extract_pdf_from_eo_detail(dr.text)
                    if not pdf_url:
                        continue

                    # cutoff: stop once we reach EO 24-01
                    if re.search(r'\bEO(?:%20|%2520|[- ])24-0?1\.pdf\b', pdf_url, re.I):
                        eo_rows.append((pdf_url, title, date_str))
                        if pdf_url not in seen_items:
                            seen_items.add(pdf_url)
                            item_urls.append(pdf_url)
                        hit_cutoff = True
                        break

                    eo_rows.append((pdf_url, title, date_str))
                    if pdf_url not in seen_items:
                        seen_items.add(pdf_url)
                        item_urls.append(pdf_url)
                        page_new += 1
                        if len(item_urls) >= limit:
                            return item_urls, eo_rows

                print(f"FL EO year={y} page={page} new={page_new} total={len(item_urls)}")

                if hit_cutoff:
                    break

                if len(seen_items) == before:
                    break

                await asyncio.sleep(0.15)


        continue

    return item_urls, eo_rows



# ---------- California (gov.ca.gov) category helpers ----------

# Canonical CA categories you care about (by slug)
CA_CATEGORY_SLUG_TO_STATUS = {
    "press-releases": "press_release",
    "executive-orders": "executive_order",
    "featured": "featured",
    "first-partner": "first_partner",
    "media-advisories": "media_advisory",
    "proclamations": "proclamation",
}

# Match ANY anchor href that points to /category/<slug>/...
_CA_CAT_HREF_RE = re.compile(
    r'(?is)href=["\'](?P<href>(?:https?://www\.gov\.ca\.gov)?/category/[^"\']+)["\']'
)

def _ca_categories_from_html(html: str) -> list[str]:
    """
    Return ALL CA categories found on the page, mapped to our internal statuses.
    Matches any link whose href includes /category/<slug>/ (gov.ca.gov doesn't always use rel="category tag").
    """
    if not html:
        return []

    cats: list[str] = []
    seen: set[str] = set()

    for m in _CA_CAT_HREF_RE.finditer(html):
        href = (m.group("href") or "").strip()
        try:
            path = urlsplit(href).path.lower()
        except Exception:
            path = href.lower()

        mm = re.search(r"/category/([^/]+)/", path)
        if not mm:
            continue

        slug = mm.group(1).strip().lower()
        mapped = CA_CATEGORY_SLUG_TO_STATUS.get(slug)
        if mapped and mapped not in seen:
            seen.add(mapped)
            cats.append(mapped)

    return cats

def _pick_primary_ca_status(cats: list[str]) -> str | None:
    """
    Decide which one becomes the single 'status' value.
    Priority: EO > Press Release > Media Advisory > Proclamation > First Partner > Featured
    """
    if not cats:
        return None
    priority = [
        "executive_order",
        "press_release",
        "media_advisory",
        "proclamation",
        "first_partner",
        "featured",
    ]
    for p in priority:
        if p in cats:
            return p
    return cats[0]

async def _filter_new_external_ids(
    conn,
    source_id: str,
    urls: list[str],
) -> list[str]:
    """
    Given candidate urls (external_id), return only those not already in DB for this source.
    Uses a DB-side filter so we don't pull the full source history into Python.
    """
    if not urls:
        return []

    rows = await conn.fetch(
        "select external_id from items where source_id = $1 and external_id = any($2::text[])",
        source_id,
        urls,
    )
    existing = {r["external_id"] for r in rows if r["external_id"]}
    return [u for u in urls if u not in existing]


async def ingest_state_newsroom(states: List[str] | None = None, max_pages: int = 20, limit: int = 200) -> dict:
    targets = states or list(STATE_NEWSROOM_SITES.keys())
    out: Dict[str, dict] = {}

    async with connection() as conn:
        for state in targets:
            base = STATE_NEWSROOM_SITES.get(state)
            if not base:
                out[state] = {"upserted": 0, "skipped": "no_newsroom_configured"}
                continue

            # choose a friendly Referer per state
            ref = (
                "https://www.gov.ca.gov/" if state == "California"
                else "https://gov.texas.gov/" if state == "Texas"
                else "https://www.governor.ny.gov/" if state == "New York"
                else "https://www.illinois.gov/" if state == "Illinois"
                else "https://www.pa.gov/governor/newsroom" if state == "Pennsylvania"
                else "https://www.mass.gov/" if state == "Massachusetts"   # ✅ add
                else "https://governor.wa.gov/" if state == "Washington"
                else "https://www.flgov.com/"
            )

            client_headers = {**BROWSER_UA_HEADERS, "Referer": ref, "Accept-Language": "en-US,en;q=0.9"}

            # Massachusetts: override with true navigation-like headers
            if state == "Massachusetts":
                client_headers = {**MASS_HEADERS}  # (already includes Referer + Accept-Language)

            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
                headers=client_headers,
                follow_redirects=True,
            ) as cx:
                # create/get source
                source_id = await get_or_create_source(
                    conn,
                    f"{state} — Newsroom",
                    "state_newsroom",
                    base,
                )

                # ---------- CALIFORNIA CRON-SAFE / BACKFILL MODE ----------
                ca_existing_count = 0
                ca_backfill = False
                ca_effective_max_pages = max_pages
                ca_effective_limit = limit

                if state == "California":
                    ca_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        source_id,
                    ) or 0
                    ca_backfill = (ca_existing_count == 0)

                    # Backfill: crawl deep until CA_MIN_DATE stops pagination
                    if ca_backfill:
                        ca_effective_max_pages = max(max_pages or 0, 500)
                        ca_effective_limit = 0  # 0 = no slice
                    else:
                        # Cron runs: crawl a buffer so new items aren't hidden
                        ca_effective_max_pages = max(max_pages or 0, 60)
                        ca_effective_limit = limit
                # ---------- END CALIFORNIA MODE ----------

                # ---------- NEW YORK CRON-SAFE / BACKFILL MODE ----------
                ny_existing_count = 0
                ny_backfill = False
                ny_effective_max_pages = max_pages
                ny_effective_limit = limit

                if state == "New York":
                    ny_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        source_id,
                    ) or 0
                    ny_backfill = (ny_existing_count == 0)

                    # Backfill: crawl deep enough to reach the built-in NY cutoff logic
                    # (_collect_listing_urls stops itself when it reaches end or hits cutoff)
                    if ny_backfill:
                        ny_effective_max_pages = max(max_pages or 0, 120)  # allow reaching page~97
                        ny_effective_limit = 0  # 0 = no slice
                    else:
                        # Cron runs: crawl a buffer so “new items hidden behind old” won't be missed
                        ny_effective_max_pages = max(max_pages or 0, 40)
                        ny_effective_limit = limit
                # ---------- END NEW YORK MODE ----------


                # ---------------- PENNSYLVANIA SPECIAL CASE ----------------
                if state == "Pennsylvania":
                    # ✅ Crawl enough candidates so filtering doesn't miss new items
                    # (default limit=200 → we still fetch 350 to find new ones beyond already-ingested)
                    want = 350 if not limit else max(350, limit)
                    urls = await _collect_pa_gov_newsroom_urls(cx, want=want, page_size=50)

                    # ✅ CRON-SAFE: filter first, then apply limit (so we only process NEW items)
                    new_urls = await _filter_new_external_ids(conn, source_id, urls)
                    if limit:
                        new_urls = new_urls[:limit]
                    print(f"PA new urls: {len(new_urls)} of {len(urls)}")

                    upserted = 0
                    for url in new_urls:
                        ar = await _get(cx, url)
                        if ar.status_code >= 400 or not ar.text:
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct:
                            continue

                        html = _nz(ar.text)
                        title = _extract_h1(html) or url
                        pub_dt = _date_from_pa_article(html, url)

                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                title=excluded.title,
                                summary=excluded.summary,
                                published_at = CASE
                                    WHEN excluded.published_at IS NOT NULL THEN excluded.published_at
                                    WHEN items.published_at > (now() + interval '2 days') THEN NULL
                                    ELSE items.published_at
                                END,
                                fetched_at=now()
                            """,
                            url,
                            source_id,
                            _nz(title),
                            _nz(summary),
                            url,
                            "pennsylvania",
                            "Pennsylvania Governor",
                            _pa_status_from_url(url),
                            pub_dt,
                        )
                        upserted += 1

                    out[state] = {"upserted": upserted, "seen_urls": len(urls), "new_urls": len(new_urls)}
                    continue
                # ---------------- END PENNSYLVANIA SPECIAL CASE ----------------


                # ---------------- ILLINOIS SPECIAL CASE ----------------
                if state == "Illinois":
                    want_pages = max_pages if max_pages else 100
                    want_pages = min(want_pages, 100)

                    urls, il_meta = await _collect_il_from_appsearch(
                        cx,
                        max_pages=want_pages,
                        page_size=10,
                    )

                    # ✅ CRON-SAFE: filter first, then apply limit (so you don't miss new items)
                    new_urls = await _filter_new_external_ids(conn, source_id, urls)
                    new_urls = new_urls[:limit]
                    print(f"IL new urls: {len(new_urls)} of {len(urls)}")

                    upserted = 0
                    for url in new_urls:
                        search_title, search_desc, search_pub_dt = il_meta.get(url, ("", "", None))

                        if url.lower().endswith(".pdf"):
                            title = search_title.strip() or url.rsplit("/", 1)[-1]
                            cat_label = _il_pick_category_label(search_desc or "")
                            status = IL_STATUS_BY_LABEL.get(cat_label, "notice")
                            pub_dt = search_pub_dt or _date_from_dated_url(url) or _date_from_il_pdf_filename(url)

                            pr = await _get(cx, url)
                            if not pub_dt:
                                lm = pr.headers.get("Last-Modified")
                                if lm:
                                    try:
                                        pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                    except Exception:
                                        pass

                            pdf_text = _extract_pdf_text_from_bytes(pr.content if pr and pr.content else b"")
                            pdf_text = _nz(pdf_text)

                            summary = ""
                            if pdf_text:
                                summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                            elif search_desc:
                                summary = search_desc

                            if summary:
                                summary = _soft_normalize_caps(summary)
                                summary = await _safe_ai_polish(summary, title, url)

                            await conn.execute(
                                """
                                insert into items (
                                    external_id, source_id, title, summary, url,
                                    jurisdiction, agency, status, published_at, fetched_at
                                )
                                values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                                on conflict (external_id) do update set
                                    source_id=excluded.source_id,
                                    title=excluded.title,
                                    summary=excluded.summary,
                                    url=excluded.url,
                                    jurisdiction=excluded.jurisdiction,
                                    agency=excluded.agency,
                                    status=excluded.status,
                                    published_at = COALESCE(excluded.published_at, items.published_at),
                                    fetched_at=now()
                                """,
                                url,
                                source_id,
                                _nz(title),
                                _nz(summary),
                                url,
                                "illinois",
                                "Illinois Agencies",
                                status,
                                pub_dt,
                            )
                            upserted += 1
                            continue

                        # HTML pages
                        ar = await _get(cx, url)
                        if ar.status_code >= 400 or not ar.text:
                            if search_title or search_desc:
                                cat_label = _il_pick_category_label(search_desc or "")
                                status = IL_STATUS_BY_LABEL.get(cat_label, "notice")
                                await conn.execute(
                                    """
                                    insert into items (
                                        external_id, source_id, title, summary, url,
                                        jurisdiction, agency, status, published_at, fetched_at
                                    )
                                    values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                                    on conflict (external_id) do update set
                                        title=excluded.title,
                                        summary=excluded.summary,
                                        status=excluded.status,
                                        published_at=COALESCE(excluded.published_at, items.published_at),
                                        fetched_at=now()
                                    """,
                                    url,
                                    source_id,
                                    _nz(search_title or url),
                                    _nz(search_desc or ""),
                                    url,
                                    "illinois",
                                    "Illinois Agencies",
                                    status,
                                    search_pub_dt,
                                )
                                upserted += 1
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct:
                            continue

                        html = _nz(ar.text)
                        title = _il_extract_title(html, fallback=(search_title or url))
                        if search_title and (not title or title.lower() in _IL_GENERIC_TITLES):
                            title = search_title
                        
                        cat_label = _il_pick_category_label(search_desc or "")
                        if not cat_label:
                            page_text = _strip_html_to_text(html)
                            cat_label = _il_pick_category_label(page_text)

                        status = IL_STATUS_BY_LABEL.get(cat_label, "notice")

                        pub_dt = (
                            search_pub_dt
                            or _date_from_html_or_url(html, url)
                            or _date_from_il_html(html)
                            or _date_from_json_ld(html)
                        )

                        if not pub_dt:
                            lm = ar.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)

                        # If extractive summary is weak, prefer a real paragraph from the page
                        s = (summary or "").strip().lower()
                        if (not summary) or (len(s) < 60) or ("javascript" in s and "enable" in s):
                            text = _strip_html_to_text(html)
                            paras = [p.strip() for p in re.split(r"\n+", text) if len(p.strip()) > 80]
                            if paras:
                                summary = paras[0]

                        # ONLY use AppSearch description if it is not generic boilerplate
                        if (not summary or len((summary or "").strip()) < 60) and search_desc and not _il_desc_is_generic(search_desc):
                            summary = search_desc


                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            url,
                            source_id,
                            _nz(title),
                            _nz(summary),
                            url,
                            "illinois",
                            "Illinois Agencies",
                            status,
                            pub_dt,
                        )
                        upserted += 1

                    out[state] = {"upserted": upserted, "seen_urls": len(urls), "new_urls": len(new_urls)}
                    continue
                # ---------------- END ILLINOIS SPECIAL CASE ----------------
                # ---------------- MASSACHUSETTS SPECIAL CASE ----------------
                if state == "Massachusetts":
                    # IMPORTANT:
                    # - Cron-safe: only process NEW external_ids
                    # - Backfill-friendly: if DB for that source is empty, ingest normally (no new-filter)
                    # - Also: crawl more than limit so we don't miss new items hiding behind already-ingested ones
                    want_limit = max(500, int(limit or 0))  # MA press recent pages can contain >200; 500 is a good buffer

                    # 1) PRESS RELEASES
                    # Mass "recent" is not enough on page 0 when you have gaps.
                    # Force at least a few pages even if the endpoint passes max_pages=1.
                    want_pages = 14 if (not max_pages or int(max_pages) < 4) else int(max_pages)
                    want_pages = max(1, min(want_pages, 14))  # collector supports up to 14

                    press_urls, meta = await _collect_mass_recent_press(
                        cx,
                        max_pages=want_pages,
                        limit=want_limit,
                    )

                    # If source has zero rows, treat as backfill run (ingest everything we crawled)
                    press_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        source_id,
                    ) or 0

                    if press_existing_count == 0:
                        new_press_urls = press_urls[:]  # backfill mode
                        print(f"MA press backfill: {len(new_press_urls)} urls")
                    else:
                        new_press_urls = await _filter_new_external_ids(conn, source_id, press_urls)
                        if limit:
                            new_press_urls = new_press_urls[:limit]
                        print(f"MA press new urls: {len(new_press_urls)} of {len(press_urls)}")

                    press_upserted = 0
                    for url in new_press_urls:
                        await asyncio.sleep(0.2)

                        ar = await _get(cx, url)
                        if ar.status_code >= 400 or not ar.text:
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct:
                            continue

                        html = _nz(ar.text)

                        listing_title, listing_dt, listing_agency = meta.get(url, ("", None, ""))
                        title = _extract_h1(html) or listing_title or url

                        pub_dt = (
                            listing_dt
                            or _date_from_html_or_url(html, url)
                            or _date_from_json_ld(html)
                            or _date_from_mass_detail(html)
                        )

                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)

                        agency = listing_agency.strip() or "Massachusetts Agencies"

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            url,
                            source_id,
                            _nz(title),
                            _nz(summary),
                            url,
                            "massachusetts",
                            _nz(agency),
                            "press_release",
                            pub_dt,
                        )
                        press_upserted += 1

                    # 2) EXECUTIVE ORDERS (separate source)
                    eo_source_id = await get_or_create_source(
                        conn,
                        "Massachusetts — Executive Orders",
                        "state_executive_orders",
                        MA_EO_LANDING,
                    )

                    eo_urls = await _collect_ma_executive_order_urls(
                        cx,
                        min_eo_number=604,
                        limit=2000,  # crawl enough; we'll filter below
                    )

                    eo_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        eo_source_id,
                    ) or 0

                    if eo_existing_count == 0:
                        new_eo_urls = eo_urls[:]  # backfill mode
                        print(f"MA EO backfill: {len(new_eo_urls)} urls")
                    else:
                        new_eo_urls = await _filter_new_external_ids(conn, eo_source_id, eo_urls)
                        if limit:
                            new_eo_urls = new_eo_urls[:limit]
                        print(f"MA EO new urls: {len(new_eo_urls)} of {len(eo_urls)}")

                    eo_upserted = 0
                    for eo_url in new_eo_urls:
                        await asyncio.sleep(0.2)

                        ar = await _get(cx, eo_url, headers={**MASS_HEADERS, "Referer": MA_EO_LANDING})
                        if ar.status_code >= 400 or not ar.text:
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct:
                            continue

                        html = _nz(ar.text)
                        title = _extract_h1(html) or eo_url

                        pub_dt = (
                            _date_from_ma_eo_detail(html)
                            or _date_from_json_ld(html)
                            or _date_from_html_or_url(html, eo_url)
                        )

                        summary = summarize_extractive(title, eo_url, html, max_sentences=2, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, eo_url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            eo_url,
                            eo_source_id,
                            _nz(title),
                            _nz(summary),
                            eo_url,
                            "massachusetts",
                            "Massachusetts Governor",
                            "executive_order",
                            pub_dt,
                        )
                        eo_upserted += 1

                    out[state] = {
                        "press_upserted": press_upserted,
                        "eo_upserted": eo_upserted,
                        "press_seen_urls": len(press_urls),
                        "press_new_urls": len(new_press_urls),
                        "eo_seen_urls": len(eo_urls),
                        "eo_new_urls": len(new_eo_urls),
                    }
                    continue
                # ---------------- END MASSACHUSETTS SPECIAL CASE ----------------

                # ---------------- FLORIDA SPECIAL CASE ---------------
                if state == "Florida":
                    urls, eo_rows = await _collect_florida_urls(
                        cx,
                        max_pages=max_pages,
                        limit=limit,
                    )

                    # 🔎 DEBUG — right after crawling Florida
                    print("FL collected:", len(urls), "urls")
                    print("FL sample urls:", urls[:10])
                    print("FL EO rows(meta):", len(eo_rows), "sample:", eo_rows[:5])

                    # 🔹 Map normalized PDF URL → (title, date_str) from the EO listing table
                    eo_meta_by_url: dict[str, tuple[str, str]] = {}
                    for pdf_url, t, d in eo_rows:
                        eo_meta_by_url[_abs_flgov(pdf_url)] = (t, d)

                    # ✅ Existing external_ids already stored for this Florida source
                    existing_rows = await conn.fetch(
                        "select external_id from items where source_id = $1",
                        source_id,
                    )
                    existing_ids = {r["external_id"] for r in existing_rows if r["external_id"]}

                    # ✅ Only process brand-new URLs (normalize PDFs to absolute so keys match)
                    def _fl_ext_id(u: str) -> str:
                        uu = (u or "").strip()
                        return _abs_flgov(uu) if uu.lower().endswith(".pdf") else uu

                    new_urls: list[str] = []
                    for u in urls:
                        ext_id = _fl_ext_id(u)
                        if ext_id and ext_id not in existing_ids:
                            new_urls.append(u)

                    print("FL new urls:", len(new_urls), "of", len(urls))


                    upserted = 0
                    for url in new_urls:
                        # EO PDFs
                        # EO detail pages (listing returns these)
                        # --- Florida EO PDF path (direct PDFs) ---
                        if state == "Florida" and url.lower().endswith(".pdf"):
                            pdf_url = _abs_flgov(url)

                            listing_title, date_str = eo_meta_by_url.get(pdf_url, ("", ""))
                            title = listing_title.strip() or pdf_url.rsplit("/", 1)[-1]
                            pub_dt = _try_parse_us_date(date_str) if date_str else None

                            pr = await _get(cx, pdf_url)
                            if pr.status_code >= 400:
                                continue

                            pdf_text = _extract_pdf_text_from_bytes(pr.content or b"")
                            pdf_text = _nz(pdf_text)

                            # best date signal from the EO testimony line
                            if not pub_dt and pdf_text:
                                pub_dt = _date_from_florida_eo_pdf_text(pdf_text)

                            # fallback: Last-Modified
                            if not pub_dt:
                                lm = pr.headers.get("Last-Modified")
                                if lm:
                                    try:
                                        pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                    except Exception:
                                        pass

                            summary = ""
                            if pdf_text:
                                summary = summarize_text(pdf_text, max_sentences=3, max_chars=700) or ""
                                summary = _soft_normalize_caps(summary)
                                summary = await _safe_ai_polish(summary, title, pdf_url)

                            await conn.execute(
                                """
                                insert into items (
                                    external_id, source_id, title, summary, url,
                                    jurisdiction, agency, status, published_at, fetched_at
                                )
                                values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                                on conflict (external_id) do update set
                                    source_id=excluded.source_id,
                                    title=excluded.title,
                                    summary=excluded.summary,
                                    url=excluded.url,
                                    jurisdiction=excluded.jurisdiction,
                                    agency=excluded.agency,
                                    status=excluded.status,
                                    published_at = COALESCE(excluded.published_at, items.published_at),
                                    fetched_at=now()
                                """,
                                pdf_url,
                                source_id,
                                _nz(title),
                                _nz(summary),
                                pdf_url,
                                "florida",
                                "Florida Governor",
                                "executive_order",
                                pub_dt,
                            )
                            upserted += 1
                            continue

                        # For Florida HTML, we only care about press releases.
                        if "/eog/news/press/" not in url.lower():
                            continue

                        # Normal HTML newsroom articles: press, emergency, press-kit, proclamations, etc.
                        ar = await _get(cx, url)
                        if ar.status_code >= 400:
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct or not ar.text:
                            continue

                        html = _nz(ar.text)
                        title = _extract_h1(html) or url

                        # Florida newsroom pages put "November 17, 2025" in the body,
                        # not in meta tags, so parse that first.
                        pub_dt = (
                            _date_from_flgov_html(html, url)
                            or _date_from_html_or_url(html, url)
                            or _date_from_json_ld(html)
                        )

                        if not pub_dt:
                            lm = ar.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        # 🔹 FINAL FALLBACK: ask the LLM to parse the date from plain text
                        if not pub_dt:
                            try:
                                text = _strip_html_to_text(html)
                                ai_dt = await _safe_ai_extract_flgov_date(text, url)
                                if ai_dt:
                                    pub_dt = ai_dt
                            except Exception:
                                # swallow errors so ingest doesn’t break
                                pass


                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                        if not summary:
                            text = _strip_html_to_text(html)
                            paras = [p.strip() for p in re.split(r'\n+', text) if len(p.strip()) > 60]
                            if paras:
                                summary = paras[0]

                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)

                        title = _nz(title)
                        summary = _nz(summary)
                        url_safe = _nz(url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            url_safe,
                            source_id,
                            title,
                            summary or "",
                            url_safe,
                            state.lower(),
                            f"{state} Governor",
                            "press_release",
                            pub_dt,
                        )
                        upserted += 1

                    out[state] = {"upserted": upserted, "seen_urls": len(urls), "new_urls": len(new_urls)}
                    # Skip generic logic for Florida
                    continue
                # ---------------- END FLORIDA SPECIAL CASE ----------------
                # ---------------- WASHINGTON SPECIAL CASE ----------------
                if state == "Washington":
                    want_limit = limit if limit else 500
                    want_pages = max_pages if max_pages else 50

                    # -------------------------
                    # 1) PRESS RELEASES
                    # -------------------------
                    press_urls_all = await _collect_wa_news_urls(cx, max_pages=want_pages, limit=want_limit)

                    press_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        source_id,
                    ) or 0

                    if press_existing_count == 0:
                        # backfill mode
                        press_urls = press_urls_all[:]
                        press_mode = "backfill"
                    else:
                        # cron-safe mode: only new external_ids
                        press_urls = await _filter_new_external_ids(conn, source_id, press_urls_all)
                        if limit:
                            press_urls = press_urls[:limit]
                        press_mode = "cron_safe"

                    upserted_press = 0
                    for url in press_urls:
                        ar = await _get(cx, url)
                        if ar.status_code >= 400 or not ar.text:
                            continue

                        ct = (ar.headers.get("Content-Type") or "").lower()
                        if "html" not in ct:
                            continue

                        html = _nz(ar.text)
                        title = _extract_h1(html) or url

                        pub_dt = (
                            _date_from_html_or_url(html, url)
                            or _date_from_json_ld(html)
                            or _date_from_wa_html(html)
                        )
                        if not pub_dt:
                            lm = ar.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            # ✅ only polishing NEW items because press_urls is filtered above
                            summary = await _safe_ai_polish(summary, title, url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            url,
                            source_id,
                            _nz(title),
                            _nz(summary),
                            url,
                            "washington",
                            "Washington Governor",
                            "press_release",
                            pub_dt,
                        )
                        upserted_press += 1

                    # -------------------------
                    # 2) EXECUTIVE ORDERS (separate source)
                    # -------------------------
                    eo_source_id = await get_or_create_source(
                        conn,
                        "Washington — Executive Orders",
                        "state_executive_orders",
                        WA_EO_CURRENT_URL,
                    )

                    eo_rows_all = await _collect_wa_executive_orders(
                        cx,
                        max_pages_each=want_pages,
                        limit_each=2000,
                    )

                    eo_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        eo_source_id,
                    ) or 0

                    if eo_existing_count == 0:
                        eo_rows = eo_rows_all[:]
                        eo_mode = "backfill"
                    else:
                        eo_urls_all = [pdf_url for (_, _, pdf_url, _) in eo_rows_all if pdf_url]
                        eo_new_urls = set(await _filter_new_external_ids(conn, eo_source_id, eo_urls_all))
                        if limit:
                            # keep order while limiting
                            limited = []
                            for (_, _, pdf_url, _) in eo_rows_all:
                                if pdf_url in eo_new_urls:
                                    limited.append(pdf_url)
                                    if len(limited) >= limit:
                                        break
                            eo_new_urls = set(limited)
                        eo_rows = [row for row in eo_rows_all if row[2] in eo_new_urls]
                        eo_mode = "cron_safe"

                    upserted_eo = 0
                    for eo_number, eo_title, pdf_url, issued_dt_fallback in eo_rows:
                        pr = await _get(
                            cx,
                            pdf_url,
                            read_timeout=120.0,
                            headers={**BROWSER_UA_HEADERS, "Referer": WA_EO_CURRENT_URL},
                        )
                        if pr.status_code >= 400 or not pr.content:
                            continue

                        pdf_text = _extract_pdf_text_from_bytes(pr.content or b"")
                        pdf_text = _nz(pdf_text)

                        dt_pdf = _wa_date_from_pdf_text(pdf_text)
                        pub_dt = dt_pdf or issued_dt_fallback

                        if issued_dt_fallback and dt_pdf:
                            if issued_dt_fallback.year < 2000 and dt_pdf.year >= 2000:
                                pub_dt = dt_pdf

                        if not pub_dt:
                            lm = pr.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        final_title = (eo_title or "").strip()
                        if eo_number and eo_number not in final_title:
                            final_title = f"{eo_number} - {final_title}" if final_title else eo_number
                        if not final_title:
                            final_title = eo_number or pdf_url.rsplit("/", 1)[-1]

                        summary = ""
                        if pdf_text and len(pdf_text.strip()) >= 200:
                            summary = summarize_text(pdf_text, max_sentences=3, max_chars=900) or ""
                            if summary:
                                summary = _soft_normalize_caps(summary)
                                # ✅ only polishing NEW items because eo_rows is filtered above
                                summary = await _safe_ai_polish(summary, final_title, pdf_url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            pdf_url,
                            eo_source_id,
                            _nz(final_title),
                            _nz(summary),
                            pdf_url,
                            "washington",
                            "Washington Governor",
                            "executive_order",
                            pub_dt,
                        )
                        upserted_eo += 1

                    # -------------------------
                    # 3) PROCLAMATIONS (separate source)
                    # -------------------------
                    proc_source_id = await get_or_create_source(
                        conn,
                        "Washington — Proclamations",
                        "state_proclamations",
                        WA_PROC_URL,
                    )

                    proc_list_all = await _collect_wa_proclamation_pdfs(
                        cx,
                        max_pages=want_pages,
                        limit=2000,
                        stop_at_pdf=WA_PROC_STOP_AT_PDF,
                    )

                    proc_existing_count = await conn.fetchval(
                        "select count(*) from items where source_id = $1",
                        proc_source_id,
                    ) or 0

                    if proc_existing_count == 0:
                        proc_list = proc_list_all[:]
                        proc_mode = "backfill"
                    else:
                        proc_urls_all = [pdf_url for (pdf_url, _) in proc_list_all if pdf_url]
                        proc_new_urls = set(await _filter_new_external_ids(conn, proc_source_id, proc_urls_all))
                        if limit:
                            limited = []
                            for (pdf_url, _) in proc_list_all:
                                if pdf_url in proc_new_urls:
                                    limited.append(pdf_url)
                                    if len(limited) >= limit:
                                        break
                            proc_new_urls = set(limited)
                        proc_list = [row for row in proc_list_all if row[0] in proc_new_urls]
                        proc_mode = "cron_safe"

                    upserted_proc = 0
                    for pdf_url, title_guess in proc_list:
                        pr = await _get(
                            cx,
                            pdf_url,
                            read_timeout=120.0,
                            headers={**BROWSER_UA_HEADERS, "Referer": WA_PROC_URL},
                        )
                        if pr.status_code >= 400 or not pr.content:
                            continue

                        pdf_text = _extract_pdf_text_from_bytes(pr.content or b"")
                        pdf_text = _nz(pdf_text)

                        pub_dt = _wa_date_from_proc_pdf_text(pdf_text)
                        if not pub_dt:
                            lm = pr.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        final_title = (title_guess or "").strip() or pdf_url.rsplit("/", 1)[-1]

                        summary = ""
                        if pdf_text and len(pdf_text.strip()) >= 200:
                            summary = summarize_text(pdf_text, max_sentences=3, max_chars=900) or ""
                            if summary:
                                summary = _soft_normalize_caps(summary)
                                # ✅ only polishing NEW items because proc_list is filtered above
                                summary = await _safe_ai_polish(summary, final_title, pdf_url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            pdf_url,
                            proc_source_id,
                            _nz(final_title),
                            _nz(summary),
                            pdf_url,
                            "washington",
                            "Washington Governor",
                            "proclamation",
                            pub_dt,
                        )
                        upserted_proc += 1

                    out[state] = {
                        "press_mode": press_mode,
                        "eo_mode": eo_mode,
                        "proc_mode": proc_mode,
                        "press_upserted": upserted_press,
                        "eo_upserted": upserted_eo,
                        "proc_upserted": upserted_proc,
                        "press_seen_urls": len(press_urls_all),
                        "press_new_urls": len(press_urls),
                        "eo_seen_urls": len(eo_rows_all),
                        "eo_new_urls": len(eo_rows),
                        "proc_seen_urls": len(proc_list_all),
                        "proc_new_urls": len(proc_list),
                    }
                    continue
                # ---------------- END WASHINGTON SPECIAL CASE ----------------


                # 1) collect links ...
                if state == "Texas":
                    # Texas: /news archive already contains everything; category
                    # listings are redundant and just duplicate work.
                    roots = [base]
                else:
                    roots = [base] + STATE_EXTRA_LISTS.get(state, [])

                found_urls: List[str] = []
                eo_rows: List[tuple[str, str, str]] = []

                for root in roots:
                    if state == "Texas":
                        print("TX listing root:", root)

                    r = await _get(cx, root)
                    if r.status_code >= 400 or not r.text:
                        if state == "Texas":
                            print("TX listing root FAILED with status:", r.status_code)
                        continue

                    # ✅ FLORIDA EO: do NOT use _collect_listing_urls on /executive-orders
                    if state == "Florida" and "/eog/news/executive-orders" in root:
                        eo_pdf_urls = await _collect_florida_eo_pdfs(cx, years=[2026, 2025, 2024])
                        found_urls.extend(eo_pdf_urls)
                        continue

                    # default behavior for everything else (including Florida press)
                    if state == "California":
                        mp = ca_effective_max_pages
                    elif state == "New York":
                        mp = ny_effective_max_pages
                    else:
                        mp = max_pages

                    found_urls.extend(await _collect_listing_urls(cx, root, mp))


                urls = list(dict.fromkeys(found_urls))


                # Drop obvious asset files (keep PDFs)
                ASSET_EXT_RE = re.compile(
                    r'\.(?:css|js|json|xml|rss|atom|jpg|jpeg|png|gif|svg|ico|webp|bmp|tiff|mp4|mp3|wav|avi|mov|zip|rar|7z|tar|gz|docx?|xlsx?|pptx?)$',
                    re.I
                )
                urls = [u for u in urls if (u.lower().endswith(".pdf") or not ASSET_EXT_RE.search(u))]

                # Illinois: keep only article-like HTML paths and any PDFs
                if state == "Illinois":
                    urls = [u for u in urls if _is_il_article_like(u)]

                # Belt-and-suspenders: drop non-web links
                urls = [u for u in urls if not u.lower().startswith(("mailto:", "tel:"))]

                # New York: skip PDFs (often image-only)
                if state == "New York":
                    cleaned: List[str] = []
                    for u in urls:
                        parts = urlsplit(u)
                        path = parts.path or ""
                        # keep detail newsroom pages like /news/statement-governor-kathy-hochul-128
                        if "/news/" in path and not parts.query:
                            cleaned.append(u)
                        # and EO detail pages like /executive-order/no-54-...
                        elif "/executive-order/" in path:
                            cleaned.append(u)
                    urls = cleaned


                # (then your FL EO table merge and limit)
                eo_pdf_urls = [u for (u, _, _) in eo_rows if u not in urls] if state == "Florida" else []
                if state == "Florida":
                    urls = urls + eo_pdf_urls  # don't slice yet for TX-style cron safety

                # ---------------- CRON-SAFE FILTERING ----------------
                new_urls = urls

                # Texas (existing behavior)
                if state == "Texas":
                    new_urls = await _filter_new_external_ids(conn, source_id, urls)
                    new_urls = new_urls[:limit]
                    print(f"TX new urls: {len(new_urls)} of {len(urls)}")

                # California (NEW behavior)
                elif state == "California":
                    if not ca_backfill:
                        # ✅ Cron-safe: filter FIRST, then apply limit
                        new_urls = await _filter_new_external_ids(conn, source_id, urls)
                        if ca_effective_limit:
                            new_urls = new_urls[:ca_effective_limit]
                        print(f"CA new urls: {len(new_urls)} of {len(urls)}")
                    else:
                        # ✅ Backfill: ingest everything we crawled (cutoff stops pagination)
                        new_urls = urls
                        print(f"CA backfill urls: {len(new_urls)} (existing was 0)")

                # New York (NEW behavior)
                elif state == "New York":
                    if not ny_backfill:
                        # ✅ Cron-safe: filter FIRST, then apply limit
                        new_urls = await _filter_new_external_ids(conn, source_id, urls)
                        if ny_effective_limit:
                            new_urls = new_urls[:ny_effective_limit]
                        print(f"NY new urls: {len(new_urls)} of {len(urls)}")
                    else:
                        # ✅ Backfill: ingest everything we crawled (until cutoff stops pagination)
                        new_urls = urls
                        print(f"NY backfill urls: {len(new_urls)} (existing was 0)")

                # Other states keep old behavior (slice early)
                else:
                    new_urls = urls[:limit]


                # 2) fetch each article and upsert
                upserted = 0
                for idx, url in enumerate(new_urls, start=1):
                    if state == "Texas":
                        print(f"TX item {idx}/{len(new_urls)}:", url)
                    if state == "New York":
                        print(f"NY item {idx}/{len(new_urls)}:", url)

                    # 🗽 NY network hardening: small jitter every 25 items
                    if state == "New York" and idx % 25 == 0:
                        await asyncio.sleep(0.25)

                    # --- Florida EO PDF path ---
                    # --- Florida EO PDF path (direct PDFs from _collect_florida_eo_pdfs) ---
                    if state == "Florida" and url.lower().endswith(".pdf"):
                        pdf_url = url
                        title = pdf_url.rsplit("/", 1)[-1]  # you can improve later (e.g. parse EO number)
                        pub_dt = None

                        pr = await _get(cx, pdf_url)
                        if pr.status_code >= 400:
                            continue

                        pdf_text = _extract_pdf_text_from_bytes(pr.content or b"")
                        pdf_text = _nz(pdf_text)

                        # Date from EO testimony line (best signal)
                        if pdf_text:
                            pub_dt = _date_from_florida_eo_pdf_text(pdf_text)

                        # Fallback: Last-Modified header if needed
                        if not pub_dt:
                            lm = pr.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass

                        summary = ""
                        if pdf_text:
                            summary = summarize_text(pdf_text, max_sentences=3, max_chars=700) or ""
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, pdf_url)

                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            pdf_url,
                            source_id,
                            _nz(title),
                            _nz(summary),
                            pdf_url,
                            "florida",
                            "Florida Governor",
                            "executive_order",
                            pub_dt,
                        )
                        upserted += 1
                        continue

                    # --- Illinois PDF path (IPA / CleanEnergy / EnergyEquity etc.) ---
                    if state == "Illinois" and url.lower().endswith(".pdf"):
                        title = url.rsplit("/", 1)[-1]
                        pr = await _get(cx, url)
                        if pr.status_code >= 400:
                            # still upsert using listing title/desc + status + pub_dt
                            pdf_text = ""
                        else:
                            pdf_text = _extract_pdf_text_from_bytes(pr.content or b"")

                        # date from URL or filename, else Last-Modified header
                        pub_dt = _date_from_dated_url(url) or _date_from_il_pdf_filename(url)
                        if not pub_dt:
                            lm = pr.headers.get("Last-Modified")
                            if lm:
                                try:
                                    pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                                except Exception:
                                    pass
                        pdf_text = _extract_pdf_text_from_bytes(pr.content if pr and pr.content else b"")
                        pdf_text = _nz(pdf_text)
                        summary = ""
                        if pdf_text:
                            summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                            if summary:
                                summary = _soft_normalize_caps(summary)
                                summary = await _safe_ai_polish(summary, title, url)
                        
                        title = _nz(title)
                        summary = _nz(summary)
                        url = _nz(url)
                        await conn.execute(
                            """
                            insert into items (
                                external_id, source_id, title, summary, url,
                                jurisdiction, agency, status, published_at, fetched_at
                            )
                            values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                            on conflict (external_id) do update set
                                source_id=excluded.source_id,
                                title=excluded.title,
                                summary=excluded.summary,
                                url=excluded.url,
                                jurisdiction=excluded.jurisdiction,
                                agency=excluded.agency,
                                status=excluded.status,
                                published_at = COALESCE(excluded.published_at, items.published_at),
                                fetched_at=now()
                            """,
                            url,                 # external_id
                            source_id,
                            title,
                            summary or "",
                            url,
                            state.lower(),
                            "Illinois Agencies",
                            "notice",
                            pub_dt,
                        )
                        upserted += 1
                        continue

                    # --- Normal HTML newsroom article path (CA + FL + NY EO HTML) ---
                    ar = await _get(cx, url)
                    if ar.status_code >= 400:
                        continue

                    # Guard on content-type to ensure we only summarize real HTML
                    ct = (ar.headers.get("Content-Type") or "").lower()
                    if "html" not in ct:
                        # Not HTML (e.g., css, json, xml); skip
                        continue

                    if not ar.text:
                        continue

                    html = _nz(ar.text)
                    title = _extract_h1(html) or url

                    # 1st: generic meta or URL date
                    pub_dt = _date_from_html_or_url(html, url)

                    # 2nd: Texas-specific "December 5, 2025 | Austin, Texas | Press Release" line
                    if not pub_dt and state == "Texas":
                        pub_dt = _date_from_texas_html(html)

                    # 2nd: Illinois-specific patterns
                    if not pub_dt and "illinois.gov" in url:
                        pub_dt = _date_from_il_html(html)

                    # 3rd: JSON-LD (many IL pages have it)
                    if not pub_dt:
                        pub_dt = _date_from_json_ld(html)

                     # 4th: New York header "Month DD, YYYY" line
                    if not pub_dt and state == "New York":
                        pub_dt = _date_from_nygov_html(html)

                    # 5th: Last-Modified header
                    if not pub_dt:
                        lm = ar.headers.get("Last-Modified")
                        if lm:
                            try:
                                pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                            except Exception:
                                pass

                    # 🔹 Texas: stop ingesting items older than Jan 1, 2024
                    if state == "Texas" and pub_dt is not None and pub_dt < TEXAS_MIN_DATE:
                        print("TX older than cutoff, skipping:", url, "date:", pub_dt)
                        continue

                    # 🔹 New York: only keep newsroom items from 2025-06-01 onward.
                    # IMPORTANT: do NOT apply this to executive orders – you want all EOs.
                    if (
                        state == "New York"
                        and "/news/" in url
                        and "/executive-order/" not in url
                        and pub_dt is not None
                        and pub_dt < NY_NEWS_MIN_DATE
                    ):
                        continue

                    summary = None

                    # If this is a New York EO HTML page, try to summarize the linked PDF instead
                    if state == "New York" and "/executive-order/" in url:
                        m_pdf = _NY_EO_PDF_RE.search(html)
                        if m_pdf:
                            pdf_url = _abs_nygov(m_pdf.group("u"))
                            pr = await _get(cx, pdf_url)
                            pdf_text = _extract_pdf_text_from_bytes(pr.content if pr and pr.content else b"")
                            if pdf_text:
                                summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)

                    # If no PDF summary (or not NY EO), fall back to HTML extractive summary
                    if not summary:
                        summary = summarize_extractive(title, url, html, max_sentences=2, max_chars=700)
                        if not summary:
                            text = _strip_html_to_text(html)
                            paras = [p.strip() for p in re.split(r'\n+', text) if len(p.strip()) > 60]
                            if paras:
                                summary = paras[0]

                    if summary:
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, url)

                    # choose correct status for HTML pages
                    status = "notice"
                    ca_cats = []

                    if state == "California":
                        print(
                            "CA has /category/ in html?",
                            ("/category/" in html.lower()),
                            "url:",
                            url,
                        )

                        ca_cats = _ca_categories_from_html(html)
                        primary = _pick_primary_ca_status(ca_cats)
                        print("CA categories:", ca_cats, "primary:", primary, "url:", url)

                        if primary:
                            status = primary

                    elif state == "New York" and "/executive-order/" in url:
                        status = "executive_order"

                    elif state == "Texas":
                        cat = _category_from_texas_html(html)
                        if cat:
                            status = cat

                    elif state == "New York":
                        cat = _category_from_nygov_html(html)
                        if cat:
                            status = cat
                    
                    # ✅ ADD THESE LINES RIGHT HERE
                    categories = None
                    if state == "California":
                        categories = ca_cats

                    title = _nz(title)
                    summary = _nz(summary)
                    url = _nz(url)

                    # upsert
                    await conn.execute(
                        """
                        insert into items (
                            external_id, source_id, title, summary, url,
                            jurisdiction, agency, status, published_at, categories, fetched_at
                        )
                        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
                        on conflict (external_id) do update set
                            source_id=excluded.source_id,
                            title=excluded.title,
                            summary=excluded.summary,
                            url=excluded.url,
                            jurisdiction=excluded.jurisdiction,
                            agency=excluded.agency,
                            status=excluded.status,
                            categories=excluded.categories,
                            published_at = COALESCE(excluded.published_at, items.published_at),
                            fetched_at=now()
                        """,
                        url,
                        source_id,
                        title,
                        summary or "",
                        url,
                        state.lower(),
                        ("Illinois Agencies" if state == "Illinois" else f"{state} Governor"),
                        status,
                        pub_dt,
                        categories,     # ✅ NEW arg ($10)
                    )
                    upserted += 1 

                out[state] = {"upserted": upserted, "seen_urls": len(urls), "new_urls": len(new_urls)}

    return out
