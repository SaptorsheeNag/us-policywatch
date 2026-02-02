# app/ingest_states3.py
from __future__ import annotations
import html as html_lib
import asyncio
from urllib.parse import urljoin
from urllib.parse import urlsplit, unquote, urlunsplit
from email.utils import parsedate_to_datetime
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
import io
from pypdf import PdfReader
from playwright.async_api import async_playwright, Page
import httpx
from dotenv import load_dotenv
from pathlib import Path
from uuid import UUID

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)

from .db import connection
from .ingest_federal_register import get_or_create_source
from .summarize import (
    summarize_text,
    _soft_normalize_caps,
    _strip_html_to_text,
    BROWSER_UA_HEADERS,
)
from .ai_summarizer import ai_polish_summary

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


# ----------------------------
# Michigan config (Whitmer)
# ----------------------------

MI_JURISDICTION = "michigan"
MI_AGENCY = "Michigan Governor (Whitmer)"

MI_PUBLIC_PAGES = {
    "press_releases": "https://www.michigan.gov/whitmer/news/press-releases",
    "proclamations": "https://www.michigan.gov/whitmer/news/proclamations",
    "state_orders_and_directives": "https://www.michigan.gov/whitmer/news/state-orders-and-directives",
}

MI_STATUS_MAP = {
    "press_releases": "press_release",
    "proclamations": "proclamation",
    "state_orders_and_directives": "executive_order",  # mixed EO/ED, keep as executive_order status
}

# Cutoffs (include this item, then stop)
MI_PRESS_CUTOFF_URL = "https://www.michigan.gov/whitmer/news/press-releases/2025/01/06/whitmer-lowers-flags-to-honor-former-state-representative-james-mcbryde"
MI_PROC_CUTOFF_URL = "https://www.michigan.gov/whitmer/news/proclamations/2023/12/30/january-30-2024-fred-korematsu-day"
MI_ORDERS_CUTOFF_URL = "https://www.michigan.gov/whitmer/news/state-orders-and-directives/2024/01/05/executive-order-2024_1-michigan-developmental-disabilities-council"

# Your DevTools SXA listing endpoints
MI_SXA = {
    "press_releases": {
        "base": "https://www.michigan.gov/whitmer/sxa/search/results/",
        "params": {
            "s": "{B7A692F7-5CC1-4AC2-8F1D-380CA54F9735}|{62E9FB6A-7717-4EF1-832C-E5ECBB9BB2D9}",
            "itemid": "{DBE1F425-5DD1-4626-81EA-C19119DBC337}",
            "sc_lang": "en",
            "sig": "",
            "autoFireSearch": "true",
            "v": "{B7A22BE8-17FC-44A5-83BC-F54442A57941}",
            "o": "Article Date,Descending",
        },
        "referer": MI_PUBLIC_PAGES["press_releases"],
        "cutoff_url": MI_PRESS_CUTOFF_URL,
        "source_name": "Michigan — Press Releases",
        "status": MI_STATUS_MAP["press_releases"],
    },
    "proclamations": {
        "base": "https://www.michigan.gov/whitmer/sxa/search/results/",
        "params": {
            "s": "{01462918-0C02-4313-835D-C974112D4E80}|{62E9FB6A-7717-4EF1-832C-E5ECBB9BB2D9}",
            "itemid": "{A93F24AA-BCAB-46E1-9700-BCE0FBFA56F1}",
            "sc_lang": "en",
            "sig": "",
            "autoFireSearch": "true",
            "v": "{66B61C4F-96A9-41F3-A608-4CBBC5A3AC74}",
            "o": "Article Date,Descending",
        },
        "referer": MI_PUBLIC_PAGES["proclamations"],
        "cutoff_url": MI_PROC_CUTOFF_URL,
        "source_name": "Michigan — Proclamations",
        "status": MI_STATUS_MAP["proclamations"],
    },
    "state_orders_and_directives": {
        "base": "https://www.michigan.gov/whitmer/sxa/search/results/",
        "params": {
            "s": "{C9013E3C-454C-49EA-B55C-DF2B4CC1F0A6}|{62E9FB6A-7717-4EF1-832C-E5ECBB9BB2D9}",
            "itemid": "{43F87B23-1E6B-407D-868A-F04B024FDDBA}",
            "sc_lang": "en",
            "sig": "",
            "autoFireSearch": "true",
            "v": "{66B61C4F-96A9-41F3-A608-4CBBC5A3AC74}",
            "o": "Article Date,Descending",
        },
        "referer": MI_PUBLIC_PAGES["state_orders_and_directives"],
        "cutoff_url": MI_ORDERS_CUTOFF_URL,
        "source_name": "Michigan — State Orders & Directives",
        "status": MI_STATUS_MAP["state_orders_and_directives"],
    },
}

# ----------------------------
# Tennessee config
# ----------------------------

TN_JURISDICTION = "tennessee"
TN_AGENCY_PRESS = "Tennessee Dept. of Finance & Administration"
TN_AGENCY_SOS = "Tennessee Secretary of State"

TN_PUBLIC_PAGES = {
    "press_releases": "https://www.tn.gov/news.press-releases.html",
    "executive_orders": "https://sos.tn.gov/publications/services/executive-orders-governor-bill-lee",
    "proclamations": "https://tnsos.net/publications/proclamations/",
}

TN_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

# Cutoffs (include this item, then stop)
TN_PRESS_CUTOFF_URL = "https://www.tn.gov/finance/news/2025/1/6/giles-county-resident-convicted-of-tenncare-fraud.html"
TN_EO_CUTOFF_URL = "https://publications.tnsosfiles.com/pub/execorders/exec-orders-lee104.pdf"
TN_PROC_CUTOFF_URL = "https://tnsos.net/publications/proclamations/files/2586.pdf"
TN_PRESS_CUTOFF_DT = datetime(2025, 1, 6, tzinfo=timezone.utc)

# ----------------------------
# Helpers
# ----------------------------

_SXA_URL_RE = re.compile(
    r"""(?P<url>(?:https?:\/\/www\.michigan\.gov)?\/whitmer\/sxa\/search\/results\/\?(?:[^"'<>]+))""",
    re.I,
)

def _extract_sig_for_itemid(html: str, itemid: str) -> str | None:
    if not html or not itemid:
        return None

    h = html_lib.unescape(html)
    h = h.replace("\\u0026", "&").replace("\\/", "/")

    # --- normalize itemid variants ---
    itemid_raw = itemid
    itemid_nobraces = itemid.strip().strip("{}")
    itemid_lower = itemid_raw.lower()
    itemid_nobraces_lower = itemid_nobraces.lower()

    # common URL-encoded variant: %7B...%7D
    itemid_urlenc = itemid_raw.replace("{", "%7B").replace("}", "%7D")
    itemid_urlenc_lower = itemid_urlenc.lower()

    # 1) primary: find actual results URL and parse query
    for m in _SXA_URL_RE.finditer(h):
        u = m.group("url")
        if u.startswith("/"):
            u = "https://www.michigan.gov" + u

        parts = urlsplit(u)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))

        q_itemid = (q.get("itemid") or "").strip()
        q_itemid_nobraces = q_itemid.strip("{}").lower()

        if q_itemid == itemid_raw and q.get("sig"):
            return q["sig"]
        if q_itemid_nobraces == itemid_nobraces_lower and q.get("sig"):
            return q["sig"]

    # 2) fallback: look for "sig":"..." near occurrences of the itemid
    # Search windows around itemid variants
    needles = [itemid_lower, itemid_nobraces_lower, itemid_urlenc_lower]
    hay = h.lower()

    for needle in needles:
        if not needle:
            continue
        start = 0
        while True:
            idx = hay.find(needle, start)
            if idx == -1:
                break

            # take a window around match
            lo = max(0, idx - 1500)
            hi = min(len(h), idx + 1500)
            chunk = h[lo:hi]

            # patterns where sig appears as JSON or query-like text
            # sometimes it is not called "sig" — could be "signature", "searchSignature", etc.
            m1 = re.search(
                r'''["'](?:sig|signature|searchSignature|search_signature|searchSig|sxaSignature)["']\s*:\s*["']([^"']+)["']''',
                chunk,
                re.I,
            )
            if m1:
                return m1.group(1).strip()

            m2 = re.search(r'''(?:\?|&|\\u0026)(?:sig|signature|searchSignature|search_signature|searchSig)=([^&"'<>\s]+)''', chunk, re.I)
            if m2:
                return m2.group(1).strip()

            # sometimes sig is in data attributes like data-sig="..."
            m3 = re.search(r'''data[-_]?sig\s*=\s*["']([^"']+)["']''', chunk, re.I)
            if m3:
                return m3.group(1).strip()

            # ✅ broader: data-signature / data-searchsignature / data-searchsig, etc
            m4 = re.search(
                r'''data[-_]?(?:sig|signature|searchsignature|search_signature|searchsig)\s*=\s*["']([^"']+)["']''',
                chunk,
                re.I,
            )
            if m4:
                return m4.group(1).strip()

            start = idx + len(needle)


    # 3) last resort: global scan for a sig value anywhere (rarely safe, but helps debugging)
    m_global = re.search(
        r'''["'](?:sig|signature|searchSignature|search_signature|searchSig|sxaSignature)["']\s*:\s*["']([^"']+)["']''',
        h,
        re.I,
    )
    if m_global:
        return m_global.group(1).strip()


    return None

# ----------------------------
# PDF date parsing (TN EO + Procs)
# ----------------------------

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
}

# EO PDFs often show: "NOV 2 0 2024" (day digits split)
_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}

# "January 6, 2024"
_DATE_MONTH_DAY_YEAR = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.I,
)

# "this 22nd day of May 2024" (numeric)
_DATE_THIS_DAY_OF_NUM = re.compile(
    r"\bthis\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s+(\d{4})\b",
    re.I,
)

# "this twenty-second day of May 2024" (word ordinal)
_DATE_THIS_DAY_OF_WORD = re.compile(
    r"\bthis\s+([a-z\- ]{3,30})\s+day\s+of\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s+(\d{4})\b",
    re.I,
)

# "NOV 2 0 2024" or "NOV 20 2024"
_DATE_EO_ABBR = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)\.?\s+([0-9][0-9\s]{0,4})\s+(\d{4})\b",
    re.I,
)

# Proclamation tail variants:
# "DATED this 3rd day of May, 2024"
# "on this twenty second day of May 2024"
# sometimes: "2 0 2 4"
_DATE_PROC_TAIL = re.compile(
    r"\b(?:dated\s+)?(?:on\s+)?this\s+([a-z0-9\- ]{1,35})\s+day\s+of\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s*,?\s+([0-9][0-9\s]{3,6})\b",
    re.I,
)

def _ordinal_words_to_int(s: str) -> Optional[int]:
    """
    Robust 1..31 parsing from:
      - "22nd"
      - "twenty-second"
      - "twenty second"
      - "thirty first"
    """
    if not s:
        return None

    t = s.strip().lower()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("–", "-").replace("—", "-")

    # numeric ordinal
    m = re.match(r"^(\d{1,2})(?:st|nd|rd|th)?$", t)
    if m:
        v = int(m.group(1))
        return v if 1 <= v <= 31 else None

    # normalize hyphens to spaces and split
    t = t.replace("-", " ")
    parts = [p for p in t.split(" ") if p and p != "and"]

    ones = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6,
        "seventh": 7, "eighth": 8, "ninth": 9,
        "tenth": 10, "eleventh": 11, "twelfth": 12, "thirteenth": 13, "fourteenth": 14,
        "fifteenth": 15, "sixteenth": 16, "seventeenth": 17, "eighteenth": 18, "nineteenth": 19,
    }
    tens = {"twenty": 20, "thirty": 30, "twentieth": 20, "thirtieth": 30}

    if len(parts) == 1:
        return ones.get(parts[0]) or tens.get(parts[0])

    if len(parts) == 2 and parts[0] in tens and parts[1] in ones:
        v = tens[parts[0]] + ones[parts[1]]
        return v if 1 <= v <= 31 else None

    return None

def _parse_eo_published_date_from_text(text: str) -> Optional[datetime]:
    """
    EOs: prefer month-abbr near top (NOV 2 0 2024), fallback Month DD, YYYY near top.
    """
    if not text:
        return None

    full = re.sub(r"\s+", " ", text).strip()
    head = full[:20000]

    # 1) EO style: NOV 2 0 2024 / NOV 20 2024 (HEAD)
    m = _DATE_EO_ABBR.search(head)
    if m:
        mon = _MONTH_ABBR.get(m.group(1).lower().rstrip("."), None)
        day_str = re.sub(r"\s+", "", m.group(2))  # "2 0" -> "20"
        try:
            day = int(day_str)
            year = int(m.group(3))
            if mon and 1 <= day <= 31:
                return datetime(year, mon, day, tzinfo=timezone.utc)
        except Exception:
            pass

    # 2) Month DD, YYYY near top
    m = _DATE_MONTH_DAY_YEAR.search(head)
    if m:
        month = _MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        year = int(m.group(3))
        return datetime(year, month, day, tzinfo=timezone.utc)

    return None


def _parse_proc_published_date_from_text(text: str) -> Optional[datetime]:
    """
    Proclamations: ONLY accept the tail signing line like:
      "on this seventh day of April 2025"
    Avoids WHEREAS dates like "December 10, 1948".
    """
    if not text:
        return None

    full = re.sub(r"\s+", " ", text).strip()
    tail = full[-20000:] if len(full) > 20000 else full

    # Take the LAST match in the tail (safest if multiple dates exist)
    matches = list(_DATE_PROC_TAIL.finditer(tail))
    if not matches:
        return None
    m = matches[-1]

    day_raw = (m.group(1) or "").strip()
    month = _MONTHS[m.group(2).lower()]
    year_str = re.sub(r"\s+", "", (m.group(3) or "").strip())  # "2 0 2 5" -> "2025"

    try:
        year = int(year_str)
    except Exception:
        return None

    # day can be numeric ordinal or word ordinal
    mday = re.match(r"^(\d{1,2})(?:st|nd|rd|th)?$", day_raw.lower())
    if mday:
        day = int(mday.group(1))
    else:
        day = _ordinal_words_to_int(day_raw)

    if day and 1 <= day <= 31:
        return datetime(year, month, day, tzinfo=timezone.utc)

    return None


async def _ensure_sig(client: httpx.AsyncClient, cfg: dict) -> None:
    """
    If cfg['params']['sig'] is blank, load the public page and discover it.
    """
    if cfg["params"].get("sig"):
        return

    referer = cfg["referer"]
    itemid = cfg["params"].get("itemid")

    r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=45.0)
    r.raise_for_status()

    txt = (r.text or "")
    print("MI html contains itemid?", itemid in txt, itemid.strip("{}") in txt, itemid.replace("{","%7B").replace("}","%7D") in txt)

    # ---- debug: show snippet around itemid (first occurrence) ----
    try:
        idx = txt.lower().find(itemid.strip("{}").lower())
        if idx != -1:
            lo = max(0, idx - 800)
            hi = min(len(txt), idx + 800)
            snippet = txt[lo:hi]
            print("\n--- MI ITEMID SNIPPET START ---\n", snippet, "\n--- MI ITEMID SNIPPET END ---\n")
    except Exception:
        pass


    sig = _extract_sig_for_itemid(r.text or "", itemid)
    cfg["params"]["sig"] = sig or ""

    # ✅ useful debug
    if not sig:
        print("MI sig NOT FOUND for", referer, "itemid=", itemid)


_URL_DATE_RE = re.compile(r"/(?P<y>\d{4})/(?P<m>\d{1,2})/(?P<d>\d{1,2})/")

# SXA JSON often contains an HTML blob; we extract links out of it.
# Keep it broad, then filter to michigan.gov/whitmer paths.
_HREF_RE = re.compile(r'href=["\'](?P<href>[^"\']+)["\']', re.I)

def _abs_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("/"):
        return "https://www.michigan.gov" + u
    return u

def _published_from_url(u: str) -> Optional[datetime]:
    if not u:
        return None
    m = _URL_DATE_RE.search(u)
    if not m:
        return None
    try:
        y = int(m.group("y"))
        mo = int(m.group("m"))
        d = int(m.group("d"))
        return datetime(y, mo, d, tzinfo=timezone.utc)
    except Exception:
        return None

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
}

_HREF_ANY_RE = re.compile(r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\']')

def _collect_abs_hrefs(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    out: List[str] = []
    for m in _HREF_ANY_RE.finditer(html):
        href = (m.group("href") or "").strip()
        if not href:
            continue
        # skip page anchors, mailto, js
        if href.startswith("#") or href.lower().startswith(("mailto:", "javascript:")):
            continue
        abs_u = urljoin(base_url, href)
        abs_u = _norm_url(abs_u)  # ✅ ADD THIS
        out.append(abs_u)
    # dedupe preserve order
    seen = set()
    final = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        final.append(u)
    return final

def _with_paging(base: str, params: Dict[str, str], *, p: int, e: int) -> str:
    # Add/override p & e
    all_params = dict(params)
    all_params["p"] = str(p)
    all_params["e"] = str(e)

    parts = list(urlsplit(base))
    # merge with any existing query in base
    q = dict(parse_qsl(parts[3], keep_blank_values=True))
    q.update(all_params)
    parts[3] = urlencode(q, doseq=True)
    return urlunsplit(parts)

async def _get_jsonish(client: httpx.AsyncClient, url: str, *, referer: str) -> Tuple[int, str, dict | None]:
    headers = {
        **BROWSER_UA_HEADERS,
        "accept": "application/json, text/javascript, */*; q=0.01",
        "x-requested-with": "XMLHttpRequest",
        "referer": referer,
        "cache-control": "no-cache",
        "pragma": "no-cache",

        # ✅ Sitecore context (often required)
        "cookie": "shell#lang=en; sxa_site=Whitmer",
    }
    r = await client.get(url, headers=headers, timeout=httpx.Timeout(45.0, read=45.0))
    text = r.text or ""
    data = None
    try:
        data = r.json()
    except Exception:
        data = None
    return r.status_code, text, data

def _extract_html_blob(json_data: dict) -> str:
    """
    SXA search JSON varies by component. Common patterns:
      - {"Results":"<li>...</li>"} or {"results":"..."}
      - {"Html":"..."} or {"html":"..."}
      - {"SearchResults":"..."} etc
    We pull the *largest* string value as the HTML blob fallback.
    """
    if not isinstance(json_data, dict):
        return ""

    # common keys first
    for k in ["Results", "results", "Html", "html", "SearchResults", "searchResults", "RenderedContent", "renderedContent"]:
        v = json_data.get(k)
        if isinstance(v, str) and v.strip():
            return v

    # fallback: largest string value
    best = ""
    for v in json_data.values():
        if isinstance(v, str) and len(v) > len(best):
            best = v
    return best

from typing import Any

def _urls_from_sxa_payload(payload_text: str, payload_json: Any) -> List[str]:
    html_blob = ""
    # Case A (Michigan): "Results" is a list of objects, not an HTML blob
    if isinstance(payload_json, dict):
        results = payload_json.get("Results")
        if isinstance(results, list) and results:
            extracted: List[str] = []

            for row in results:
                if not isinstance(row, dict):
                    continue

                # common field names where the URL lives
                for key in ("Url", "url", "Link", "link", "ItemUrl", "itemUrl"):
                    u = row.get(key)
                    if isinstance(u, str) and u.strip():
                        extracted.append(_abs_url(u))
                        break
                else:
                    # fallback: scan string values for a newsroom URL
                    for v in row.values():
                        if isinstance(v, str) and "/whitmer/news/" in v:
                            m = re.search(
                                r"https?://www\.michigan\.gov/whitmer/news/[^\"'\s<>]+",
                                v,
                            )
                            if m:
                                extracted.append(m.group(0))
                                break

            # filter + dedupe while preserving order
            seen = set()
            final: List[str] = []
            for u in extracted:
                if not u:
                    continue
                if "www.michigan.gov/whitmer/news/" not in u:
                    continue
                if u in seen:
                    continue
                seen.add(u)
                final.append(u)

            if final:
                return final


    if isinstance(payload_json, dict):
        html_blob = _extract_html_blob(payload_json)
    elif isinstance(payload_json, str) and payload_json.strip():
        # sometimes the JSON itself is an HTML string
        html_blob = payload_json

    # if no html blob, try raw text (sometimes still JSON string but ok)
    hay = html_blob or payload_text or ""

    found: List[str] = []
    for m in _HREF_RE.finditer(hay):
        u = _abs_url(m.group("href"))
        if not u:
            continue
        # only keep Whitmer newsroom items
        if "www.michigan.gov/whitmer/news/" not in u:
            continue
        found.append(u)

    # de-dupe while preserving order
    seen = set()
    out = []
    for u in found:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

# ----------------------------
# Tennessee helpers
# ----------------------------

def _tn_press_list_url(page_idx: int) -> str:
    """
    TN press releases:
      page 0 -> https://www.tn.gov/news.press-releases.html
      page 1 -> https://www.tn.gov/news.press-releases.2.html
      page 2 -> https://www.tn.gov/news.press-releases.3.html
    """
    if page_idx == 0:
        return "https://www.tn.gov/news.press-releases.html"
    return f"https://www.tn.gov/news.press-releases.{page_idx+1}.html"


def _tn_proclamations_list_url(page_idx: int) -> str:
    """
    Proclamations:
      page 0 -> https://tnsos.net/publications/proclamations/
      page 1 -> ...?page=2
      page 2 -> ...?page=3
    """
    if page_idx == 0:
        return TN_PUBLIC_PAGES["proclamations"]
    return f"{TN_PUBLIC_PAGES['proclamations']}?Search=&SearchGovernor=&sort=&page={page_idx+1}"

_TN_PRESS_DETAIL_RE = re.compile(
    r"https?://www\.tn\.gov/(?!news\.press-releases)(?:[^\"'\s<>]+/)*\d{4}/\d{1,2}/\d{1,2}/[^\"'\s<>]+\.html",
    re.I,
)

_TN_PROC_PDF_RE = re.compile(
    r"https?://(?:tnsos\.net/publications/proclamations/files|publications\.tnsosfiles\.com/pub/proclamations)/\d+\.pdf",
    re.I
)

_TN_EO_PDF_RE = re.compile(r"https?://publications\.tnsosfiles\.com/pub/execorders/exec-orders-lee\d+\.pdf", re.I)

def _extract_urls_matching(html: str, rx: re.Pattern) -> List[str]:
    if not html:
        return []
    found = []
    for m in rx.finditer(html):
        # if regex has named group u, use it
        u = m.groupdict().get("u") if hasattr(m, "groupdict") else None
        found.append((u or m.group(0)))
    # de-dupe preserve order
    seen = set()
    out = []
    for u in found:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

def _extract_anchor_map(html: str) -> Dict[str, str]:
    """
    Returns {absolute_url: anchor_text} for <a href="...">TEXT</a>.
    Used for proclamations/EOS so we can store better titles even for PDFs.
    """
    if not html:
        return {}
    # crude but works well enough for these pages
    anchors = re.finditer(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>\s*(?P<txt>.*?)\s*</a>',
        html,
    )
    out: Dict[str, str] = {}
    for m in anchors:
        href = (m.group("href") or "").strip()
        txt = (m.group("txt") or "").strip()
        if not href:
            continue
        # strip nested tags from txt
        txt_clean = re.sub(r"(?is)<.*?>", " ", txt)
        txt_clean = re.sub(r"\s+", " ", txt_clean).strip()
        # absolutize common TN patterns
        if href.startswith("/"):
            # decide domain by context later; keep as-is unless it looks like proclamations pdf
            if href.startswith("/publications/proclamations/files/"):
                href = "https://tnsos.net" + href
            else:
                href = "https://www.tn.gov" + href
        out[href] = txt_clean
    return out

def _title_from_url_fallback(url: str) -> str:
    if not url:
        return ""
    u = url.rstrip("/")  # ✅ critical
    tail = u.rsplit("/", 1)[-1]
    return (tail or u)[:200]


async def _fetch_detail_for_summary(client: httpx.AsyncClient, url: str, *, referer: str) -> Tuple[str, str]:
    """
    Returns (title, summary_text_source).
    We keep it simple: strip HTML to text and summarize.
    """
    headers = {**BROWSER_UA_HEADERS, "referer": referer}
    r = await client.get(url, headers=headers, timeout=httpx.Timeout(45.0, read=45.0))
    r.raise_for_status()

    html = r.text or ""
    # title: use <title> if possible
    title = ""
    m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()

    text = _strip_html_to_text(html)
    # Keep summary source reasonably sized
    text = (text or "").strip()
    if len(text) > 35000:
        text = text[:35000]
    return title, text

async def _safe_ai_polish(summary: str, title: str, url: str) -> str:
    summary = (summary or "").strip()
    if not summary:
        return ""
    try:
        return (await ai_polish_summary(summary, title=title, url=url)) or summary
    except Exception:
        return summary
    
async def _filter_new_external_ids(conn, source_id: UUID, urls: list[str]) -> list[str]:
    """
    Return only URLs that are not already present for this source_id.
    Keeps input order.

    Checks in:
      - public.item_external_ids.external_id (if exists)
      - public.items.external_id
      - public.items.url
    """
    urls = [u for u in urls if u]
    if not urls:
        return []

    existing: set[str] = set()

    # 1) item_external_ids (if present)
    try:
        rows = await conn.fetch(
            "select external_id from public.item_external_ids where source_id=$1 and external_id = any($2::text[])",
            source_id,
            urls,
        )
        existing |= {r["external_id"] for r in rows if r.get("external_id")}
    except Exception:
        pass

    # 2) items.external_id
    rows2 = await conn.fetch(
        "select external_id from public.items where source_id=$1 and external_id = any($2::text[])",
        source_id,
        urls,
    )
    existing |= {r["external_id"] for r in rows2 if r.get("external_id")}

    # 3) items.url
    rows3 = await conn.fetch(
        "select url from public.items where source_id=$1 and url = any($2::text[])",
        source_id,
        urls,
    )
    existing |= {r["url"] for r in rows3 if r.get("url")}

    return [u for u in urls if u not in existing]

@dataclass
class MISectionResult:
    fetched_urls: int = 0          # how many URLs we saw from SXA listing
    new_urls: int = 0              # how many of those were NEW (cron mode)
    upserted: int = 0              # how many inserted/updated (should ~= new_urls in cron)
    stopped_at_cutoff: bool = False


async def _ingest_mi_section(
    *,
    section_key: str,
    source_id: int,
    backfill: bool,
    limit_each: int,
    max_pages_each: int,
    page_size: int = 10,
) -> MISectionResult:
    cfg = MI_SXA[section_key]
    referer = cfg["referer"]
    cutoff_url = cfg["cutoff_url"]
    source_name = cfg["source_name"]
    status = cfg["status"]

    # Cron-safe caps (only apply when not backfill)
    if not backfill:
        # enough depth to catch "missed for weeks" without letting users DOS you
        max_pages_each = max(1, min(int(max_pages_each or 1), 20))      # <= 20 pages
        limit_each = max(50, min(int(limit_each or 300), 1000))         # <= 1000 inserts max

    out = MISectionResult()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # MI endpoint works with sig="" (response Signature is blank anyway)
        seen_urls: set[str] = set()
        stop = False


        for page_idx in range(0, max_pages_each):
            if stop or out.upserted >= limit_each:
                break

            # ---- paging: MI uses weird semantics; probe on first page ----
            # Michigan SXA: HTML shows "p":10 in data-properties.
            # Treat p as page size, and e as either page index OR offset. Try both.
            cand1 = _with_paging(cfg["base"], cfg["params"], p=page_size, e=page_idx)              # e = page#
            cand2 = _with_paging(cfg["base"], cfg["params"], p=page_size, e=page_idx * page_size) # e = offset

            code1, text1, data1 = await _get_jsonish(client, cand1, referer=referer)
            urls1 = _urls_from_sxa_payload(text1, data1)

            code2, text2, data2 = await _get_jsonish(client, cand2, referer=referer)
            urls2 = _urls_from_sxa_payload(text2, data2)

            if urls1:
                urls = urls1
                list_url = cand1
            elif urls2:
                urls = urls2
                list_url = cand2
            else:
                if page_idx == 0:
                    print("MI no urls via both paging styles")
                    print("MI cand1=", cand1)
                    print("MI resp1=", (text1 or "")[:300])
                    print("MI cand2=", cand2)
                    print("MI resp2=", (text2 or "")[:300])
                break

            # ✅ enforce newest -> oldest (some SXA paging can flip)
            def _k(u: str):
                dt = _published_from_url(u)
                return dt or datetime.min.replace(tzinfo=timezone.utc)

            urls = sorted(urls, key=_k, reverse=True)

            out.fetched_urls += len(urls)

            async with connection() as conn:
                # If cron_safe: only act on NEW urls for this source
                urls_to_process = urls
                if not backfill:
                    urls_to_process = await _filter_new_external_ids(conn, source_id, urls)

                    out.new_urls += len(urls_to_process)   # ✅ ADD THIS

                    # If nothing new on this page, stop early (fast cron)
                    if not urls_to_process:
                        break

                for detail_url in urls_to_process:
                    if detail_url in seen_urls:
                        continue
                    seen_urls.add(detail_url)

                    pub_dt = _published_from_url(detail_url)

                    # pull detail page for title + text to summarize
                    try:
                        title, body_text = await _fetch_detail_for_summary(client, detail_url, referer=referer)
                    except Exception:
                        title, body_text = (detail_url, "")

                    # summarize + polish (ONLY for new items in cron mode)
                    summary = ""
                    if body_text:
                        summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, detail_url)

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
                        detail_url,       # external_id
                        source_id,
                        (title or detail_url)[:500],
                        (summary or "")[:4000],
                        detail_url,
                        MI_JURISDICTION,
                        MI_AGENCY,
                        status,
                        pub_dt,
                    )
                    out.upserted += 1

                    # cutoff handling (include it, then stop)
                    if detail_url == cutoff_url:
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    if out.upserted >= limit_each:
                        break

            # be nice to origin
            await asyncio.sleep(0.15)

    return out


# ----------------------------
# Public ingester
# ----------------------------

async def ingest_michigan(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "michigan", "counts": {}}

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            MI_SXA["press_releases"]["source_name"],
            f"{MI_JURISDICTION}:press_releases",
            MI_SXA["press_releases"]["referer"],
        )
        src_proc = await get_or_create_source(
            conn,
            MI_SXA["proclamations"]["source_name"],
            f"{MI_JURISDICTION}:proclamations",
            MI_SXA["proclamations"]["referer"],
        )
        src_orders = await get_or_create_source(
            conn,
            MI_SXA["state_orders_and_directives"]["source_name"],
            f"{MI_JURISDICTION}:state_orders_and_directives",
            MI_SXA["state_orders_and_directives"]["referer"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        proc_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0
        orders_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_orders) or 0

    press_backfill = (press_existing == 0)
    proc_backfill = (proc_existing == 0)
    orders_backfill = (orders_existing == 0)

    press = await _ingest_mi_section(
        section_key="press_releases",
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
        page_size=10,
    )
    proc = await _ingest_mi_section(
        section_key="proclamations",
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
        page_size=10,
    )
    orders = await _ingest_mi_section(
        section_key="state_orders_and_directives",
        source_id=src_orders,
        backfill=orders_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
        page_size=10,
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": "backfill" if press_backfill else "cron_safe",
            "seen_total": press_existing,
        },
        "proclamations": {
            "fetched_urls": proc.fetched_urls,
            "upserted": proc.upserted,
            "stopped_at_cutoff": proc.stopped_at_cutoff,
            "mode": "backfill" if proc_backfill else "cron_safe",
            "seen_total": proc_existing,
        },
        "state_orders_and_directives": {
            "fetched_urls": orders.fetched_urls,
            "upserted": orders.upserted,
            "stopped_at_cutoff": orders.stopped_at_cutoff,
            "mode": "backfill" if orders_backfill else "cron_safe",
            "seen_total": orders_existing,
        },
    }

    print(
        f"MI PR mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.new_urls if not press_backfill else press.upserted} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"MI PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
        f"new={proc.new_urls if not proc_backfill else proc.upserted} "
        f"fetched={proc.fetched_urls} seen_total={proc_existing}"
    )
    print(
        f"MI ORDERS mode={'backfill' if orders_backfill else 'cron_safe'} "
        f"new={orders.new_urls if not orders_backfill else orders.upserted} "
        f"fetched={orders.fetched_urls} seen_total={orders_existing}"
    )

    return out

# ----------------------------
# Tennessee ingesters
# ----------------------------

@dataclass
class TNSectionResult:
    fetched_urls: int = 0      # URLs seen in listing(s)
    new_urls: int = 0          # NEW URLs (cron mode)
    upserted: int = 0          # inserted/updated (cron ~= new_urls)
    stopped_at_cutoff: bool = False

async def _upsert_item(
    *,
    url: str,
    title: str,
    summary: str,
    jurisdiction: str,
    agency: str,
    status: str,
    source_name: str,
    source_key: str,
    referer: str,
    published_at: Optional[datetime],
) -> None:
    async with connection() as conn:
        source_id = await get_or_create_source(conn, source_name, source_key, referer)

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
            (title or url)[:500],
            (summary or "")[:4000],
            url,
            jurisdiction,
            agency,
            status,
            published_at,
        )

async def _pw_fetch_detail_for_summary(page, url: str, *, referer: str) -> tuple[str, str]:
    # set referer via goto options
    resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
    html = await page.content()

    title = ""
    m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()

    text = _strip_html_to_text(html)
    text = (text or "").strip()
    if len(text) > 35000:
        text = text[:35000]
    return (title or _title_from_url_fallback(url)), text

async def _pw_fetch_detail_html_title_text(page, url: str, *, referer: str) -> tuple[str, str, str]:
    """
    Playwright-rendered fetch for sites that block httpx (NV does).
    Returns (html, title, text). Never raises; returns empty strings on failure.
    """
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
        if resp and resp.status >= 400:
            return ("", "", "")

        # give WAF / scripts a moment
        await page.wait_for_timeout(500)

        html = await page.content()

        title = ""
        try:
            t = await page.title()
            if t:
                title = re.sub(r"\s+", " ", t).strip()
        except Exception:
            pass

        # fallback to <title> tag if page.title() empty
        if not title:
            m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html or "")
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip()

        text = _strip_html_to_text(html) or ""
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > 35000:
            text = text[:35000]

        return (html or "", title or "", text or "")
    except Exception:
        return ("", "", "")


async def _pw_get_text_page(page, url: str) -> tuple[int, str]:
    resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    html = await page.content()
    status = resp.status if resp else 0
    return status, html

async def _fetch_pdf_text(client: httpx.AsyncClient, url: str, *, referer: str) -> str:
    r = await client.get(url, headers={**BROWSER_UA_HEADERS, "referer": referer}, timeout=httpx.Timeout(60.0, read=60.0))
    r.raise_for_status()

    data = r.content
    if not data:
        return ""

    try:
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages[:25]:  # cap pages so you don't explode runtime
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
        text = "\n".join(parts)
        return re.sub(r"\s+", " ", text).strip()
    except Exception:
        return ""
    
_PDF_META_DATE_RE = re.compile(r"D:(\d{4})(\d{2})(\d{2})")

def _pdf_meta_date(reader: PdfReader) -> Optional[datetime]:
    try:
        md = reader.metadata or {}
        for k in ("/ModDate", "/CreationDate"):
            v = md.get(k)
            if isinstance(v, str):
                m = _PDF_META_DATE_RE.search(v)
                if m:
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    return datetime(y, mo, d, tzinfo=timezone.utc)
    except Exception:
        pass
    return None

def _bytes_look_like_html(b: bytes) -> bool:
    if not b:
        return False
    head = b.lstrip()[:80].lower()
    return (
        head.startswith(b"<")
        or b"<!doctype html" in head
        or b"<html" in head
        or b"<head" in head
        or b"<body" in head
    )

async def _fetch_pdf_text_and_meta(
    client: httpx.AsyncClient, url: str, *, referer: str
) -> tuple[str, Optional[datetime]]:
    headers = {**BROWSER_UA_HEADERS, "referer": referer, "Accept": "application/pdf,*/*"}

    data = b""
    content_type = ""  # only available for httpx path

    try:
        r = await client.get(url, headers=headers, timeout=httpx.Timeout(60.0, read=60.0))
        r.raise_for_status()
        data = r.content or b""
        content_type = (r.headers.get("content-type") or "").lower()

        # ✅ SANITY CHECK (httpx path): HTML / not-a-PDF response
        if (("pdf" not in content_type) and _bytes_look_like_html(data)) or (
            data and not data.lstrip().startswith(b"%PDF-")
        ):
            return ("", None)

    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response else None

        # Only intercept the specific "blocked" statuses; otherwise keep old behavior
        if code in (400, 403, 406):
            print("[PDF] httpx blocked, retrying via Playwright:", url, "status=", code)
            data = await _pw_fetch_bytes(url, referer=referer)

            if not data:
                # behave gracefully: return empty instead of crashing ingestion
                return ("", None)

            # ✅ SANITY CHECK (playwright path): no headers, so sniff bytes only
            if _bytes_look_like_html(data) or (data and not data.lstrip().startswith(b"%PDF-")):
                return ("", None)
        else:
            raise

    if not data:
        return ("", None)

    try:
        reader = PdfReader(io.BytesIO(data))
        meta_dt = _pdf_meta_date(reader)

        parts = []
        for page in reader.pages[:25]:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
        text = re.sub(r"\s+", " ", "\n".join(parts)).strip()
        return (text, meta_dt)
    except Exception:
        return ("", None)
    
async def _ingest_tn_press_releases(
    *,
    source_id: int,
    backfill: bool,
    limit_each: int,
    max_pages_each: int,
) -> TNSectionResult:
    out = TNSectionResult()
    cutoff_url = TN_PRESS_CUTOFF_URL
    referer = TN_PUBLIC_PAGES["press_releases"]

    cutoff_dt = TN_PRESS_CUTOFF_DT  # e.g. datetime(2025,1,6,tzinfo=timezone.utc)

    source_name = "Tennessee — Press Releases (F&A)"
    source_key = f"{TN_JURISDICTION}:press_releases"
    status = TN_STATUS_MAP["press_releases"]

    # Cron-safe caps (only when not backfill)
    if not backfill:
        # enough to catch missed weeks/months; still bounded
        max_pages_each = max(1, min(int(max_pages_each or 1), 120))   # <= 120 listing pages
        limit_each = max(50, min(int(limit_each or 800), 3000))       # <= 3000 new items

    seen: set[str] = set()
    stop = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=BROWSER_UA_HEADERS.get("user-agent"),
        )
        page = await context.new_page()

        try:
            for page_idx in range(max_pages_each):
                if stop or out.upserted >= limit_each:
                    break

                list_url = _tn_press_list_url(page_idx)

                # Fetch listing page via Playwright
                resp = await page.goto(list_url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                status_code = resp.status if resp else 0

                # TN pages beyond the end often 404
                if status_code == 404:
                    break
                if status_code and status_code >= 400:
                    raise RuntimeError(f"TN list page failed {status_code}: {list_url}")

                html = await page.content()

                # Extract detail URLs from listing
                # Extract detail URLs from listing (ALL TN dept newsroom detail pages)
                hrefs = _collect_abs_hrefs(html, list_url)

                # Most reliable: match TN detail pages with /YYYY/M/D/...html anywhere on tn.gov
                urls = [
                    u for u in hrefs
                    if u.startswith("https://www.tn.gov/")
                    and _URL_DATE_RE.search(u)            # /2026/1/26/... etc
                    and u.lower().endswith(".html")
                    and "news.press-releases" not in u    # avoid list pages
                ]

                # Fallback: regex scrape from HTML if hrefs miss anything (rare but helps)
                if not urls:
                    urls = _extract_urls_matching(html, _TN_PRESS_DETAIL_RE)

                # normalize + dedupe preserve order
                urls = [_norm_url(u) for u in urls if u]
                seen_local = set()
                urls = [u for u in urls if not (u in seen_local or seen_local.add(u))]

                if not urls:
                    break

                # ✅ enforce newest -> oldest
                def _k(u: str):
                    dt = _published_from_url(u)
                    return dt or datetime.min.replace(tzinfo=timezone.utc)

                urls = sorted(urls, key=_k, reverse=True)

                # ✅ hard date cutoff: never go older than cutoff_dt
                urls = [
                    u for u in urls
                    if (_published_from_url(u) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff_dt
                ]

                # if nothing on this page survives the cutoff, older pages will be even older → stop paging
                if not urls:
                    break


                out.fetched_urls += len(urls)

                # --- cron-safe: only process NEW urls ---
                async with connection() as conn:
                    urls_to_process = urls
                    if not backfill:
                        urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                        out.new_urls += len(urls_to_process)

                        # If page has nothing new, stop early (keeps cron fast)
                        if not urls_to_process:
                            break

                for detail_url in urls_to_process:
                    if stop or out.upserted >= limit_each:
                        break
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    pub_dt = _published_from_url(detail_url)

                    try:
                        title, body_text = await _pw_fetch_detail_for_summary(page, detail_url, referer=referer)
                    except Exception:
                        title, body_text = (_title_from_url_fallback(detail_url), "")

                    summary = ""
                    if body_text:
                        summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, detail_url)

                    # upsert unchanged (but only called for new urls in cron mode)
                    await _upsert_item(
                        url=detail_url,
                        title=title,
                        summary=summary,
                        jurisdiction=TN_JURISDICTION,
                        agency=TN_AGENCY_PRESS,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=pub_dt,
                    )
                    out.upserted += 1

                    if detail_url == cutoff_url:
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    await asyncio.sleep(0.05)

                await asyncio.sleep(0.15)

        finally:
            await context.close()
            await browser.close()

    return out

async def _ingest_tn_executive_orders(
    *,
    source_id: int,
    backfill: bool,
    limit_each: int,
) -> TNSectionResult:
    out = TNSectionResult()
    referer = TN_PUBLIC_PAGES["executive_orders"]
    cutoff_url = TN_EO_CUTOFF_URL

    source_name = "Tennessee — Executive Orders (Governor Bill Lee)"
    source_key = f"{TN_JURISDICTION}:executive_orders"
    status = TN_STATUS_MAP["executive_orders"]

    # ✅ ADD CRON CAPS RIGHT HERE
    if not backfill:
        # EO page can accumulate; give more room, still safe
        limit_each = max(30, min(int(limit_each or 400), 1500))

    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
        r.raise_for_status()

        html = r.text or ""
        anchor_map = _extract_anchor_map(html)

        pdf_urls = _extract_urls_matching(html, _TN_EO_PDF_RE)
        if not pdf_urls:
            return out

        out.fetched_urls = len(pdf_urls)

        # cron-safe: only process NEW pdf urls
        async with connection() as conn:
            pdfs_to_process = pdf_urls
            if not backfill:
                pdfs_to_process = await _filter_new_external_ids(conn, source_id, pdf_urls)
                out.new_urls += len(pdfs_to_process)

                # if none new, fast exit
                if not pdfs_to_process:
                    return out

        for pdf_url in pdfs_to_process:
            # build a decent title from surrounding anchor text if we have it
            title = anchor_map.get(pdf_url) or _title_from_url_fallback(pdf_url)
            if title.isdigit():
                title = f"Executive Order {title}"
            elif "exec-orders-lee" in pdf_url and "Executive Order" not in title:
                m = re.search(r"exec-orders-lee(\d+)\.pdf", pdf_url)
                if m:
                    title = f"Executive Order {m.group(1)}"

            # PDFs: we store URL + title; summary blank (unless you later add PDF text extraction)
            # ✅ PDFs: extract text -> summarize
            pdf_text = await _fetch_pdf_text(client, pdf_url, referer=referer)
            pub_dt = _parse_eo_published_date_from_text(pdf_text) if pdf_text else None

            summary = ""
            if pdf_text:
                summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, pdf_url)

            await _upsert_item(
                url=pdf_url,
                title=title,
                summary=summary,
                jurisdiction=TN_JURISDICTION,
                agency=TN_AGENCY_SOS,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer,
                published_at=pub_dt, 

            )

            out.upserted += 1

            if pdf_url == cutoff_url:
                out.stopped_at_cutoff = True
                break

            if out.upserted >= limit_each:
                break

            await asyncio.sleep(0.05)

    return out

async def _ingest_tn_proclamations(
    *,
    source_id: int,
    backfill: bool,
    limit_each: int,
    max_pages_each: int,
) -> TNSectionResult:
    out = TNSectionResult()
    cutoff_url = TN_PROC_CUTOFF_URL

    source_name = "Tennessee — Proclamations"
    source_key = f"{TN_JURISDICTION}:proclamations"
    status = TN_STATUS_MAP["proclamations"]
    seen: set[str] = set()

    # ✅ ADD CRON CAPS RIGHT HERE
    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 25))
        limit_each = max(30, min(int(limit_each or 400), 1500))

    async with httpx.AsyncClient(follow_redirects=True) as client:
        stop = False
        for page_idx in range(max_pages_each):
            if stop or out.upserted >= limit_each:
                break

            list_url = _tn_proclamations_list_url(page_idx)
            r = await client.get(list_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()

            html = r.text or ""
            anchor_map = _extract_anchor_map(html)
            hrefs = _collect_abs_hrefs(html, list_url)
            pdf_urls = [u for u in hrefs if _TN_PROC_PDF_RE.search(u)]
            if not pdf_urls:
                break

            out.fetched_urls += len(pdf_urls)

            # cron-safe: only process NEW urls
            async with connection() as conn:
                pdfs_to_process = pdf_urls
                if not backfill:
                    pdfs_to_process = await _filter_new_external_ids(conn, source_id, pdf_urls)
                    out.new_urls += len(pdfs_to_process)

                    # if none new on this page, stop early
                    if not pdfs_to_process:
                        break

            for pdf_url in pdfs_to_process:
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)

                title = anchor_map.get(pdf_url) or _title_from_url_fallback(pdf_url)

                pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, pdf_url, referer=TN_PUBLIC_PAGES["proclamations"])
                pub_dt = _parse_proc_published_date_from_text(pdf_text) or meta_dt

                summary = ""
                if pdf_text:
                    summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, pdf_url)

                await _upsert_item(
                    url=pdf_url,
                    title=title,
                    summary=summary,
                    jurisdiction=TN_JURISDICTION,
                    agency=TN_AGENCY_SOS,
                    status=status,
                    source_name=source_name,
                    source_key=source_key,
                    referer=TN_PUBLIC_PAGES["proclamations"],
                    published_at=pub_dt,
                )

                out.upserted += 1

                if pdf_url == cutoff_url:
                    out.stopped_at_cutoff = True
                    stop = True
                    break

                if out.upserted >= limit_each:
                    break

            await asyncio.sleep(0.1)

    return out

async def ingest_tennessee(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "tennessee", "counts": {}}

    async with connection() as conn:
        # Create/get sources once
        src_press = await get_or_create_source(
            conn,
            "Tennessee — Press Releases (F&A)",
            f"{TN_JURISDICTION}:press_releases",
            TN_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "Tennessee — Executive Orders (Governor Bill Lee)",
            f"{TN_JURISDICTION}:executive_orders",
            TN_PUBLIC_PAGES["executive_orders"],
        )
        src_proc = await get_or_create_source(
            conn,
            "Tennessee — Proclamations",
            f"{TN_JURISDICTION}:proclamations",
            TN_PUBLIC_PAGES["proclamations"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0
        proc_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0

    press_backfill = (press_existing == 0)
    eo_backfill = (eo_existing == 0)
    proc_backfill = (proc_existing == 0)

    press = await _ingest_tn_press_releases(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_tn_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )
    procs = await _ingest_tn_proclamations(
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )

    # Terminal prints (like MN/MI)
    print(
        f"TN PR mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.upserted if press_backfill else press.new_urls} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"TN EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing}"
    )
    print(
        f"TN PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
        f"new={procs.upserted if proc_backfill else procs.new_urls} "
        f"fetched={procs.fetched_urls} seen_total={proc_existing}"
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "new_urls": press.new_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": "backfill" if press_backfill else "cron_safe",
            "seen_total": press_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": "backfill" if eo_backfill else "cron_safe",
            "seen_total": eo_existing,
        },
        "proclamations": {
            "fetched_urls": procs.fetched_urls,
            "new_urls": procs.new_urls,
            "upserted": procs.upserted,
            "stopped_at_cutoff": procs.stopped_at_cutoff,
            "mode": "backfill" if proc_backfill else "cron_safe",
            "seen_total": proc_existing,
        },
    }
    return out

# ----------------------------
# North Carolina config
# ----------------------------

NC_JURISDICTION = "north_carolina"
NC_AGENCY = "North Carolina Governor's Office"
NC_AGENCY_PRESS = "North Carolina State Agencies (nc.gov)"
NC_AGENCY_EO_PROC = "North Carolina Governor's Office"

NC_PUBLIC_PAGES = {
    "press_releases": "https://www.nc.gov/press-releases",
    "executive_orders": "https://governor.nc.gov/news/executive-orders",
    "proclamations": "https://governor.nc.gov/news/procs",
}

NC_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

# Cutoffs (include this item, then stop)
NC_PRESS_CUTOFF_URL = "https://www.dpi.nc.gov/news/press-releases/2025/09/15/skills-future-initiative-kicks-advancing-portrait-graduate-vision"
NC_EO_CUTOFF_URL = "https://governor.nc.gov/executive-order-no-300"
NC_PROC_CUTOFF_URL = "https://governor.nc.gov/governor-proclaims-national-radon-action-month-2024"


# ----------------------------
# North Carolina helpers
# ----------------------------

# press releases detail URLs come from many agency domains, but almost all are:
#   https://<agency>.nc.gov/news/press-releases/YYYY/MM/DD/slug
_NC_PRESS_DETAIL_RE = re.compile(
    r"https?://[^\"'\s<>]+/news/press-releases/\d{4}/\d{1,2}/\d{1,2}/[^\"'\s<>]+",
    re.I,
)

def _nc_press_list_url(page_idx: int) -> str:
    # nc.gov supports ?page=0,1,2...; page=0 is fine
    return f"{NC_PUBLIC_PAGES['press_releases']}?page={page_idx}"

def _nc_eo_list_url(page_idx: int) -> str:
    return f"{NC_PUBLIC_PAGES['executive_orders']}?page={page_idx}"

def _nc_proc_list_url(page_idx: int) -> str:
    return f"{NC_PUBLIC_PAGES['proclamations']}?page={page_idx}"

def _parse_us_date(s: str) -> Optional[datetime]:
    """
    Parse 'December 22, 2025' -> UTC datetime.
    """
    if not s:
        return None
    t = re.sub(r"\s+", " ", s).strip()
    try:
        dt = datetime.strptime(t, "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    
_NC_PROC_FIRST_PUBLISHED_RE = re.compile(
    r"\bFirst\s+Published\s+([A-Z][a-z]+ \d{1,2}, \d{4})\b"
)

_NC_PROC_PDF_LINE_DATE_RE = re.compile(
    r"\bPDF\s*•.*?[-–]\s*([A-Z][a-z]+ \d{1,2}, \d{4})\b"
)

def _nc_proc_published_from_text(detail_text: str) -> Optional[datetime]:
    """
    Extract NC proclamation published date from detail page text.
    Supports:
      - "First Published November 24, 2025"
      - "PDF • 1.85 MB - November 24, 2025"
    """
    if not detail_text:
        return None

    t = re.sub(r"\s+", " ", detail_text).strip()

    m = _NC_PROC_FIRST_PUBLISHED_RE.search(t)
    if m:
        return _parse_us_date(m.group(1))

    # sometimes only the PDF line is present / easier to catch
    m2 = _NC_PROC_PDF_LINE_DATE_RE.search(t)
    if m2:
        return _parse_us_date(m2.group(1))

    return None

def _strip_tags_keep_text(s: str) -> str:
    s = re.sub(r"(?is)<.*?>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _extract_nc_table_rows_with_date(
    html: str,
    base_url: str,
) -> List[Tuple[str, str, Optional[datetime]]]:
    """
    For governor.nc.gov listings, rows are typically:
      <tr> ... <a href="...">Title</a> ... <td>DATE</td> ...
    We return [(abs_url, title, parsed_date)].
    This is intentionally flexible (works for both EO + procs).
    """
    if not html:
        return []

    rows = re.findall(r"(?is)<tr\b.*?>.*?</tr>", html)
    out: List[Tuple[str, str, Optional[datetime]]] = []

    for row in rows:
        am = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row)
        if not am:
            continue

        href = (am.group(1) or "").strip()
        title_html = (am.group(2) or "").strip()
        if not href:
            continue

        abs_url = urljoin(base_url, href)
        title = _strip_tags_keep_text(title_html)

        # Find a date-like td in the row
        # (EO list uses "Last Updated", procs list has date column)
        dts = re.findall(r"(?is)<td[^>]*>\s*([A-Z][a-z]+ \d{1,2}, \d{4})\s*</td>", row)
        pub_dt = _parse_us_date(dts[-1]) if dts else None

        out.append((abs_url, title, pub_dt))

    # de-dupe by URL preserve order
    seen = set()
    final: List[Tuple[str, str, Optional[datetime]]] = []
    for u, t, d in out:
        if u in seen:
            continue
        seen.add(u)
        final.append((u, t, d))
    return final

async def _nc_resolve_eo_content_url(client: httpx.AsyncClient, detail_url: str) -> Tuple[str, bool]:
    """
    For many 2024 EOs, the detail page has a PDF viewer at /open.
    We return (content_url, is_pdf).
    - If /open is reachable and looks like a PDF -> is_pdf True
    - Else we keep the detail_url as HTML -> is_pdf False
    """
    u = (detail_url or "").strip().rstrip("/")
    if not u:
        return ("", False)

    # try /open first for executive-order-no-XXX pages
    candidate = u + "/open"
    try:
        r = await client.get(candidate, headers={**BROWSER_UA_HEADERS, "referer": NC_PUBLIC_PAGES["executive_orders"]}, timeout=httpx.Timeout(45.0, read=45.0))
        # If it exists and is a PDF, use it.
        ct = (r.headers.get("content-type") or "").lower()
        if r.status_code < 400 and ("application/pdf" in ct or candidate.lower().endswith("/open")):
            # Some servers return pdf even if content-type is generic; we still treat /open as pdf-ish
            # We'll attempt PdfReader parsing; if that fails, summary will fall back later.
            return (candidate, True)
    except Exception:
        pass

    # fallback: use the HTML detail page itself
    return (u, False)


# ----------------------------
# North Carolina ingesters
# ----------------------------

@dataclass
class NCSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"


async def _ingest_nc_press_releases(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> NCSectionResult:
    out = NCSectionResult()
    cutoff_url = NC_PRESS_CUTOFF_URL
    referer = NC_PUBLIC_PAGES["press_releases"]

    source_name = "North Carolina — Press Releases (nc.gov)"
    source_key = f"{NC_JURISDICTION}:press_releases"
    status = NC_STATUS_MAP["press_releases"]

    out = NCSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        # bounded cron defaults; still respects your passed values, but prevents “10,000 pages”
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    seen: set[str] = set()
    stop = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for page_idx in range(max_pages_each):
            if stop or out.upserted >= limit_each:
                break

            list_url = _nc_press_list_url(page_idx)
            r = await client.get(list_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""

            urls = _extract_urls_matching(html, _NC_PRESS_DETAIL_RE)
            if not urls:
                break

            # ✅ enforce newest -> oldest
            def _k(u: str):
                dt = _published_from_url(u)
                return dt or datetime.min.replace(tzinfo=timezone.utc)

            urls = sorted(urls, key=_k, reverse=True)

            # ✅ listing-level cutoff: stop paging past cutoff even if cutoff is already in DB
            stop_after_this_page = False
            cutoff_norm = cutoff_url.rstrip("/")
            urls_norm = [u.rstrip("/") for u in urls]
            if cutoff_norm in urls_norm:
                idx = urls_norm.index(cutoff_norm)
                urls = urls[: idx + 1]
                stop_after_this_page = True

            out.fetched_urls += len(urls)

            # --- cron-safe: only process NEW urls ---
            async with connection() as conn:
                urls_to_process = urls
                if not backfill:
                    urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                    out.new_urls += len(urls_to_process)

                    # If page has nothing new, stop early (cron fast)
                    if not urls_to_process:
                        break

            for detail_url in urls_to_process:
                if stop or out.upserted >= limit_each:
                    break
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                pub_dt = _published_from_url(detail_url)  # works for /YYYY/MM/DD/ pattern

                try:
                    title, body_text = await _fetch_detail_for_summary(client, detail_url, referer=referer)
                except Exception:
                    title, body_text = (_title_from_url_fallback(detail_url), "")

                summary = ""
                if body_text:
                    summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, detail_url)

                await _upsert_item(
                    url=detail_url,
                    title=title,
                    summary=summary,
                    jurisdiction=NC_JURISDICTION,
                    agency=NC_AGENCY_PRESS,
                    status=status,
                    source_name=source_name,
                    source_key=source_key,
                    referer=referer,
                    published_at=pub_dt,
                )
                out.upserted += 1

                if detail_url == cutoff_url:
                    out.stopped_at_cutoff = True
                    stop = True
                    break

                await asyncio.sleep(0.05)
            
            # ✅ after finishing this listing page, if we included cutoff, stop paging older pages
            if stop_after_this_page:
                out.stopped_at_cutoff = True
                break

            await asyncio.sleep(0.15)

    return out


async def _ingest_nc_executive_orders(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> NCSectionResult:
    out = NCSectionResult()
    cutoff_url = NC_EO_CUTOFF_URL
    referer = NC_PUBLIC_PAGES["executive_orders"]

    source_name = "North Carolina — Executive Orders"
    source_key = f"{NC_JURISDICTION}:executive_orders"
    status = NC_STATUS_MAP["executive_orders"]

    out = NCSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))


    seen: set[str] = set()
    stop = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for page_idx in range(max_pages_each):
            if stop or out.upserted >= limit_each:
                break

            list_url = _nc_eo_list_url(page_idx)
            r = await client.get(list_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""

            rows = _extract_nc_table_rows_with_date(html, list_url)
            if not rows:
                break

            # ✅ listing-level cutoff trim
            stop_after_this_page = False
            cutoff_norm = cutoff_url.rstrip("/")
            rows_norm_urls = [(u or "").rstrip("/") for (u, _, _) in rows]
            if cutoff_norm in rows_norm_urls:
                idx = rows_norm_urls.index(cutoff_norm)
                rows = rows[: idx + 1]
                stop_after_this_page = True

            out.fetched_urls += len(rows)

            # --- cron-safe: only process NEW urls ---
            async with connection() as conn:
                rows_to_process = rows
                if not backfill:
                    row_urls = [u for (u, _, _) in rows if u]
                    new_urls = await _filter_new_external_ids(conn, source_id, row_urls)
                    new_set = set(new_urls)
                    rows_to_process = [r for r in rows if (r[0] in new_set)]
                    out.new_urls += len(rows_to_process)

                    if not rows_to_process:
                        break

            for detail_url, title_from_list, list_dt in rows_to_process:
                if stop or out.upserted >= limit_each:
                    break
                if not detail_url:
                    continue
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                content_url, is_pdf = await _nc_resolve_eo_content_url(client, detail_url)

                title = title_from_list or _title_from_url_fallback(detail_url)
                published_at = list_dt

                summary = ""

                if is_pdf and content_url:
                    pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, content_url, referer=referer)
                    published_at = _parse_eo_published_date_from_text(pdf_text) or published_at or meta_dt

                    if pdf_text:
                        summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, content_url)  # ✅ polish using the PDF URL

                    store_url = detail_url  # ✅ ALWAYS store canonical detail URL in DB
                else:
                    store_url = detail_url  # ✅ ALWAYS store canonical detail URL in DB
                    try:
                        title2, body_text = await _fetch_detail_for_summary(client, store_url, referer=referer)
                        if title2 and title2.strip():
                            title = title2
                    except Exception:
                        body_text = ""

                    if body_text:
                        summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, store_url)

                await _upsert_item(
                    url=store_url,
                    title=title,
                    summary=summary,
                    jurisdiction=NC_JURISDICTION,
                    agency=NC_AGENCY_EO_PROC,
                    status=status,
                    source_name=source_name,
                    source_key=source_key,
                    referer=referer,
                    published_at=published_at,
                )
                out.upserted += 1

                # cutoff handling (include it, then stop)
                if detail_url.rstrip("/") == cutoff_url.rstrip("/"):
                    out.stopped_at_cutoff = True
                    stop = True
                    break

                await asyncio.sleep(0.05)

            # ✅ after finishing this listing page, if we included cutoff, stop paging older pages
            if stop_after_this_page:
                out.stopped_at_cutoff = True
                break

            await asyncio.sleep(0.15)

    return out


async def _ingest_nc_proclamations(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> NCSectionResult:
    cutoff_url = NC_PROC_CUTOFF_URL
    referer = NC_PUBLIC_PAGES["proclamations"]

    source_name = "North Carolina — Proclamations"
    source_key = f"{NC_JURISDICTION}:proclamations"
    status = NC_STATUS_MAP["proclamations"]

    out = NCSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    seen: set[str] = set()
    stop = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for page_idx in range(max_pages_each):
            if stop or out.upserted >= limit_each:
                break

            list_url = _nc_proc_list_url(page_idx)
            r = await client.get(list_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""

            rows = _extract_nc_table_rows_with_date(html, list_url)
            if not rows:
                break
            
            # ✅ listing-level cutoff trim
            stop_after_this_page = False
            cutoff_norm = cutoff_url.rstrip("/")
            rows_norm_urls = [(u or "").rstrip("/") for (u, _, _) in rows]
            if cutoff_norm in rows_norm_urls:
                idx = rows_norm_urls.index(cutoff_norm)
                rows = rows[: idx + 1]
                stop_after_this_page = True

            out.fetched_urls += len(rows)

            # --- cron-safe: only process NEW urls ---
            async with connection() as conn:
                rows_to_process = rows
                if not backfill:
                    row_urls = [u for (u, _, _) in rows if u]
                    new_urls = await _filter_new_external_ids(conn, source_id, row_urls)
                    new_set = set(new_urls)
                    rows_to_process = [r for r in rows if (r[0] in new_set)]
                    out.new_urls += len(rows_to_process)

                    if not rows_to_process:
                        break

            for detail_url, title_from_list, list_dt in rows_to_process:
                if stop or out.upserted >= limit_each:
                    break
                if not detail_url:
                    continue
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                # For procs, list_dt is usually correct; still fetch detail for summary
                title = title_from_list or _title_from_url_fallback(detail_url)
                published_at = list_dt

                try:
                    title2, body_text = await _fetch_detail_for_summary(client, detail_url, referer=referer)
                    if title2 and title2.strip():
                        title = title2
                    
                    # ✅ FIX: override list_dt with "First Published" / "PDF • ... - DATE" if found
                    published_at = _nc_proc_published_from_text(body_text) or published_at

                except Exception:
                    body_text = ""

                summary = ""
                if body_text:
                    summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, detail_url)

                await _upsert_item(
                    url=detail_url,
                    title=title,
                    summary=summary,
                    jurisdiction=NC_JURISDICTION,
                    agency=NC_AGENCY_EO_PROC,
                    status=status,
                    source_name=source_name,
                    source_key=source_key,
                    referer=referer,
                    published_at=published_at,
                )
                out.upserted += 1

                if detail_url.rstrip("/") == cutoff_url.rstrip("/"):
                    out.stopped_at_cutoff = True
                    stop = True
                    break

                await asyncio.sleep(0.05)
            
            # ✅ after finishing this listing page, if we included cutoff, stop paging older pages
            if stop_after_this_page:
                out.stopped_at_cutoff = True
                break

            await asyncio.sleep(0.15)

    return out

async def ingest_north_carolina(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "north_carolina", "counts": {}}

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            "North Carolina — Press Releases (nc.gov)",
            f"{NC_JURISDICTION}:press_releases",
            NC_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "North Carolina — Executive Orders",
            f"{NC_JURISDICTION}:executive_orders",
            NC_PUBLIC_PAGES["executive_orders"],
        )
        src_proc = await get_or_create_source(
            conn,
            "North Carolina — Proclamations",
            f"{NC_JURISDICTION}:proclamations",
            NC_PUBLIC_PAGES["proclamations"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing    = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0
        proc_existing  = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0

    press_backfill = (press_existing == 0)
    eo_backfill    = (eo_existing == 0)
    proc_backfill  = (proc_existing == 0)

    press = await _ingest_nc_press_releases(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_nc_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    procs = await _ingest_nc_proclamations(
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "new_urls": press.new_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": press.mode,
            "seen_total": press_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": eos.mode,
            "seen_total": eo_existing,
        },
        "proclamations": {
            "fetched_urls": procs.fetched_urls,
            "new_urls": procs.new_urls,
            "upserted": procs.upserted,
            "stopped_at_cutoff": procs.stopped_at_cutoff,
            "mode": procs.mode,
            "seen_total": proc_existing,
        },
    }
    print(
    f"""
    [NC INGEST COMPLETE]
    press: fetched={press.fetched_urls} new={press.new_urls} upserted={press.upserted} seen_before={press_existing} mode={press.mode}
    eo:    fetched={eos.fetched_urls}   new={eos.new_urls}   upserted={eos.upserted}   seen_before={eo_existing}   mode={eos.mode}
    proc:  fetched={procs.fetched_urls} new={procs.new_urls} upserted={procs.upserted} seen_before={proc_existing} mode={procs.mode}
    """
    )
    return out

# ----------------------------
# South Carolina config (McMaster)
# ----------------------------

SC_JURISDICTION = "south_carolina"
SC_AGENCY = "South Carolina Governor (McMaster)"

SC_PUBLIC_PAGES = {
    "news": "https://governor.sc.gov/news/archive",
    "executive_orders": "https://governor.sc.gov/executive-branch/executive-orders",
}

SC_STATUS_MAP = {
    "news": "news",
    "executive_orders": "executive_order",
}

# Cutoffs (include this item, then stop)
SC_NEWS_CUTOFF_URL = "https://governor.sc.gov/news/2025-01/gov-henry-mcmaster-lt-gov-pamela-s-evette-and-first-lady-peggy-mcmasters-weekly"
SC_EO_CUTOFF_URL = "https://governor.sc.gov/sites/governor/files/Documents/Executive-Orders/2024-01-09%20FINAL%20Executive%20Order%20No.%202024-01%20-%20Transportation%20Waivers%20to%20Address%20Severe%20Weather%20Event.pdf"

# ----------------------------
# South Carolina helpers
# ----------------------------

_SC_DRUPAL_VIEW_DOMID_RE = re.compile(r'"view_dom_id"\s*:\s*"([^"]+)"', re.I)
_SC_AJAX_PAGE_STATE_THEME_RE = re.compile(r'"ajaxPageState"\s*:\s*{[^}]*"theme"\s*:\s*"([^"]+)"', re.I)
_SC_AJAX_PAGE_STATE_LIBS_RE = re.compile(r'"ajaxPageState"\s*:\s*{[^}]*"libraries"\s*:\s*"([^"]+)"', re.I)

_SC_NEWS_DETAIL_RE = re.compile(
    r"""(?P<u>(?:https?://governor\.sc\.gov)?/news/\d{4}-\d{2}/[^"' \s<>]+)""",
    re.I,
)


_SC_NEWS_DATE_RE = re.compile(
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*(\d{2})/(\d{2})/(\d{4})\b",
    re.I,
)

_SC_EO_TEXT_PDF_RE = re.compile(
    r"https?://governor\.sc\.gov/sites/governor/files/Documents/Executive-Orders/[^\"'\s<>]*%20FINAL%20Executive%20Order%20No\.\s*%20?\d{4}-\d{2}[^\"'\s<>]*\.pdf",
    re.I,
)

_SC_EO_NO_RE = re.compile(
    r"EXECUTIVE\s+ORDER\s+NO\.?\s*(\d{4}-\d{2})",
    re.I,
)

_SC_META_TAG_RE = re.compile(
    r'(?is)<meta\b[^>]*(?:name|property)\s*=\s*["\'](?P<k>[^"\']+)["\'][^>]*content\s*=\s*["\'](?P<v>[^"\']+)["\'][^>]*>'
)

_SC_TIME_DT_RE = re.compile(r'(?is)<time\b[^>]*datetime=["\']([^"\']+)["\']', re.I)

_SC_NEWS_MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.I,
)

def _sc_parse_month_date(s: str) -> Optional[datetime]:
    try:
        month = _MONTHS[s.group(1).lower()]
        day = int(s.group(2))
        year = int(s.group(3))
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None

def _sc_extract_meta_map(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not html:
        return out
    for m in _SC_META_TAG_RE.finditer(html):
        k = (m.group("k") or "").strip().lower()
        v = (m.group("v") or "").strip()
        if k and v and k not in out:
            out[k] = v
    return out

def _sc_parse_iso_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    t = s.strip()
    # handle "2025-12-30" and "2025-12-30T13:45:00Z" etc
    try:
        # normalize Z -> +00:00
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # fallback: common date-only
    try:
        dt2 = datetime.strptime(t[:10], "%Y-%m-%d")
        return dt2.replace(tzinfo=timezone.utc)
    except Exception:
        return None

_SC_JSONLD_RE = re.compile(r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>\s*(.*?)\s*</script>')
def _sc_extract_jsonld_dates(html: str) -> Tuple[Optional[datetime], Optional[str]]:
    """
    Returns (datePublished, headline) if present.
    """
    if not html:
        return (None, None)

    for m in _SC_JSONLD_RE.finditer(html):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        # JSON-LD can be dict or list
        candidates = obj if isinstance(obj, list) else [obj]
        for c in candidates:
            if not isinstance(c, dict):
                continue
            dp = c.get("datePublished") or c.get("dateCreated") or c.get("dateModified")
            headline = c.get("headline") or c.get("name")
            dt = _sc_parse_iso_date(dp) if isinstance(dp, str) else None
            hl = headline.strip() if isinstance(headline, str) else None
            if dt or hl:
                return (dt, hl)

    return (None, None)

def _sc_pick_title_from_html(html: str, fallback: str) -> str:
    if not html:
        return fallback

    def _clean(s: str) -> str:
        s = html_lib.unescape(s or "")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _looks_bad(s: str) -> bool:
        t = (s or "").strip().lower()
        if not t:
            return True
        # site-wide / generic titles we DON'T want as item titles
        if "south carolina office of the governor" in t:
            return True
        if t in {"news", "archive"}:
            return True
        return False

    meta = _sc_extract_meta_map(html)

    # ✅ 1) Prefer social meta titles (these are usually true headlines)
    for k in ("og:title", "twitter:title"):
        v = meta.get(k)
        v = _clean(v) if v else ""
        if v and not _looks_bad(v):
            return v[:500]

    # ✅ 2) Prefer H1 (SC detail pages have the real headline here)
    m = re.search(r"(?is)<h1[^>]*>\s*(.*?)\s*</h1>", html)
    if m:
        t = re.sub(r"(?is)<.*?>", " ", m.group(1))
        t = _clean(t)
        if t and not _looks_bad(t):
            return t[:500]

    # ✅ 3) JSON-LD headline/name (if present)
    _, hl = _sc_extract_jsonld_dates(html)
    hl = _clean(hl) if hl else ""
    if hl and not _looks_bad(hl):
        return hl[:500]

    # ✅ 4) <title> (strip site suffix)
    m2 = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m2:
        t = _clean(m2.group(1))
        # common patterns: "Headline | Site Name"
        for sep in (" | ", " – ", " - "):
            if sep in t:
                t = t.split(sep)[0].strip()
                break
        if t and not _looks_bad(t):
            return t[:500]

    return (fallback or "")[:500] or fallback

def _sc_parse_news_published_at(html: str) -> Optional[datetime]:
    """
    Try meta/JSON-LD/time first; fallback to visible dates.
    """
    if not html:
        return None

    meta = _sc_extract_meta_map(html)

    for k in (
        "article:published_time",
        "article:modified_time",
        "og:updated_time",
        "og:published_time",
        "date",
        "dc.date",
        "dcterms.date",
        "dcterms.created",
        "dcterms.issued",
    ):
        v = meta.get(k)
        if v:
            dt = _sc_parse_iso_date(v)
            if dt:
                return dt

    dt_jsonld, _ = _sc_extract_jsonld_dates(html)
    if dt_jsonld:
        return dt_jsonld

    mtime = _SC_TIME_DT_RE.search(html)
    if mtime:
        dt = _sc_parse_iso_date(mtime.group(1))
        if dt:
            return dt

    # ✅ NEW: visible "December 30, 2025" style date anywhere in rendered HTML/text
    compact = re.sub(r"\s+", " ", html)
    m_vis = _SC_NEWS_MONTH_DATE_RE.search(compact)
    if m_vis:
        return _sc_parse_month_date(m_vis)

    # existing fallback (rare on these pages)
    m = _SC_NEWS_DATE_RE.search(compact)
    if m:
        try:
            mm = int(m.group(1))
            dd = int(m.group(2))
            yy = int(m.group(3))
            return datetime(yy, mm, dd, tzinfo=timezone.utc)
        except Exception:
            pass

    return None

def _sc_news_list_url(page_idx: int) -> str:
    # SC archive supports ?page=0,1,2...
    return f"{SC_PUBLIC_PAGES['news']}?page={page_idx}"

def _sc_title_from_pdf_text(pdf_text: str, fallback: str) -> str:
    if not pdf_text:
        return fallback
    t = re.sub(r"\s+", " ", pdf_text).strip()
    m = _SC_EO_NO_RE.search(t[:5000])  # very top is enough
    if m:
        return f"Executive Order {m.group(1)}"
    return fallback

async def _sc_fetch_news_archive_bootstrap(client: httpx.AsyncClient) -> dict:
    """
    Loads /news/archive once and extracts:
      - view_dom_id
      - ajaxPageState.theme
      - ajaxPageState.libraries
    These make the Drupal Views ajax endpoint stable.
    """
    referer = SC_PUBLIC_PAGES["news"]
    r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
    r.raise_for_status()
    html = r.text or ""

    view_dom_id = None
    m = _SC_DRUPAL_VIEW_DOMID_RE.search(html)
    if m:
        view_dom_id = m.group(1).strip()

    theme = "governorpalmetto"
    m2 = _SC_AJAX_PAGE_STATE_THEME_RE.search(html)
    if m2:
        theme = m2.group(1).strip() or theme

    libraries = ""
    m3 = _SC_AJAX_PAGE_STATE_LIBS_RE.search(html)
    if m3:
        libraries = m3.group(1).strip() or ""

    return {
        "view_dom_id": view_dom_id or "",
        "theme": theme,
        "libraries": libraries,
    }

def _sc_build_news_ajax_url(*, page_idx: int, bootstrap: dict) -> str:
    """
    The DevTools endpoint is Drupal Views ajax.
    page_idx is 0-based for the pager in the request (?page=1 means 2nd page).
    """
    base = "https://governor.sc.gov/views/ajax"
    params = {
        "_wrapper_format": "drupal_ajax",
        "view_name": "news",
        "view_display_id": "page_2",
        "view_args": "",
        "view_path": "/news/archive",
        "view_base_path": "news/archive",
        "view_dom_id": bootstrap.get("view_dom_id", ""),
        "pager_element": "0",
        "page": str(page_idx),
        "_drupal_ajax": "1",
        "ajax_page_state[theme]": bootstrap.get("theme", "governorpalmetto"),
        "ajax_page_state[theme_token]": "",
    }
    libs = (bootstrap.get("libraries") or "").strip()
    if libs:
        params["ajax_page_state[libraries]"] = libs

    return base + "?" + urlencode(params, doseq=True)

def _sc_extract_news_urls_from_drupal_ajax(payload_json: object) -> List[str]:
    """
    Drupal ajax returns a JSON array of command objects.
    We search any HTML 'data' fields for /news/YYYY-MM/... links.
    """
    if not payload_json:
        return []
    blobs: List[str] = []

    if isinstance(payload_json, list):
        for row in payload_json:
            if isinstance(row, dict):
                d = row.get("data")
                if isinstance(d, str) and d.strip():
                    blobs.append(d)

    hay = "\n".join(blobs)
    found = _extract_urls_matching(hay, _SC_NEWS_DETAIL_RE)
    # de-dupe preserve order
    seen = set()
    out: List[str] = []
    for u in found:
        u = u.strip()
        if not u:
            continue
        if u.startswith("/"):
            u = urljoin("https://governor.sc.gov", u)
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

def _parse_sc_eo_published_date_from_text(text: str) -> Optional[datetime]:
    """
    SC EOs: parse signing line like:
      'GIVEN UNDER MY HAND ... THIS 30th DAY OF DECEMBER, 2025.'
    Use the LAST 'this <day> day of <Month> <Year>' match (tail-safe).
    """
    if not text:
        return None
    full = re.sub(r"\s+", " ", text).strip()
    tail = full[-20000:] if len(full) > 20000 else full

    matches = list(_DATE_THIS_DAY_OF_NUM.finditer(tail))
    if matches:
        m = matches[-1]
        day = int(m.group(1))
        month = _MONTHS[m.group(2).lower()]
        year = int(m.group(3))
        return datetime(year, month, day, tzinfo=timezone.utc)

    # fallback: Month DD, YYYY (rare)
    return _parse_eo_published_date_from_text(full)

# ----------------------------
# South Carolina ingesters
# ----------------------------

@dataclass
class SCSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"

async def _ingest_south_carolina_news(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> SCSectionResult:
    out = SCSectionResult()
    referer = SC_PUBLIC_PAGES["news"]
    cutoff_url = SC_NEWS_CUTOFF_URL

    source_name = "South Carolina — News"
    source_key = f"{SC_JURISDICTION}:news"
    status = SC_STATUS_MAP["news"]

    out = SCSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    seen: set[str] = set()
    stop = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        bootstrap = await _sc_fetch_news_archive_bootstrap(client)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=BROWSER_UA_HEADERS.get("user-agent"))
            page = await context.new_page()

            try:
                for page_idx in range(max_pages_each):
                    if stop or out.upserted >= limit_each:
                        break

                    urls: List[str] = []

                    # (keep your existing ajax + html fallback listing logic)
                    headers = {
                        **BROWSER_UA_HEADERS,
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "x-requested-with": "XMLHttpRequest",
                        "referer": referer,
                        "cache-control": "no-cache",
                        "pragma": "no-cache",
                    }

                    try:
                        ajax_url = _sc_build_news_ajax_url(page_idx=page_idx, bootstrap=bootstrap)
                        r = await client.get(ajax_url, headers=headers, timeout=httpx.Timeout(45.0, read=45.0))
                        payload = r.json() if r.status_code < 400 else None
                        urls = _sc_extract_news_urls_from_drupal_ajax(payload)
                    except Exception:
                        urls = []

                    if not urls:
                        list_url = _sc_news_list_url(page_idx)
                        r2 = await client.get(list_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
                        if r2.status_code >= 400:
                            break
                        html2 = r2.text or ""
                        urls = _extract_urls_matching(html2, _SC_NEWS_DETAIL_RE)
                        cleaned = []
                        seen2 = set()
                        for u in urls:
                            u = (u or "").strip()
                            if not u:
                                continue
                            if u.startswith("/"):
                                u = urljoin("https://governor.sc.gov", u)
                            if u in seen2:
                                continue
                            seen2.add(u)
                            cleaned.append(u)
                        urls = cleaned

                    if not urls:
                        break

                    # ✅ listing-level cutoff: stop paging past cutoff even if cutoff is already in DB
                    stop_after_this_page = False
                    cutoff_norm = cutoff_url.rstrip("/")
                    urls_norm = [u.rstrip("/") for u in urls]
                    if cutoff_norm in urls_norm:
                        idx = urls_norm.index(cutoff_norm)
                        urls = urls[: idx + 1]
                        stop_after_this_page = True

                    out.fetched_urls += len(urls)

                    # --- cron-safe: only process NEW urls ---
                    async with connection() as conn:
                        urls_to_process = urls
                        if not backfill:
                            urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                            out.new_urls += len(urls_to_process)

                            # If this page has nothing new, stop early (cron fast)
                            if not urls_to_process:
                                break

                    for detail_url in urls_to_process:
                        if stop or out.upserted >= limit_each:
                            break
                        if detail_url in seen:
                            continue
                        seen.add(detail_url)

                        # ✅ Playwright-rendered detail fetch
                        try:
                            await page.goto(detail_url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                            # if SC injects after DOMContentLoaded, give it a beat
                            await page.wait_for_timeout(800)
                            html = await page.content()

                            # ✅ ADD IT RIGHT HERE
                            if out.upserted == 0:
                                compact = re.sub(r"\s+", " ", html)
                                m = _SC_NEWS_MONTH_DATE_RE.search(compact)
                                print("SC detail has 'Month DD, YYYY' date?", bool(m), "match=", (m.group(0) if m else None))

                            # 🔎 DEBUG: inspect what Playwright actually sees (only when we're not upserting anything)
                            if out.upserted == 0:
                                print("SC detail has og:title?", "og:title" in html.lower())
                                print("SC detail has json-ld?", "application/ld+json" in html.lower())
                                print("SC detail has <h1>?", "<h1" in html.lower())
                                print(
                                    "SC detail has Tue, mm/dd/yyyy?",
                                    bool(_SC_NEWS_DATE_RE.search(re.sub(r"\s+", " ", html))),
                                )

                        except Exception:
                            html = ""

                        title = _sc_pick_title_from_html(html, _title_from_url_fallback(detail_url))
                        published_at = _sc_parse_news_published_at(html)

                        body_text = _strip_html_to_text(html) if html else ""
                        body_text = (body_text or "").strip()
                        if len(body_text) > 35000:
                            body_text = body_text[:35000]

                        summary = ""
                        if body_text:
                            summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, detail_url)

                        await _upsert_item(
                            url=detail_url,
                            title=title,
                            summary=summary,
                            jurisdiction=SC_JURISDICTION,
                            agency=SC_AGENCY,
                            status=status,
                            source_name=source_name,
                            source_key=source_key,
                            referer=referer,
                            published_at=published_at,
                        )
                        out.upserted += 1

                        if detail_url.rstrip("/") == cutoff_url.rstrip("/"):
                            out.stopped_at_cutoff = True
                            stop = True
                            break

                        await asyncio.sleep(0.05)
                    
                    # ✅ after finishing this listing page, if we included cutoff, stop paging older pages
                    if stop_after_this_page:
                        out.stopped_at_cutoff = True
                        break

                    await asyncio.sleep(0.15)
            finally:
                await context.close()
                await browser.close()

    return out

async def _ingest_south_carolina_executive_orders(*, source_id: int, backfill: bool, limit_each: int) -> SCSectionResult:
    out = SCSectionResult()
    referer = SC_PUBLIC_PAGES["executive_orders"]
    cutoff_url = SC_EO_CUTOFF_URL

    source_name = "South Carolina — Executive Orders"
    source_key = f"{SC_JURISDICTION}:executive_orders"
    status = SC_STATUS_MAP["executive_orders"]

    out = SCSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        limit_each = max(25, min(int(limit_each or 500), 1500))

    stop_after_this_page = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
        r.raise_for_status()
        html = r.text or ""

        # Grab ONLY the "Text Alternative (PDF)" links (FINAL ...)
        pdf_urls = _extract_urls_matching(html, _SC_EO_TEXT_PDF_RE)
        if not pdf_urls:
            return out
        
        # ✅ listing-level cutoff trim (include cutoff, then stop)
        cutoff_norm = cutoff_url.rstrip("/")
        pdf_norm = [u.rstrip("/") for u in pdf_urls]
        if cutoff_norm in pdf_norm:
            idx = pdf_norm.index(cutoff_norm)
            pdf_urls = pdf_urls[: idx + 1]
            stop_after_this_page = True

        out.fetched_urls = len(pdf_urls)

        # Better titles from anchor text when possible
        anchor_map = _extract_anchor_map(html)

        # --- cron-safe: only process NEW urls ---
        async with connection() as conn:
            pdfs_to_process = pdf_urls
            if not backfill:
                pdfs_to_process = await _filter_new_external_ids(conn, source_id, pdf_urls)
                out.new_urls += len(pdfs_to_process)
                if not pdfs_to_process:
                    return out

        for pdf_url in pdfs_to_process:
            fallback_title = anchor_map.get(pdf_url) or _title_from_url_fallback(pdf_url)

            pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, pdf_url, referer=referer)
            title = _sc_title_from_pdf_text(pdf_text, fallback_title)

            published_at = _parse_sc_eo_published_date_from_text(pdf_text) or meta_dt

            summary = ""
            if pdf_text:
                summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, pdf_url)

            await _upsert_item(
                url=pdf_url,
                title=title,
                summary=summary,
                jurisdiction=SC_JURISDICTION,
                agency=SC_AGENCY,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if pdf_url.rstrip("/") == cutoff_url.rstrip("/"):
                out.stopped_at_cutoff = True
                break

            if out.upserted >= limit_each:
                break

            await asyncio.sleep(0.05)
        
    # ✅ if we trimmed the list to include cutoff, mark stopped_at_cutoff
    if stop_after_this_page:
        out.stopped_at_cutoff = True

    return out

async def ingest_south_carolina(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "south_carolina", "counts": {}}

    async with connection() as conn:
        src_news = await get_or_create_source(
            conn,
            "South Carolina — News",
            f"{SC_JURISDICTION}:news",
            SC_PUBLIC_PAGES["news"],
        )
        src_eo = await get_or_create_source(
            conn,
            "South Carolina — Executive Orders",
            f"{SC_JURISDICTION}:executive_orders",
            SC_PUBLIC_PAGES["executive_orders"],
        )

        news_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_news) or 0
        eo_existing   = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0

    news_backfill = (news_existing == 0)
    eo_backfill   = (eo_existing == 0)

    news = await _ingest_south_carolina_news(
        source_id=src_news,
        backfill=news_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_south_carolina_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )

    out["counts"] = {
        "news": {
            "fetched_urls": news.fetched_urls,
            "new_urls": news.new_urls,
            "upserted": news.upserted,
            "stopped_at_cutoff": news.stopped_at_cutoff,
            "mode": news.mode,
            "seen_total": news_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": eos.mode,
            "seen_total": eo_existing,
        },
    }

    print(
        f"""
[SC INGEST COMPLETE]
news: fetched={news.fetched_urls} new={news.new_urls} upserted={news.upserted} seen_before={news_existing} mode={news.mode}
eo:   fetched={eos.fetched_urls}  new={eos.new_urls}  upserted={eos.upserted}  seen_before={eo_existing}  mode={eos.mode}
"""
    )

    return out

# ----------------------------
# Oregon config
# ----------------------------

OR_JURISDICTION = "oregon"
OR_AGENCY_PRESS = "Oregon Newsroom (State Agencies)"
OR_AGENCY_GOV = "Oregon Governor's Office"

OR_PUBLIC_PAGES = {
    "press_releases": "https://apps.oregon.gov/oregon-newsroom/OR/Posts/Search/?featured=true",
    "executive_orders": "https://www.oregon.gov/gov/Pages/executive-orders.aspx",
}

OR_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
}

# Cutoffs (include this item, then stop)
OR_PRESS_CUTOFF_URL = "https://apps.oregon.gov/oregon-newsroom/OR/DOR/Posts/Post/Changes-make-it-easier-to-claim-tax-benefits-when-saving-for-your-first-home"
OR_EO_CUTOFF_URL = "https://www.oregon.gov/gov/eo/eo-24-01.pdf"

# ----------------------------
# Oregon helpers
# ----------------------------

_OR_NEWSROOM_DETAIL_RE = re.compile(
    r"""(?P<u>(?:https?://apps\.oregon\.gov)?/oregon-newsroom/(?:OR|or)/[^"'\s<>]+/Posts/Post/[^"'\s<>]+)""",
    re.I,
)


_OR_US_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.I,
)

_OR_META_TAG_RE = re.compile(
    r'(?is)<meta\b[^>]*(?:property|name)\s*=\s*["\'](?P<k>[^"\']+)["\'][^>]*content\s*=\s*["\'](?P<v>[^"\']+)["\'][^>]*>'
)

def _looks_generic_or_title_bad(t: str) -> bool:
    x = (t or "").strip().lower()
    if not x:
        return True
    bad = {
        "newsroom",
        "oregon newsroom",
        "state of oregon",
        "home",
    }
    if x in bad:
        return True
    # also catch titles like "Newsroom | State of Oregon"
    if x.startswith("newsroom |") or x.startswith("newsroom -"):
        return True
    return False

def _strip_download_prefix(t: str) -> str:
    t = re.sub(r"(?i)^\s*download\s*", "", (t or "").strip())
    return re.sub(r"\s+", " ", t).strip()


async def _pw_extract_best_title(page, fallback: str) -> str:
    """
    Prefer rendered H1/meta, not the raw HTML snapshot.
    """
    def clean(s: str) -> str:
        s = html_lib.unescape(s or "")
        s = re.sub(r"\s+", " ", s).strip()
        # strip common suffix patterns
        for sep in (" | ", " – ", " - "):
            if sep in s:
                s = s.split(sep)[0].strip()
                break
        return s

    # 1) H1 (rendered)
    try:
        h1 = page.locator("h1").first
        if await h1.count() > 0:
            t = clean(await h1.inner_text())
            if t and not _looks_generic_or_title_bad(t):
                return t[:500]
    except Exception:
        pass

    # 2) og:title
    try:
        og = page.locator("meta[property='og:title']").first
        if await og.count() > 0:
            t = clean(await og.get_attribute("content") or "")
            if t and not _looks_generic_or_title_bad(t):
                return t[:500]
    except Exception:
        pass

    # 3) twitter:title
    try:
        tw = page.locator("meta[name='twitter:title']").first
        if await tw.count() > 0:
            t = clean(await tw.get_attribute("content") or "")
            if t and not _looks_generic_or_title_bad(t):
                return t[:500]
    except Exception:
        pass

    # 4) page.title()
    try:
        t = clean(await page.title())
        if t and not _looks_generic_or_title_bad(t):
            return t[:500]
    except Exception:
        pass

    return (fallback or "")[:500]


def _or_extract_meta_map(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not html:
        return out
    for m in _OR_META_TAG_RE.finditer(html):
        k = (m.group("k") or "").strip().lower()
        v = (m.group("v") or "").strip()
        if k and v and k not in out:
            out[k] = html_lib.unescape(v)
    return out

def _or_pick_title_from_html(html: str, fallback: str) -> str:
    if not html:
        return (fallback or "")[:500]

    def clean(s: str) -> str:
        s = html_lib.unescape(s or "")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    meta = _or_extract_meta_map(html)
    for k in ("og:title", "twitter:title"):
        v = clean(meta.get(k, ""))
        if v:
            return v[:500]

    m = re.search(r"(?is)<h1[^>]*>\s*(.*?)\s*</h1>", html)
    if m:
        t = re.sub(r"(?is)<.*?>", " ", m.group(1))
        t = clean(t)
        if t:
            return t[:500]

    m2 = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m2:
        t = clean(m2.group(1))
        # strip common suffix patterns
        for sep in (" | ", " – ", " - "):
            if sep in t:
                t = t.split(sep)[0].strip()
                break
        if t:
            return t[:500]

    return (fallback or "")[:500]

# Also accept "January 7 2025" (no comma) just in case
_OR_US_DATE_RE2 = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,)?\s+(\d{4})\b",
    re.I,
)

def _or_parse_published_at_from_html(html: str) -> Optional[datetime]:
    if not html:
        return None
    compact = re.sub(r"\s+", " ", html)
    m = _OR_US_DATE_RE2.search(compact)
    if not m:
        return None
    try:
        month = _MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        year = int(m.group(3))
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None


def _or_press_list_url(page_idx: int) -> str:
    # page 0: /OR/Posts/Search/?featured=true
    # page 1+: /or/Posts/Search?featured=true&page=1
    if page_idx == 0:
        return OR_PUBLIC_PAGES["press_releases"]
    return f"https://apps.oregon.gov/oregon-newsroom/or/Posts/Search?featured=true&page={page_idx}"

def _or_parse_published_at_from_html(html: str) -> Optional[datetime]:
    """
    Oregon newsroom detail pages show something like:
      'Press Release · January 7, 2025'
      'Article · December 31, 2025'
    We just grab the first 'Month DD, YYYY' we see.
    """
    if not html:
        return None
    compact = re.sub(r"\s+", " ", html)
    m = _OR_US_DATE_RE.search(compact)
    if not m:
        return None
    try:
        month = _MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        year = int(m.group(3))
        return datetime(year, month, day, tzinfo=timezone.utc)
    except Exception:
        return None

def _or_strip_tags(s: str) -> str:
    s = re.sub(r"(?is)<.*?>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _or_extract_eo_rows(html: str, base_url: str) -> List[Tuple[str, str, str]]:
    """
    Parse EO rows from https://www.oregon.gov/gov/Pages/executive-orders.aspx
    Returns list of (eo_number, description, pdf_url) in page order (newest first).
    """
    if not html:
        return []

    rows = re.findall(r"(?is)<tr\b.*?>.*?</tr>", html)
    out: List[Tuple[str, str, str]] = []

    for row in rows:
        # PDF link
        lm = re.search(r'(?is)href=["\'](?P<href>[^"\']+\.pdf)["\']', row)
        if not lm:
            continue

        pdf_url = urljoin(base_url, (lm.group("href") or "").strip())
        if not pdf_url.lower().endswith(".pdf"):
            continue

        # td1: EO number, td2: description (best-effort)
        tds = re.findall(r"(?is)<td[^>]*>\s*(.*?)\s*</td>", row)
        if len(tds) >= 2:
            eo_number = _or_strip_tags(tds[0])[:50]
            desc = _or_strip_tags(tds[1])[:500]
        else:
            eo_number = ""
            desc = ""

        out.append((eo_number, desc, pdf_url))

    # de-dupe by pdf_url preserve order
    seen = set()
    final: List[Tuple[str, str, str]] = []
    for n, d, u in out:
        if u in seen:
            continue
        seen.add(u)
        final.append((n, d, u))
    return final

def _or_title_for_eo(eo_number: str, desc: str, pdf_url: str) -> str:
    eo_number = (eo_number or "").strip()
    desc = (desc or "").strip()

    # If number looks like "2025-31" etc, prefer "Executive Order 25-31" formatting when possible.
    title_num = eo_number
    if eo_number and "Executive" not in eo_number:
        title_num = f"Executive Order {eo_number}"

    if desc:
        if title_num:
            return f"{title_num} — {desc}"[:500]
        return desc[:500]

    # fallback to filename
    return (_title_from_url_fallback(pdf_url) or pdf_url)[:500]

def _or_parse_eo_published_date_from_text(text: str) -> Optional[datetime]:
    """
    Oregon EO PDFs typically contain:
      'Done at Salem, Oregon, this 9th day of December, 2025.'
    We'll use the tail-safe 'this <day> day of <Month> <Year>' patterns you already have.
    """
    if not text:
        return None

    full = re.sub(r"\s+", " ", text).strip()
    tail = full[-20000:] if len(full) > 20000 else full

    # numeric ordinal: this 9th day of December, 2025
    matches_num = list(_DATE_THIS_DAY_OF_NUM.finditer(tail))
    if matches_num:
        m = matches_num[-1]
        try:
            day = int(m.group(1))
            month = _MONTHS[m.group(2).lower()]
            year = int(m.group(3))
            return datetime(year, month, day, tzinfo=timezone.utc)
        except Exception:
            pass

    # word ordinal: this twenty-second day of May 2024
    matches_word = list(_DATE_THIS_DAY_OF_WORD.finditer(tail))
    if matches_word:
        m = matches_word[-1]
        day = _ordinal_words_to_int((m.group(1) or "").strip())
        try:
            month = _MONTHS[m.group(2).lower()]
            year = int(m.group(3))
            if day and 1 <= day <= 31:
                return datetime(year, month, day, tzinfo=timezone.utc)
        except Exception:
            pass

    # fallback to your generic EO parser
    return _parse_eo_published_date_from_text(full)

async def _or_fetch_pdf_text_with_optional_ocr(client: httpx.AsyncClient, url: str, *, referer: str) -> Tuple[str, Optional[datetime]]:
    """
    1) Try pypdf text extraction (+ meta date).
    2) If extracted text is too small, attempt OCR if deps exist:
       - PyMuPDF (fitz) to render pages
       - pytesseract to OCR
    If OCR deps aren't installed, returns whatever pypdf got.
    """
    pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, url, referer=referer)

    # if we got decent text, keep it
    if pdf_text and len(pdf_text.strip()) >= 200:
        return (pdf_text, meta_dt)

    # OCR fallback (optional)
    try:
        import fitz  # PyMuPDF
        import pytesseract
    except Exception:
        return (pdf_text or "", meta_dt)

    try:
        r = await client.get(url, headers={**BROWSER_UA_HEADERS, "referer": referer}, timeout=httpx.Timeout(60.0, read=60.0))
        r.raise_for_status()
        data = r.content or b""
        if not data:
            return (pdf_text or "", meta_dt)

        doc = fitz.open(stream=data, filetype="pdf")
        texts: List[str] = []

        # OCR only first few pages (EOs are usually 1–3 pages)
        max_pages = min(len(doc), 5)
        for i in range(max_pages):
            page = doc.load_page(i)
            pix = page.get_pixmap(dpi=200)
            img_bytes = pix.tobytes("png")

            from PIL import Image
            import io as _io
            img = Image.open(_io.BytesIO(img_bytes))
            t = pytesseract.image_to_string(img) or ""
            t = re.sub(r"\s+", " ", t).strip()
            if t:
                texts.append(t)

        ocr_text = " ".join(texts).strip()
        if ocr_text:
            return (ocr_text, meta_dt)

    except Exception:
        pass

    return (pdf_text or "", meta_dt)

def _or_norm_eo_store_url(u: str) -> str:
    """
    Canonicalize Oregon EO PDF URLs so db comparisons are stable.
    Important: governor site sometimes uses eo_23-01.pdf vs eo-23-01.pdf.
    We store the hyphen form consistently.
    """
    u = (u or "").strip()
    if not u:
        return ""
    u = _norm_url(u)  # your global normalizer (strip slash, params, etc)
    # canonicalize eo_YY-NN.pdf -> eo-YY-NN.pdf
    u = re.sub(r"(?i)/eo_(\d{2}-\d{2}\.pdf)$", r"/eo-\1", u)
    return u



# ----------------------------
# Oregon ingesters
# ----------------------------
@dataclass
class ORSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"

async def _ingest_oregon_press(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> ORSectionResult:
    out = ORSectionResult()

    out = ORSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        # Cron-safe caps (don’t let someone pass 10,000 pages and re-scan forever)
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    referer = OR_PUBLIC_PAGES["press_releases"]
    cutoff_url = OR_PRESS_CUTOFF_URL

    source_name = "Oregon — Newsroom (Featured Feed)"
    source_key = f"{OR_JURISDICTION}:press_releases"
    status = OR_STATUS_MAP["press_releases"]

    seen: set[str] = set()
    stop = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=BROWSER_UA_HEADERS.get("user-agent"))
        page = await context.new_page()

        try:
            for page_idx in range(max_pages_each):
                if stop or out.upserted >= limit_each:
                    break

                list_url = _or_press_list_url(page_idx)

                resp = await page.goto(list_url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                if resp and resp.status >= 400:
                    break

                await page.wait_for_timeout(600)
                html = await page.content()

                urls = _extract_urls_matching(html, _OR_NEWSROOM_DETAIL_RE)

                # normalize relative -> absolute
                cleaned = []
                seen_u = set()
                for u in urls:
                    if not u:
                        continue
                    u = u.strip()
                    if u.startswith("/"):
                        u = urljoin("https://apps.oregon.gov", u)
                    if u in seen_u:
                        continue
                    seen_u.add(u)
                    cleaned.append(u)
                urls = cleaned

                if not urls:
                    break

                # ✅ listing-level cutoff: if cutoff is on this page, keep newest..cutoff (inclusive) then stop after page
                stop_after_this_page = False
                cutoff_norm = cutoff_url.rstrip("/")
                urls_norm = [u.rstrip("/") for u in urls]
                if cutoff_norm in urls_norm:
                    idx = urls_norm.index(cutoff_norm)
                    urls = urls[: idx + 1]
                    stop_after_this_page = True


                out.fetched_urls += len(urls)

                # --- cron-safe: only process NEW urls ---
                async with connection() as conn:
                    urls_to_process = urls
                    if not backfill:
                        urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                        out.new_urls += len(urls_to_process)

                        # If this page has nothing new, stop early (cron fast)
                        if not urls_to_process:
                            break

                for detail_url in urls_to_process:
                    if stop or out.upserted >= limit_each:
                        break
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    # detail via browser
                    try:
                        await page.goto(detail_url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                        await page.wait_for_timeout(600)
                        html2 = await page.content()
                    except Exception:
                        html2 = ""

                    fallback = _title_from_url_fallback(detail_url)
                    title = _or_pick_title_from_html(html2, fallback)

                    # ✅ If still generic, ask Playwright DOM for the rendered title
                    if _looks_generic_or_title_bad(title):
                        title = await _pw_extract_best_title(page, fallback)
                    body_text = _strip_html_to_text(html2) if html2 else ""
                    body_text = (body_text or "").strip()
                    if len(body_text) > 35000:
                        body_text = body_text[:35000]

                    pub_dt = _or_parse_published_at_from_html(html2) if html2 else None

                    summary = ""
                    if body_text:
                        summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, detail_url)

                    await _upsert_item(
                        url=detail_url,
                        title=title,
                        summary=summary,
                        jurisdiction=OR_JURISDICTION,
                        agency=OR_AGENCY_PRESS,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=pub_dt,
                    )
                    out.upserted += 1

                    if detail_url.rstrip("/") == cutoff_url.rstrip("/"):
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    await asyncio.sleep(0.05)
                
                if stop_after_this_page:
                    out.stopped_at_cutoff = True
                    break

                await asyncio.sleep(0.15)

        finally:
            await context.close()
            await browser.close()

    return out

_OR_EO_PDF_RE = re.compile(r"https?://www\.oregon\.gov/gov/eo/[^\"'\s<>]+\.pdf", re.I)

async def _ingest_oregon_executive_orders(*, source_id: int, backfill: bool, limit_each: int) -> ORSectionResult:
    out = ORSectionResult()

    out = ORSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        limit_each = max(25, min(int(limit_each or 500), 1500))

    referer = OR_PUBLIC_PAGES["executive_orders"]
    cutoff_url = OR_EO_CUTOFF_URL

    source_name = "Oregon — Executive Orders"
    source_key = f"{OR_JURISDICTION}:executive_orders"
    status = OR_STATUS_MAP["executive_orders"]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=BROWSER_UA_HEADERS.get("user-agent"))
        page = await context.new_page()

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                resp = await page.goto(referer, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                if resp and resp.status >= 400:
                    return out

                # give scripts time to render the EO list
                await page.wait_for_timeout(1200)
                html = await page.content()

                # ✅ NEW: parse the EO table rows (number, description, pdf)
                rows = _or_extract_eo_rows(html, referer)  # (eo_number, desc, pdf_url)
                if not rows:
                    return out

                # ✅ listing-level cutoff: keep newest..cutoff (inclusive)
                cutoff_store = (_or_norm_eo_store_url(cutoff_url) or cutoff_url).rstrip("/")
                stop_after_this_page = False

                # Normalize each row's PDF to canonical store URL for stable comparison
                rows_norm = []
                for (n, d, pdf) in rows:
                    if not pdf:
                        continue
                    store = (_or_norm_eo_store_url(pdf) or pdf).rstrip("/")
                    rows_norm.append((store, (n, d, pdf)))

                norm_urls = [store for (store, _) in rows_norm]

                # If cutoff is present on the page, truncate rows to newest..cutoff (inclusive)
                if cutoff_store in norm_urls:
                    idx = norm_urls.index(cutoff_store)
                    rows = [row for (_, row) in rows_norm[: idx + 1]]
                    stop_after_this_page = True

                out.fetched_urls = len(rows)

                # --- cron-safe: only process NEW pdf urls ---
                async with connection() as conn:
                    rows_to_process = rows
                    if not backfill:
                        # build (row, store_url) pairs so filtering uses canonical urls
                        pairs = []
                        for eo_number, desc, pdf_url in rows:
                            if not pdf_url:
                                continue
                            store_url = _or_norm_eo_store_url(pdf_url)
                            if not store_url:
                                continue
                            pairs.append(((eo_number, desc, pdf_url), store_url))

                        store_urls = [s for (_, s) in pairs]
                        new_store_urls = await _filter_new_external_ids(conn, source_id, store_urls)
                        new_set = set(new_store_urls)

                        # keep only rows whose canonical store_url is new
                        rows_to_process = [row for (row, s) in pairs if s in new_set]
                        out.new_urls += len(rows_to_process)

                        # If none new, fast exit
                        if not rows_to_process:
                            if stop_after_this_page:
                                out.stopped_at_cutoff = True
                            return out

                for eo_number, desc, pdf_url in rows_to_process:
                    if out.upserted >= limit_each:
                        break

                    # ✅ ADD THESE 2 LINES RIGHT HERE (immediately after the loop starts)
                    fetch_url = pdf_url
                    store_url = _or_norm_eo_store_url(pdf_url) or pdf_url

                    # ✅ Title from table (no "Download")
                    title = _or_title_for_eo(eo_number, desc, pdf_url)
                    title = _strip_download_prefix(title)

                    # ✅ CHANGE pdf_url -> fetch_url here (download/parse)
                    pdf_text, meta_dt = await _or_fetch_pdf_text_with_optional_ocr(
                        client, fetch_url, referer=referer
                    )
                    published_at = _or_parse_eo_published_date_from_text(pdf_text) or meta_dt

                    summary = ""
                    if pdf_text and len(pdf_text.strip()) >= 200:
                        summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)

                        # ✅ CHANGE pdf_url -> store_url here (polish should track canonical)
                        summary = await _safe_ai_polish(summary, title, store_url)
                    else:
                        # ✅ scanned/image PDF fallback: use description as summary
                        summary = (desc or "").strip()
                        if summary:
                            summary = summary[:700]

                    # ✅ CHANGE url=pdf_url -> url=store_url here (DB should store canonical)
                    await _upsert_item(
                        url=store_url,
                        title=title,
                        summary=summary,
                        jurisdiction=OR_JURISDICTION,
                        agency=OR_AGENCY_GOV,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=published_at,
                    )
                    out.upserted += 1

                    # ✅ cutoff compare should use store_url (you already wrote this, but it only works now)
                    if store_url.rstrip("/") == _or_norm_eo_store_url(cutoff_url).rstrip("/"):
                        out.stopped_at_cutoff = True
                        break

                    await asyncio.sleep(0.05)
                
                # ✅ if we truncated at cutoff, report it
                if stop_after_this_page:
                    out.stopped_at_cutoff = True

            finally:
                await context.close()
                await browser.close()

    return out

async def ingest_oregon(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "oregon", "counts": {}}

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            "Oregon — Newsroom (Featured Feed)",
            f"{OR_JURISDICTION}:press_releases",
            OR_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "Oregon — Executive Orders",
            f"{OR_JURISDICTION}:executive_orders",
            OR_PUBLIC_PAGES["executive_orders"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing    = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0

    press_backfill = (press_existing == 0)
    eo_backfill    = (eo_existing == 0)

    press = await _ingest_oregon_press(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_oregon_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )

    # Terminal prints (like SC/NC)
    print(
        f"OR PRESS mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.upserted if press_backfill else press.new_urls} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"OR EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing}"
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "new_urls": press.new_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": press.mode,
            "seen_total": press_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": eos.mode,
            "seen_total": eo_existing,
        },
    }
    return out

# ----------------------------
# Nevada config (Lombardo)
# ----------------------------

NV_JURISDICTION = "nevada"
NV_AGENCY = "Nevada Governor (Joe Lombardo)"

NV_PUBLIC_PAGES = {
    "press_releases": "https://gov.nv.gov/Newsroom/PRs/news-releases/",
    "executive_orders": "https://gov.nv.gov/Newsroom/ExecOrders/Executive-Orders/",
    "proclamations": "https://gov.nv.gov/Newsroom/Proclamations/Proclamations/",
}

NV_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

# Cutoffs (include this item, then stop)
NV_PRESS_CUTOFF_URL = "https://gov.nv.gov/Newsroom/PRs/2025/2025-1-3_2025-state-address/"
NV_EO_CUTOFF_URL = "https://gov.nv.gov/Newsroom/ExecOrders/2024/executive-order-2024-001/"
NV_PROC_CUTOFF_URL = "https://gov.nv.gov/Newsroom/Proclamations/2024/Jan/A_Day_in_Honor_of_Greg_Dykes/"


# ----------------------------
# Nevada helpers
# ----------------------------

_NV_PRESS_DETAIL_RE = re.compile(
    r"https?://gov\.nv\.gov/Newsroom/PRs/\d{4}/[^\"'\s<>]+/?",
    re.I,
)

_NV_EO_DETAIL_RE = re.compile(
    r"https?://gov\.nv\.gov/Newsroom/ExecOrders/\d{4}/executive-order-\d{4}-\d{3}/?",
    re.I,
)

_NV_PROC_MONTH_RE = re.compile(
    r"https?://gov\.nv\.gov/Newsroom/Proclamations/(?P<y>\d{4})/(?P<m>[A-Za-z]+)_(?P=y)/?",
    re.I,
)

_NV_PROC_ITEM_RE = re.compile(
    r"https?://gov\.nv\.gov/Newsroom/Proclamations/\d{4}/[A-Za-z]{3,}/[^\"'\s<>]+/?",
    re.I,
)

# Month listing rows look like:
# <li> 1/27/2026 <a href="/Newsroom/Proclamations/2026/jan/International_Holocaust_Remembrance_Day_In_Nevada/">...</a>
_NV_PROC_LISTING_ROW_RE = re.compile(
    r'(?is)<li[^>]*>.*?(?P<mdy>\d{1,2}/\d{1,2}/\d{4}).*?<a[^>]+href=["\'](?P<href>[^"\']+)["\']'
)

def _nv_parse_proc_listing_dates(month_html: str, month_url: str) -> dict[str, datetime]:
    """
    Returns: {normalized_detail_url: published_at_dt_utc}
    """
    out: dict[str, datetime] = {}
    for m in _NV_PROC_LISTING_ROW_RE.finditer(month_html or ""):
        mdy = (m.group("mdy") or "").strip()
        href = (m.group("href") or "").strip()
        if not (mdy and href):
            continue

        try:
            dt = datetime.strptime(mdy, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        abs_url = _norm_url(urljoin(month_url, href).rstrip("/") + "/")
        out[abs_url] = dt

    return out


# press release detail pages show "December 31, 2025" (and location line above it).
_NV_US_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,)?\s+\d{4}\b",
    re.I,
)

_NV_PDF_RE = re.compile(r'(?i)href=["\']([^"\']+\.pdf)["\']')

def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # httpx.URL will percent-encode unicode safely
    try:
        return str(httpx.URL(u))
    except Exception:
        return u

def _nv_find_pdf_url(html: str, base_url: str, *, kind: str) -> Optional[str]:
    """
    Pick the most likely attachment PDF from a NV detail page.
    Avoid nav/footer PDFs AND ADA remediation/policy PDFs.
    kind: "proc" | "eo"
    """
    if not html:
        return None

    # collect ALL pdf hrefs
    hrefs = re.findall(r'(?is)href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', html)
    if not hrefs:
        return None

    abs_urls = [_norm_url(urljoin(base_url, h.strip())) for h in hrefs]
    abs_urls = [u for u in abs_urls if u]

    # ✅ hard block known junk patterns (THIS is your missing part)
    blocked = (
        "adahelp.nv.gov/remediation",      # ADA wrapper pages (not real PDFs)
        "ada_websiteguidelines",           # ADA guidelines PDF
        "/partners/policies/",             # policy docs area (often not the proc/eo)
        "/content/Home/",                  # nav/footer junk
        "3Year%20Plan",
        "3%20Year%20Plan",
        "Presentation.pdf",
    )

    def is_blocked(u: str) -> bool:
        ul = (u or "").lower()
        return any(b.lower() in ul for b in blocked)

    abs_urls = [u for u in abs_urls if not is_blocked(u)]
    if not abs_urls:
        return None

    def score(u: str) -> int:
        ul = u.lower()
        s = 0

        # ✅ aggressively downrank ADA/policy even if it slips through
        if "adahelp.nv.gov/remediation" in ul:
            return -10_000
        if "ada_websiteguidelines" in ul or "/partners/policies/" in ul:
            return -500

        # allowlist by section
        if kind == "proc":
            if "/proclamations/" in ul or "proclamation" in ul:
                s += 10

        if kind == "eo":
            if "/execorders/" in ul or "execorder" in ul or "executive-order" in ul:
                s += 10

        # prefer PDFs living under uploadedFiles (typical NV attachments)
        if "/uploadedfiles/" in ul:
            s += 4

        # prefer pdfs whose filename shares slug-ish tokens from base_url
        slug_bits = [x for x in re.split(r"[^a-z0-9]+", base_url.lower()) if len(x) >= 6]
        for b in slug_bits[:6]:
            if b in ul:
                s += 1

        return s

    # ✅ If this is a proclamation, REQUIRE it to look like a real attachment.
    # This prevents random site PDFs from being selected.
    if kind == "proc":
        def looks_like_proc_attachment(u: str) -> bool:
            ul = u.lower()
            return (
                ("/uploadedfiles/" in ul) or
                ("/proclamations/" in ul) or
                ("proclamation" in ul)
            )

        abs_urls = [u for u in abs_urls if looks_like_proc_attachment(u)]
        if not abs_urls:
            return None

    abs_urls.sort(key=score, reverse=True)
    best = abs_urls[0]
    return best if score(best) > 0 else None


def _parse_nv_us_date_from_html(html: str) -> Optional[datetime]:
    if not html:
        return None
    compact = re.sub(r"\s+", " ", html)
    m = _NV_US_DATE_RE.search(compact)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(0), "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# EO signing line:
# "..., this 17th day of October, in the year two thousand twenty-five."
_NV_EO_WITNESS_RE = re.compile(
    r"\bthis\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+"
    r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s*,?\s+in\s+the\s+year\s+(?P<year_words>[a-z \-]+)\b",
    re.I,
)

# NV proclamation signing line variants include:
# - "... this the 22nd day of January 2024"
# - "... this 24th of April, 2024"
# - "... this 13st day of September, in the year two thousand twenty-four."
# - "... this 3rd day of Novermber, 2025"
_NV_PROC_WITNESS_RE = re.compile(
    r"\bIN\s+WITNESS\s+WHEREOF\b.*?\bthis\s+"
    r"(?:the\s+)?"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*"
    r"(?:"                       # optional connectors
    r"day\s+of\s+|"               # "day of"
    r"of\s+|"                     # "of"
    r")?"
    r"\s*(?P<month>[A-Za-z]+)\s*"
    r"(?:,?\s*"
    r"(?P<year>\d{4})"            # numeric year
    r"|,?\s*in\s+the\s+year\s+(?P<year_words>[a-z \-]+)"  # year words
    r")\b",
    re.I | re.S,
)

_NV_MONTH_FIX = {
    # common typos we’ve seen / expect
    "novermber": "november",
    "novemebr": "november",
    "novmber": "november",
    "septmber": "september",
    "febuary": "february",
}

def _nv_month_token_to_num(tok: str) -> Optional[int]:
    if not tok:
        return None
    t = re.sub(r"[^a-z]", "", tok.lower()).strip()
    if not t:
        return None
    t = _NV_MONTH_FIX.get(t, t)

    # exact full month
    if t in _MONTHS:
        return _MONTHS[t]

    # fallback: first 3 letters (rare, but safe)
    if len(t) >= 3:
        t3 = t[:3]
        abbr = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        if t3 in abbr:
            return abbr[t3]

    return None


def _parse_nv_proc_signed_date_from_text(text: str) -> Optional[datetime]:
    """
    Nevada Proclamations: ONLY accept the date from the "IN WITNESS WHEREOF" signing line.
    Supports:
      - this the 22nd day of January 2024
      - this 24th of April, 2024
      - this 13st day of September, in the year two thousand twenty-four.
      - month typos like Novermber
    """
    if not text:
        return None

    full = re.sub(r"\s+", " ", text).strip()
    tail = full[-30000:] if len(full) > 30000 else full

    matches = list(_NV_PROC_WITNESS_RE.finditer(tail))
    if not matches:
        return None

    m = matches[-1]  # tail-safe: choose last

    # day (accept bad suffix like 13st)
    try:
        day = int(m.group("day"))
    except Exception:
        return None
    if not (1 <= day <= 31):
        return None

    # month (fix typos)
    month_num = _nv_month_token_to_num(m.group("month") or "")
    if not month_num:
        return None

    # year (digits or words)
    year = None
    y = (m.group("year") or "").strip()
    yw = (m.group("year_words") or "").strip()

    if y:
        try:
            year = int(y)
        except Exception:
            year = None
    elif yw:
        year = _nv_year_words_to_int(yw)

    if not year:
        return None

    # sanity guard (prevents weird ancient years)
    if year < 1990 or year > 2100:
        return None

    return datetime(year, month_num, day, tzinfo=timezone.utc)

_NV_BAD_TITLES = {
    "joe lombardo, governor of nevada",
    "governor of nevada",
}

def _nv_pick_title_from_html(html: str, fallback: str) -> str:
    if not html:
        return (fallback or "")[:500]

    def clean(s: str) -> str:
        s = html_lib.unescape(s or "")
        s = re.sub(r"\s+", " ", s).strip()
        # strip common suffixes
        for sep in (" | ", " – ", " - "):
            if sep in s:
                s = s.split(sep)[0].strip()
                break
        return s

    def bad(s: str) -> bool:
        t = (s or "").strip().lower()
        return (not t) or (t in _NV_BAD_TITLES)

    # og:title
    m = re.search(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html)
    if m:
        t = clean(m.group(1))
        if not bad(t):
            return t[:500]

    # twitter:title
    m = re.search(r'(?is)<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']', html)
    if m:
        t = clean(m.group(1))
        if not bad(t):
            return t[:500]

    # h1
    m = re.search(r"(?is)<h1[^>]*>\s*(.*?)\s*</h1>", html)
    if m:
        t = re.sub(r"(?is)<.*?>", " ", m.group(1))
        t = clean(t)
        if not bad(t):
            return t[:500]

    # <title>
    m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m:
        t = clean(m.group(1))
        if not bad(t):
            return t[:500]

    return (fallback or "")[:500]



def _nv_year_words_to_int(s: str) -> Optional[int]:
    """
    Handles Nevada's EO year words like:
      "two thousand twenty-four"
      "two thousand twenty-five"
    """
    if not s:
        return None
    t = re.sub(r"[^a-z\- ]+", " ", s.lower()).strip()
    t = re.sub(r"\s+", " ", t)
    if "two thousand" not in t:
        return None

    # after "two thousand" we expect "twenty-four", "twenty five", etc
    tail = t.split("two thousand", 1)[1].strip()
    tail = tail.replace("-", " ")
    tail = re.sub(r"\s+", " ", tail).strip()

    # Map only what we need (expand if they ever use other year words)
    year_map = {
        "twenty four": 2024,
        "twenty five": 2025,
        "twenty six": 2026,
        "twenty seven": 2027,
        "twenty eight": 2028,
        "twenty nine": 2029,
        "thirty": 2030,
        "thirty one": 2031,
        "thirty two": 2032,
        "thirty three": 2033,
        "thirty four": 2034,
        "thirty five": 2035,
    }
    if tail in year_map:
        return year_map[tail]

    # fallback: if they use digits somewhere (rare)
    m = re.search(r"\b(20\d{2})\b", t)
    if m:
        return int(m.group(1))
    return None


def _parse_nv_eo_signed_date_from_text(text: str) -> Optional[datetime]:
    """
    EO pages are HTML, not PDFs. We parse from the signing line near the end.
    Prefer the LAST match (tail-safe).
    """
    if not text:
        return None

    full = re.sub(r"\s+", " ", text).strip()
    tail = full[-25000:] if len(full) > 25000 else full

    matches = list(_NV_EO_WITNESS_RE.finditer(tail))
    if not matches:
        # Sometimes (rare) EOs might show numeric year without "in the year ..." —
        # try the generic proclamation tail parser you already have:
        return _parse_proc_published_date_from_text(full)

    m = matches[-1]
    day = int(m.group("day"))
    month = _MONTHS[m.group("month").lower()]
    year = _nv_year_words_to_int(m.group("year_words") or "")
    if year and 1 <= day <= 31:
        return datetime(year, month, day, tzinfo=timezone.utc)
    return None


def _nv_next_page_url(html: str, base_url: str) -> Optional[str]:
    """
    Best-effort pagination for the press releases listing.
    Looks for rel="next" or anchor text containing Next/Older.
    """
    if not html:
        return None

    # rel=next
    m = re.search(r'(?is)<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
    if m:
        return urljoin(base_url, m.group(1).strip())

    # anchor with Next/Older
    m2 = re.search(
        r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*(?:Next|Older|›|»)\s*</a>',
        html,
    )
    if m2:
        return urljoin(base_url, m2.group(1).strip())

    return None


def _nv_month_name_to_num(s: str) -> int:
    m = (s or "").strip().lower()
    return _MONTHS.get(m, 0)


def _nv_sort_proc_month_urls(urls: List[str]) -> List[str]:
    """
    Sort month index URLs newest -> oldest based on /YYYY/<Month>_YYYY/.
    """
    items: List[Tuple[int, int, str]] = []
    for u in urls:
        mm = _NV_PROC_MONTH_RE.match(u or "")
        if not mm:
            continue
        y = int(mm.group("y"))
        mon = _nv_month_name_to_num(mm.group("m"))
        if mon:
            items.append((y, mon, u))
    items.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [u for _, _, u in items]


# ----------------------------
# Nevada ingesters
# ----------------------------
@dataclass
class NVSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"

async def _fetch_detail_html_title_text(client: httpx.AsyncClient, url: str, *, referer: str) -> tuple[str, str, str]:
    """
    Returns (html, title, text).
    Never raises; returns empty strings on failure.
    """
    try:
        headers = {**BROWSER_UA_HEADERS, "referer": referer}
        r = await client.get(url, headers=headers, timeout=httpx.Timeout(60.0, read=60.0))

        # ✅ (4) bail on HTTP errors (prevents ingesting "Bad Request" pages)
        if r.status_code >= 400:
            return ("", "", "")

        html = r.text or ""

        title = ""
        m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
        if m:
            title = re.sub(r"\s+", " ", m.group(1)).strip()

        text = _strip_html_to_text(html) or ""
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > 35000:
            text = text[:35000]

        return (html, title, text)
    except Exception:
        return ("", "", "")

async def _ingest_nevada_press_releases(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> NVSectionResult:
    out = NVSectionResult()
    out = NVSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    referer = NV_PUBLIC_PAGES["press_releases"]
    cutoff_url = _norm_url(NV_PRESS_CUTOFF_URL).rstrip("/") + "/"

    source_name = "Nevada — Press Releases"
    source_key = f"{NV_JURISDICTION}:press_releases"
    status = NV_STATUS_MAP["press_releases"]

    seen: set[str] = set()
    stop = False
    next_url: Optional[str] = referer

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=BROWSER_UA_HEADERS.get("user-agent"))
        page = await context.new_page()

        try:
            for _page_idx in range(max_pages_each):
                if stop or out.upserted >= limit_each or not next_url:
                    break

                resp = await page.goto(next_url, wait_until="domcontentloaded", timeout=60_000, referer=referer)
                if resp and resp.status >= 400:
                    break

                await page.wait_for_timeout(500)
                html = await page.content()

                hrefs = _collect_abs_hrefs(html, next_url)

                urls = []
                for u in hrefs:
                    u2 = (u or "").strip()
                    if not u2:
                        continue
                    if "/Newsroom/PRs/" in u2 and re.search(r"/Newsroom/PRs/\d{4}/", u2):
                        urls.append(_norm_url(u2.rstrip("/") + "/"))

                # de-dupe preserve order
                dedup = []
                seen2 = set()
                for u in urls:
                    u = (u or "").strip()
                    if not u:
                        continue
                    u = u.rstrip("/") + "/"
                    if u in seen2:
                        continue
                    seen2.add(u)
                    dedup.append(u)
                urls = dedup

                if not urls:
                    break

                # ✅ listing-level cutoff: newest..cutoff (inclusive)
                stop_after_this_page = False
                cutoff_norm = cutoff_url.rstrip("/")
                urls_norm = [u.rstrip("/") for u in urls]
                if cutoff_norm in urls_norm:
                    idx = urls_norm.index(cutoff_norm)
                    urls = urls[: idx + 1]
                    stop_after_this_page = True


                out.fetched_urls += len(urls)

                urls_to_process = urls
                async with connection() as conn:
                    if not backfill:
                        urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                        out.new_urls += len(urls_to_process)
                        if not urls_to_process:
                            if stop_after_this_page:
                                out.stopped_at_cutoff = True
                            # only stop fast if the newest page has nothing new
                            if _page_idx == 0:
                                break

                for detail_url in urls_to_process:
                    if stop or out.upserted >= limit_each:
                        break
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    html2, title_raw, text = await _pw_fetch_detail_html_title_text(page, detail_url, referer=referer)
                    if not html2 and not text:
                        continue

                    title = (title_raw or _title_from_url_fallback(detail_url)).strip()
                    pub_dt = _parse_nv_us_date_from_html(html2)

                    summary = ""
                    if text:
                        summary = summarize_text(text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, detail_url)

                    await _upsert_item(
                        url=detail_url,
                        title=title,
                        summary=summary,
                        jurisdiction=NV_JURISDICTION,
                        agency=NV_AGENCY,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=pub_dt,
                    )
                    out.upserted += 1

                    if detail_url == cutoff_url:
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    await asyncio.sleep(0.05)
                
                if stop_after_this_page:
                    out.stopped_at_cutoff = True
                    break

                next_url = _nv_next_page_url(html, next_url)
                await asyncio.sleep(0.15)

        finally:
            await context.close()
            await browser.close()

    return out

async def _ingest_nevada_executive_orders(*, source_id: int, backfill: bool, limit_each: int) -> NVSectionResult:
    out = NVSectionResult()
    out = NVSectionResult(mode="backfill" if backfill else "cron_safe")
    if not backfill:
        limit_each = max(25, min(int(limit_each or 500), 1500))

    referer = NV_PUBLIC_PAGES["executive_orders"]
    cutoff_url = _norm_url(NV_EO_CUTOFF_URL).rstrip("/") + "/"

    source_name = "Nevada — Executive Orders"
    source_key = f"{NV_JURISDICTION}:executive_orders"
    status = NV_STATUS_MAP["executive_orders"]

    seen: set[str] = set()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
        r.raise_for_status()
        html_list = r.text or ""

        hrefs = _collect_abs_hrefs(html_list, referer)
        urls = []
        for u in hrefs:
            u2 = (u or "").strip()
            if not u2:
                continue
            if "/Newsroom/ExecOrders/" in u2 and re.search(r"/Newsroom/ExecOrders/\d{4}/", u2) and "executive-order-" in u2.lower():
                urls.append(_norm_url(u2.rstrip("/") + "/"))

        # de-dupe preserve order
        dedup = []
        seen2 = set()
        for u in urls:
            u = (u or "").strip()
            if not u:
                continue
            u = u.rstrip("/") + "/"
            if u in seen2:
                continue
            seen2.add(u)
            dedup.append(u)
        urls = dedup

        if not urls:
            return out
        
        # ✅ listing-level cutoff: newest..cutoff (inclusive)
        stop_after_this_page = False
        cutoff_norm = cutoff_url.rstrip("/")
        urls_norm = [u.rstrip("/") for u in urls]
        if cutoff_norm in urls_norm:
            idx = urls_norm.index(cutoff_norm)
            urls = urls[: idx + 1]
            stop_after_this_page = True

        out.fetched_urls = len(urls)

        urls_to_process = urls
        async with connection() as conn:
            if not backfill:
                urls_to_process = await _filter_new_external_ids(conn, source_id, urls)
                out.new_urls += len(urls_to_process)
                if not urls_to_process:
                    if stop_after_this_page:
                        out.stopped_at_cutoff = True
                    return out


        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=BROWSER_UA_HEADERS.get("user-agent"))
            page = await context.new_page()

            try:
                for detail_url in urls_to_process:
                    if out.upserted >= limit_each:
                        break

                    detail_url = _norm_url(detail_url.rstrip("/") + "/")
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    html, title_raw, text = await _pw_fetch_detail_html_title_text(page, detail_url, referer=referer)
                    if not html and not text:
                        continue

                    title = (title_raw or _title_from_url_fallback(detail_url)).strip()
                    pub_dt = _parse_nv_eo_signed_date_from_text(text)

                    # if EO has a PDF, prefer parsing date from PDF text too
                    pdf_url = _nv_find_pdf_url(html, detail_url, kind="eo")
                    if pdf_url:
                        try:
                            pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, pdf_url, referer=referer)
                            if pdf_text and len(pdf_text.strip()) > 200:
                                text = pdf_text
                                pub_dt = _parse_nv_eo_signed_date_from_text(text) or meta_dt or pub_dt
                            else:
                                # ✅ don't pass a dead/irrelevant PDF into AI polish
                                pdf_url = None
                        except Exception:
                            pdf_url = None

                    summary = ""
                    if text:
                        summary = summarize_text(text, max_sentences=2, max_chars=700) or ""
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, (pdf_url or detail_url))

                    await _upsert_item(
                        url=detail_url,
                        title=title,
                        summary=summary,
                        jurisdiction=NV_JURISDICTION,
                        agency=NV_AGENCY,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=pub_dt,
                    )
                    out.upserted += 1

                    if detail_url == cutoff_url:
                        out.stopped_at_cutoff = True
                        break

                    await asyncio.sleep(0.05)
            
                if stop_after_this_page:
                    out.stopped_at_cutoff = True

            finally:
                await context.close()
                await browser.close()

    return out

async def _ingest_nevada_proclamations(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> NVSectionResult:
    out = NVSectionResult()
    out = NVSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 24))  # months
        limit_each = max(25, min(int(limit_each or 500), 1500))

    referer = NV_PUBLIC_PAGES["proclamations"]
    cutoff_url = _norm_url(NV_PROC_CUTOFF_URL).rstrip("/") + "/"

    source_name = "Nevada — Proclamations"
    source_key = f"{NV_JURISDICTION}:proclamations"
    status = NV_STATUS_MAP["proclamations"]

    seen: set[str] = set()
    stop = False

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 1) fetch the year/month hub and collect month index URLs (2025 + 2024)
        r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
        r.raise_for_status()
        hub_html = r.text or ""

        hub_hrefs = _collect_abs_hrefs(hub_html, referer)

        # 1) collect month urls directly visible on hub
        month_urls: set[str] = set()
        for u in hub_hrefs:
            u2 = _norm_url((u or "").rstrip("/") + "/")
            mm = _NV_PROC_MONTH_RE.match(u2)
            if mm:
                y = int(mm.group("y"))
                if y >= 2024:
                    month_urls.add(u2)

        # 2) collect year index urls (…/Proclamations/2026/) from hub
        year_urls: set[str] = set()
        for u in hub_hrefs:
            u2 = _norm_url((u or "").rstrip("/") + "/")
            m = re.search(r"/Newsroom/Proclamations/(\d{4})/?$", u2.rstrip("/") + "/")
            if m and int(m.group(1)) >= 2024:
                year_urls.add(u2)

        # 3) ALWAYS include current year index (future-proof even if hub isn’t updated)
        now_utc = datetime.now(timezone.utc)
        year_urls.add(f"https://gov.nv.gov/Newsroom/Proclamations/{now_utc.year}/")

        # 4) fetch each year index page and extract month urls
        for yurl in sorted(year_urls, reverse=True):
            try:
                ry = await client.get(yurl, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
                if ry.status_code >= 400:
                    continue
                yh = ry.text or ""
                yh_hrefs = _collect_abs_hrefs(yh, yurl)
                for u in yh_hrefs:
                    u2 = _norm_url((u or "").rstrip("/") + "/")
                    mm = _NV_PROC_MONTH_RE.match(u2)
                    if mm:
                        y = int(mm.group("y"))
                        if y >= 2024:
                            month_urls.add(u2)
            except Exception:
                pass

        # finalize ordered list newest -> oldest
        month_urls = _nv_sort_proc_month_urls(list(month_urls))

        # ✅ HARD SEED: generate recent month index URLs (last 36 months) and probe them
        def _nv_month_index_url(year: int, month: int) -> str:
            name = [
                "", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ][month]
            return f"https://gov.nv.gov/Newsroom/Proclamations/{year}/{name}_{year}/"

        seed_months: list[str] = []
        now = datetime.now(timezone.utc)
        y = now.year
        m = now.month

        for _ in range(36):
            seed_months.append(_nv_month_index_url(y, m))
            m -= 1
            if m == 0:
                m = 12
                y -= 1

        for mu in seed_months:
            if mu in month_urls:
                continue
            try:
                rr = await client.get(mu, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(20.0, read=20.0))
                if rr.status_code < 400:
                    month_urls.append(mu)
            except Exception:
                pass

        month_urls = _nv_sort_proc_month_urls(list(set(month_urls)))

        if max_pages_each and len(month_urls) > max_pages_each:
            month_urls = month_urls[:max_pages_each]

        # ✅ 2) Create Playwright ONCE for all detail item fetches
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=BROWSER_UA_HEADERS.get("user-agent")
            )
            page = await context.new_page()

            try:
                # 3) walk month pages newest -> oldest; inside each, ingest item links
                for month_url in month_urls:
                    if stop or out.upserted >= limit_each:
                        break

                    rm = await client.get(month_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
                    if rm.status_code >= 400:
                        continue
                    mh = rm.text or ""

                    # ✅ Primary: dates from the month listing page (li rows)
                    listing_dates = _nv_parse_proc_listing_dates(mh, month_url)

                    item_hrefs = _collect_abs_hrefs(mh, month_url)
                    item_urls = []
                    for u in item_hrefs:
                        u2 = (u or "").strip()
                        if not u2:
                            continue
                        u2 = _norm_url(u2.rstrip("/") + "/")
                        if u2.rstrip("/") == month_url.rstrip("/"):
                            continue
                        if _NV_PROC_ITEM_RE.match(u2):
                            # keep years >= 2024 (future-proof)
                            ym = re.search(r"/Proclamations/(\d{4})/", u2)
                            if ym and int(ym.group(1)) >= 2024:
                                item_urls.append(u2)

                    seen3 = set()
                    final_items: List[str] = []
                    for u in item_urls:
                        if u in seen3:
                            continue
                        seen3.add(u)
                        final_items.append(u)
                    item_urls = final_items

                    # ✅ listing-level cutoff (within month page): newest..cutoff inclusive
                    stop_after_this_month = False
                    cutoff_norm = cutoff_url.rstrip("/")
                    items_norm = [u.rstrip("/") for u in item_urls]
                    if cutoff_norm in items_norm:
                        idx = items_norm.index(cutoff_norm)
                        item_urls = item_urls[: idx + 1]
                        stop_after_this_month = True

                    out.fetched_urls += len(item_urls)

                    item_urls_to_process = item_urls
                    async with connection() as conn:
                        if not backfill:
                            item_urls_to_process = await _filter_new_external_ids(conn, source_id, item_urls)
                            out.new_urls += len(item_urls_to_process)

                            # cron-safe: if newest month has nothing new, stop scanning older months
                            if not item_urls_to_process:
                                if stop_after_this_month:
                                    out.stopped_at_cutoff = True
                                # cron-safe: only stop early if this is the newest month page (first one)
                                if month_url == month_urls[0]:
                                    break
                                else:
                                    continue


                    for detail_url in item_urls_to_process:
                        detail_url = _norm_url(detail_url.rstrip("/") + "/")
                        if stop or out.upserted >= limit_each:
                            break
                        if detail_url in seen:
                            continue
                        seen.add(detail_url)

                        # ✅ CHANGE: use Playwright instead of httpx for detail pages
                        html, title_raw, text = await _pw_fetch_detail_html_title_text(
                            page, detail_url, referer=referer
                        )

                        if not html and not text:
                            continue

                        fallback_title = (title_raw or _title_from_url_fallback(detail_url)).strip()
                        title = _nv_pick_title_from_html(html, fallback_title)

                        # ✅ 1) PRIMARY: listing page date (most reliable)
                        pub_dt = listing_dates.get(detail_url)

                        # ✅ 2) SECONDARY: "IN WITNESS WHEREOF" signing line on the detail page text
                        if not pub_dt:
                            pub_dt = _parse_nv_proc_signed_date_from_text(text)

                        # ✅ 3) LAST RESORT: generic fallback
                        if not pub_dt:
                            pub_dt = _parse_proc_published_date_from_text(text)

                        summary = ""
                        text_for_summary = text or ""

                        pdf_url = _nv_find_pdf_url(html, detail_url, kind="proc")
                        if pdf_url:
                            try:
                                pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, pdf_url, referer=referer)
                                if pdf_text and len(pdf_text.strip()) > 200:
                                    text_for_summary = pdf_text

                                    # ✅ only override if listing date wasn't available
                                    if not pub_dt:
                                        pub_dt = (
                                            _parse_nv_proc_signed_date_from_text(pdf_text)
                                            or _parse_proc_published_date_from_text(pdf_text)
                                            or meta_dt
                                            or pub_dt
                                        )
                                    else:
                                        pub_dt = pub_dt or meta_dt
                                else:
                                    # ✅ don't pass a dead/irrelevant PDF into AI polish
                                    pub_dt = pub_dt or meta_dt
                                    pdf_url = None
                            except Exception:
                                pdf_url = None


                        if text_for_summary:
                            summary = summarize_text(text_for_summary, max_sentences=2, max_chars=700) or ""
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, (pdf_url or detail_url))

                        await _upsert_item(
                            url=detail_url,
                            title=title,
                            summary=summary,
                            jurisdiction=NV_JURISDICTION,
                            agency=NV_AGENCY,
                            status=status,
                            source_name=source_name,
                            source_key=source_key,
                            referer=referer,
                            published_at=pub_dt,
                        )
                        out.upserted += 1

                        if detail_url == cutoff_url:
                            out.stopped_at_cutoff = True
                            stop = True
                            break

                        await asyncio.sleep(0.05)

                    if stop_after_this_month:
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    await asyncio.sleep(0.15)

            finally:
                await context.close()
                await browser.close()

    return out

async def ingest_nevada(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "nevada", "counts": {}}

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            "Nevada — Press Releases",
            f"{NV_JURISDICTION}:press_releases",
            NV_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "Nevada — Executive Orders",
            f"{NV_JURISDICTION}:executive_orders",
            NV_PUBLIC_PAGES["executive_orders"],
        )
        src_proc = await get_or_create_source(
            conn,
            "Nevada — Proclamations",
            f"{NV_JURISDICTION}:proclamations",
            NV_PUBLIC_PAGES["proclamations"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing    = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0
        proc_existing  = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0

    press_backfill = (press_existing == 0)
    eo_backfill    = (eo_existing == 0)
    proc_backfill  = (proc_existing == 0)

    press = await _ingest_nevada_press_releases(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_nevada_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )
    procs = await _ingest_nevada_proclamations(
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )

    # Terminal prints (like SC/NC/OR)
    print(
        f"NV PRESS mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.upserted if press_backfill else press.new_urls} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"NV EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing}"
    )
    print(
        f"NV PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
        f"new={procs.upserted if proc_backfill else procs.new_urls} "
        f"fetched={procs.fetched_urls} seen_total={proc_existing}"
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "new_urls": press.new_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": press.mode,
            "seen_total": press_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": eos.mode,
            "seen_total": eo_existing,
        },
        "proclamations": {
            "fetched_urls": procs.fetched_urls,
            "new_urls": procs.new_urls,
            "upserted": procs.upserted,
            "stopped_at_cutoff": procs.stopped_at_cutoff,
            "mode": procs.mode,
            "seen_total": proc_existing,
        },
    }
    return out

# ----------------------------
# Wisconsin config (Evers)
# ----------------------------

WI_JURISDICTION = "wisconsin"
WI_AGENCY = "Wisconsin Governor (Tony Evers)"

WI_PUBLIC_PAGES = {
    "press_releases": "https://evers.wi.gov/Pages/Newsroom/Press-Releases.aspx",
    "executive_orders": "https://evers.wi.gov/Pages/Newsroom/Executive-Orders.aspx",
    "proclamations": "https://evers.wi.gov/Pages/Newsroom/Proclamations.aspx",
}

WI_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

# Cutoffs (include this item, then stop)
WI_PRESS_CUTOFF_URL = "https://content.govdelivery.com/accounts/WIGOV/bulletins/3c8eb71"
WI_EO_CUTOFF_URL = "https://evers.wi.gov/Documents/EO/EO220-HealthcareTaskForce.pdf"
WI_PROC_CUTOFF_URL = "https://evers.wi.gov/Documents/010125_Accessible%20Proclamation_60th%20Anniversary%20of%20the%20Wisconsin%20Wild%20Rivers%20Act.pdf"


# ----------------------------
# Wisconsin helpers
# ----------------------------

_WI_US_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})\b",
    re.I,
)

_WI_US_MONTH_YEAR_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
    re.I,
)

_WI_GOVDELIVERY_DATE_RE = re.compile(
    r"\bFOR\s+IMMEDIATE\s+RELEASE\s*:\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})\b",
    re.I,
)

# "..., do hereby proclaim December 18 2025, as ..."
_WI_PROCLAIM_RE = re.compile(
    r"\bdo\s+hereby\s+proclaim\s+(?P<when>.{3,200}?)\s*(?:,)?\s+as\b",
    re.I,
)

# More tolerant: any <a href="...">...</a> then find a Month DD, YYYY nearby.
_WI_ANCHOR_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>'
)

# "January 20 2025" (no comma) OR "January 20, 2025"
_WI_MONTH_DAY_YEAR_NO_COMMA_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,)?\s+(\d{4})\b",
    re.I,
)

# "January 2026"
_WI_MONTH_YEAR_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
    re.I,
)

# "January 1 through December 31 2025"
_WI_RANGE_RE = re.compile(
    r"\b(?P<m1>January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"(?P<d1>\d{1,2})\s+through\s+"
    r"(?P<m2>January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"(?P<d2>\d{1,2})\s+(?P<y>\d{4})\b",
    re.I,
)

def _wi_parse_proclaim_when_to_date(when: str) -> Optional[datetime]:
    """
    Extract a deterministic published_at from the proclamation body line:
      'do hereby proclaim January 2026 as'
      'do hereby proclaim November 20 2025 as'
      'do hereby proclaim January 1 through December 31 2025 as'

    Strategy:
      - If it's a range, use the START date (Jan 1 in the example).
      - If Month Year, use day=1.
      - If Month Day Year, use that.
    """
    if not when:
        return None

    t = re.sub(r"\s+", " ", when).strip()
    t = t.replace("–", "through").replace("—", "through")  # just in case

    m = _WI_RANGE_RE.search(t)
    if m:
        y = int(m.group("y"))
        mo = _MONTHS[m.group("m1").lower()]
        d = int(m.group("d1"))
        return datetime(y, mo, d, tzinfo=timezone.utc)

    m2 = _WI_MONTH_DAY_YEAR_NO_COMMA_RE.search(t)
    if m2:
        mo = _MONTHS[m2.group(1).lower()]
        d = int(m2.group(2))
        y = int(m2.group(3))
        return datetime(y, mo, d, tzinfo=timezone.utc)

    m3 = _WI_MONTH_YEAR_RE.search(t)
    if m3:
        mo = _MONTHS[m3.group(1).lower()]
        y = int(m3.group(2))
        return datetime(y, mo, 1, tzinfo=timezone.utc)

    return None

def _wi_proc_published_from_pdf_text(pdf_text: str) -> Optional[datetime]:
    """
    Pull the date from the proclamation body:
      'NOW, THEREFORE, I, Tony Evers, Governor of the State of Wisconsin,
       do hereby proclaim ... as'
    """
    if not pdf_text:
        return None

    full = re.sub(r"\s+", " ", pdf_text).strip()
    # look in the first chunk; proclaim line usually appears early-ish
    head = full[:40000]

    m = _WI_PROCLAIM_RE.search(head)
    if not m:
        return None

    when = (m.group("when") or "").strip()
    return _wi_parse_proclaim_when_to_date(when)

def _wi_is_bad_proc_title(t: str) -> bool:
    x = (t or "").strip().lower()
    if not x:
        return True
    return x in {
        "accessible version",
        "accessible version.",
        "accessible proclamation",
        "accessible proclamation.",
    }

def _wi_title_from_pdf_url(pdf_url: str, fallback: str) -> str:
    """
    Many WI proclamation anchors are 'Accessible Version'.
    Use the PDF filename instead (it usually contains the proclamation subject).
    Example:
      010125_Accessible%20Proclamation_60th%20Anniversary%20of%20...pdf
      -> '60th Anniversary of the Wisconsin Wild Rivers Act'
    """
    if not pdf_url:
        return (fallback or "")[:500]

    path = urlsplit(pdf_url).path
    fname = unquote(path.split("/")[-1] or "")
    base = re.sub(r"(?i)\.pdf$", "", fname).strip()

    # common prefix patterns like "010125_" or "20250101_"
    base = re.sub(r"^\d{6,8}_", "", base)

    # remove "Accessible Proclamation" prefix chunk
    base = re.sub(r"(?i)\baccessible\s+proclamation\b[:\s_-]*", "", base).strip()
    base = re.sub(r"(?i)\baccessible\b[:\s_-]*", "", base).strip()

    base = base.replace("_", " ")
    base = re.sub(r"\s+", " ", base).strip()

    if base:
        return base[:500]

    return (fallback or pdf_url)[:500]


def _wi_parse_us_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    t = re.sub(r"\s+", " ", s).strip()
    m = _WI_US_DATE_RE.search(t)
    if not m:
        return None
    month = _MONTHS[m.group(1).lower()]
    day = int(m.group(2))
    year = int(m.group(3))
    return datetime(year, month, day, tzinfo=timezone.utc)

def _wi_normalize_abs(url: str, base: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    u = urljoin(base, u)
    return _norm_url(u)

def _wi_extract_index_items(html: str, base_url: str) -> List[Tuple[str, str, Optional[datetime]]]:
    """
    Returns [(abs_url, title_text, published_at_from_index)] in page order.

    Strategy:
    - Grab every anchor.
    - Clean title text.
    - Look in the next ~600 chars after the anchor for a US date (Month DD, YYYY).
    - This survives different markup structures (spans/divs/nbsp/etc.).
    """
    if not html:
        return []

    out: List[Tuple[str, str, Optional[datetime]]] = []
    for m in _WI_ANCHOR_RE.finditer(html):
        href = (m.group("href") or "").strip()
        title_html = (m.group("title") or "").strip()

        # clean title (strip nested tags)
        title = re.sub(r"(?is)<.*?>", " ", title_html)
        title = re.sub(r"\s+", " ", html_lib.unescape(title)).strip()

        abs_url = _wi_normalize_abs(href, base_url)
        if not abs_url:
            continue

        # Find date near the anchor (often in nearby span/div)
        tail = html[m.end(): m.end() + 800]
        tail = html_lib.unescape(tail)
        pub_dt = _wi_parse_us_date(tail)

        out.append((abs_url, title, pub_dt))

    # de-dupe by URL preserve order
    seen = set()
    final: List[Tuple[str, str, Optional[datetime]]] = []
    for u, t, d in out:
        u = u.rstrip("/")
        if u in seen:
            continue
        seen.add(u)
        final.append((u, t, d))

    return final

def _wi_press_title_from_html(html: str, fallback: str) -> str:
    if not html:
        return (fallback or "")[:500]

    # Prefer H1 if present
    m = re.search(r"(?is)<h1[^>]*>\s*(.*?)\s*</h1>", html)
    if m:
        t = re.sub(r"(?is)<.*?>", " ", m.group(1))
        t = re.sub(r"\s+", " ", html_lib.unescape(t)).strip()
        if t:
            return t[:500]

    # fallback to og:title
    m2 = re.search(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html)
    if m2:
        t = re.sub(r"\s+", " ", html_lib.unescape(m2.group(1))).strip()
        if t:
            return t[:500]

    # fallback to <title> (strip suffix)
    m3 = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
    if m3:
        t = re.sub(r"\s+", " ", html_lib.unescape(m3.group(1))).strip()
        for sep in (" | ", " – ", " - "):
            if sep in t:
                t = t.split(sep)[0].strip()
                break
        if t:
            return t[:500]

    return (fallback or "")[:500]

def _wi_press_published_from_html(html: str) -> Optional[datetime]:
    if not html:
        return None
    compact = re.sub(r"\s+", " ", html)
    m = _WI_GOVDELIVERY_DATE_RE.search(compact)
    if not m:
        return None
    month = _MONTHS[m.group(1).lower()]
    day = int(m.group(2))
    year = int(m.group(3))
    return datetime(year, month, day, tzinfo=timezone.utc)


# ----------------------------
# Wisconsin ingesters
# ----------------------------

@dataclass
class WISectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"

async def _pw_render_html(url: str, *, wait_ms: int = 1500, scrolls: int = 3) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent=BROWSER_UA_HEADERS.get("User-Agent"))
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # let client-side rendering finish
            await page.wait_for_timeout(wait_ms)

            # some pages lazy-load lists on scroll
            for _ in range(scrolls):
                await page.mouse.wheel(0, 4000)
                await page.wait_for_timeout(800)

            html = await page.content()
            await browser.close()

            if resp and resp.status >= 400:
                return ""
            return html or ""
    except Exception:
        return ""
    
async def _pw_fetch_bytes(url: str, *, referer: str, wait_ms: int = 0) -> bytes:
    """
    Fetch raw bytes using Playwright's network request context.
    This avoids page.goto() "Download is starting" for PDFs.
    """
    try:
        async with async_playwright() as p:
            req = await p.request.new_context(
                extra_http_headers={
                    "referer": referer,
                    "accept": "application/pdf,application/octet-stream,*/*",
                    "accept-language": "en-US,en;q=0.9",
                },
                user_agent=BROWSER_UA_HEADERS.get("User-Agent"),
            )

            resp = await req.get(url, timeout=60_000)
            status = resp.status
            data = await resp.body() if status < 400 else b""

            await req.dispose()

            if status >= 400:
                print("[PW] request.get failed:", url, "status=", status)
                return b""

            return data or b""
    except Exception as e:
        print("[PW] fetch bytes failed:", url, repr(e))
        return b""

async def _ingest_wi_press_releases(*, source_id: int, backfill: bool, limit_each: int) -> WISectionResult:
    out = WISectionResult(mode="backfill" if backfill else "cron_safe")
    referer = WI_PUBLIC_PAGES["press_releases"]
    cutoff_url = _norm_url(WI_PRESS_CUTOFF_URL).rstrip("/")

    source_name = "Wisconsin — Press Releases"
    source_key = f"{WI_JURISDICTION}:press_releases"
    status = WI_STATUS_MAP["press_releases"]

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _pw_render_html(referer)
        if not html:
            # fallback in case Playwright fails
            r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""

        items = _wi_extract_index_items(html, referer)

        print("[WI:press] index html length:", len(html))
        print("[WI:press] extracted items:", len(items))


        # Press list contains lots of non-govdelivery links sometimes; keep GovDelivery bulletins only
        urls = [(u, t, d) for (u, t, d) in items if "content.govdelivery.com/accounts/WIGOV/bulletins/" in u]

        print("[WI:press] filtered urls:", len(urls))
        if urls:
            print("[WI:press] first url:", urls[0][0])
        print("[WI:press] sample:", [u for (u, _, _) in urls[:3]])

        out.fetched_urls = len(urls)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        cutoff_norm = cutoff_url.rstrip("/")
        urls_norm = [u.rstrip("/") for (u, _, _) in urls]
        if cutoff_norm in urls_norm:
            idx = urls_norm.index(cutoff_norm)
            urls = urls[: idx + 1]
            stop_after_index = True

        out.fetched_urls = len(urls)

        # ✅ cron-safe: only process new URLs (so we don't re-polish old items)
        urls_to_process = urls
        async with connection() as conn:
            if not backfill:
                only_urls = [u for (u, _, _) in urls]
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                new_set = set(new_only)
                urls_to_process = [(u, t, d) for (u, t, d) in urls if u in new_set]
                out.new_urls = len(urls_to_process)

                # cron-safe: if nothing new, exit fast
                if not urls_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out


        stop = False
        for detail_url, list_title, list_dt in urls_to_process:
            if stop or out.upserted >= limit_each:
                break

            # fetch detail (GovDelivery bulletin)
            try:
                resp = await client.get(detail_url, headers={**BROWSER_UA_HEADERS, "referer": referer}, timeout=httpx.Timeout(45.0, read=45.0))
                resp.raise_for_status()
                dhtml = resp.text or ""
                body_text = _strip_html_to_text(dhtml) or ""
                if len(body_text) > 35000:
                    body_text = body_text[:35000]

                title = _wi_press_title_from_html(dhtml, list_title or _title_from_url_fallback(detail_url))
                pub_dt = _wi_press_published_from_html(dhtml) or list_dt
            except Exception:
                title = list_title or _title_from_url_fallback(detail_url)
                body_text = ""
                pub_dt = list_dt

            summary = ""
            if body_text:
                summary = summarize_text(body_text, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, detail_url)

            await _upsert_item(
                url=detail_url,
                title=title,
                summary=summary,
                jurisdiction=WI_JURISDICTION,
                agency=WI_AGENCY,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer,
                published_at=pub_dt,
            )
            out.upserted += 1

            if _norm_url(detail_url).rstrip("/") == cutoff_url:
                out.stopped_at_cutoff = True
                stop = True
                break

            await asyncio.sleep(0.05)

    return out

async def _ingest_wi_executive_orders(*, source_id: int, backfill: bool, limit_each: int) -> WISectionResult:
    out = WISectionResult(mode="backfill" if backfill else "cron_safe")
    referer = WI_PUBLIC_PAGES["executive_orders"]
    cutoff_url = _norm_url(WI_EO_CUTOFF_URL).rstrip("/")

    source_name = "Wisconsin — Executive Orders"
    source_key = f"{WI_JURISDICTION}:executive_orders"
    status = WI_STATUS_MAP["executive_orders"]

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _pw_render_html(referer)
        if not html:
            r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""

        items = _wi_extract_index_items(html, referer)

        print("[WI:eo] index html length:", len(html))
        print("[WI:eo] extracted items:", len(items))


        # Keep EO PDFs only
        rows = [(u, t, d) for (u, t, d) in items if "/Documents/EO/" in u and u.lower().endswith(".pdf")]

        print("[WI:eo] filtered urls:", len(rows))
        if rows:
            print("[WI:eo] first pdf:", rows[0][0])
        print("[WI:eo] sample:", [u for (u, _, _) in rows[:3]])

        out.fetched_urls = len(rows)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        cutoff_norm = cutoff_url.rstrip("/")
        rows_norm = [u.rstrip("/") for (u, _, _) in rows]
        if cutoff_norm in rows_norm:
            idx = rows_norm.index(cutoff_norm)
            rows = rows[: idx + 1]
            stop_after_index = True

        out.fetched_urls = len(rows)

        # ✅ cron-safe: only upsert NEW PDFs
        rows_to_process = rows
        async with connection() as conn:
            if not backfill:
                only_urls = [u for (u, _, _) in rows]
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                new_set = set(new_only)
                rows_to_process = [(u, t, d) for (u, t, d) in rows if u in new_set]
                out.new_urls = len(rows_to_process)

                if not rows_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out


        stop = False
        for pdf_url, title_from_list, list_dt in rows_to_process:
            if stop or out.upserted >= limit_each:
                break

            title = (title_from_list or _title_from_url_fallback(pdf_url))[:500]
            published_at = list_dt  # ✅ source of truth because PDFs are scanned in WI

            # summary intentionally blank (scanned PDFs)
            await _upsert_item(
                url=pdf_url,
                title=title,
                summary="",
                jurisdiction=WI_JURISDICTION,
                agency=WI_AGENCY,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if _norm_url(pdf_url).rstrip("/") == cutoff_url:
                out.stopped_at_cutoff = True
                stop = True
                break

            await asyncio.sleep(0.02)

    return out

async def _ingest_wi_proclamations(*, source_id: int, backfill: bool, limit_each: int) -> WISectionResult:
    out = WISectionResult(mode="backfill" if backfill else "cron_safe")
    referer = WI_PUBLIC_PAGES["proclamations"]
    cutoff_url = _norm_url(WI_PROC_CUTOFF_URL).rstrip("/")

    source_name = "Wisconsin — Proclamations"
    source_key = f"{WI_JURISDICTION}:proclamations"
    status = WI_STATUS_MAP["proclamations"]

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _pw_render_html(referer)
        if not html:
            r = await client.get(referer, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r.raise_for_status()
            html = r.text or ""


        # Prefer "Accessible" PDF links only
        hrefs = _collect_abs_hrefs(html, referer)

        print("[WI:proc] index html length:", len(html))
        print("[WI:proc] extracted hrefs:", len(hrefs))

        pdf_urls = [
            _norm_url(u)
            for u in hrefs
            if u.lower().endswith(".pdf")
            and "/documents/" in u.lower()
            and "accessible%20proclamation" in u.lower()
        ]


        # de-dupe preserve order
        seen = set()
        clean = []
        for u in pdf_urls:
            u = u.rstrip("/")
            if u in seen:
                continue
            seen.add(u)
            clean.append(u)
        pdf_urls = clean

        print("[WI:proc] filtered pdf_urls:", len(pdf_urls))
        if pdf_urls:
            print("[WI:proc] first pdf:", pdf_urls[0])
        print("[WI:proc] sample:", pdf_urls[:3])

        out.fetched_urls = len(pdf_urls)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        cutoff_norm = cutoff_url.rstrip("/")
        pdfs_norm = [u.rstrip("/") for u in pdf_urls]
        if cutoff_norm in pdfs_norm:
            idx = pdfs_norm.index(cutoff_norm)
            pdf_urls = pdf_urls[: idx + 1]
            stop_after_index = True

        out.fetched_urls = len(pdf_urls)

        # ✅ cron-safe: only fetch PDFs + polish NEW ones
        pdf_urls_to_process = pdf_urls
        async with connection() as conn:
            if not backfill:
                new_only = await _filter_new_external_ids(conn, source_id, pdf_urls)
                new_set = set(new_only)
                pdf_urls_to_process = [u for u in pdf_urls if u in new_set]
                out.new_urls = len(pdf_urls_to_process)

                if not pdf_urls_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out


        # Better titles from anchor text if possible
        anchor_map = _extract_anchor_map(html)

        stop = False
        for pdf_url in pdf_urls_to_process:
            if stop or out.upserted >= limit_each:
                break

            title_from_list = anchor_map.get(pdf_url)
            title = title_from_list or _title_from_url_fallback(pdf_url)
            if _wi_is_bad_proc_title(title):
                title = _wi_title_from_pdf_url(pdf_url, title)
            title = (title or pdf_url)[:500]

            try:
                pdf_text, meta_dt = await _fetch_pdf_text_and_meta(client, pdf_url, referer=referer)
            except Exception as e:
                print("[WI:proc] PDF fetch failed:", pdf_url, "|", repr(e))
                await asyncio.sleep(0.05)
                continue

            # ✅ published_at from body line first, then fallback to meta
            published_at = _wi_proc_published_from_pdf_text(pdf_text) or meta_dt

            # ✅ summary from PDF text (guard: avoid short junk)
            summary = ""
            if pdf_text and len(pdf_text.strip()) >= 200:
                summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, pdf_url)


            await _upsert_item(
                url=pdf_url,
                title=title,
                summary=summary,
                jurisdiction=WI_JURISDICTION,
                agency=WI_AGENCY,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if _norm_url(pdf_url).rstrip("/") == cutoff_url:
                out.stopped_at_cutoff = True
                stop = True
                break

            await asyncio.sleep(0.03)

    return out

async def ingest_wisconsin(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    """
    Wisconsin (Evers):
      - Press releases: single index page -> GovDelivery bulletins (HTML). published_at from "FOR IMMEDIATE RELEASE".
      - Executive orders: single index page -> PDFs (often scanned). published_at from index listing date.
      - Proclamations: single index page -> Accessible PDFs. published_at extracted from "do hereby proclaim ... , as".
    """
    # cron-safe caps (only when NOT backfill)
    # (keeps behavior aligned with other states; doesn't change fetching logic)
    limit_each = int(limit_each or 5000)

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            "Wisconsin — Press Releases",
            f"{WI_JURISDICTION}:press_releases",
            WI_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "Wisconsin — Executive Orders",
            f"{WI_JURISDICTION}:executive_orders",
            WI_PUBLIC_PAGES["executive_orders"],
        )
        src_proc = await get_or_create_source(
            conn,
            "Wisconsin — Proclamations",
            f"{WI_JURISDICTION}:proclamations",
            WI_PUBLIC_PAGES["proclamations"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing    = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0
        proc_existing  = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0

    press_backfill = (press_existing == 0)
    eo_backfill    = (eo_existing == 0)
    proc_backfill  = (proc_existing == 0)

    # If not backfill, clamp to a sane cron window (doesn't affect backfill full runs)
    if not press_backfill:
        limit_each = max(25, min(limit_each, 1500))
    if not eo_backfill:
        limit_each = max(25, min(limit_each, 1500))
    if not proc_backfill:
        limit_each = max(25, min(limit_each, 1500))

    press = await _ingest_wi_press_releases(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_each,
    )
    eos = await _ingest_wi_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )
    procs = await _ingest_wi_proclamations(
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_each,
    )

    print(
        f"WI PRESS mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.upserted if press_backfill else press.new_urls} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"WI EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing}"
    )
    print(
        f"WI PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
        f"new={procs.upserted if proc_backfill else procs.new_urls} "
        f"fetched={procs.fetched_urls} seen_total={proc_existing}"
    )

    return {
        "ok": True,
        "state": "wisconsin",
        "counts": {
            "press_releases": {
                "fetched_urls": press.fetched_urls,
                "new_urls": press.new_urls,
                "upserted": press.upserted,
                "stopped_at_cutoff": press.stopped_at_cutoff,
                "mode": press.mode,
                "seen_total": press_existing,
            },
            "executive_orders": {
                "fetched_urls": eos.fetched_urls,
                "new_urls": eos.new_urls,
                "upserted": eos.upserted,
                "stopped_at_cutoff": eos.stopped_at_cutoff,
                "mode": eos.mode,
                "seen_total": eo_existing,
            },
            "proclamations": {
                "fetched_urls": procs.fetched_urls,
                "new_urls": procs.new_urls,
                "upserted": procs.upserted,
                "stopped_at_cutoff": procs.stopped_at_cutoff,
                "mode": procs.mode,
                "seen_total": proc_existing,
            },
        },
    }

# ----------------------------
# Iowa config (Reynolds)
# ----------------------------

IA_JURISDICTION = "iowa"
IA_AGENCY_GOV = "Iowa Governor (Kim Reynolds)"
IA_AGENCY_EO = "Iowa Legislature (Executive Orders)"
IA_AGENCY_PROCS = "Iowa Homeland Security & Emergency Management"

IA_PUBLIC_PAGES = {
    # Newsroom listing (pagination uses ?page=N and an instance_overrides_key)
    "press_releases": "https://governor.iowa.gov/newsroom",
    "executive_orders": "https://www.legis.iowa.gov/publications/otherResources/executiveOrders",
    "proclamations": "https://homelandsecurity.iowa.gov/disasters/governors-disaster-proclamations",
}

IA_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

# Cutoffs (include this item, then stop)
IA_PRESS_CUTOFF_URL = "https://governor.iowa.gov/press-release/2025-01-02/gov-reynolds-extends-disaster-proclamation-highly-pathogenic-avian-influenza"
IA_EO_CUTOFF_URL = "https://www.legis.iowa.gov/docs/publications/EO/1138032.pdf"  # first EO of 2020
IA_PROC_CUTOFF_URL = "https://homelandsecurity.iowa.gov/media/160/download?inline"  # first proc of 2024

# The newsroom listing requires an instance_overrides_key param.
# We keep it as a constant (it changes rarely, but if it does, update this value).
IA_NEWSROOM_INSTANCE_KEY = "OvZtdVShpxYi3TPZjzk1sYSjY568083dsX3Fw76lcks"


# ----------------------------
# Iowa helpers
# ----------------------------

_IA_URL_DATE_RE = re.compile(r"/press-release/(?P<d>\d{4}-\d{2}-\d{2})/")
_IA_US_DATE_LINE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})\b",
    re.I,
)


def _ia_published_at_from_press_url(url: str) -> Optional[datetime]:
    """Best-effort published_at for governor.iowa.gov press releases."""
    if not url:
        return None
    m = _IA_URL_DATE_RE.search(url)
    if m:
        try:
            y, mo, d = m.group("d").split("-")
            return datetime(int(y), int(mo), int(d), tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _ia_clean_title(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return ""
    # common suffixes
    for suf in ["| Governor Kim Reynolds", "| Office of the Governor", "| State of Iowa"]:
        if suf in t:
            t = t.split(suf, 1)[0].strip()
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _ia_extract_press_urls_titles_and_blurbs(html: str, base_url: str) -> List[tuple[str, str, str]]:
    """Extract (absolute_url, title, blurb) for press releases from Iowa newsroom listing."""
    if not html:
        return []

    out: List[tuple[str, str, str]] = []
    seen: set[str] = set()

    link_re = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>/press-release/[^"\']+)["\'][^>]*>\s*(?P<title>.*?)\s*</a>'
    )

    for m in link_re.finditer(html):
        href = (m.group("href") or "").strip()
        if not href:
            continue

        abs_url = urljoin(base_url, href)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        title = _ia_strip_tags(m.group("title") or "")

        # grab the first <p> after the link as the listing blurb (fallback summary)
        tail = html[m.end() : m.end() + 1400]
        blurb = ""
        pm = re.search(r'(?is)<p[^>]*>(?P<p>.*?)</p>', tail)
        if pm:
            blurb = _ia_strip_tags(pm.group("p"))

        if len(blurb) > 500:
            blurb = blurb[:500].rsplit(" ", 1)[0].strip()

        out.append((abs_url, title, blurb))

    return out


def _ia_parse_mmddyyyy(s: str) -> Optional[datetime]:
    """Parse MM/DD/YYYY into UTC datetime."""
    if not s:
        return None
    m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", s)
    if not m:
        return None
    try:
        mo = int(m.group(1))
        d = int(m.group(2))
        y = int(m.group(3))
        return datetime(y, mo, d, tzinfo=timezone.utc)
    except Exception:
        return None


def _ia_strip_tags(s: str) -> str:
    s = (s or "")
    s = re.sub(r"(?is)<script.*?>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?>.*?</style>", " ", s)
    s = re.sub(r"(?is)<.*?>", " ", s)
    s = html_lib.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

@dataclass
class IASectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"


# ----------------------------
# Iowa ingesters
# ----------------------------

async def _ia_fetch_press_detail_text(
    client: httpx.AsyncClient,
    url: str,
    *,
    referer: str,
) -> tuple[str, str]:
    """Fetch Iowa press release page and extract usable text for summarization."""
    headers = {**BROWSER_UA_HEADERS, "referer": referer}
    r = await client.get(url, headers=headers, timeout=httpx.Timeout(45.0, read=45.0))
    r.raise_for_status()
    html = r.text or ""

    # Title: prefer <h1>, fallback to <title>
    title = ""
    m = re.search(r'(?is)<h1[^>]*>(.*?)</h1>', html)
    if m:
        title = _ia_strip_tags(m.group(1))
    if not title:
        m2 = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html)
        if m2:
            title = _ia_strip_tags(m2.group(1))

    # Body: try main/article first; else strip full page
    body_html = ""
    for pat in [
        r"(?is)<main[^>]*>(.*?)</main>",
        r"(?is)<article[^>]*>(.*?)</article>",
        r'(?is)<div[^>]+class=["\'][^"\']*(?:node__content|layout__region--content|field--name-body)[^"\']*["\'][^>]*>(.*?)</div>',
    ]:
        mm = re.search(pat, html)
        if mm:
            body_html = mm.group(1)
            break

    text = _ia_strip_tags(body_html or html)
    if len(text) > 35000:
        text = text[:35000]

    return (title or ""), (text or "")


async def _ingest_iowa_press_releases(*, source_id: int, backfill: bool, limit_each: int, max_pages_each: int) -> IASectionResult:
    out = IASectionResult(mode="backfill" if backfill else "cron_safe")

    base = IA_PUBLIC_PAGES["press_releases"]
    referer = base
    cutoff = _norm_url(IA_PRESS_CUTOFF_URL).rstrip("/")

    params_base = {"instance_overrides_key": IA_NEWSROOM_INSTANCE_KEY}

    async with httpx.AsyncClient(follow_redirects=True) as client:
        stop = False
        page = 0   # ✅ FIX: Iowa uses page=0 for latest
        while not stop and page <= max_pages_each and out.upserted < limit_each:
            params = dict(params_base)
            params["page"] = str(page)
            url = base + "?" + urlencode(params)

            try:
                r = await client.get(
                    url,
                    headers={**BROWSER_UA_HEADERS, "referer": referer},
                    timeout=httpx.Timeout(45.0, read=45.0),
                )
                r.raise_for_status()
                html = r.text or ""
            except Exception as e:
                print("[IA:press] listing fetch failed:", url, "|", repr(e))
                break

            pairs = _ia_extract_press_urls_titles_and_blurbs(html, base)
            if not pairs:
                break

            # ✅ listing-level cutoff: include cutoff, then stop scanning older
            stop_after_this_page = False
            cutoff_norm = cutoff.rstrip("/")
            pairs_norm = [ _norm_url(u).rstrip("/") for (u, _, _) in pairs ]
            if cutoff_norm in pairs_norm:
                idx = pairs_norm.index(cutoff_norm)
                pairs = pairs[: idx + 1]
                stop_after_this_page = True

            out.fetched_urls += len(pairs)

            # ✅ cron-safe: only process NEW URLs on this page
            pairs_to_process = pairs
            async with connection() as conn:
                if not backfill:
                    only_urls = [u for (u, _, _) in pairs]
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                    new_set = set(new_only)
                    pairs_to_process = [(u, t, b) for (u, t, b) in pairs if u in new_set]
                    out.new_urls += len(pairs_to_process)

                    # cron-safe: if newest page has nothing new, stop fast
                    if not pairs_to_process and page == 0:
                        if stop_after_this_page:
                            out.stopped_at_cutoff = True
                        return out


            for detail_url, list_title, blurb in pairs_to_process:
                if out.upserted >= limit_each:
                    break

                norm_detail = _norm_url(detail_url).rstrip("/")

                # ✅ better text extraction for Iowa press pages
                try:
                    page_title, text = await _ia_fetch_press_detail_text(client, detail_url, referer=referer)
                except Exception as e:
                    print("[IA:press] detail fetch failed:", detail_url, "|", repr(e))
                    await asyncio.sleep(0.05)
                    continue

                title = _ia_clean_title(list_title) or _ia_clean_title(page_title) or _title_from_url_fallback(detail_url)
                title = (title or detail_url)[:500]

                published_at = _ia_published_at_from_press_url(detail_url)
                if not published_at and text:
                    m = _IA_US_DATE_LINE_RE.search(text)
                    if m:
                        mo = _MONTHS[m.group(1).lower()]
                        d = int(m.group(2))
                        y = int(m.group(3))
                        published_at = datetime(y, mo, d, tzinfo=timezone.utc)

                summary = ""
                if text and len(text.strip()) >= 200:
                    summary = summarize_text(text, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, detail_url)

                # ✅ fallback: never store NULL if listing blurb exists
                if not summary:
                    summary = (blurb or "").strip()

                await _upsert_item(
                    url=detail_url,
                    title=title,
                    summary=summary,
                    jurisdiction=IA_JURISDICTION,
                    agency=IA_AGENCY_GOV,
                    status=IA_STATUS_MAP["press_releases"],
                    source_name="Iowa — Press Releases",
                    source_key=f"{IA_JURISDICTION}:press_releases",
                    referer=referer,
                    published_at=published_at,
                )
                out.upserted += 1

                if norm_detail == cutoff:
                    out.stopped_at_cutoff = True
                    stop = True
                    break

                await asyncio.sleep(0.03)
            
            if stop_after_this_page:
                out.stopped_at_cutoff = True
                return out

            page += 1
            await asyncio.sleep(0.12)

    return out

async def _ingest_iowa_executive_orders(*, source_id: int, backfill: bool, limit_each: int) -> IASectionResult:
    out = IASectionResult(mode="backfill" if backfill else "cron_safe")

    base = IA_PUBLIC_PAGES["executive_orders"]
    referer = base
    cutoff = _norm_url(IA_EO_CUTOFF_URL).rstrip("/")

    # Parse table rows: Order Date | Executive Order (PDF link) | Description | ...
    row_re = re.compile(
        r"(?is)<tr[^>]*>\s*<td[^>]*>(?P<odate>\d{2}/\d{2}/\d{4})</td>\s*"
        r"<td[^>]*>.*?<a[^>]+href=[\"\'](?P<href>[^\"\']+)[\"\'][^>]*>(?P<title>.*?)</a>.*?</td>\s*"
        r"<td[^>]*>(?P<desc>.*?)</td>",
    )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            r = await client.get(
                base,
                headers={**BROWSER_UA_HEADERS, "referer": referer},
                timeout=httpx.Timeout(45.0, read=45.0),
            )
            r.raise_for_status()
            html = r.text or ""
        except Exception as e:
            print("[IA:eo] listing fetch failed:", base, "|", repr(e))
            return out

        rows: list[tuple[str, str, str, Optional[datetime]]] = []  # (pdf_url, title, desc, published_at)

        for m in row_re.finditer(html):
            odate = (m.group("odate") or "").strip()
            href = (m.group("href") or "").strip()
            title = _ia_strip_tags(m.group("title") or "")
            desc = _ia_strip_tags(m.group("desc") or "")

            if not href:
                continue

            pdf_url = urljoin(base, href)

            # Some entries may link to a non-pdf page; try to resolve to a PDF if needed
            if ".pdf" not in pdf_url.lower():
                try:
                    r2 = await client.get(
                        pdf_url,
                        headers={**BROWSER_UA_HEADERS, "referer": referer},
                        timeout=httpx.Timeout(45.0, read=45.0),
                    )
                    r2.raise_for_status()
                    h2 = r2.text or ""
                    mm = re.search(r'(?is)href=["\'](?P<u>[^"\']+\.pdf)["\']', h2)
                    if mm:
                        pdf_url = urljoin(pdf_url, mm.group("u"))
                except Exception:
                    pass

            published_at = _ia_parse_mmddyyyy(odate)
            rows.append((pdf_url, title, desc, published_at))

        # newest-first assumption: table is already newest->oldest; keep order
        out.fetched_urls = len(rows)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        cutoff_norm = cutoff.rstrip("/")
        rows_norm = [ _norm_url(u).rstrip("/") for (u, _, _, _) in rows ]
        if cutoff_norm in rows_norm:
            idx = rows_norm.index(cutoff_norm)
            rows = rows[: idx + 1]
            stop_after_index = True
            out.fetched_urls = len(rows)

        rows_to_process = rows
        async with connection() as conn:
            if not backfill:
                only_urls = [u for (u, _, _, _) in rows]
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                new_set = set(new_only)
                rows_to_process = [(u, t, d, p) for (u, t, d, p) in rows if u in new_set]
                out.new_urls = len(rows_to_process)

                if not rows_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out

        stop = False
        for pdf_url, title, desc, published_at in rows_to_process:
            if stop or out.upserted >= limit_each:
                break

            # Summary from description (EO PDFs are scanned)
            summary = desc
            if summary:
                summary = summarize_text(summary, max_sentences=2, max_chars=700) or summary
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title or "Iowa Executive Order", pdf_url)

            final_title = (title or _title_from_url_fallback(pdf_url))[:500]

            await _upsert_item(
                url=pdf_url,
                title=final_title,
                summary=summary,
                jurisdiction=IA_JURISDICTION,
                agency=IA_AGENCY_EO,
                status=IA_STATUS_MAP["executive_orders"],
                source_name="Iowa — Executive Orders",
                source_key=f"{IA_JURISDICTION}:executive_orders",
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if _norm_url(pdf_url).rstrip("/") == cutoff:
                out.stopped_at_cutoff = True
                stop = True
                break

            await asyncio.sleep(0.02)

        return out

async def _ingest_iowa_proclamations(*, source_id: int, backfill: bool, limit_each: int) -> IASectionResult:
    out = IASectionResult(mode="backfill" if backfill else "cron_safe")

    base = IA_PUBLIC_PAGES["proclamations"]
    referer = base
    cutoff = _norm_url(IA_PROC_CUTOFF_URL).rstrip("/")

    # Row pattern: Date | Proc# | Incident (with PDF link) | ...
    row_re = re.compile(
        r"(?is)<tr[^>]*>\s*<td[^>]*>(?P<md>\d{1,2}/\d{1,2})</td>\s*"
        r"<td[^>]*>(?P<proc>\d{4}-\d+)</td>\s*"
        r"<td[^>]*>(?P<cell>.*?)</td>",
    )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            r = await client.get(
                base,
                headers={**BROWSER_UA_HEADERS, "referer": referer},
                timeout=httpx.Timeout(45.0, read=45.0),
            )
            r.raise_for_status()
            html = r.text or ""
        except Exception as e:
            print("[IA:proc] listing fetch failed:", base, "|", repr(e))
            return out

        rows: list[tuple[str, str, str, Optional[datetime]]] = []  # (pdf_url, title, incident_text, published_at)

        for m in row_re.finditer(html):
            md = (m.group("md") or "").strip()
            proc = (m.group("proc") or "").strip()
            cell = (m.group("cell") or "")

            mm = re.search(r'(?is)href=["\'](?P<href>/media/\d+/download\?inline)["\']', cell)
            if not mm:
                continue
            pdf_url = urljoin(base, mm.group("href"))

            t_anchor = ""
            mm2 = re.search(
                r'(?is)<a[^>]+href=["\']/media/\d+/download\?inline["\'][^>]*>(?P<t>.*?)</a>',
                cell,
            )
            if mm2:
                t_anchor = _ia_strip_tags(mm2.group("t") or "")

            incident_text = _ia_strip_tags(cell)

            year = None
            try:
                year = int(proc.split("-", 1)[0])
            except Exception:
                year = None

            published_at = None
            try:
                mo_s, d_s = md.split("/")
                if year:
                    published_at = datetime(int(year), int(mo_s), int(d_s), tzinfo=timezone.utc)
            except Exception:
                published_at = None

            title = f"{t_anchor or 'Disaster Proclamation'} ({proc})"[:500]
            rows.append((pdf_url, title, incident_text, published_at))

        out.fetched_urls = len(rows)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        cutoff_norm = cutoff.rstrip("/")
        rows_norm = [ _norm_url(u).rstrip("/") for (u, _, _, _) in rows ]
        if cutoff_norm in rows_norm:
            idx = rows_norm.index(cutoff_norm)
            rows = rows[: idx + 1]
            stop_after_index = True
            out.fetched_urls = len(rows)

        rows_to_process = rows
        async with connection() as conn:
            if not backfill:
                only_urls = [u for (u, _, _, _) in rows]
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                new_set = set(new_only)
                rows_to_process = [(u, t, it, p) for (u, t, it, p) in rows if u in new_set]
                out.new_urls = len(rows_to_process)

                if not rows_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out

        stop = False
        for pdf_url, title, incident_text, published_at in rows_to_process:
            if stop or out.upserted >= limit_each:
                break

            summary = incident_text
            if summary:
                summary = summarize_text(summary, max_sentences=2, max_chars=700) or summary
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, pdf_url)

            await _upsert_item(
                url=pdf_url,
                title=title,
                summary=summary,
                jurisdiction=IA_JURISDICTION,
                agency=IA_AGENCY_PROCS,
                status=IA_STATUS_MAP["proclamations"],
                source_name="Iowa — Disaster Proclamations",
                source_key=f"{IA_JURISDICTION}:proclamations",
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if _norm_url(pdf_url).rstrip("/") == cutoff:
                out.stopped_at_cutoff = True
                stop = True
                break

            await asyncio.sleep(0.02)

        return out

async def ingest_iowa(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    out = {"ok": True, "state": "iowa", "counts": {}}

    limit_each = int(limit_each or 5000)
    max_pages_each = int(max_pages_each or 500)

    async with connection() as conn:
        src_press = await get_or_create_source(
            conn,
            "Iowa — Press Releases",
            f"{IA_JURISDICTION}:press_releases",
            IA_PUBLIC_PAGES["press_releases"],
        )
        src_eo = await get_or_create_source(
            conn,
            "Iowa — Executive Orders",
            f"{IA_JURISDICTION}:executive_orders",
            IA_PUBLIC_PAGES["executive_orders"],
        )
        src_proc = await get_or_create_source(
            conn,
            "Iowa — Disaster Proclamations",
            f"{IA_JURISDICTION}:proclamations",
            IA_PUBLIC_PAGES["proclamations"],
        )

        press_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_press) or 0
        eo_existing    = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0
        proc_existing  = await conn.fetchval("select count(*) from items where source_id=$1", src_proc) or 0

    press_backfill = (press_existing == 0)
    eo_backfill    = (eo_existing == 0)
    proc_backfill  = (proc_existing == 0)

    # cron-safe caps (don’t affect backfill)
    if not press_backfill:
        limit_press = max(25, min(limit_each, 1500))
        pages_press = max(1, min(max_pages_each, 10))
    else:
        limit_press = limit_each
        pages_press = max_pages_each

    limit_eo = limit_each if eo_backfill else max(25, min(limit_each, 1500))
    limit_proc = limit_each if proc_backfill else max(25, min(limit_each, 1500))

    press = await _ingest_iowa_press_releases(
        source_id=src_press,
        backfill=press_backfill,
        limit_each=limit_press,
        max_pages_each=pages_press,
    )
    eos = await _ingest_iowa_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_eo,
    )
    procs = await _ingest_iowa_proclamations(
        source_id=src_proc,
        backfill=proc_backfill,
        limit_each=limit_proc,
    )

    print(
        f"IA PRESS mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press.upserted if press_backfill else press.new_urls} "
        f"fetched={press.fetched_urls} seen_total={press_existing}"
    )
    print(
        f"IA EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing}"
    )
    print(
        f"IA PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
        f"new={procs.upserted if proc_backfill else procs.new_urls} "
        f"fetched={procs.fetched_urls} seen_total={proc_existing}"
    )

    out["counts"] = {
        "press_releases": {
            "fetched_urls": press.fetched_urls,
            "new_urls": press.new_urls,
            "upserted": press.upserted,
            "stopped_at_cutoff": press.stopped_at_cutoff,
            "mode": press.mode,
            "seen_total": press_existing,
        },
        "executive_orders": {
            "fetched_urls": eos.fetched_urls,
            "new_urls": eos.new_urls,
            "upserted": eos.upserted,
            "stopped_at_cutoff": eos.stopped_at_cutoff,
            "mode": eos.mode,
            "seen_total": eo_existing,
        },
        "proclamations": {
            "fetched_urls": procs.fetched_urls,
            "new_urls": procs.new_urls,
            "upserted": procs.upserted,
            "stopped_at_cutoff": procs.stopped_at_cutoff,
            "mode": procs.mode,
            "seen_total": proc_existing,
        },
    }
    return out

# ----------------------------
# Missouri config (Kehoe)
# ----------------------------

async def _pw_fetch_html(url: str, referer: str | None = None) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=BROWSER_UA_HEADERS.get("User-Agent"),
            extra_http_headers={"Referer": referer} if referer else {},
        )
        page = await ctx.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        html = await page.content()
        await browser.close()
        return (html or "").strip()

MO_JURISDICTION = "missouri"
MO_AGENCY = "Missouri Governor (Mike Kehoe)"

MO_PUBLIC_PAGES = {
    "press_releases": "https://governor.mo.gov/press-releases",
    "executive_orders": "https://governor.mo.gov/actions/executive-orders",
    "proclamations": "https://governor.mo.gov/actions/proclamations",
}

MO_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

MO_PRESS_CUTOFF_URL = "https://governor.mo.gov/press-releases/archive/governor-elect-kehoe-announces-trish-vincent-director-missouri-department"
MO_PROC_CUTOFF_URL = "https://governor.mo.gov/proclamations/governor-kehoe-orders-flags-fly-half-staff-honor-howard-county-firefighter-larry"

MO_SOS_EO_INDEX = {
    2025: "https://www.sos.mo.gov/library/reference/orders/2025",
    2024: "https://www.sos.mo.gov/library/reference/orders/2024",
}

def _mo_sos_eo_index_years() -> list[int]:
    this_year = datetime.now(timezone.utc).year
    return list(range(this_year, 2024 - 1, -1))



def _mo_abs(base: str, href: str) -> str:
    return urljoin(base, href or "")


def _mo_parse_any_date(text_: str) -> datetime | None:
    """
    Missouri pages use both:
      - "January 7, 2026" (press releases)
      - "12/16/2025" (proclamations list)
    """
    s = (text_ or "").strip()
    if not s:
        return None

    # MM/DD/YYYY
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", s)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(yy, mm, dd, tzinfo=timezone.utc)

    # Month D, YYYY
    try:
        dt = datetime.strptime(s, "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _mo_parse_eo_signed_date_from_text(txt: str) -> datetime | None:
    t = (txt or "").replace("\u00a0", " ")
    m = re.search(
        r"\bon\s+(?:this|the)\s+(\d{1,2})(?:st|nd|rd|th)?\s+day of\s+([A-Za-z]+),\s+(\d{4})\b",
        t,
        flags=re.I,
    )
    if not m:
        return None

    day = int(m.group(1))
    month_name = m.group(2).strip().lower()
    year = int(m.group(3))

    months = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }
    mm = months.get(month_name)
    if not mm:
        return None

    return datetime(year, mm, day, tzinfo=timezone.utc)


def _mo_extract_drupal_ajax_params(first_html: str) -> tuple[str | None, str | None, str | None]:
    """
    Extract the minimum Drupal Views params needed to call /views/ajax.
    Returns: (view_dom_id, theme, libraries)
    """
    h = first_html or ""

    # view_dom_id is usually present on the views wrapper
    dom_id = None
    m = re.search(r'data-drupal-views-dom-id="([^"]+)"', h, flags=re.I)
    if m:
        dom_id = m.group(1).strip()

    # ajax_page_state is usually embedded as JSON-ish in the page
    theme = None
    libraries = None

    # theme
    m = re.search(r'"ajaxPageState"\s*:\s*\{[^}]*"theme"\s*:\s*"([^"]+)"', h, flags=re.I)
    if m:
        theme = m.group(1).strip()

    # libraries (big, but needed on some sites)
    m = re.search(r'"ajaxPageState"\s*:\s*\{.*?"libraries"\s*:\s*"([^"]+)"', h, flags=re.I | re.S)
    if m:
        libraries = m.group(1).strip()

    return dom_id, theme, libraries


def _mo_build_drupal_ajax_url(page: int, view_dom_id: str | None, theme: str | None, libraries: str | None) -> str:
    """
    Build the Drupal Views AJAX URL for Missouri press releases.
    We keep the stable bits you captured in DevTools and only vary page + view_dom_id.
    """
    # These match your DevTools capture (stable for this view)
    params = {
        "_wrapper_format": "drupal_ajax",
        "view_name": "news",
        "view_display_id": "block_3",
        "view_args": "",
        "view_path": "/node/8511",
        "view_base_path": "press-releases/archive",
        "view_dom_id": view_dom_id or "",
        "pager_element": "0",
        "page": str(page),
        "_drupal_ajax": "1",
        "ajax_page_state[theme]": theme or "governor_2018",
        "ajax_page_state[theme_token]": "",
        "ajax_page_state[libraries]": libraries or "",
    }

    return "https://governor.mo.gov/views/ajax?" + urlencode(params, doseq=True)


def _mo_extract_drupal_ajax_html(json_obj) -> str:
    if not json_obj:
        return ""
    if isinstance(json_obj, dict):
        return str(json_obj.get("data") or "")
    if isinstance(json_obj, list):
        parts = []
        for cmd in json_obj:
            if isinstance(cmd, dict):
                d = cmd.get("data")
                if isinstance(d, str) and d.strip():
                    parts.append(d)
        return "\n".join(parts)
    return ""


def _mo_parse_press_rows(html: str) -> list[tuple[str, str, datetime | None]]:
    """
    Returns [(title, url, published_at)]
    Only keeps real press release links.
    """
    out: list[tuple[str, str, datetime | None]] = []
    if not html:
        return out

    # split by views row blocks (works for both page html and ajax snippet)
    chunks = re.split(r"\bviews-row\b", html)

    for ch in chunks:
        m = re.search(r'href="([^"]+)"[^>]*>([^<]+)</a>', ch, flags=re.I)
        if not m:
            continue

        href = html_lib.unescape(m.group(1) or "").strip()
        title = html_lib.unescape(m.group(2) or "").strip()

        if not href or href.startswith("#"):
            continue

        # ✅ ONLY accept press-release paths (filters Skip-to-main-content, header links, etc.)
        if not (href.startswith("/press-releases/") or href.startswith("press-releases/")):
            continue

        url = _mo_abs(MO_PUBLIC_PAGES["press_releases"], href)

        # date like "January 7, 2026"
        dm = re.search(r"\b([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", ch)
        published_at = _mo_parse_any_date(dm.group(1)) if dm else None

        if title and url:
            out.append((title, url, published_at))

    # de-dupe by url while preserving order
    seen = set()
    dedup = []
    for t, u, d in out:
        if u in seen:
            continue
        seen.add(u)
        dedup.append((t, u, d))
    return dedup

def _mo_html_to_text_fallback(html: str) -> str:
    """Convert HTML to readable text without BeautifulSoup."""
    if not html:
        return ""

    # drop scripts/styles
    html = re.sub(r"<(script|style|noscript)\b[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)

    # preserve some structure
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    html = re.sub(r"</p\s*>", "\n", html, flags=re.I)
    html = re.sub(r"</h[1-6]\s*>", "\n", html, flags=re.I)
    html = re.sub(r"</li\s*>", "\n", html, flags=re.I)
    html = re.sub(r"</div\s*>", "\n", html, flags=re.I)

    # remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", html)

    # decode entities
    text = html_lib.unescape(text)

    # normalize whitespace
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()

def _mo_extract_press_body_text(html: str) -> str:
    if not html:
        return ""

    # Use our local fallback (does NOT rely on bs4)
    txt = _mo_html_to_text_fallback(html)
    if not txt:
        return ""

    # trim obvious boilerplate
    for marker in [
        "## Contact Us",
        "Contact Us",
        "Office of Governor Mike Kehoe",
    ]:
        idx = txt.lower().find(marker.lower())
        if idx != -1 and idx > 200:
            txt = txt[:idx].strip()

    # drop common nav junk lines
    bad_prefixes = (
        "skip to main content",
        "toggle navigation",
        "mobile navigation",
    )

    lines = []
    for ln in txt.splitlines():
        l = (ln or "").strip()
        if not l:
            continue
        low = l.lower()
        if low.startswith(bad_prefixes):
            continue
        if low in {"facebook", "twitter", "email"}:
            continue
        lines.append(l)

    out = "\n".join(lines).strip()
    return out


def _mo_parse_proc_rows(html: str) -> list[tuple[str, str, datetime | None]]:
    """
    Proclamations listing: find /proclamations/... links and extract a nearby date.
    Supports MM/DD/YYYY, Month D, YYYY, or datetime="YYYY-MM-DD".
    """
    out: list[tuple[str, str, datetime | None]] = []
    if not html:
        return out

    # Find proclamation links
    for m in re.finditer(
        r'href="([^"]*?/proclamations/[^"]+)"[^>]*>([^<]+)</a>',
        html,
        flags=re.I | re.S,
    ):
        href = html_lib.unescape(m.group(1) or "").strip()
        title = html_lib.unescape(m.group(2) or "").strip()
        url = _mo_abs(MO_PUBLIC_PAGES["proclamations"], href)

        # Take a window around the link to locate a date
        start = max(0, m.start() - 250)
        end = min(len(html), m.end() + 250)
        window = html[start:end]

        published_at = None

        # datetime="YYYY-MM-DD"
        dm = re.search(r'datetime="(\d{4}-\d{2}-\d{2})"', window, flags=re.I)
        if dm:
            try:
                published_at = datetime.fromisoformat(dm.group(1)).replace(tzinfo=timezone.utc)
            except Exception:
                published_at = None

        # MM/DD/YYYY
        if not published_at:
            dm = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", window)
            if dm:
                published_at = _mo_parse_any_date(dm.group(1))

        # Month D, YYYY
        if not published_at:
            dm = re.search(r"\b([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", window)
            if dm:
                published_at = _mo_parse_any_date(dm.group(1))

        if title and url:
            out.append((title, url, published_at))

    # de-dupe preserve order
    seen = set()
    dedup = []
    for t, u, d in out:
        if u in seen:
            continue
        seen.add(u)
        dedup.append((t, u, d))
    return dedup


def _mo_parse_sos_eo_links(html: str, year: int) -> list[str]:
    if not html:
        return []

    links = re.findall(
        rf'href=["\']([^"\']*(?:/library/reference/orders/{year}/eo\d+))["\']',
        html,
        flags=re.I,
    )

    abs_links = []
    for l in links:
        if l.startswith("http"):
            abs_links.append(l)
        else:
            abs_links.append("https://www.sos.mo.gov" + l)

    seen = set()
    out = []
    for u in abs_links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

async def _mo_fetch_text(client: httpx.AsyncClient, url: str, referer: str) -> str:
    headers = {
        **BROWSER_UA_HEADERS,
        "Referer": referer,
        # Stronger than gzip/deflate: tells servers "do NOT compress"
        "Accept-Encoding": "identity",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    r = await client.get(url, headers=headers)

    print(
        "MO FETCH",
        url,
        "status",
        r.status_code,
        "enc",
        r.headers.get("Content-Encoding"),
        "len(text)",
        len(r.text or ""),
    )

    r.raise_for_status()

    # If the server ignores Accept-Encoding and still sends Brotli (or we get junk),
    # httpx may give us empty/garbled text. Detect and fall back to Playwright.
    enc = (r.headers.get("Content-Encoding") or "").lower()
    txt = (r.text or "").strip()

    # Common failure signatures: empty text, or still compressed, or looks like a JS challenge shell.
    looks_bad = (
        not txt
        or "br" in enc
        or (len(txt) < 200 and "<html" not in txt.lower())
    )

    if looks_bad:
        try:
            pw_html = await _pw_fetch_html(url, referer=referer)
            if pw_html:
                return pw_html
        except Exception:
            pass  # fall through

    # last resort: return whatever we got
    return txt


async def _mo_fetch_json(client: httpx.AsyncClient, url: str, referer: str):
    headers = {
        **BROWSER_UA_HEADERS,
        "Referer": referer,
        # CRITICAL: avoid br (brotli)
        "Accept-Encoding": "gzip, deflate",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    r = await client.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

async def _count_items_for_source(conn, source_id: UUID) -> int:
    return int(await conn.fetchval(
        "select count(*)::int from public.items where source_id = $1",
        source_id
    ) or 0)

async def _get_or_create_source_row(*, name: str, kind: str, base_url: Optional[str]) -> Dict[str, Any]:
    """
    Your schema:
      public.sources(id uuid pk, name text unique, kind text not null, base_url text)

    So: name must be UNIQUE and kind must be NOT NULL.
    """
    q = """
    insert into public.sources (name, kind, base_url)
    values ($1, $2, $3)
    on conflict (name) do update
    set kind = excluded.kind,
        base_url = coalesce(excluded.base_url, public.sources.base_url)
    returning id, name, kind, base_url
    """
    async with connection() as conn:
        row = await conn.fetchrow(q, name, kind, base_url)
        return dict(row)


@dataclass
class MOSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"  # or "cron_safe"

async def _ingest_mo_press_releases(*, source_id: UUID, backfill: bool, limit_each: int, max_pages_each: int) -> MOSectionResult:
    out = MOSectionResult(mode="backfill" if backfill else "cron_safe")
    cutoff_url = _norm_url(MO_PRESS_CUTOFF_URL).rstrip("/")
    referer = MO_PUBLIC_PAGES["press_releases"]

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        first_html = await _mo_fetch_text(client, referer, referer)

        view_dom_id, theme, libraries = _mo_extract_drupal_ajax_params(first_html)
        rows = _mo_parse_press_rows(first_html)

        # ✅ listing-level cutoff: include cutoff, then stop scanning older
        stop_after_index = False
        rows_norm = [_norm_url(u).rstrip("/") for (_, u, _) in rows]
        if cutoff_url in rows_norm:
            idx = rows_norm.index(cutoff_url)
            rows = rows[: idx + 1]
            stop_after_index = True

        out.fetched_urls += len(rows)

        # ✅ cron-safe: only process NEW URLs (and if none, exit fast)
        rows_to_process = rows
        async with connection() as conn:
            if not backfill:
                only_urls = [u for (_, u, _) in rows]
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                new_set = set(new_only)
                rows_to_process = [(t, u, d) for (t, u, d) in rows if u in new_set]
                out.new_urls += len(rows_to_process)

                if not rows_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out

        for title, url, published_at in rows_to_process:
            if out.upserted >= limit_each:
                return out

            stop_after_this = (_norm_url(url).rstrip("/") == cutoff_url)

            body_html = await _mo_fetch_text(client, url, referer)
            body_txt = _mo_extract_press_body_text(body_html)

            print("MO PRESS BODY LEN:", len(body_txt), url)
            print("MO PRESS BODY START:", (body_txt[:200] if body_txt else "EMPTY"))

            summary = ""
            if body_txt and len(body_txt.strip()) >= 200:
                summary = summarize_text(body_txt, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, url)

            await _upsert_item(
                url=url,
                title=title,
                summary=summary,
                jurisdiction=MO_JURISDICTION,
                agency=MO_AGENCY,
                status=MO_STATUS_MAP["press_releases"],
                source_name="Missouri — Press Releases",
                source_key="mo_press",
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

            if stop_after_this:
                out.stopped_at_cutoff = True
                return out

        # -----------------------------
        # 1) Try normal ?page=N first
        # -----------------------------
        for page in range(1, max_pages_each + 1):
            if out.upserted >= limit_each:
                break

            page_url = f"{referer}?page={page}"
            html_page = await _mo_fetch_text(client, page_url, referer)
            rows_p = _mo_parse_press_rows(html_page)

            if not rows_p:
                break

            # ✅ listing-level cutoff (include cutoff then stop scanning older)
            stop_after_index_p = False
            rows_p_norm = [_norm_url(u).rstrip("/") for (_, u, _) in rows_p]
            if cutoff_url in rows_p_norm:
                idx = rows_p_norm.index(cutoff_url)
                rows_p = rows_p[: idx + 1]
                stop_after_index_p = True

            out.fetched_urls += len(rows_p)

            # ✅ cron-safe: only process NEW urls from this page
            rows_p_to_process = rows_p
            async with connection() as conn:
                if not backfill:
                    only_urls = [u for (_, u, _) in rows_p]
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                    new_set = set(new_only)
                    rows_p_to_process = [(t, u, d) for (t, u, d) in rows_p if u in new_set]
                    out.new_urls += len(rows_p_to_process)

                    # cron-safe: if this page has nothing new, stop scanning older pages
                    if not rows_p_to_process:
                        if stop_after_index_p:
                            out.stopped_at_cutoff = True
                        return out

            for title, url, published_at in rows_p_to_process:
                if out.upserted >= limit_each:
                    return out

                stop_after_this = (_norm_url(url).rstrip("/") == cutoff_url)

                body_html = await _mo_fetch_text(client, url, referer)
                body_txt = _mo_extract_press_body_text(body_html)

                summary = ""
                if body_txt and len(body_txt.strip()) >= 200:
                    summary = summarize_text(body_txt, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, url)

                await _upsert_item(
                    url=url,
                    title=title,
                    summary=summary,
                    jurisdiction=MO_JURISDICTION,
                    agency=MO_AGENCY,
                    status=MO_STATUS_MAP["press_releases"],
                    source_name="Missouri — Press Releases",
                    source_key="mo_press",
                    referer=page_url,
                    published_at=published_at,
                )
                out.upserted += 1

                if stop_after_this:
                    out.stopped_at_cutoff = True
                    return out

            if stop_after_index_p:
                out.stopped_at_cutoff = True
                return out

        # -----------------------------
        # 2) Drupal AJAX pagination
        # -----------------------------
        for page in range(1, max_pages_each + 1):
            if out.upserted >= limit_each:
                break

            ajax_url = _mo_build_drupal_ajax_url(page=page, view_dom_id=view_dom_id, theme=theme, libraries=libraries)
            j = await _mo_fetch_json(client, ajax_url, referer)
            snippet = _mo_extract_drupal_ajax_html(j)
            rows2 = _mo_parse_press_rows(snippet)

            if not rows2:
                break

            # ✅ listing-level cutoff (include cutoff then stop scanning older)
            stop_after_index_2 = False
            rows2_norm = [_norm_url(u).rstrip("/") for (_, u, _) in rows2]
            if cutoff_url in rows2_norm:
                idx = rows2_norm.index(cutoff_url)
                rows2 = rows2[: idx + 1]
                stop_after_index_2 = True

            out.fetched_urls += len(rows2)

            # ✅ cron-safe: only process NEW urls from ajax page
            rows2_to_process = rows2
            async with connection() as conn:
                if not backfill:
                    only_urls = [u for (_, u, _) in rows2]
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                    new_set = set(new_only)
                    rows2_to_process = [(t, u, d) for (t, u, d) in rows2 if u in new_set]
                    out.new_urls += len(rows2_to_process)

                    if not rows2_to_process:
                        if stop_after_index_2:
                            out.stopped_at_cutoff = True
                        return out

            for title, url, published_at in rows2_to_process:
                if out.upserted >= limit_each:
                    return out

                stop_after_this = (_norm_url(url).rstrip("/") == cutoff_url)

                body_html = await _mo_fetch_text(client, url, referer)
                body_txt = _mo_extract_press_body_text(body_html)

                summary = ""
                if body_txt and len(body_txt.strip()) >= 200:
                    summary = summarize_text(body_txt, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, url)

                await _upsert_item(
                    url=url,
                    title=title,
                    summary=summary,
                    jurisdiction=MO_JURISDICTION,
                    agency=MO_AGENCY,
                    status=MO_STATUS_MAP["press_releases"],
                    source_name="Missouri — Press Releases",
                    source_key="mo_press",
                    referer=referer,
                    published_at=published_at,
                )
                out.upserted += 1

                if stop_after_this:
                    out.stopped_at_cutoff = True
                    return out

            if stop_after_index_2:
                out.stopped_at_cutoff = True
                return out

    return out


async def _ingest_mo_executive_orders(*, source_id: UUID, backfill: bool, limit_each: int) -> MOSectionResult:
    out = MOSectionResult(mode="backfill" if backfill else "cron_safe")
    referer = "https://www.sos.mo.gov/library/reference/orders"

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        all_links: list[str] = []
        for year in _mo_sos_eo_index_years():
            idx_url = f"https://www.sos.mo.gov/library/reference/orders/{year}"
            try:
                idx_html = await _mo_fetch_text(client, idx_url, referer)
            except Exception:
                continue
            all_links.extend(_mo_parse_sos_eo_links(idx_html, year))

        seen = set()
        eo_links: list[str] = []
        for u in all_links:
            if u in seen:
                continue
            seen.add(u)
            eo_links.append(u)

        # ✅ counts
        out.fetched_urls = len(eo_links)

        # ✅ cron-safe: only process NEW urls
        eo_links_to_process = eo_links
        async with connection() as conn:
            if not backfill:
                new_only = await _filter_new_external_ids(conn, source_id, eo_links)
                new_set = set(new_only)
                eo_links_to_process = [u for u in eo_links if u in new_set]
                out.new_urls = len(eo_links_to_process)

                if not eo_links_to_process:
                    return out

        for eo_url in eo_links_to_process:
            if out.upserted >= limit_each:
                break

            html = await _mo_fetch_text(client, eo_url, referer)
            txt = _strip_html_to_text(html)
            published_at = _mo_parse_eo_signed_date_from_text(txt)

            title = ""
            m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
            if m:
                title = html_lib.unescape(m.group(1)).strip()
                title = re.sub(r"\s*\|\s*.*$", "", title).strip()
            if not title:
                title = eo_url.rstrip("/").split("/")[-1].upper()

            summary = ""
            if txt and len(txt.strip()) >= 200:
                summary = summarize_text(txt, max_sentences=2, max_chars=700) or ""
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, eo_url)

            await _upsert_item(
                url=eo_url,
                title=title,
                summary=summary,
                jurisdiction=MO_JURISDICTION,
                agency=MO_AGENCY,
                status=MO_STATUS_MAP["executive_orders"],
                source_name="Missouri — Executive Orders",
                source_key="mo_eo",
                referer=referer,
                published_at=published_at,
            )
            out.upserted += 1

    return out

async def _ingest_mo_proclamations(
    *,
    source_id: UUID,
    backfill: bool,
    limit_each: int,
    max_pages_each: int,
) -> MOSectionResult:
    out = MOSectionResult(mode="backfill" if backfill else "cron_safe")
    base = MO_PUBLIC_PAGES["proclamations"]
    referer = base
    cutoff_norm = _norm_url(MO_PROC_CUTOFF_URL).rstrip("/")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for page in range(0, max_pages_each):
            if out.upserted >= limit_each:
                break

            url = base if page == 0 else f"{base}?page={page}"
            html = await _mo_fetch_text(client, url, referer)
            rows = _mo_parse_proc_rows(html)
            if not rows:
                break

            # ✅ listing-level cutoff: include cutoff, then stop scanning older
            stop_after_index = False
            rows_norm = [_norm_url(u).rstrip("/") for (_, u, _) in rows]
            if cutoff_norm in rows_norm:
                idx = rows_norm.index(cutoff_norm)
                rows = rows[: idx + 1]
                stop_after_index = True

            out.fetched_urls += len(rows)

            # ✅ cron-safe: only process NEW urls from this page
            rows_to_process = rows
            async with connection() as conn:
                if not backfill:
                    only_urls = [u for (_, u, _) in rows]
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)
                    new_set = set(new_only)
                    rows_to_process = [(t, u, d) for (t, u, d) in rows if u in new_set]
                    out.new_urls += len(rows_to_process)

                    # cron-safe: if nothing new on this page, stop scanning older
                    if not rows_to_process:
                        if stop_after_index:
                            out.stopped_at_cutoff = True
                        return out

            for title, item_url, published_at in rows_to_process:
                if out.upserted >= limit_each:
                    break

                stop_after_this = (_norm_url(item_url).rstrip("/") == cutoff_norm)

                await _upsert_item(
                    url=item_url,
                    title=title,
                    summary="",  # keep empty (image PDFs)
                    jurisdiction=MO_JURISDICTION,
                    agency=MO_AGENCY,
                    status=MO_STATUS_MAP["proclamations"],
                    source_name="Missouri — Proclamations",
                    source_key="mo_proc",
                    referer=url,
                    published_at=published_at,
                )
                out.upserted += 1

                if stop_after_this:
                    out.stopped_at_cutoff = True
                    return out

            if stop_after_index:
                out.stopped_at_cutoff = True
                return out

    return out

async def ingest_missouri(limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    """
    Cron-safe Missouri ingest:
      - first run (empty DB) => backfill=True => ingest until cutoff
      - cron runs (DB already has items) => backfill=False => only process NEW urls, fast-exit when none
    Also prints the standard "seen/total/new/upserted/mode/cutoff" line like other states.
    """

    # --- create / get sources (per section) ---
    press_source = await _get_or_create_source_row(
        name="Missouri — Press Releases",
        kind="mo_press",  # keep EXACTLY what your frontend expects
        base_url=MO_PUBLIC_PAGES["press_releases"],
    )

    eo_source = await _get_or_create_source_row(
        name="Missouri — Executive Orders",
        kind="mo_eo",
        base_url="https://www.sos.mo.gov/library/reference/orders",
    )

    proc_source = await _get_or_create_source_row(
        name="Missouri — Proclamations",
        kind="mo_proc",
        base_url=MO_PUBLIC_PAGES["proclamations"],
    )

    # --- decide backfill vs cron-safe based on DB counts ---
    async with connection() as conn:
        press_count = await _count_items_for_source(conn, press_source["id"])
        eo_count = await _count_items_for_source(conn, eo_source["id"])
        proc_count = await _count_items_for_source(conn, proc_source["id"])

    press_backfill = (press_count == 0)
    eo_backfill = (eo_count == 0)
    proc_backfill = (proc_count == 0)

    # --- run sections ---
    press = await _ingest_mo_press_releases(
        source_id=press_source["id"],
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    print(
        f"MISSOURI press_releases: seen={press.fetched_urls} new={press.new_urls} "
        f"upserted={press.upserted} mode={press.mode} cutoff={press.stopped_at_cutoff}"
    )

    eos = await _ingest_mo_executive_orders(
        source_id=eo_source["id"],
        backfill=eo_backfill,
        limit_each=limit_each,
    )
    print(
        f"MISSOURI executive_orders: seen={eos.fetched_urls} new={eos.new_urls} "
        f"upserted={eos.upserted} mode={eos.mode} cutoff={eos.stopped_at_cutoff}"
    )

    procs = await _ingest_mo_proclamations(
        source_id=proc_source["id"],
        backfill=proc_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    print(
        f"MISSOURI proclamations: seen={procs.fetched_urls} new={procs.new_urls} "
        f"upserted={procs.upserted} mode={procs.mode} cutoff={procs.stopped_at_cutoff}"
    )

    # --- aggregate (like other states) ---
    total_seen = press.fetched_urls + eos.fetched_urls + procs.fetched_urls
    total_new = press.new_urls + eos.new_urls + procs.new_urls
    total_upserted = press.upserted + eos.upserted + procs.upserted

    any_cutoff = press.stopped_at_cutoff or eos.stopped_at_cutoff or procs.stopped_at_cutoff

    print(
        f"MISSOURI TOTAL: seen={total_seen} new={total_new} upserted={total_upserted} cutoff={any_cutoff}"
    )

    return {
        "state": MO_JURISDICTION,
        "press_releases": press.__dict__,
        "executive_orders": eos.__dict__,
        "proclamations": procs.__dict__,
        "totals": {
            "seen": total_seen,
            "new": total_new,
            "upserted": total_upserted,
            "cutoff": any_cutoff,
        },
    }

# ----------------------------
# Kansas config (Kelly)
# ----------------------------

KS_JURISDICTION = "kansas"
KS_AGENCY = "Kansas Governor (Laura Kelly)"

KS_PUBLIC_PAGES = {
    "press_releases": "https://www.governor.ks.gov/newsroom/press-releases",
    "executive_orders": "https://www.governor.ks.gov/newsroom/executive-orders/",
}

KS_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
}

# stop after INCLUDING this item
KS_PRESS_CUTOFF_URL = "https://www.governor.ks.gov/Home/Components/News/News/469/56?arch=1"


def _ks_abs(href: str) -> str:
    return urljoin("https://www.governor.ks.gov", href or "")

def _ks_canon_url(url: str) -> str:
    """
    Canonical KS item URLs:
    - strip querystring + fragment
    - remove trailing slash
    """
    if not url:
        return ""
    parts = urlsplit(url)
    path = (parts.path or "").rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _ks_parse_post_date_from_text(txt: str) -> datetime | None:
    """
    Kansas item pages show:
      Post Date: 01/10/2025
    Sometimes time appears in list, but the item page has a clean Post Date.
    """
    t = (txt or "").replace("\u00a0", " ")
    m = re.search(r"Post Date:\s*(\d{1,2}/\d{1,2}/\d{4})", t, flags=re.I)
    if not m:
        return None
    try:
        mm, dd, yy = m.group(1).split("/")
        return datetime(int(yy), int(mm), int(dd), tzinfo=timezone.utc)
    except Exception:
        return None


def _ks_html_anchor_text(a_inner_html: str) -> str:
    # strip nested tags inside <a>...</a>
    s = re.sub(r"<[^>]+>", " ", a_inner_html or "")
    s = html_lib.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _ks_parse_press_list_rows(html: str) -> list[tuple[str, str, datetime | None]]:
    """
    Listing pages contain cards linking to:
      /Home/Components/News/News/<id>/56
    Returns [(title, url, published_at_guess)]
    We will still fetch item page to get authoritative Post Date.
    """
    out: list[tuple[str, str, datetime | None]] = []
    if not html:
        return out

    # anchor to item pages (press release component is /56)
    # capture href + inner HTML of <a ...>...</a>
    for m in re.finditer(
        r"<a[^>]+href\s*=\s*['\"]([^'\"]*?/Home/Components/News/News/\d+/56[^'\"]*)['\"][^>]*>(.*?)</a>",
        html,
        flags=re.I | re.S,
    ):
        href = html_lib.unescape(m.group(1) or "").strip()
        title = _ks_html_anchor_text(m.group(2) or "")
        if not title:
            # fallback: try title or aria-label on the <a> tag itself
            a_tag = m.group(0).split(">")[0] + ">"
            am = re.search(r'(title|aria-label)\s*=\s*["\']([^"\']+)["\']', a_tag, flags=re.I)
            if am:
                title = html_lib.unescape(am.group(2)).strip()
        if not href or not title:
            continue

        url = _ks_canon_url(_ks_abs(href))

        # optional date near the link (MM/DD/YYYY) – best-effort only
        window_start = max(0, m.start() - 350)
        window_end = min(len(html), m.end() + 350)
        window = html[window_start:window_end]
        dm = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", window)
        published_at_guess = None
        if dm:
            try:
                mm, dd, yy = dm.group(1).split("/")
                published_at_guess = datetime(int(yy), int(mm), int(dd), tzinfo=timezone.utc)
            except Exception:
                published_at_guess = None

        out.append((title, url, published_at_guess))

    # de-dupe by url while preserving order
    seen = set()
    dedup = []
    for t, u, d in out:
        if u in seen:
            continue
        seen.add(u)
        dedup.append((t, u, d))
    return dedup


def _ks_extract_item_body_text(html: str) -> str:
    """
    Convert the whole page to text, then trim common boilerplate.
    We rely on your project's _strip_html_to_text.
    """
    if not html:
        return ""

    txt = (_strip_html_to_text(html) or "").strip()
    if not txt:
        return ""

    # trim footer-ish chunks
    for marker in [
        "Design By GRANICUS",
        "Connecting People & Government",
        "Statehouse Address:",
    ]:
        idx = txt.lower().find(marker.lower())
        if idx != -1 and idx > 300:
            txt = txt[:idx].strip()

    # drop a few obvious nav lines at the top
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    cleaned = []
    for ln in lines:
        low = ln.lower()
        if low.startswith("skip to main content"):
            continue
        cleaned.append(ln)

    return "\n".join(cleaned).strip()

async def _ks_pw_make_renderer(*, referer: str):
    """
    Create one Playwright browser/context/page that we can reuse for all KS listing pages.
    """
    p = await async_playwright().start()

    browser = await p.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    ctx = await browser.new_context(
        user_agent=BROWSER_UA_HEADERS.get("User-Agent"),
        locale="en-US",
        viewport={"width": 1365, "height": 900},
        extra_http_headers={
            "Referer": referer,
            "Accept-Language": "en-US,en;q=0.9",
        },
    )

    page = await ctx.new_page()

    # Hide webdriver once
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
    """)

    return p, browser, ctx, page


async def _ks_pw_close_renderer(p, browser, ctx):
    try:
        await ctx.close()
    except Exception:
        pass
    try:
        await browser.close()
    except Exception:
        pass
    try:
        await p.stop()
    except Exception:
        pass
    
async def _ks_pw_render_with_page(
    page: Page,
    url: str,
    *,
    wait_ms: int = 1200,
    scrolls: int = 1,
) -> str:
    """
    Reuse the same page for each listing URL to avoid relaunching Chromium.
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        selectors = [
            'a[href*="/Home/Components/News/News/"]',
            ".news-item",
            ".component-news",
            "h1:has-text('Press Releases')",
        ]
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=10_000)
                break
            except Exception:
                pass

        await page.wait_for_timeout(wait_ms)

        for _ in range(scrolls):
            await page.mouse.wheel(0, 4000)
            await page.wait_for_timeout(500)

        html = (await page.content()) or ""

        # If we got WAF HTML, retry once with reload
        if "Access Denied" in html or "$(SERVE_403)" in html:
            await page.wait_for_timeout(1200)
            await page.reload(wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(wait_ms)
            html = (await page.content()) or ""

        return html.strip()
    except Exception:
        return ""

async def _ks_fetch_text(
    client: httpx.AsyncClient,
    url: str,
    referer: str,
    pw_page=None,  # <-- NEW
) -> str:
    # Listing pages: use Playwright renderer IF provided
    if ("/newsroom/press-releases" in url) or ("/newsroom/executive-orders" in url):
        if pw_page is not None:
            html = await _ks_pw_render_with_page(pw_page, url, wait_ms=1200, scrolls=1)
            return (html or "").strip()

        # fallback (should rarely happen)
        return ""

    # Item pages: keep using httpx
    headers = {
        **BROWSER_UA_HEADERS,
        "Referer": referer,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = await client.get(url, headers=headers)
    text = (r.text or "").strip()

    # If blocked on item pages, you can optionally fallback to pw_page too
    if r.status_code == 403 and pw_page is not None:
        html = await _ks_pw_render_with_page(pw_page, url, wait_ms=1000, scrolls=0)
        if html and "<html" in html.lower():
            return html.strip()
        return ""

    if r.status_code == 200 and len(text) < 2000 and pw_page is not None:
        html = await _ks_pw_render_with_page(pw_page, url, wait_ms=1000, scrolls=0)
        if html and "<html" in html.lower():
            return html.strip()
        return text

    r.raise_for_status()
    return text

def _ks_press_list_url(npage: int) -> str:
    """
    Current news:
      page 1: /newsroom/press-releases
      page 2+: /newsroom/press-releases/-npage-2
    """
    base = KS_PUBLIC_PAGES["press_releases"].rstrip("/")
    if npage <= 1:
        return base
    return f"{base}/-npage-{npage}"


def _ks_press_archive_list_url(arch: int, npage: int) -> str:
    """
    Archive 1:
      page 1: /newsroom/press-releases/-arch-1
      page 2+: /newsroom/press-releases/-arch-1/-npage-2
    """
    base = KS_PUBLIC_PAGES["press_releases"].rstrip("/")
    if npage <= 1:
        return f"{base}/-arch-{arch}"
    return f"{base}/-arch-{arch}/-npage-{npage}"


async def _ingest_kansas_press_releases(*, source_id: UUID, backfill: bool, limit_each: int, max_pages_each: int) -> dict:
    out = {
        "fetched_urls": 0,
        "new_urls": 0,
        "upserted": 0,
        "stopped_at_cutoff": False,
        "mode": "backfill" if backfill else "cron_safe",
    }
    referer = KS_PUBLIC_PAGES["press_releases"]

    seen_item_urls: set[str] = set()
    cutoff_canon = _ks_canon_url(KS_PRESS_CUTOFF_URL)

    p = browser = ctx = pw_page = None
    p, browser, ctx, pw_page = await _ks_pw_make_renderer(referer=referer)

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:

            async def handle_item(title: str, url: str, published_guess: datetime | None, list_referer: str, npage: int, is_archive: bool) -> bool:
                nonlocal out

                canon = _ks_canon_url(url)
                if canon in seen_item_urls:
                    return False
                seen_item_urls.add(canon)

                stop_after_this = (is_archive and canon == cutoff_canon)

                item_html = await _ks_fetch_text(client, url, list_referer, pw_page=pw_page)
                item_txt = _ks_extract_item_body_text(item_html)

                published_at = _ks_parse_post_date_from_text(item_txt) or published_guess

                summary = ""
                if item_txt and len(item_txt.strip()) >= 200:
                    summary = summarize_text(item_txt, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, canon)  # only when we have text

                await _upsert_item(
                    url=canon,
                    title=title,
                    summary=summary,
                    jurisdiction=KS_JURISDICTION,
                    agency=KS_AGENCY,
                    status=KS_STATUS_MAP["press_releases"],
                    source_name="Kansas — Press Releases",
                    source_key="ks_press",
                    referer=list_referer,
                    published_at=published_at,
                )
                out["upserted"] += 1
                return stop_after_this

            # -----------------------------
            # A) Current news pages
            # -----------------------------
            for npage in range(1, max_pages_each + 1):
                if out["upserted"] >= limit_each:
                    return out  # ✅ ok now, finally will still run

                list_url = _ks_press_list_url(npage)
                html_page = await _ks_fetch_text(client, list_url, referer, pw_page=pw_page)

                rows = _ks_parse_press_list_rows(html_page)
                if not rows:
                    break

                # ✅ COUNT seen on listing
                out["fetched_urls"] += len(rows)

                # ✅ CRON-SAFE: only process NEW urls
                rows_to_process = rows
                if not backfill:
                    only_urls = [u for (_, u, _) in rows]
                    async with connection() as conn:
                        new_only = await _filter_new_external_ids(conn, source_id, only_urls)

                    new_set = set(new_only)
                    rows_to_process = [(t, u, d) for (t, u, d) in rows if u in new_set]
                    out["new_urls"] += len(rows_to_process)

                    # If this page has nothing new, stop scanning older pages
                    if not rows_to_process:
                        return out

                for title, url, published_guess in rows_to_process:
                    if out["upserted"] >= limit_each:
                        return out

                    should_stop = await handle_item(title, url, published_guess, list_url, npage=npage, is_archive=False)
                    if should_stop:
                        out["stopped_at_cutoff"] = True
                        return out

            # -----------------------------
            # B) Archived news (-arch-1)
            # -----------------------------
            arch = 1
            for npage in range(1, max_pages_each + 1):
                if out["upserted"] >= limit_each:
                    return out

                list_url = _ks_press_archive_list_url(arch=arch, npage=npage)
                html_page = await _ks_fetch_text(client, list_url, referer, pw_page=pw_page)

                rows = _ks_parse_press_list_rows(html_page)
                if not rows:
                    break

                # ✅ listing-level cutoff trim (include cutoff then stop scanning older)
                stop_after_index = False
                rows_norm = [_ks_canon_url(u) for (_, u, _) in rows]
                if cutoff_canon in rows_norm:
                    idx = rows_norm.index(cutoff_canon)
                    rows = rows[: idx + 1]
                    stop_after_index = True

                # ✅ COUNT seen on listing
                out["fetched_urls"] += len(rows)

                # ✅ CRON-SAFE: only process NEW urls
                rows_to_process = rows
                if not backfill:
                    only_urls = [u for (_, u, _) in rows]
                    async with connection() as conn:
                        new_only = await _filter_new_external_ids(conn, source_id, only_urls)

                    new_set = set(new_only)
                    rows_to_process = [(t, u, d) for (t, u, d) in rows if u in new_set]
                    out["new_urls"] += len(rows_to_process)

                    # If nothing new on this page, stop early (cron-safe)
                    if not rows_to_process:
                        if stop_after_index:
                            out["stopped_at_cutoff"] = True
                        return out

                for title, url, published_guess in rows_to_process:
                    if out["upserted"] >= limit_each:
                        return out

                    should_stop = await handle_item(title, url, published_guess, list_url, npage=npage, is_archive=True)
                    if should_stop:
                        out["stopped_at_cutoff"] = True
                        return out

                # if we reached cutoff in listing trimming, we are done after processing it
                if stop_after_index:
                    out["stopped_at_cutoff"] = True
                    return out

        return out

    finally:
        # ✅ this ALWAYS runs, even if we returned early above
        await _ks_pw_close_renderer(p, browser, ctx)

async def _ingest_kansas_executive_orders(*, source_id: UUID, backfill: bool, limit_each: int) -> dict:
    out = {
        "fetched_urls": 0,
        "new_urls": 0,
        "upserted": 0,
        "mode": "backfill" if backfill else "cron_safe",
    }
    referer = KS_PUBLIC_PAGES["executive_orders"]

    seen_eo_urls: set[str] = set()

    p = browser = ctx = pw_page = None
    p, browser, ctx, pw_page = await _ks_pw_make_renderer(referer=referer)


    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            html_page = await _ks_fetch_text(client, referer, referer, pw_page=pw_page)

            # listing links use /57 for EO component pages
            links = []
            for m in re.finditer(
                r"href\s*=\s*['\"]([^'\"]*?/Home/Components/News/News/\d+/57[^'\"]*)['\"]",
                html_page,
                flags=re.I,
            ):
                href = html_lib.unescape(m.group(1) or "").strip()
                if not href:
                    continue
                links.append(_ks_canon_url(_ks_abs(href)))

            seen = set()
            eo_urls = []
            for u in links:
                if u in seen:
                    continue
                seen.add(u)
                eo_urls.append(u)

            # ✅ listing count
            out["fetched_urls"] = len(eo_urls)

            # ✅ CRON-SAFE: only process NEW urls
            eo_urls_to_process = eo_urls
            if not backfill:
                async with connection() as conn:
                    new_only = await _filter_new_external_ids(conn, source_id, eo_urls)
                new_set = set(new_only)
                eo_urls_to_process = [u for u in eo_urls if u in new_set]
                out["new_urls"] = len(eo_urls_to_process)

                if not eo_urls_to_process:
                    return out

            for eo_url in eo_urls_to_process:
                if out["upserted"] >= limit_each:
                    break

                # ✅ DEDUPE EO URLS HERE (add this block)
                canon_eo = _ks_canon_url(eo_url)
                if canon_eo in seen_eo_urls:
                    continue
                seen_eo_urls.add(canon_eo)
                eo_url = canon_eo

                item_html = await _ks_fetch_text(client, eo_url, referer, pw_page=pw_page)
                item_txt = _ks_extract_item_body_text(item_html)

                published_at = _ks_parse_post_date_from_text(item_txt)

                title = ""
                tm = re.search(r"<title>(.*?)</title>", item_html, flags=re.I | re.S)
                if tm:
                    title = html_lib.unescape(tm.group(1)).strip()
                    title = re.sub(r"\s*-\s*Kansas.*$", "", title).strip()

                if not title:
                    lines = [ln.strip() for ln in (item_txt or "").splitlines() if ln.strip()]
                    title = lines[0] if lines else eo_url.rstrip("/").split("/")[-1]

                summary = ""
                if item_txt and len(item_txt.strip()) >= 120:
                    summary = summarize_text(item_txt, max_sentences=2, max_chars=700) or ""
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, eo_url)

                await _upsert_item(
                    url=eo_url,
                    title=title,
                    summary=summary,
                    jurisdiction=KS_JURISDICTION,
                    agency=KS_AGENCY,
                    status=KS_STATUS_MAP["executive_orders"],
                    source_name="Kansas — Executive Orders",
                    source_key="ks_eo",
                    referer=referer,
                    published_at=published_at,
                )
                out["upserted"] += 1

        return out

    finally:
        await _ks_pw_close_renderer(p, browser, ctx)

async def ingest_kansas(limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    press_source = await _get_or_create_source_row(
        name="Kansas — Press Releases",
        kind="ks_press",
        base_url=KS_PUBLIC_PAGES["press_releases"],
    )

    eo_source = await _get_or_create_source_row(
        name="Kansas — Executive Orders",
        kind="ks_eo",
        base_url=KS_PUBLIC_PAGES["executive_orders"],
    )

    async with connection() as conn:
        press_count = await _count_items_for_source(conn, press_source["id"])
        eo_count = await _count_items_for_source(conn, eo_source["id"])

    press_backfill = (press_count == 0)
    eo_backfill = (eo_count == 0)

    press = await _ingest_kansas_press_releases(
        source_id=press_source["id"],
        backfill=press_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )

    eos = await _ingest_kansas_executive_orders(
        source_id=eo_source["id"],
        backfill=eo_backfill,
        limit_each=limit_each,
    )

    # ✅ cron/backfill terminal prints (like MI/TN/NC)
    print(
        f"KS PRESS mode={'backfill' if press_backfill else 'cron_safe'} "
        f"new={press['upserted'] if press_backfill else press['new_urls']} "
        f"fetched={press['fetched_urls']} seen_total={press_count} "
        f"cutoff={press.get('stopped_at_cutoff', False)}"
    )
    print(
        f"KS EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos['upserted'] if eo_backfill else eos['new_urls']} "
        f"fetched={eos['fetched_urls']} seen_total={eo_count}"
    )

    return {"state": KS_JURISDICTION, "press_releases": press, "executive_orders": eos}

# ----------------------------
# New Mexico config (nm.gov + Governor)
# ----------------------------

NM_JURISDICTION = "new_mexico"
NM_AGENCY_NEWS = "New Mexico State Government (nm.gov)"
NM_AGENCY_GOV = "New Mexico Governor (Michelle Lujan Grisham)"

NM_PUBLIC_PAGES = {
    "news": "https://www.nm.gov/news/",
    "executive_orders": "https://www.governor.state.nm.us/about-the-governor/executive-orders/",
    "executive_orders_archive": "https://www.governor.state.nm.us/about-the-governor/executive-orders/executive-orders-archive/",
}

NM_STATUS_MAP = {
    "news": "press_release",
    "executive_orders": "executive_order",
}

# include this item, then stop (1st item of 2025 you specified)
NM_NEWS_CUTOFF_URL = "https://mailchi.mp/state.nm.us/release-17287019"

# EOs: you want all 2025–2026 from main page, and all 2024 from archive (down to 2024-001)
NM_EO_2025_OLDEST = "https://www.governor.state.nm.us/wp-content/uploads/2025/01/Executive-Order-2025-001.pdf"
NM_EO_2024_OLDEST = "https://www.governor.state.nm.us/wp-content/uploads/2024/01/Executive-Order-2024-001.pdf"

# DevTools endpoint
NM_NEWS_API = "https://prod.nmgov.rtsclients.com/api/Public/GetNews"

# ----------------------------
# New Mexico helpers
# ----------------------------

def _nm_norm(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    try:
        parts = urlsplit(u)
        u = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))  # drop ?query and #fragment
    except Exception:
        pass
    return _norm_url(u)  # ✅ keep trailing slash if present (matches DB)

def _nm_parse_dt_from_url(u: str) -> Optional[datetime]:
    if not u:
        return None
    s = str(u)

    # /YYYY/MM/DD/  (strong)
    m = re.search(r"/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/", s)
    if m:
        try:
            return datetime(int(m.group("y")), int(m.group("m")), int(m.group("d")), tzinfo=timezone.utc)
        except Exception:
            pass

    # YYYY-MM-DD anywhere (strong)
    m = re.search(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})", s)
    if m:
        try:
            return datetime(int(m.group("y")), int(m.group("m")), int(m.group("d")), tzinfo=timezone.utc)
        except Exception:
            pass
    
    # ✅ YYYY.MM.DD or YYYY_MM_DD or YYYY-MM-DD in filenames (Tax PDFs, etc.)
    m = re.search(r"(?P<y>\d{4})[._-](?P<m>\d{2})[._-](?P<d>\d{2})", s)
    if m:
        try:
            return datetime(int(m.group("y")), int(m.group("m")), int(m.group("d")), tzinfo=timezone.utc)
        except Exception:
            pass


    return None

def _nm_abs_detail_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # many items are already absolute; keep them
    if u.startswith("http://") or u.startswith("https://"):
        return _norm_url(u)
    # safest fallback (rare)
    return _norm_url(urljoin("https://www.nm.gov/", u.lstrip("/")))

def _nm_is_governor_eo_pdf(url: str) -> bool:
    u = (url or "").lower()
    if "governor.state.nm.us" not in u:
        return False
    if "/wp-content/uploads/" not in u:
        return False
    if "executive-order-" in u and u.endswith(".pdf"):
        return True
    return False

def _nm_is_junk_viewer(url: str) -> bool:
    u = (url or "").lower().strip()
    return (
        "acrobat.adobe.com" in u
        or "adobe.com/id/urn:aaid:" in u
    )


def _nm_find_any_http_url(it: dict) -> str:
    """
    Fallback: if API doesn't use url/link/permalink keys, scan the object
    (and one nested level) for any http(s) URL.
    """
    if not isinstance(it, dict):
        return ""
    for v in it.values():
        if isinstance(v, str) and v.startswith(("http://", "https://")):
            return v
        if isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, str) and vv.startswith(("http://", "https://")):
                    return vv
    return ""


def _nm_parse_dt_any(v) -> Optional[datetime]:
    if v is None:
        return None

    # numeric epoch
    if isinstance(v, (int, float)):
        try:
            ts = float(v)
            if ts > 2_000_000_000_000:  # millis
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    s = str(v).strip()
    if not s:
        return None

    # /Date(....)/
    m = re.search(r"Date\((\d+)\)", s)
    if m:
        try:
            ts = int(m.group(1))
            if ts > 2_000_000_000_000:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    # ISO-ish
    try:
        ss = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ss)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass

    # YYYY-MM-DD (no time)
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            dt = datetime.strptime(s, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # MM/DD/YYYY
    try:
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
            dt = datetime.strptime(s, "%m/%d/%Y")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # Month DD, YYYY inside string
    try:
        mm = re.search(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
            s,
            flags=re.I,
        )
        if mm:
            dt = datetime.strptime(mm.group(0), "%B %d, %Y")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # Normalize periods in abbreviated months ("Sept.", "Oct.")
    s2 = re.sub(r"\.", "", s)
    s2 = re.sub(r"\s+", " ", s2).strip()

    # ✅ YYYY-MM-DD inside a longer string
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", s2)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # ✅ MM/DD/YYYY inside a longer string
    m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", s2)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%m/%d/%Y")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # "31 Dec, 2025" / "31 Dec 2025"
    try:
        if re.fullmatch(r"\d{1,2}\s+[A-Za-z]{3,9},\s+\d{4}", s2) or re.fullmatch(r"\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}", s2):
            s3 = s2.replace(",", "")
            # try abbreviated then full month
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    dt = datetime.strptime(s3, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
    except Exception:
        pass

    # "Dec 31, 2025" / "December 31, 2025"
    try:
        if re.fullmatch(r"[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}", s2):
            for fmt in ("%b %d, %Y", "%B %d, %Y"):
                try:
                    dt = datetime.strptime(s2, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
    except Exception:
        pass


    return None

def _nm_parse_dt_from_pdf_url(pdf_url: str) -> Optional[datetime]:
    """
    EO PDFs are scanned; use URL path /YYYY/MM/ when present.
    Example: .../uploads/2026/01/Executive-Order-2026-001.pdf
    We'll set day=1 (best-effort).
    """
    if not pdf_url:
        return None
    m = re.search(r"/uploads/(?P<y>\d{4})/(?P<m>\d{2})/", pdf_url)
    if not m:
        return None
    try:
        y = int(m.group("y"))
        mo = int(m.group("m"))
        return datetime(y, mo, 1, tzinfo=timezone.utc)
    except Exception:
        return None

def _nm_strip_mailchimp_tracking(url: str) -> str:
    """
    Normalize for cutoff comparisons:
      - remove querystring + fragment
      - normalize + strip trailing slash
    """
    if not url:
        return ""
    u = url.strip()
    try:
        parts = urlsplit(u)
        u = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))  # drop query/fragment
    except Exception:
        pass
    return _nm_norm(u)

def _nm_extract_textish(html: str) -> str:
    txt = _strip_html_to_text(html or "")
    txt = (txt or "").strip()
    if len(txt) > 35000:
        txt = txt[:35000]
    return txt

def _nm_title_from_html(html: str, fallback: str) -> str:
    title = ""
    m = re.search(r"(?is)<title>\s*(.*?)\s*</title>", html or "")
    if m:
        title = re.sub(r"\s+", " ", html_lib.unescape(m.group(1))).strip()
        # drop common suffixes
        title = re.sub(r"\s*\|\s*.*$", "", title).strip()
        title = re.sub(r"\s*-\s*The State of New Mexico\s*$", "", title, flags=re.I).strip()
    return title or fallback

def _nm_find_dt_in_item(it: dict) -> Optional[datetime]:
    """
    The nm.gov API fields vary; scan common keys + any key containing 'date'/'publish'/'created'.
    """
    if not isinstance(it, dict):
        return None

    # common explicit keys first
    for k in (
        "publishedAt", "publishedDate", "publishDate", "publish_date",
        "date", "Date",
        "createdAt", "createdOn", "CreatedAt", "CreatedOn",
        "updatedAt", "UpdatedAt",
        "PublishDate", "PublicationDate", "publicationDate",
    ):
        if k in it and it.get(k) is not None:
            dt = _nm_parse_dt_any(it.get(k))
            if dt:
                return dt

    # handle nested dict like {"publicationDate": {"date": "..."}}
    for k, v in it.items():
        kl = str(k).lower()
        if any(tok in kl for tok in ("publish", "date", "created", "updated")):
            if isinstance(v, dict):
                for subk in ("date", "value", "utc", "iso", "published", "created"):
                    if subk in v:
                        dt = _nm_parse_dt_any(v.get(subk))
                        if dt:
                            return dt
            else:
                dt = _nm_parse_dt_any(v)
                if dt:
                    return dt
    return None


def _nm_parse_dt_from_html(html: str) -> Optional[datetime]:
    """
    Fallback for agency sites: try meta tags, <time datetime>, then visible text.
    """
    if not html:
        return None
    
    # JSON-LD: {"datePublished": "..."} / {"dateModified": "..."}
    for m in re.finditer(r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html):
        blob = (m.group(1) or "").strip()
        if not blob:
            continue
        try:
            data = json.loads(blob)
        except Exception:
            continue

        candidates = []
        if isinstance(data, dict):
            candidates.append(data)
        elif isinstance(data, list):
            candidates.extend([x for x in data if isinstance(x, dict)])

        for obj in candidates:
            for k in ("datePublished", "dateModified", "publishDate", "published", "date"):
                if k in obj and obj.get(k):
                    dt = _nm_parse_dt_any(obj.get(k))
                    if dt:
                        return dt


    # meta tags commonly used by press release pages
    meta = re.search(
        r'(?is)<meta[^>]+(?:property|name)=["\'](?:article:published_time|article:modified_time|og:updated_time|pubdate|date|dc\.date|datepublished|sailthru\.date)["\'][^>]+content=["\']([^"\']+)["\']',
        html,
    )
    if meta:
        dt = _nm_parse_dt_any(meta.group(1))
        if dt:
            return dt

    # <time datetime="...">
    tm = re.search(r'(?is)<time[^>]+datetime=["\']([^"\']+)["\']', html)
    if tm:
        dt = _nm_parse_dt_any(tm.group(1))
        if dt:
            return dt

    # look for a clear date in the first chunk of visible text
    txt = _strip_html_to_text(html)
    txt = re.sub(r"\s+", " ", (txt or "")).strip()
    head = txt[:4000]

    # ✅ Abbrev month with optional period: "Nov. 21, 2025" / "Nov 21, 2025"
    m = re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b", head, flags=re.I)
    if m:
        s_abbrev = m.group(0).replace(".", "").strip()
        # datetime %b expects "Sep" not "Sept"
        s_abbrev = re.sub(r"\bSept\b", "Sep", s_abbrev, flags=re.I)
        try:
            dt = datetime.strptime(s_abbrev, "%b %d, %Y")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass


    # ✅ "31 Dec, 2025" / "31 Dec 2025" directly in visible text (HED, some agency CMS)
    m = re.search(r"\b\d{1,2}\s+[A-Za-z]{3,9},\s+\d{4}\b", head)
    if m:
        dt = _nm_parse_dt_any(m.group(0))
        if dt:
            return dt

    m = re.search(r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b", head)
    if m:
        dt = _nm_parse_dt_any(m.group(0))
        if dt:
            return dt


    # Thursday, January 8, 2026 -> parse the "January 8, 2026" part
    m1 = re.search(
        r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
        head,
        flags=re.I,
    )
    if m1:
        m2 = re.search(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
            m1.group(0),
            flags=re.I,
        )
        if m2:
            dt = _nm_parse_dt_any(m2.group(0))
            if dt:
                return dt

    # MM/DD/YYYY
    m = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", head)
    if m:
        dt = _nm_parse_dt_any(m.group(0))
        if dt:
            return dt

    # YYYY-MM-DD
    m = re.search(r"\b\d{4}-\d{2}-\d{2}\b", head)
    if m:
        dt = _nm_parse_dt_any(m.group(0))
        if dt:
            return dt

    # Month DD, YYYY
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
        head,
        flags=re.I,
    )
    if m:
        dt = _nm_parse_dt_any(m.group(0))
        if dt:
            return dt

    return None

async def _external_id_exists(conn, source_id, external_id: str) -> bool:
    """
    Returns True if we've already seen this external_id for this source_id.

    IMPORTANT: In your DB, you might be tracking seen URLs in:
      - public.item_external_ids (preferred)
      - public.items.external_id
      - public.items.url
    So we check all three.
    """
    if not external_id:
        return False

    # 1) item_external_ids (if table exists and is used)
    try:
        v = await conn.fetchval(
            "select 1 from public.item_external_ids where source_id=$1 and external_id=$2 limit 1",
            source_id,
            external_id,
        )
        if v:
            return True
    except Exception:
        # ignore — table might not exist in some envs
        pass

    # 2) items.external_id
    v2 = await conn.fetchval(
        "select 1 from public.items where source_id=$1 and external_id=$2 limit 1",
        source_id,
        external_id,
    )
    if v2:
        return True

    # 3) items.url
    v3 = await conn.fetchval(
        "select 1 from public.items where source_id=$1 and url=$2 limit 1",
        source_id,
        external_id,
    )
    return bool(v3)


def _nm_parse_eo_link_blocks(html: str, base_url: str) -> List[Tuple[str, str]]:
    """
    Returns list of (pdf_url, description_text) from EO pages.

    The governor EO pages are basically:
      <a href="...Executive-Order-2026-001.pdf">Executive Order 2026-001</a>
      Authorizing ... (description text)
      <a href="...">Executive Order ...</a>
      ...

    We'll:
      - find all PDF href anchors containing "Executive-Order-YYYY-NNN.pdf"
      - for each anchor, capture the nearby following text until next <a ...Executive-Order...> or next heading.
    """
    if not html:
        return []

    # capture anchor + a bit of following html for description
    # (keep it permissive; governor site markup can change)
    pat = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']*Executive-Order-\d{4}-\d{3}\.pdf)[^"\']*["\'][^>]*>.*?</a>(?P<after>.*?)(?=<a[^>]+href=["\'][^"\']*Executive-Order-\d{4}-\d{3}\.pdf|<h\d|\Z)'
    )
    out: List[Tuple[str, str]] = []
    for m in pat.finditer(html):
        href = (m.group("href") or "").strip()
        after = (m.group("after") or "").strip()

        pdf_url = _norm_url(urljoin(base_url, href))
        # turn "after" chunk into text and compress
        desc = _strip_html_to_text(after)
        desc = re.sub(r"\s+", " ", (desc or "")).strip()

        # If desc is empty, try first paragraph-ish block
        if not desc:
            mdesc = re.search(r'(?is)<p[^>]*>\s*(?!\s*<a\b)(.*?)</p>', after)
            if mdesc:
                desc = _strip_html_to_text(mdesc.group(1))
                desc = re.sub(r"\s+", " ", (desc or "")).strip()

        # Last resort: plain text directly after the link / br / whitespace
        if not desc:
            mbr = re.search(r'(?is)(?:<br\s*/?>|\s)+\s*([^<]{10,800})', after)
            if mbr:
                desc = re.sub(r"\s+", " ", mbr.group(1)).strip()

        # If desc still empty, grab a context window around the link (handles "desc before link" layouts)
        if not desc:
            try:
                hlow = (html or "").lower()
                needle = (href or "").lower()
                idx = hlow.find(needle)
                if idx != -1:
                    window = html[max(0, idx - 1800): idx + 1800]
                    ctx = _strip_html_to_text(window)
                    ctx = re.sub(r"\s+", " ", (ctx or "")).strip()

                    # remove the obvious "Executive Order 2026-001" repetition if present
                    ctx = re.sub(r"Executive\s+Order\s+\d{4}-\d{3}", "", ctx, flags=re.I).strip()
                    if len(ctx) > 30:
                        desc = ctx[:800].rstrip()
            except Exception:
                pass

        # keep it reasonably short
        if len(desc) > 800:
            desc = desc[:800].rstrip() + "…"

        out.append((pdf_url, desc))


    # de-dupe preserve order
    seen = set()
    final: List[Tuple[str, str]] = []
    for pdf_url, desc in out:
        key = _nm_norm(pdf_url)
        if key in seen:
            continue
        seen.add(key)
        final.append((pdf_url, desc))
    return final

# ----------------------------
# New Mexico ingesters
# ----------------------------

@dataclass
class NMSectionResult:
    fetched_urls: int = 0
    new_urls: int = 0
    upserted: int = 0
    stopped_at_cutoff: bool = False
    mode: str = "backfill"   # or "cron_safe"

async def _ingest_new_mexico_news(*, source_id: UUID, backfill: bool, limit_each: int, max_pages_each: int) -> NMSectionResult:
    """
    Uses DevTools JSON endpoint:
      /api/Public/GetNews?take=10&skip=...
    then fetches each detail URL HTML for summary/title polish.
    """
    cutoff = _nm_strip_mailchimp_tracking(NM_NEWS_CUTOFF_URL)
    referer = NM_PUBLIC_PAGES["news"]

    source_name = "New Mexico — Latest News (nm.gov)"
    source_key = f"{NM_JURISDICTION}:nm_gov_news"
    status = NM_STATUS_MAP["news"]

    out = NMSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        max_pages_each = max(1, min(int(max_pages_each or 1), 50))
        limit_each = max(25, min(int(limit_each or 500), 1500))

    take = 10
    seen: set[str] = set()
    stop = False
    stalled_pages = 0  # ✅ allow a few empty/repeat pages before stopping

    # ============================================================
    # ✅ (A) OPEN ONE DB CONNECTION FOR THE WHOLE CRON-SAFE RUN
    # ============================================================
    conn = None
    conn_cm = None
    if not backfill:
        conn_cm = connection()
        conn = await conn_cm.__aenter__()

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            for page_idx in range(max_pages_each):
                if stop or out.upserted >= limit_each:
                    break

                skip = page_idx * take
                params = {"take": str(take), "skip": str(skip), "agencyId": "", "tagId": ""}

                r = await client.get(
                    NM_NEWS_API,
                    params=params,
                    headers={**BROWSER_UA_HEADERS, "origin": "https://www.nm.gov", "referer": "https://www.nm.gov/"},
                    timeout=httpx.Timeout(45.0, read=45.0),
                )
                r.raise_for_status()

                try:
                    data = r.json()
                except Exception:
                    data = None

                # API might return { items: [...] } or just [...]
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    for k in ("items", "data", "results", "news", "News", "Items"):
                        v = data.get(k)
                        if isinstance(v, list):
                            items = v
                            break

                # ✅ DEBUG: print first item keys + url candidates (only once)
                if page_idx == 0 and items:
                    sample = items[0] if isinstance(items[0], dict) else {}
                    print("NM NEWS sample keys:", list(sample.keys())[:25] if isinstance(sample, dict) else type(sample))
                    if isinstance(sample, dict):
                        print(
                            "NM NEWS sample url candidates:",
                            sample.get("url"),
                            sample.get("link"),
                            sample.get("permalink"),
                            sample.get("detailUrl"),
                            sample.get("publicUrl"),
                            sample.get("WebUrl"),
                        )

                if not items:
                    break

                # ✅ listing-level cutoff include then stop scanning older pages
                stop_after_index = False
                cutoff_norm = cutoff

                # build a cheap per-item URL list for cutoff detection + cron filtering
                item_urls = []
                for it in items:
                    if not isinstance(it, dict):
                        item_urls.append("")
                        continue

                    raw_url = (
                        it.get("url")
                        or it.get("link")
                        or it.get("permalink")
                        or it.get("detailUrl")
                        or it.get("publicUrl")
                        or it.get("WebUrl")
                        or _nm_find_any_http_url(it)
                    )
                    u = _nm_abs_detail_url(raw_url or "")
                    if _nm_is_governor_eo_pdf(u):
                        u = ""  # keep EOs out of NEWS
                    item_urls.append(u)

                # if cutoff appears in this page, trim items to include it and stop after this page
                item_norms = [_nm_strip_mailchimp_tracking(u) if u else "" for u in item_urls]
                if cutoff_norm in item_norms:
                    idx = item_norms.index(cutoff_norm)
                    items = items[: idx + 1]
                    item_urls = item_urls[: idx + 1]
                    stop_after_index = True

                out.fetched_urls += len(items)

                # ✅ cron-safe: only process NEW urls from this page
                urls_to_process_set: set[str] | None = None

                # map: pre-redirect norm -> canonical final URL (string)
                canon_map: dict[str, str] = {}

                if not backfill:
                    # 1) Canonicalize each listing URL to its final redirected URL (cheap: only ~10 URLs/page)
                    canon_urls: list[str] = []
                    for u in item_urls:
                        if not u:
                            continue
                        if _nm_is_governor_eo_pdf(u):
                            continue
                        if _nm_is_junk_viewer(u):
                            continue

                        raw_norm = _nm_norm(u)

                        try:
                            rr = await client.get(
                                u,
                                headers=BROWSER_UA_HEADERS,
                                timeout=httpx.Timeout(20.0, read=20.0),
                            )
                            final_u = str(rr.url)
                        except Exception:
                            final_u = u

                        canon_map[raw_norm] = final_u
                        canon_urls.append(final_u)

                    # 2) Normalize canonical URLs for DB comparison
                    only_urls = [_nm_norm(u) for u in canon_urls if u]

                    # de-dupe preserve order
                    seen_local = set()
                    only_urls = [u for u in only_urls if not (u in seen_local or seen_local.add(u))]

                    # ============================================================
                    # ✅ (B) REUSE THE SAME DB CONN (NO async with connection())
                    # ============================================================
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)

                    urls_to_process_set = set(new_only)
                    out.new_urls += len(new_only)

                    # cron-safe fast-exit:
                    if not new_only:
                        if stop_after_index:
                            out.stopped_at_cutoff = True
                        return out

                for it in items:
                    if stop or out.upserted >= limit_each:
                        break

                    if not isinstance(it, dict):
                        continue

                    raw_url = (
                        it.get("url")
                        or it.get("link")
                        or it.get("permalink")
                        or it.get("detailUrl")
                        or it.get("publicUrl")
                        or it.get("WebUrl")
                        or _nm_find_any_http_url(it)
                    )
                    detail_url = _nm_abs_detail_url(raw_url or "")
                    if not detail_url:
                        continue

                    if _nm_is_governor_eo_pdf(detail_url):
                        continue
                    if _nm_is_junk_viewer(detail_url):
                        continue

                    # ✅ IMPORTANT: in cron-safe, swap listing URL -> canonical final URL BEFORE filtering
                    if (not backfill) and urls_to_process_set is not None:
                        detail_url = canon_map.get(_nm_norm(detail_url), detail_url)

                    norm = _nm_norm(detail_url)

                    # ✅ cron-safe: skip anything not in the "new urls" set for this page
                    if (not backfill) and urls_to_process_set is not None and (norm not in urls_to_process_set):
                        continue

                    # ✅ DEBUG + HARD GUARD (pre-fetch)
                    if (not backfill) and conn is not None:
                        exists = await _external_id_exists(conn, source_id, norm)
                        print("NM GUARD prefetch:", norm, "exists=", exists)
                        if exists:
                            continue

                    if norm in seen:
                        continue
                    seen.add(norm)

                    pub_dt = _nm_find_dt_in_item(it)

                    api_blurb = ""
                    for k in ("summary", "description", "excerpt", "teaser", "body", "content", "text", "Summary", "Description", "Body", "Content"):
                        v = it.get(k)
                        if isinstance(v, str) and v.strip():
                            api_blurb = _strip_html_to_text(v).strip()
                            break
                    if api_blurb and len(api_blurb) > 2000:
                        api_blurb = api_blurb[:2000].rstrip() + "…"

                    title = ""
                    body_txt = ""
                    html = ""
                    pdf_text = ""

                    try:
                        dr = await client.get(
                            detail_url,
                            headers=BROWSER_UA_HEADERS,
                            timeout=httpx.Timeout(45.0, read=45.0),
                        )

                        final_url = str(dr.url)
                        ct = (dr.headers.get("content-type") or "").lower()
                        is_pdf = ("application/pdf" in ct) or final_url.lower().endswith(".pdf")

                        final_norm = _nm_norm(final_url)

                        # ============================================================
                        # ✅ (D) HARD GUARD #2: IF REDIRECTED FINAL URL EXISTS IN DB, SKIP
                        # ============================================================
                        if (not backfill) and conn is not None:
                            if await _external_id_exists(conn, source_id, final_norm):
                                continue

                        # ✅ canonicalize to final redirected URL + dedupe on canonical
                        if final_norm != norm:
                            detail_url = final_url
                            norm = final_norm
                            if norm in seen:
                                continue
                            seen.add(norm)

                        if dr.status_code < 400:
                            if not is_pdf:
                                html = dr.text or ""
                                title = _nm_title_from_html(html, _title_from_url_fallback(detail_url))
                                body_txt = _nm_extract_textish(html)

                                if (not body_txt) or (len(body_txt.strip()) < 200):
                                    if api_blurb:
                                        body_txt = api_blurb

                                if not pub_dt:
                                    pub_dt = _nm_parse_dt_from_html(html)

                            else:
                                title = _title_from_url_fallback(detail_url)
                                body_txt = ""

                                pdf_text = ""
                                try:
                                    pdf_bytes = dr.content
                                    reader = PdfReader(io.BytesIO(pdf_bytes))
                                    pages = reader.pages[:2] if reader.pages else []
                                    pdf_text = "\n".join([(p.extract_text() or "") for p in pages]).strip()
                                except Exception:
                                    pdf_text = ""

                                if pdf_text:
                                    body_txt = pdf_text[:35000]
                                else:
                                    body_txt = api_blurb or ""

                            if not pub_dt:
                                pub_dt = _nm_parse_dt_from_url(detail_url)

                            if is_pdf and not pub_dt and pdf_text:
                                pub_dt = _nm_parse_dt_any(pdf_text) or pub_dt

                        else:
                            title = title or _title_from_url_fallback(detail_url)
                            body_txt = body_txt or api_blurb or ""
                            if not pub_dt:
                                pub_dt = _nm_parse_dt_from_url(detail_url)

                    except Exception:
                        title = _title_from_url_fallback(detail_url)
                        body_txt = api_blurb or ""
                        if not pub_dt:
                            pub_dt = _nm_parse_dt_from_url(detail_url)

                    if not pub_dt and api_blurb:
                        pub_dt = _nm_parse_dt_any(api_blurb) or pub_dt
                    if not pub_dt and pdf_text:
                        pub_dt = _nm_parse_dt_any(pdf_text) or pub_dt

                    summary = ""
                    bt = (body_txt or "").strip()

                    if len(bt) >= 200:
                        summary = summarize_text(bt, max_sentences=2, max_chars=700) or ""
                    elif len(bt) >= 60:
                        summary = bt[:700].rstrip()
                    else:
                        ab = (api_blurb or "").strip()
                        if len(ab) >= 60:
                            summary = ab[:700].rstrip()

                    if summary:
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, detail_url)

                    await _upsert_item(
                        url=detail_url,
                        title=title or _title_from_url_fallback(detail_url),
                        summary=summary,
                        jurisdiction=NM_JURISDICTION,
                        agency=NM_AGENCY_NEWS,
                        status=status,
                        source_name=source_name,
                        source_key=source_key,
                        referer=referer,
                        published_at=pub_dt,
                    )
                    out.upserted += 1

                    if _nm_strip_mailchimp_tracking(detail_url) == cutoff:
                        out.stopped_at_cutoff = True
                        stop = True
                        break

                    await asyncio.sleep(0.03)

                if stop_after_index:
                    out.stopped_at_cutoff = True
                    return out

                if not items:
                    stalled_pages += 1
                    if stalled_pages >= 5:
                        break
                else:
                    stalled_pages = 0

                await asyncio.sleep(0.12)

        return out

    finally:
        # ============================================================
        # ✅ (E) ALWAYS CLOSE THE DB CONNECTION (even on early return)
        # ============================================================
        if (not backfill) and (conn_cm is not None):
            await conn_cm.__aexit__(None, None, None)


async def _ingest_new_mexico_executive_orders(*, source_id: UUID, backfill: bool, limit_each: int) -> NMSectionResult:
    """
    1) Scrape 2025–2026 from NM_PUBLIC_PAGES["executive_orders"]
    2) Then scrape 2024 from NM_PUBLIC_PAGES["executive_orders_archive"]
    PDFs are scanned, so summary comes from the description text under each EO link.
    published_at is best-effort from the /uploads/YYYY/MM/ path.
    """
    out = NMSectionResult()
    out = NMSectionResult(mode="backfill" if backfill else "cron_safe")

    if not backfill:
        limit_each = max(25, min(int(limit_each or 500), 1500))
    referer_main = NM_PUBLIC_PAGES["executive_orders"]
    referer_arch = NM_PUBLIC_PAGES["executive_orders_archive"]

    source_name = "New Mexico — Executive Orders"
    source_key = f"{NM_JURISDICTION}:executive_orders"
    status = NM_STATUS_MAP["executive_orders"]

    seen: set[str] = set()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # -------- main page (2025–2026) --------
        r = await client.get(referer_main, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
        r.raise_for_status()
        html = r.text or ""
        blocks = _nm_parse_eo_link_blocks(html, referer_main)

        eo_urls = [pdf_url for (pdf_url, _) in blocks]
        out.fetched_urls += len(eo_urls)

        blocks_to_process = blocks
        if not backfill:
            only_urls = [_nm_norm(u) for u in eo_urls if u]
            seen_local = set()
            only_urls = [u for u in only_urls if not (u in seen_local or seen_local.add(u))]

            async with connection() as conn:
                new_only = await _filter_new_external_ids(conn, source_id, only_urls)

            new_set = set(new_only)
            blocks_to_process = [(u, d) for (u, d) in blocks if _nm_norm(u) in new_set]
            out.new_urls += len(blocks_to_process)

            if not blocks_to_process:
                # don't return yet; archive might still have new things
                pass


        for pdf_url, desc in blocks_to_process:
            if out.upserted >= limit_each:
                break
            n = _nm_norm(pdf_url)
            if n in seen:
                continue
            seen.add(n)

            published_at = _nm_parse_dt_from_pdf_url(pdf_url)
            # title from filename (good enough)
            title = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "").replace("-", " ").strip()
            title = title or pdf_url

            summary = (desc or "").strip()

            # If page description is missing (common for newer EOs), try extracting PDF text
            if not summary:
                try:
                    pr = await client.get(pdf_url, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
                    if pr.status_code < 400:
                        reader = PdfReader(io.BytesIO(pr.content))
                        pages = reader.pages[:2] if reader.pages else []
                        pdf_text = "\n".join([(p.extract_text() or "") for p in pages]).strip()
                        if len(pdf_text) >= 200:
                            summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""
                except Exception:
                    pass

            if summary:
                # don't re-summarize; page already provides a short description
                summary = _soft_normalize_caps(summary)
                summary = await _safe_ai_polish(summary, title, pdf_url)

            await _upsert_item(
                url=pdf_url,
                title=title,
                summary=summary,
                jurisdiction=NM_JURISDICTION,
                agency=NM_AGENCY_GOV,
                status=status,
                source_name=source_name,
                source_key=source_key,
                referer=referer_main,
                published_at=published_at,
            )
            out.upserted += 1

        # -------- archive page (2024) --------
        if out.upserted < limit_each:
            r2 = await client.get(referer_arch, headers=BROWSER_UA_HEADERS, timeout=httpx.Timeout(45.0, read=45.0))
            r2.raise_for_status()
            html2 = r2.text or ""
            blocks2 = _nm_parse_eo_link_blocks(html2, referer_arch)

            # ✅ filter to 2024 only (keep your existing logic)
            blocks2_2024 = [(u, d) for (u, d) in blocks2 if "/uploads/2024/" in (u or "")]

            # ✅ listing-level cutoff trim: include 2024-001 then stop scanning older
            stop_after_index = False
            norms = [_nm_norm(u) for (u, _) in blocks2_2024]
            cutoff_norm = _nm_norm(NM_EO_2024_OLDEST)
            if cutoff_norm in norms:
                idx = norms.index(cutoff_norm)
                blocks2_2024 = blocks2_2024[: idx + 1]
                stop_after_index = True

            out.fetched_urls += len(blocks2_2024)

            blocks2_to_process = blocks2_2024
            if not backfill:
                only_urls = [_nm_norm(u) for (u, _) in blocks2_2024 if u]
                seen_local = set()
                only_urls = [u for u in only_urls if not (u in seen_local or seen_local.add(u))]

                async with connection() as conn:
                    new_only = await _filter_new_external_ids(conn, source_id, only_urls)

                new_set = set(new_only)
                blocks2_to_process = [(u, d) for (u, d) in blocks2_2024 if _nm_norm(u) in new_set]
                out.new_urls += len(blocks2_to_process)

                # ✅ cron-safe fast exit if archive page has nothing new
                if not blocks2_to_process:
                    if stop_after_index:
                        out.stopped_at_cutoff = True
                    return out

            for pdf_url, desc in blocks2_to_process:
                if out.upserted >= limit_each:
                    break

                n = _nm_norm(pdf_url)
                if n in seen:
                    continue
                seen.add(n)

                published_at = _nm_parse_dt_from_pdf_url(pdf_url)
                title = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "").replace("-", " ").strip()
                title = title or pdf_url

                summary = (desc or "").strip()
                if summary:
                    summary = _soft_normalize_caps(summary)
                    summary = await _safe_ai_polish(summary, title, pdf_url)

                await _upsert_item(
                    url=pdf_url,
                    title=title,
                    summary=summary,
                    jurisdiction=NM_JURISDICTION,
                    agency=NM_AGENCY_GOV,
                    status=status,
                    source_name=source_name,
                    source_key=source_key,
                    referer=referer_arch,
                    published_at=published_at,
                )
                out.upserted += 1

                # ✅ stop once we include the first EO of 2024
                if _nm_norm(pdf_url) == cutoff_norm:
                    out.stopped_at_cutoff = True
                    break

            if stop_after_index:
                out.stopped_at_cutoff = True
                return out

    return out

async def ingest_new_mexico(*, limit_each: int = 5000, max_pages_each: int = 500) -> dict:
    source_name_news = "New Mexico — Latest News (nm.gov)"
    source_key_news = f"{NM_JURISDICTION}:nm_gov_news"
    source_name_eo = "New Mexico — Executive Orders"
    source_key_eo = f"{NM_JURISDICTION}:executive_orders"

    referer_news = NM_PUBLIC_PAGES["news"]
    referer_eo = NM_PUBLIC_PAGES["executive_orders"]

    async with connection() as conn:
        src_news = await get_or_create_source(conn, source_name_news, source_key_news, referer_news)
        src_eo = await get_or_create_source(conn, source_name_eo, source_key_eo, referer_eo)

        news_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_news) or 0
        eo_existing = await conn.fetchval("select count(*) from items where source_id=$1", src_eo) or 0

    news_backfill = (news_existing == 0)
    eo_backfill = (eo_existing == 0)

    print("NM DEBUG existing:", {"news_existing": news_existing, "eo_existing": eo_existing})
    print("NM DEBUG backfill:", {"news_backfill": news_backfill, "eo_backfill": eo_backfill})

    news = await _ingest_new_mexico_news(
        source_id=src_news,
        backfill=news_backfill,
        limit_each=limit_each,
        max_pages_each=max_pages_each,
    )
    eos = await _ingest_new_mexico_executive_orders(
        source_id=src_eo,
        backfill=eo_backfill,
        limit_each=limit_each,
    )

    # ✅ terminal prints (like other states)
    print(
        f"NM NEWS mode={'backfill' if news_backfill else 'cron_safe'} "
        f"new={news.upserted if news_backfill else news.new_urls} "
        f"fetched={news.fetched_urls} seen_total={news_existing} "
        f"cutoff={news.stopped_at_cutoff}"
    )
    print(
        f"NM EO mode={'backfill' if eo_backfill else 'cron_safe'} "
        f"new={eos.upserted if eo_backfill else eos.new_urls} "
        f"fetched={eos.fetched_urls} seen_total={eo_existing} "
        f"cutoff={eos.stopped_at_cutoff}"
    )

    return {
        "ok": True,
        "state": NM_JURISDICTION,
        "counts": {
            "press_releases": {
                "fetched_urls": news.fetched_urls,
                "new_urls": news.new_urls,
                "upserted": news.upserted,
                "stopped_at_cutoff": news.stopped_at_cutoff,
                "mode": news.mode,
                "seen_total": news_existing,
            },
            "executive_orders": {
                "fetched_urls": eos.fetched_urls,
                "new_urls": eos.new_urls,
                "upserted": eos.upserted,
                "stopped_at_cutoff": eos.stopped_at_cutoff,
                "mode": eos.mode,
                "seen_total": eo_existing,
            },
        },
    }


# ----------------------------
# Registry of v3 ingesters
# ----------------------------

INGESTERS_V3 = {
    "michigan": ingest_michigan,
    "mi": ingest_michigan,  # alias
    "tennessee": ingest_tennessee,
    "tn": ingest_tennessee,  # alias
    "north_carolina": ingest_north_carolina,
    "nc": ingest_north_carolina,
    "south_carolina": ingest_south_carolina,
    "sc": ingest_south_carolina,
    "oregon": ingest_oregon,
    "or": ingest_oregon,
    "nevada": ingest_nevada,
    "nv": ingest_nevada,  # alias
    "wisconsin": ingest_wisconsin,
    "wi": ingest_wisconsin,
    "iowa": ingest_iowa,
    "ia": ingest_iowa,
    "missouri": ingest_missouri,
    "mo": ingest_missouri,
    "kansas": ingest_kansas,
    "ks": ingest_kansas,
    "new_mexico": ingest_new_mexico,
    "nm": ingest_new_mexico,
}
