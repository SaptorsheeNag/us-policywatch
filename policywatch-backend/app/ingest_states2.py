# app/ingest_states2.py
from __future__ import annotations
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)
import xml.etree.ElementTree as ET
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, urljoin, unquote
import html as _html
import httpx
from email.utils import parsedate_to_datetime
import json
from .db import connection
from .ingest_federal_register import get_or_create_source
from .summarize import (
    summarize_extractive,
    summarize_text,
    _soft_normalize_caps,
    BROWSER_UA_HEADERS,
    _strip_html_to_text,
)
from .ai_summarizer import ai_polish_summary
import io
from pypdf import PdfReader
import os
# ----------------------------
# PDF extraction (robust)
# ----------------------------
_pdf_extract_text = None
try:
    from pdfminer.high_level import extract_text as _pdf_extract_text
except Exception:
    _pdf_extract_text = None

def _extract_pdf_text_from_bytes(data: bytes) -> str:
    """
    Best-effort PDF -> text.
    1) pdfminer.six (best for layout/text PDFs)
    2) pypdf fallback (better than returning "")
    """
    if not data:
        return ""

    # 1) pdfminer
    if _pdf_extract_text is not None:
        try:
            bio = io.BytesIO(data)
            return _pdf_extract_text(bio) or ""
        except Exception:
            pass

    # 2) pypdf fallback
    try:
        from pypdf import PdfReader
        bio = io.BytesIO(data)
        reader = PdfReader(bio)
        out = []
        for page in reader.pages:
            try:
                out.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(out).strip()
    except Exception:
        return ""

# ----------------------------
# Ohio config
# ----------------------------
# ----------------------------
# Ohio config
# ----------------------------

# Public “landing” pages (good for referer + defines section identity)
OH_PUBLIC_PAGES = {
    "news": "https://governor.ohio.gov/media/news-and-media",
    "appointments": "https://governor.ohio.gov/media/appointments",
    "executive_orders": "https://governor.ohio.gov/media/executive-orders",
}

# The real XHR listing endpoints (what you captured in DevTools)
OH_COMPONENT_LISTING = {
    "news": "https://governor.ohio.gov/wps/wcm/connect/gov/Ohio%20Content%20English/governor"
            "?source=library&srv=cmpnt&cmpntid=67577e5a-f3af-4498-a9e3-0696d21ac7c9"
            "&location=Ohio%20Content%20English%2F%2Fgovernor%2Fmedia%2Fnews-and-media&category=",

    "appointments": "https://governor.ohio.gov/wps/wcm/connect/gov/Ohio%20Content%20English/governor"
                    "?source=library&srv=cmpnt&cmpntid=67577e5a-f3af-4498-a9e3-0696d21ac7c9"
                    "&location=Ohio%20Content%20English%2F%2Fgovernor%2Fmedia%2Fappointments&category=",

    "executive_orders": "https://governor.ohio.gov/wps/wcm/connect/gov/Ohio%20Content%20English/governor"
                        "?source=library&srv=cmpnt&cmpntid=aa7a4aa9-f871-4f6e-a646-3b6a162a51dd"
                        "&location=Ohio%20Content%20English%2F%2Fgovernor%2Fmedia%2Fexecutive-orders&category=",
}

# Where the detail pages actually live (the URLs you want to ingest)
OH_SECTION_PREFIX = {
    "news": "/media/news-and-media/",
    "appointments": "/media/appointments/",
    "executive_orders": "/media/executive-orders/",
}


# Your stop cutoffs
OH_NEWS_CUTOFF_URL = "https://governor.ohio.gov/media/news-and-media/governor-dewine-signs-bills-into-law-issues-line-item-vetoes"
OH_APPTS_CUTOFF_URL = "https://governor.ohio.gov/media/appointments/governor-dewine-appoints-washburn-to-chillicothe-municipal-court"

STATUS_MAP = {
    "news": "news",
    "appointments": "appointment",
    "executive_orders": "executive_order",
}

# ----------------------------
# Arizona config (Drupal Views AJAX)
# ----------------------------

AZ_PUBLIC_PAGES = {
    "press_releases": "https://azgovernor.gov/news-releases",
    "executive_orders": "https://azgovernor.gov/executive-orders",

    # ✅ NEW: Proclamations live on goyff.az.gov
    "proclamations": "https://goyff.az.gov/proclamations",
}

AZ_VIEWS_AJAX = "https://azgovernor.gov/views/ajax"

# stop at the first item of 2025 (inclusive)
AZ_PRESS_CUTOFF_URL = "https://azgovernor.gov/office-arizona-governor/news/2025/01/transcript-governor-hobbs-2025-state-state-address"

# ✅ NEW: proclamations listing + cutoff (first item of 2024)
AZ_PROC_LISTING_URL = "https://goyff.az.gov/proclamations?field_featured_categories_tid%5B4%5D=4&sort_order=DESC&sort_by=created&page=0%2C0%2C0"
AZ_PROC_CUTOFF_URL  = "https://goyff.az.gov/proclamations/HTPM2024"

AZ_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    # ✅ NEW
    "proclamations": "proclamation",
}

# ----------------------------
# Virginia config (static listing pages)
# ----------------------------

VA_PUBLIC_PAGES = {
    "news_releases": "https://www.governor.virginia.gov/newsroom/news-releases/",
    "proclamations": "https://www.governor.virginia.gov/newsroom/proclamations/",
    "executive_orders": "https://www.governor.virginia.gov/executive-actions/#orders",
}

VA_NEWS_FEED_URL = "https://www.governor.virginia.gov/newsroom/news-releases/govnewsfeed.php/?137083"
VA_NEWS_LATEST_URL = "https://www.governor.virginia.gov/newsroom/news-releases/2025/december/name-1072620-en.html"


# stop at the first item of 2025 (inclusive)
VA_NEWS_CUTOFF_URL = "https://www.governor.virginia.gov/newsroom/news-releases/2025/january/name-1038144-en.html"

VA_STATUS_MAP = {
    "news_releases": "news",
    "proclamations": "proclamation",
    "executive_orders": "executive_order",
}

# --- Virginia listing scanners ---
_VA_NEWS_DETAIL_PATH_RE = re.compile(r"^/newsroom/news-releases/.*\.html?$", re.I)

_VA_NEWS_YEAR_IN_URL_RE = re.compile(r"/newsroom/news-releases/(\d{4})/", re.I)

# ----------------------------
# Georgia config
# ----------------------------

GA_PUBLIC_PAGES = {
    "press_releases_2025": "https://gov.georgia.gov/press-releases/2025",
    # ✅ NEW (EO home page that lists years)
    "executive_orders_home": "https://gov.georgia.gov/executive-action/executive-orders",
}

GA_STATUS_MAP = {
    "press_releases": "press_release",
    # ✅ NEW
    "executive_orders": "executive_order",
}

GA_JURISDICTION = "georgia"
GA_AGENCY = "Georgia Governor"

# accept /press-releases/YYYY-MM-DD/...
_GA_PRESS_DETAIL_RE = re.compile(r"^/press-releases/20\d{2}-\d{2}-\d{2}/", re.I)

_GA_DATE_IN_PATH_RE = re.compile(r"^/press-releases/(?P<d>\d{4}-\d{2}-\d{2})/", re.I)

def _date_from_ga_url(url: str) -> datetime | None:
    try:
        path = urlsplit(url).path
        m = _GA_DATE_IN_PATH_RE.match(path)
        if not m:
            return None
        d = m.group("d")
        return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    
# ----------------------------
# Georgia EO helpers
# ----------------------------

GA_EO_CUTOFF_URL = "https://gov.georgia.gov/document/2024-executive-order/02062405/download"

# allow trailing slash, querystrings, etc.
_GA_EO_YEAR_LINK_RE = re.compile(
    r'href=["\'](?P<u>(?:https?://gov\.georgia\.gov)?/executive-action/executive-orders/\d{4})(?:/)?(?:\?[^"\']*)?["\']',
    re.I,
)

# Captures:
#   1) download link: /document/2026-executive-order/01082601/download
#   2) document number text: 01.08.26.01
#   3) description cell HTML (we’ll strip tags)
_GA_EO_ROW_RE = re.compile(
    r"<tr[^>]*>.*?"
    r'href=["\'](?P<href>/document/\d{4}-executive-order/\d+/download)["\'][^>]*>(?P<num>.*?)</a>'
    r".*?</td>\s*<td[^>]*>(?P<desc>.*?)</td>"
    r".*?</tr>",
    re.I | re.S,
)

def _ga_norm_abs(u: str) -> str:
    if not u:
        return ""
    if u.startswith("http"):
        return urlunsplit(urlsplit(u)._replace(query="", fragment=""))
    return urlunsplit(urlsplit(urljoin("https://gov.georgia.gov", u))._replace(query="", fragment=""))

def _ga_eo_date_from_number(num: str) -> datetime | None:
    """
    num like '12.31.24.01' => 2024-12-31 UTC
    """
    try:
        parts = [p.strip() for p in (num or "").split(".")]
        if len(parts) < 3:
            return None
        mm = int(parts[0])
        dd = int(parts[1])
        yy = int(parts[2])
        year = 2000 + yy  # GA site is modern; treat YY as 20YY
        return datetime(year, mm, dd, tzinfo=timezone.utc)
    except Exception:
        return None

async def _collect_ga_eo_year_urls(cx: httpx.AsyncClient, home_url: str) -> list[str]:
    r = await _get(cx, home_url)
    if r.status_code >= 400 or not r.text:
        return []

    years = []
    for m in _GA_EO_YEAR_LINK_RE.finditer(r.text):
        years.append(_ga_norm_abs(m.group("u")))

    years = list(dict.fromkeys(years))  # dedupe preserve order

    # Sort descending by year in path
    def _year_key(u: str) -> int:
        m2 = re.search(r"/(\d{4})$", urlsplit(u).path or "")
        return int(m2.group(1)) if m2 else 0

    years.sort(key=_year_key, reverse=True)
    return years

async def _collect_ga_eo_rows_from_year_page(
    cx: httpx.AsyncClient,
    year_url: str,
) -> list[tuple[str, str, str, datetime | None]]:
    """
    Returns list of (download_url, eo_number, description_text, published_at)
    in page order (site is typically newest-first).
    """
    r = await _get(cx, year_url, headers={"Referer": GA_PUBLIC_PAGES["executive_orders_home"]})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text
    out: list[tuple[str, str, str, datetime | None]] = []

    for m in _GA_EO_ROW_RE.finditer(html):
        href = m.group("href") or ""
        num_raw = m.group("num") or ""
        num_txt = _strip_html_to_text(num_raw).strip()

        # pull the dotted EO number from whatever text is inside the link
        mm = re.search(r"\b(\d{2}\.\d{2}\.\d{2}\.\d{2})\b", num_txt)
        num = mm.group(1) if mm else num_txt

        desc_html = m.group("desc") or ""
        desc_txt = _strip_html_to_text(desc_html).strip()

        dl = _ga_norm_abs(href)
        pub_dt = _ga_eo_date_from_number(num)

        out.append((dl, num, desc_txt, pub_dt))

    # Dedup by URL (preserve order)
    seen = set()
    deduped = []
    for row in out:
        if row[0] in seen:
            continue
        seen.add(row[0])
        deduped.append(row)

    return deduped



def _va_year_from_news_url(u: str) -> int | None:
    if not u:
        return None
    m = _VA_NEWS_YEAR_IN_URL_RE.search(u)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

# <link rel="prev" href="...">
_VA_LINK_REL_PREV_RE = re.compile(
    r'(?is)<link[^>]+rel=["\'](?:prev|previous)["\'][^>]+href=["\'](?P<href>[^"\']+)["\']'
)

# anchors that *indicate* previous via aria-label/title/class, or contain "Previous"
_VA_PREV_ANCHOR_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\']'
    r'[^>]*(?:aria-label|title)=["\']\s*(?:Previous|Prev|Older)\b[^"\']*["\']'
)
_VA_PREV_TEXT_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>\s*[^<]{0,40}\b(?:Previous|Prev|Older)\b'
)
_VA_NAV_NEWS_LINK_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>/newsroom/news-releases/[^"\']+?\.html?)["\'][^>]*>'
)

async def _collect_va_news_urls_by_prev_links(
    cx: httpx.AsyncClient,
    *,
    start_url: str,
    max_urls: int = 5000,
    stop_at_url: str | None = None,
) -> list[tuple[str, datetime | None]]:
    """
    Walk VA news releases by following the 'Previous' link on each detail page.
    Returns [(url, published_at)] in newest->oldest order (up to stop_at_url inclusive).
    """
    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    cur = start_url
    for _ in range(max_urls):
        if not cur or cur in seen:
            break
        seen.add(cur)

        r = await _get(cx, cur, headers={"Referer": VA_PUBLIC_PAGES["news_releases"]})
        if r.status_code >= 400 or not r.text:
            break

        html = _nz(r.text)
        pub_dt = _date_from_va_news(html, cur)  # "For Immediate Release: ..."
        pub_dt = _date_guard_not_future(pub_dt)

        out.append((cur, pub_dt))

        if stop_at_url and cur == stop_at_url:
            break

        # find prev link (prefer <link rel="prev">, fallback to anchor text)
        prev_href = ""

        # 1) strongest signal
        m = _VA_LINK_REL_PREV_RE.search(html)
        if m:
            prev_href = (m.group("href") or "").strip()

        # 2) aria-label/title
        if not prev_href:
            m = _VA_PREV_ANCHOR_RE.search(html)
            if m:
                prev_href = (m.group("href") or "").strip()

        # 3) visible text contains Previous/Older
        if not prev_href:
            m = _VA_PREV_TEXT_RE.search(html)
            if m:
                prev_href = (m.group("href") or "").strip()

        # 4) fallback: if there is a "nav" section with exactly two news links,
        #    pick the one that is NOT the current URL (common prev/next widget).
        if not prev_href:
            cands = []
            for mm in _VA_NAV_NEWS_LINK_RE.finditer(html.replace("\\/", "/")):
                u = _abs_va(mm.group("href"))
                if u and u != cur and _VA_NEWS_DETAIL_PATH_RE.match(urlsplit(u).path):
                    cands.append(u)
            # pick the first distinct candidate
            if cands:
                prev_href = cands[0]

        if not prev_href:
            print("VA NEWS prev link not found on:", cur)
            break

        nxt = _abs_va(prev_href)
        # safety: only stay within VA news releases
        if not _VA_NEWS_DETAIL_PATH_RE.match(urlsplit(nxt).path):
            break

        cur = nxt

    return out


async def _collect_va_news_urls(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    max_urls: int = 5000,
    stop_at_url: str | None = None,
) -> List[str]:
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        print("VA NEWS LIST fetch failed:", r.status_code, "len=", len(r.text or ""))
        return []

    html = r.text.replace("\\/", "/")
    print("VA NEWS LIST len=", len(html), "count('/newsroom/news-releases/')=", html.count("/newsroom/news-releases/"))
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    out: List[str] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue

        # normalize to absolute
        u = _abs_va(href)
        path = urlsplit(u).path

        if not _VA_NEWS_DETAIL_PATH_RE.match(path):
            continue

        if u in seen:
            continue

        seen.add(u)
        out.append(u)

        if len(out) <= 5:
            print("VA NEWS found:", u)

        if stop_at_url and u == stop_at_url:
            break
        if len(out) >= max_urls:
            break

    return out



_VA_PROC_ROW_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>.*?</a>\s*~\s*(?P<rest>[^<]+)'
)
_VA_YEAR_RE = re.compile(r'\b(20\d{2})\b')

# --- VA date parsing helpers ---

_VA_FOR_IMMEDIATE_RE = re.compile(r'For Immediate Release:\s*([A-Za-z]{3,9}\.?\s+\d{1,2},\s+\d{4})', re.I)

def _date_from_va_news(html: str, url: str) -> datetime | None:
    """
    VA news pages include: "For Immediate Release: December 22, 2025"
    """
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _VA_FOR_IMMEDIATE_RE.search(text)
    if m:
        dt = _parse_us_month_date(m.group(1))
        return _date_guard_not_future(dt) if dt else None
    return None


# proclamations listing shows: "<a ...>Title</a>  ~ 16 Jan 2026" OR "~ 12 Jan 2026 to 16 Jan 2026"
_VA_LISTING_DATE_DMY_RE = re.compile(
    r'\b(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(20\d{2})\b',
    re.I
)

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}

_VA_PROC_DETAIL_PATH_RE = re.compile(r"^/newsroom/proclamations/proclamation-list/", re.I)

_VA_ANY_DATE_MDY_RE = re.compile(
    r'\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|'
    r'Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2},\s+(20\d{2})\b',
    re.I
)

def _parse_any_va_date(s: str) -> datetime | None:
    s = re.sub(r"\s+", " ", (s or "").strip())

    m = _VA_LISTING_DATE_DMY_RE.search(s)
    if m:
        day = int(m.group(1))
        mon = _MONTH_ABBR.get(m.group(2).lower(), 0)
        yr = int(m.group(3))
        if mon:
            try:
                return datetime(yr, mon, day, tzinfo=timezone.utc)
            except Exception:
                pass

    m2 = _VA_ANY_DATE_MDY_RE.search(s)
    if m2:
        # reuse your existing parser for "Month DD, YYYY"
        dt = _parse_us_month_date(m2.group(0))
        return dt

    return None

async def _collect_va_proclamation_urls_with_dates(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    years: set[int] | None = None,
    max_urls: int = 5000,
    stop_at_url: str | None = None,
) -> list[tuple[str, datetime | None]]:
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        print("VA PROC LIST fetch failed:", r.status_code, "len=", len(r.text or ""))
        return []

    html = r.text.replace("\\/", "/")
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    # 1) collect ALL unique (url, dt) pairs first
    pairs: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue

        u = _abs_va(href)
        path = urlsplit(u).path
        if not _VA_PROC_DETAIL_PATH_RE.match(path):
            continue
        if u in seen:
            continue

        # parse date from nearby context
        start = max(m.start() - 80, 0)
        end = min(m.end() + 400, len(html))
        context = html[start:end]
        context_text = _strip_html_to_text(context)

        dt = _parse_any_va_date(context_text)
        if years and dt and dt.year not in years:
            continue

        seen.add(u)
        pairs.append((u, dt))

    if not pairs:
        return []

    # 2) sort newest -> oldest (dates missing go last)
    pairs.sort(key=lambda x: (x[1] is not None, x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)), reverse=True)

    # 3) apply stop_at_url AFTER sorting (inclusive stop)
    out: list[tuple[str, datetime | None]] = []
    for (u, dt) in pairs:
        out.append((u, dt))
        if stop_at_url and u == stop_at_url:
            break
        if len(out) >= max_urls:
            break

    return out

# ----------------------------
# Hawaii config (WordPress category listings)
# ----------------------------

HI_PUBLIC_PAGES = {
    "all_newsroom": "https://governor.hawaii.gov/category/newsroom/",
    "press_releases": "https://governor.hawaii.gov/category/newsroom/office-of-the-governor-press-releases/",
    "executive_orders": "https://governor.hawaii.gov/category/newsroom/executive-orders/",
    "proclamations": "https://governor.hawaii.gov/category/newsroom/emergency-proclamations/",
}

# stop at the first item of 2025 (inclusive)
HI_PRESS_CUTOFF_URL = "https://governor.hawaii.gov/newsroom/office-of-the-governor-statement-gov-green-issues-statement-on-salt-lake-fireworks-tragedy/"

# stop at the first proclamation of 2025 (inclusive)
HI_PROC_CUTOFF_PDF_URL = "https://governor.hawaii.gov/wp-content/uploads/2025/01/2501078_Fourth-Proclamation-Relating-to-School-Bus-Services.pdf"

HI_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

_HI_NEWSROOM_DETAIL_RE = re.compile(r"^https?://governor\.hawaii\.gov/newsroom/[^#?]+/?$", re.I)
_HI_PDF_RE = re.compile(r"^https?://governor\.hawaii\.gov/wp-content/uploads/\d{4}/\d{2}/[^#?]+\.pdf$", re.I)

_HI_HREF_RE = re.compile(r'(?is)href=["\']([^"\']+)["\']')

_HI_ENTRY_RE = re.compile(
    r'(?is)<h[23][^>]*>\s*'
    r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>'
)

_HI_UPLOAD_PDF_RE = re.compile(
    r'(?is)href=["\'](?P<href>(?:https?://governor\.hawaii\.gov)?/wp-content/uploads/\d{4}/\d{2}/[^"\']+\.pdf)["\']'
)

_HI_TIME_DT_RE = re.compile(r'(?is)<time[^>]+datetime=["\']([^"\']+)["\']')

def _hi_first_pdf_from_html(html: str) -> str:
    """
    Find first uploads PDF in a detail page HTML.
    Supports both absolute and relative URLs.
    """
    m = _HI_UPLOAD_PDF_RE.search(html or "")
    if not m:
        return ""
    href = (m.group("href") or "").strip()
    if not href:
        return ""
    return clean_url(urljoin("https://governor.hawaii.gov/", href))

def _hi_posted_dt_from_listing_chunk(chunk: str) -> datetime | None:
    """
    Listing pages typically include either:
      - <time datetime="...">
      - or "Posted on Jan 23, 2026"
    We accept either.
    """
    m = _HI_TIME_DT_RE.search(chunk or "")
    if m:
        raw = (m.group(1) or "").strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            return _date_guard_not_future(dt)
        except Exception:
            pass

    # fallback: "Jan 23, 2026"
    text = re.sub(r"(?is)<[^>]+>", " ", chunk or "")
    text = re.sub(r"\s+", " ", text).strip()
    m2 = _US_MONTH_DATE_RE.search(text)
    if m2:
        dt2 = _parse_us_month_date(m2.group(0))
        return _date_guard_not_future(dt2) if dt2 else None

    return None

def _hi_is_detail_page_url(u: str) -> bool:
    """
    Accept internal post/detail pages, reject ONLY the archive listing pages.
    Hawaii EO/PROC permalinks may include /category/newsroom/.../<slug>/, so don't block that.
    """
    u = clean_url(u or "")
    if not u:
        return False
    ul = u.lower()

    if not ul.startswith("https://governor.hawaii.gov/"):
        return False

    # reject direct uploads
    if "/wp-content/uploads/" in ul or ul.endswith(".pdf"):
        return False

    # reject paginated archive pages
    if "/page/" in ul:
        return False

    # reject the category root pages themselves (archives), BUT allow deeper "post" URLs under them
    # e.g. allow: /category/newsroom/executive-orders/some-post/
    # reject:  /category/newsroom/executive-orders/
    if "/category/newsroom/" in ul:
        # if it's exactly an archive root (no extra path beyond the category), reject
        # (rough check: ends with one of the known category slugs + "/")
        archive_roots = [
            "/category/newsroom/executive-orders/",
            "/category/newsroom/emergency-proclamations/",
            "/category/newsroom/office-of-the-governor-press-releases/",
            "/category/newsroom/",
        ]
        for root in archive_roots:
            if ul.rstrip("/") + "/" == ("https://governor.hawaii.gov" + root):
                return False
        # otherwise it's a deeper link -> likely a post permalink -> accept
        return True

    # accept other non-upload internal pages (like /newsroom/<slug>/)
    return True



# ----------------------------
# New Jersey config (static archives)
# ----------------------------

NJ_PUBLIC_PAGES = {
    "press_releases": "https://www.nj.gov/governor/news/2026/approved/news_archive.shtml",
    "executive_orders": "https://nj.gov/infobank/eo/056murphy/approved/eo_archive.shtml",
    "administrative_orders": "https://nj.gov/governor/news/ao/approved/ao_archive.shtml",
}

# ✅ FIRST press release of the new governor (inclusive cutoff)
NJ_PRESS_CUTOFF_URL = "https://www.nj.gov/governor/news/2026/approved/20260120a.shtml"

# ✅ only new governor years going forward
NJ_PRESS_YEAR_MIN = 2026

NJ_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "administrative_orders": "administrative_order",
}

NJ_JURISDICTION = "new_jersey"
NJ_AGENCY = "New Jersey Governor"

def _abs_nj(u: str) -> str:
    if not u:
        return ""
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u.split("?")[0]
    if not u.startswith("/"):
        u = "/" + u
    # ✅ prefer www.nj.gov (matches new governor pages exactly)
    return ("https://www.nj.gov" + u).split("?")[0]

# NEW format only:
#   /governor/news/2026/approved/20260122a.shtml
_NJ_PRESS_DETAIL_RE = re.compile(
    r"^/governor/news/(?P<year>20\d{2})/approved/(?P<ymd>\d{8})(?P<suf>[a-z])\.shtml$",
    re.I,
)


def _date_from_nj_press_url(url: str) -> datetime | None:
    """
    Extracts YYYYMMDD from the press release URL.
    """
    try:
        path = urlsplit(url).path
        m = _NJ_PRESS_DETAIL_RE.match(path)
        if not m:
            return None
        ymd = m.group("ymd")
        dt = datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=timezone.utc)
        return _date_guard_not_future(dt)
    except Exception:
        return None
    
def _nj_title_from_url(url: str) -> str:
    path = urlsplit(url).path
    fname = path.rsplit("/", 1)[-1]
    fname = re.sub(r"\.shtml$", "", fname, flags=re.I)

    # strip leading date like 20250102a
    fname = re.sub(r"^\d{8}[a-z]_", "", fname, flags=re.I)

    # fallback: title case slug
    return (
        fname.replace("-", " ")
             .replace("_", " ")
             .strip()
             .title()
    )

def _nj_press_archive_url(year: int) -> str:
    return f"https://www.nj.gov/governor/news/{year}/approved/news_archive.shtml"


async def _collect_nj_press_release_pairs(
    cx: httpx.AsyncClient,
    *,
    year_min: int = 2026,
    cutoff_url: str,
    max_year_probe: int = 5,
    limit: int = 20000,
) -> list[tuple[str, datetime | None]]:
    """
    Collect press release detail URLs across:
      - current year down to year_min (inclusive)
      - STOP once cutoff_url is reached (inclusive)

    Uses only:
      https://www.nj.gov/governor/news/{year}/approved/news_archive.shtml
    """
    now_year = datetime.now(timezone.utc).year

    start_year = max(now_year, year_min)
    years_to_fetch = list(range(start_year, year_min - 1, -1))
    years_to_fetch = years_to_fetch[: max_year_probe + 1]  # safety cap

    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for y in years_to_fetch:
        archive_url = _nj_press_archive_url(y)
        r = await _get(cx, archive_url, headers={"Referer": archive_url})
        if r.status_code >= 400 or not r.text:
            # normal if future-year archive doesn't exist yet
            continue

        html = r.text.replace("\\/", "/")

        page_urls: list[str] = []
        for m in href_re.finditer(html):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            u = _abs_nj(href)
            path = urlsplit(u).path
            mm = _NJ_PRESS_DETAIL_RE.match(path)
            if not mm:
                continue

            yr = int(mm.group("year"))
            if yr != y:
                continue

            page_urls.append(u)

        for u in page_urls:
            if u in seen:
                continue
            seen.add(u)
            out.append((u, _date_from_nj_press_url(u)))

        if len(out) >= limit:
            break

    # newest -> oldest
    out.sort(
        key=lambda x: (x[1] is not None, x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)),
        reverse=True,
    )

    # stop at cutoff (inclusive)
    final: list[tuple[str, datetime | None]] = []
    for (u, dt) in out:
        final.append((u, dt))
        if u == cutoff_url:
            break

    return final

def _extract_nj_press_title(html: str) -> str:
    """
    NJ governor press release pages are old-school HTML tables and often do NOT have <h1>.
    This extracts a good headline by checking multiple patterns and ranking candidates.
    """
    if not html:
        return ""

    blob = html.replace("\\/", "/")

    candidates: list[str] = []

    # 1) og:title / twitter:title / generic meta title patterns
    meta_pats = [
        r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+name=["\']title["\'][^>]+content=["\'](.*?)["\']',
        r'(?is)<meta[^>]+name=["\']DC\.title["\'][^>]+content=["\'](.*?)["\']',
    ]
    for pat in meta_pats:
        m = re.search(pat, blob)
        if m:
            t = _clean_nj_title(m.group(1))
            if t:
                candidates.append(t)

    # 2) class-based headline containers (NJ sometimes uses these)
    class_pats = [
        r'(?is)class=["\']pressrelease["\'][^>]*>(.*?)</(?:td|div|span)>',
        r'(?is)class=["\'](?:headline|title|page-title|entry-title)["\'][^>]*>(.*?)</(?:h1|h2|div|span|td)>',
    ]
    for pat in class_pats:
        m = re.search(pat, blob)
        if m:
            t = _clean_nj_title(m.group(1))
            if t:
                candidates.append(t)

    # 3) centered bold headline near top (VERY common on NJ legacy pages)
    # pick the *best* <b> candidate, not just the first one
    for m in re.finditer(r'(?is)<b[^>]*>\s*([^<]{8,300}?)\s*</b>', blob):
        t = _clean_nj_title(m.group(1))
        if not t:
            continue
        # avoid boilerplate lines often bolded
        tl = t.lower()
        if "for immediate release" in tl:
            continue
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", t):  # just a date
            continue
        candidates.append(t)

    # 4) <title> tag fallback (often includes real headline + suffix)
    m = re.search(r'(?is)<title[^>]*>(.*?)</title>', blob)
    if m:
        t = _clean_nj_title(m.group(1))
        if t:
            candidates.append(t)

    # 5) Fallback: derive headline from visible body text near the top
    try:
        txt = _strip_html_to_text(blob)
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines()]
        lines = [ln for ln in lines if 10 <= len(ln) <= 180]

        # drop common non-headline lines
        drop_re = re.compile(
            r"(?i)^(posted on|updated|print|share|translate|back to top|home|administration|key initiatives|news and events|contact us)$"
        )

        cleaned = []
        for ln in lines[:80]:  # only scan the top
            if drop_re.search(ln):
                continue
            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", ln):
                continue
            if _is_generic_nj_title(ln):
                continue
            cleaned.append(ln)

        if cleaned:
            candidates.append(cleaned[0])
    except Exception:
        pass

    # --- rank + return best ---
    # Dedup while preserving order
    seen = set()
    uniq: list[str] = []
    for t in candidates:
        key = t.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(t)

    # drop generic
    uniq = [t for t in uniq if not _is_generic_nj_title(t)]

    if not uniq:
        return ""

    # Prefer longer, more “headline-like” titles (but not absurdly long)
    def score(t: str) -> tuple[int, int]:
        tl = t.lower()
        # penalize obviously non-headline junk
        penalty = 0
        if "http" in tl:
            penalty -= 50
        if "pdf" in tl:
            penalty -= 10
        # reward reasonable length
        ln = len(t)
        return (penalty + min(ln, 140), ln)

    uniq.sort(key=score, reverse=True)
    return uniq[0].strip()

def _is_generic_nj_title(t: str) -> bool:
    if not t:
        return True

    tl = re.sub(r"\s+", " ", t.strip().lower())

    if tl in {"news", "press release", "press releases", "home"}:
        return True

    # ✅ governor-site chrome line usually includes both Governor and Lt. Governor
    if ("governor" in tl) and ("lt. governor" in tl):
        # looks like header, not a headline
        if ("|" in tl) or ("·" in tl) or len(tl) <= 120:
            return True

    hard_bad = [
        "official site of the state of new jersey",
        "official site of the state of new jersey",
        "state of new jersey",
        "office of the governor",
        "nj.gov",
        "services | agencies | faqs",
    ]

    if any(x in tl for x in hard_bad):
        if len(tl) <= 90 or ("|" in tl) or (" - " in tl) or ("·" in tl):
            return True

    return False

def _clean_nj_title(t: str) -> str:
    t = _html.unescape(t or "")
    t = re.sub(r"(?is)<[^>]+>", " ", t)          # strip tags if any slipped in
    t = re.sub(r"\s+", " ", t).strip()
    # remove common suffixes found in <title> tags
    t = re.sub(r"(?i)\s*[\-|–|—]\s*Governor.*$", "", t).strip()
    t = re.sub(r"(?i)\s*\|\s*Governor.*$", "", t).strip()
    t = re.sub(r"(?i)\s*\|\s*State of New Jersey.*$", "", t).strip()
    return t

# ✅ EO rolling window (future-proof)
# ingest current year + previous year by default (keeps runtime reasonable)
NJ_EO_YEARS_ROLLING = 2
NJ_EO_YEAR_MIN = 2024  # keep your historical floor if you want

NJ_EO_MURPHY_PAGE = "https://nj.gov/infobank/eo/056murphy/approved/eo_archive.shtml"
NJ_EO_MURPHY_CUTOFF_PDF = _abs_nj("https://nj.gov/infobank/eo/056murphy/pdf/EO-350.pdf")  # inclusive
# (or just hardcode www)
# NJ_EO_MURPHY_CUTOFF_PDF = "https://www.nj.gov/infobank/eo/056murphy/pdf/EO-350.pdf"


_NJ_EO_PDF_NUM_RE = re.compile(r"^/infobank/eo/(?P<govdir>\d{3}[a-z]+)/pdf/EO-(?P<num>\d+)\.pdf$", re.I)

def _nj_eo_num_from_url(u: str) -> int | None:
    try:
        path = urlsplit(_abs_nj(u)).path
        m = _NJ_EO_PDF_NUM_RE.match(path)
        return int(m.group("num")) if m else None
    except Exception:
        return None

async def _collect_nj_eo_pdf_pairs_until_cutoff(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    cutoff_pdf_url: str,
    limit: int = 20000,
) -> list[tuple[str, datetime | None]]:
    """
    Collect EO PDFs from a specific EO archive page newest->oldest and stop at cutoff_pdf_url (inclusive).
    Uses Date Issued (YYYY/MM/DD) when present in the row.
    """
    cutoff_pdf_url = _abs_nj(cutoff_pdf_url)
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    rows: list[tuple[str, int, datetime | None]] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue

        u = _abs_nj(href)
        mm = _NJ_EO_PDF_NUM_RE.match(urlsplit(u).path)
        if not mm:
            continue

        if u in seen:
            continue
        seen.add(u)

        num = _nj_eo_num_from_url(u)
        if num is None:
            continue

        # Date Issued near row (same approach you already use)
        ctx = html[m.start(): m.start() + 800]
        dt = None
        dm = _NJ_DATE_YYYYMMDD_SLASH_RE.search(ctx)
        if dm:
            try:
                dt = datetime(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)), tzinfo=timezone.utc)
                dt = _date_guard_not_future(dt)
            except Exception:
                dt = None

        rows.append((u, num, dt))
        if len(rows) >= limit:
            break

    # newest first by EO number (more reliable than HTML order)
    rows.sort(key=lambda x: x[1], reverse=True)

    cutoff_num = _nj_eo_num_from_url(cutoff_pdf_url)

    print("NJ EO cutoff:", cutoff_pdf_url, "num=", cutoff_num)  # 👈 ADD THIS LINE

    out: list[tuple[str, datetime | None]] = []
    for (u, num, dt) in rows:
        # Always include down to cutoff (inclusive), then stop.
        out.append((u, dt))

        if cutoff_num is not None:
            if num <= cutoff_num:   # inclusive stop at EO-350
                break
        else:
            # fallback to URL compare if cutoff_num couldn't be parsed
            if _abs_nj(u) == cutoff_pdf_url:
                break

    return out


# ----------------------------
# NJ Executive Orders archive parsing
# ----------------------------

# EO PDFs look like: /infobank/eo/056murphy/pdf/EO-408.pdf
_NJ_EO_PDF_RE = re.compile(r"^/infobank/eo/056murphy/pdf/EO-(\d+)\.pdf$", re.I)

# Date Issued column is YYYY/MM/DD in the HTML table (as shown in your screenshot)
_NJ_DATE_YYYYMMDD_SLASH_RE = re.compile(r"\b(20\d{2})/(\d{2})/(\d{2})\b")

# ----------------------------
# NJ Executive Orders - dynamic governor discovery (future-proof)
# ----------------------------

NJ_EO_INDEX = "https://nj.gov/infobank/eo/"

# matches EO PDFs for ANY governor folder, e.g.
# /infobank/eo/056murphy/pdf/EO-411.pdf
# /infobank/eo/055christie/pdf/EO-123.pdf
_NJ_EO_PDF_ANYGOV_RE = re.compile(r"^/infobank/eo/\d{3}[a-z]+/pdf/EO-(\d+)\.pdf$", re.I)

# matches governor EO listing pages we can ingest from
# e.g. /infobank/eo/056murphy/approved/eo_archive.shtml
#      /infobank/eo/055christie/index.shtml
_NJ_EO_GOV_PAGE_RE = re.compile(
    r"^/infobank/eo/(?P<govdir>\d{3}[a-z]+)/(?:(?:approved/eo_archive\.shtml)|index\.shtml)$",
    re.I,
)

def _nj_govdir_from_url(u: str) -> str:
    """
    Extracts the governor directory token like '056murphy' from any EO URL.
    """
    try:
        path = urlsplit(u).path
        m = re.search(r"^/infobank/eo/(?P<govdir>\d{3}[a-z]+)/", path, re.I)
        return (m.group("govdir") if m else "")
    except Exception:
        return ""

async def _nj_find_latest_governor_eo_page(cx: httpx.AsyncClient) -> str:
    """
    Scrape the EO index and return the most recent governor's EO listing page.
    We pick the highest numeric prefix (e.g., 056 > 055 > 054), which tracks recency on NJ's site.
    Falls back to existing NJ_PUBLIC_PAGES['executive_orders'] if anything fails.
    """
    fallback = NJ_PUBLIC_PAGES.get("executive_orders", "")
    try:
        r = await _get(cx, NJ_EO_INDEX, headers={"Referer": NJ_EO_INDEX})
        if r.status_code >= 400 or not r.text:
            return fallback

        html = r.text.replace("\\/", "/")
        href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

        best_num = -1
        best_url = ""

        for m in href_re.finditer(html):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            u = _abs_nj(href)
            path = urlsplit(u).path
            mm = _NJ_EO_GOV_PAGE_RE.match(path)
            if not mm:
                continue

            govdir = mm.group("govdir") or ""
            # numeric prefix (first 3 digits)
            try:
                n = int(govdir[:3])
            except Exception:
                continue

            if n > best_num:
                best_num = n
                best_url = u

        return best_url or fallback
    except Exception:
        return fallback


async def _collect_nj_eo_pdf_pairs_2024_2025_anygov(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    years: set[int] = {2024, 2025},
    limit: int = 20000,
) -> list[tuple[str, datetime | None]]:
    """
    Same as _collect_nj_eo_pdf_pairs_2024_2025, but accepts EO PDFs for ANY governor folder.
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")

    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    pairs: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue

        u = _abs_nj(href)
        path = urlsplit(u).path
        mm = _NJ_EO_PDF_ANYGOV_RE.match(path)
        if not mm:
            continue

        if u in seen:
            continue
        seen.add(u)

        # Look around this href occurrence for a Date Issued like YYYY/MM/DD
        ctx = html[m.start(): m.start() + 600]  # row context is usually nearby
        dt = None
        dm = _NJ_DATE_YYYYMMDD_SLASH_RE.search(ctx)
        if dm:
            try:
                yy = int(dm.group(1))
                if yy in years:
                    dt = datetime(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)), tzinfo=timezone.utc)
                    dt = _date_guard_not_future(dt)
                else:
                    continue  # skip years we don't want
            except Exception:
                dt = None
        else:
            # if we can't find a date near it, skip (keeps behavior conservative)
            continue

        pairs.append((u, dt))
        if len(pairs) >= limit:
            break

    return pairs

async def _collect_nj_eo_pdf_pairs_2024_2025(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    years: set[int] = {2024, 2025},
    limit: int = 20000,
) -> list[tuple[str, datetime | None]]:
    """
    Fetch EO archive page and extract (pdf_url, issued_date) for rows whose year is in {2024, 2025}.
    This avoids relying on EO numbering (more robust if numbering ever changes).
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")

    # Find EO PDF hrefs, then look ahead in nearby context for the Date Issued in that row.
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    pairs: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue

        u = _abs_nj(href)
        path = urlsplit(u).path
        mm = _NJ_EO_PDF_RE.match(path)
        if not mm:
            continue

        # context window likely contains the row with date issued
        start = max(m.start() - 300, 0)
        end = min(m.end() + 800, len(html))
        ctx = _strip_html_to_text(html[start:end])

        dt = None
        md = _NJ_DATE_YYYYMMDD_SLASH_RE.search(ctx)
        if md:
            try:
                yy = int(md.group(1))
                mo = int(md.group(2))
                dd = int(md.group(3))
                if yy in years:
                    dt = datetime(yy, mo, dd, tzinfo=timezone.utc)
                    dt = _date_guard_not_future(dt)
                else:
                    continue  # skip non-2024/2025
            except Exception:
                dt = None
        else:
            # If date missing, be conservative and skip (so you don’t ingest wrong years)
            continue

        if u in seen:
            continue
        seen.add(u)
        pairs.append((u, dt))

        if len(pairs) >= limit:
            break

    # Sort newest->oldest
    pairs.sort(key=lambda x: (x[1] is not None, x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)), reverse=True)
    return pairs


# ----------------------------
# NJ Administrative Orders archive parsing
# ----------------------------

# AO PDFs look like: https://nj.gov/governor/news/ao/docs/AO_2021-3.pdf
_NJ_AO_PDF_RE = re.compile(r"^/governor/news/ao/docs/[^#?]+\.pdf$", re.I)

_NJ_MONTH_RE = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
_NJ_WD_RE = r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"

def _nj_parse_month_day_year(s: str) -> Optional[datetime]:
    s = re.sub(r"\s+", " ", (s or "").strip())
    if not s:
        return None

    # Strip weekday if present: "Thursday, March 19, 2020" -> "March 19, 2020"
    s = re.sub(rf"^{_NJ_WD_RE},\s+", "", s, flags=re.I)

    try:
        dt = datetime.strptime(s, "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _nj_ao_published_at_from_text(pdf_text: str) -> Optional[datetime]:
    """
    Extract a reliable published/effective date from NJ Administrative Order PDF text.
    Handles:
      - "... take effect on April 7, 2021"
      - "... effective at 8:00 p.m. on Thursday, March 19, 2020"
      - "... which is Saturday, March 21, 2020 at 9:00 p.m."
      - signature/date line near end: "March 24, 2020"
      - "terminate ... on Monday, November 30, 2020" but signature date exists ("November 20, 2020")
    """
    t = (pdf_text or "")
    if not t.strip():
        return None

    # Normalize: keep newlines, but reduce weird spacing so regex works across PDF line wraps
    t1 = t.replace("\r", "\n")
    t1 = re.sub(r"[ \t]+", " ", t1)

    # A flexible "Month DD, YYYY" matcher that survives line wraps
    date_pat = rf"(?:{_NJ_WD_RE},\s+)?({_NJ_MONTH_RE}\s+\d{{1,2}},\s+\d{{4}})"

    # 1) "shall take effect ... on <date>" (allow newlines between tokens)
    m = re.search(
        rf"shall take effect(?:[\s\S]*?)\bon\s+{date_pat}",
        t1,
        flags=re.I,
    )
    if m:
        dt = _nj_parse_month_day_year(m.group(1))
        if dt:
            return dt

    # 2) "effective ... on <date>" (covers: effective at 8:00 p.m. on Thursday, March 19, 2020)
    m = re.search(
        rf"\beffective(?:[\s\S]*?)\bon\s+{date_pat}",
        t1,
        flags=re.I,
    )
    if m:
        dt = _nj_parse_month_day_year(m.group(1))
        if dt:
            return dt

    # 3) "which is <date>" (covers: "which is Saturday, March 21, 2020 at 9:00 p.m.")
    m = re.search(
        rf"\bwhich is\s+{date_pat}",
        t1,
        flags=re.I,
    )
    if m:
        dt = _nj_parse_month_day_year(m.group(1))
        if dt:
            return dt

    # 4) If it says "take effect immediately", prefer a nearby explicit date first…
    if re.search(r"shall take effect immediately", t1, flags=re.I):
        m = re.search(
            rf"shall take effect immediately[\s\S]{{0,1200}}?{date_pat}",
            t1,
            flags=re.I,
        )
        if m:
            dt = _nj_parse_month_day_year(m.group(1))
            if dt:
                return dt

    # 5) Final fallback: pick the LAST Month DD, YYYY in the document
    date_only_pat = rf"{_NJ_MONTH_RE}\s+\d{{1,2}},\s+\d{{4}}"
    all_dates = re.findall(date_only_pat, t1, flags=re.I)
    if all_dates:
        dt = _nj_parse_month_day_year(all_dates[-1])
        if dt:
            return dt

    return None


async def _collect_nj_ao_pdf_urls(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    limit: int = 20000,
) -> list[str]:
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    out: list[str] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = (m.group(1) or "").strip()
        if not href:
            continue
        u = _abs_nj(href)
        if not u:
            continue
        if not _NJ_AO_PDF_RE.match(urlsplit(u).path):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= limit:
            break

    return out


def _hi_category_page(base: str, page: int) -> str:
    """
    WordPress category paging:
      page 1 = base
      page N = base + "page/N/"
    """
    base = (base or "").rstrip("/") + "/"
    if page <= 1:
        return base
    return f"{base}page/{page}/"


def _hi_date_from_pdf_filename(url: str) -> datetime | None:
    """
    Many HI PDFs start with yymmdd (e.g., 2501078_...pdf => 2025-01-07).
    We parse first 6 digits if present.
    """
    try:
        fname = urlsplit(url).path.rsplit("/", 1)[-1]
        m = re.match(r"^(?P<yymmdd>\d{6})", fname or "")
        if not m:
            return None
        yymmdd = m.group("yymmdd")
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        year = 2000 + yy
        return datetime(year, mm, dd, tzinfo=timezone.utc)
    except Exception:
        return None


async def _collect_hi_press_release_urls(
    cx: httpx.AsyncClient,
    *,
    start_url: str,
    max_pages: int = 50,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> List[str]:
    """
    Collect press release detail pages from the WP category listing.
    Keeps ONLY https://governor.hawaii.gov/newsroom/... (HTML details).
    Stops at stop_at_url (inclusive).
    """
    out: List[str] = []
    seen: set[str] = set()
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for p in range(1, max_pages + 1):
        page_url = _hi_category_page(start_url, p)
        r = await _get(cx, page_url, headers={"Referer": HI_PUBLIC_PAGES["all_newsroom"]})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")
        page_found: List[str] = []

        for m in href_re.finditer(html):
            u = (m.group(1) or "").split("#")[0].strip()
            if not u:
                continue
            # only accept newsroom detail pages
            if not _HI_NEWSROOM_DETAIL_RE.match(u):
                continue
            page_found.append(u)

        # de-dupe, preserve order
        new_count = 0
        for u in page_found:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            new_count += 1

            if stop_at_url and u == stop_at_url:
                return out

            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.1)

    return out

# ----------------------------
# Hawaii: EO/Proclamation list parsing (use "Posted on ..." from listing cards)
# ----------------------------

# --- Hawaii: EO/Proclamation listing parsing (KEEP old discovery, add date/title) ---

_HI_TIME_DT_RE = re.compile(r'(?is)<time[^>]+datetime=["\']([^"\']+)["\']')

def _hi_strip_html(s: str) -> str:
    s = re.sub(r"(?is)<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", s).strip()

def _hi_parse_posted_dt_from_article(article_html: str) -> datetime | None:
    # <time datetime="2026-01-16T...">
    m = _HI_TIME_DT_RE.search(article_html or "")
    if m:
        raw = (m.group(1) or "").strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            return _date_guard_not_future(dt)
        except Exception:
            pass

    # fallback: "Jan 16, 2026" somewhere in card
    text = _hi_strip_html(article_html or "")
    m2 = _US_MONTH_DATE_RE.search(text)
    if m2:
        dt2 = _parse_us_month_date(m2.group(0))
        return _date_guard_not_future(dt2) if dt2 else None

    return None

async def _collect_hi_pdf_items_from_category(
    cx: httpx.AsyncClient,
    *,
    start_url: str,
    max_pages: int = 50,
    limit: int = 5000,
    stop_at_pdf_url: str | None = None,
) -> List[Tuple[str, str, datetime | None]]:
    """
    Working approach:
      - Parse listing page for (detail_url, title)
      - Grab posted_dt from nearby listing chunk ("Posted on Jan 23, 2026" or <time datetime=...>)
      - Fetch detail page and extract first uploads PDF
      - Return (pdf_url, title, posted_dt)
    """
    out: List[Tuple[str, str, datetime | None]] = []
    seen_pdf: set[str] = set()

    for p in range(1, max_pages + 1):
        page_url = _hi_category_page(start_url, p)
        r = await _get(cx, page_url, headers={"Referer": HI_PUBLIC_PAGES["all_newsroom"]})
        if r.status_code >= 400 or not r.text:
            break

        page_html = r.text.replace("\\/", "/")
        matches = list(_HI_ENTRY_RE.finditer(page_html))

        print("HI LIST page:", page_url, "status=", r.status_code, "len=", len(page_html))
        print("HI LIST head:", page_html[:500])
        print("HI ENTRY matches:", len(matches))

        if not matches:
            break

        page_new = 0

        for m in matches:
            href = (m.group("href") or "").strip()
            title = re.sub(r"(?is)<[^>]+>", " ", (m.group("title") or ""))
            title = re.sub(r"\s+", " ", title).strip()

            detail_url = clean_url(urljoin("https://governor.hawaii.gov/", href))
            if not detail_url:
                continue

            # grab nearby chunk to find "Posted on ..." or <time datetime=...>
            start = m.start()
            chunk = page_html[start : min(len(page_html), start + 2500)]
            posted_dt = _hi_posted_dt_from_listing_chunk(chunk)

            # Normalize once (strip fragment/query BEFORE checks)
            detail_url_norm = clean_url(detail_url).split("#", 1)[0].split("?", 1)[0].strip()
            dul = detail_url_norm.lower()

            # If listing already links to a PDF, use it directly.
            if "/wp-content/uploads/" in dul and dul.endswith(".pdf"):
                pdf_url = detail_url_norm
            else:
                # Otherwise, it's a post/detail page: fetch it and extract first PDF
                if not _hi_is_detail_page_url(detail_url_norm):
                    print("SKIP detail_url:", detail_url)
                    continue

                dr = await _get(cx, detail_url_norm, headers={"Referer": page_url})
                if dr.status_code >= 400 or not dr.text:
                    continue

                pdf_url = _hi_first_pdf_from_html(dr.text.replace("\\/", "/"))
                if not pdf_url:
                    continue

                pdf_url = clean_url(pdf_url).split("#", 1)[0].split("?", 1)[0].strip()

            if pdf_url in seen_pdf:
                continue
            seen_pdf.add(pdf_url)

            if not title:
                # fallback to filename
                fname = urlsplit(pdf_url).path.rsplit("/", 1)[-1]
                title = (fname or pdf_url).replace(".pdf", "").replace("_", " ").replace("-", " ").strip()

            out.append((pdf_url, title, posted_dt))
            page_new += 1

            if stop_at_pdf_url and clean_url(pdf_url) == clean_url(stop_at_pdf_url):
                return out
            if len(out) >= limit:
                return out

        if page_new == 0:
            break

        await asyncio.sleep(0.1)

    return out


# ----------------------------
# Vermont config (Drupal-ish static listing pages with ?page=N)
# ----------------------------

VT_PUBLIC_PAGES = {
    "press_releases": "https://governor.vermont.gov/press_releases",
    "executive_orders": "https://governor.vermont.gov/document-types/executive-orders?page=0",
    "proclamations": "https://governor.vermont.gov/document-categories/proclamations?page=0",
}

VT_PRESS_CUTOFF_URL = "https://governor.vermont.gov/press-release/governor-phil-scott-appoints-zachary-harvey-house-representatives"
VT_PROC_CUTOFF_URL  = "https://governor.vermont.gov/document/mentoring-month-proclamation-25-1"

VT_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

_VT_KEEP_PRESS_RE = re.compile(r"^https://governor\.vermont\.gov/press-release/", re.I)
_VT_KEEP_DOC_RE   = re.compile(r"^https://governor\.vermont\.gov/document/", re.I)

_VT_PDF_RE = re.compile(r"^https://governor\.vermont\.gov/sites/scott/files/documents/[^#?]+\.pdf$", re.I)


# ----------------------------
# Colorado config (colorado.gov)
# ----------------------------

CO_PUBLIC_PAGES = {
    "press_releases": "https://www.colorado.gov/governor/news?page=0",
    "executive_orders_2025": "https://www.colorado.gov/governor/2025-executive-orders",
    "executive_orders_2024": "https://www.colorado.gov/governor/2024-executive-orders",
}

def _co_eo_year_page(year: int) -> str:
    return f"https://www.colorado.gov/governor/{year}-executive-orders"


# stop at the first item of 2025 (inclusive)
CO_PRESS_CUTOFF_URL = "https://www.colorado.gov/governor/news/governor-polis-colorado-energy-office-and-department-local-affairs-release-microgrid-roadmap"

CO_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
}

CO_JURISDICTION = "colorado"
CO_AGENCY = "Colorado Governor"

# detail pages look like: /governor/news/<slug>
_CO_PRESS_DETAIL_PATH_RE = re.compile(r"^/governor/news/[^#?]+$", re.I)

_CO_PRESS_DATE_RE = re.compile(
    r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
    r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|'
    r'Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+(20\d{2})\b',
    re.I
)

_CO_EO_SEAL_RE = re.compile(
    r"\bthis\s+"
    r"(?P<day>(?:\d{1,2})(?:st|nd|rd|th)?|[a-z]+(?:[-\s][a-z]+)?)\s+"
    r"day\s+of\s+"
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)"
    r"\s*,?\s*"
    r"(?P<year>20\d{2})\b",
    re.I,
)

_CO_GDRIVE_ID_RE = re.compile(
    r"drive\.google\.com/(?:file/d/|open\?id=|uc\?id=)(?P<id>[^/&?#]+)",
    re.I,
)

_CO_LISTING_DATE_RE = re.compile(
    r'\b(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+'
    r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+'
    r'(\d{1,2}),\s+(20\d{2})\b',
    re.I
)

def _co_parse_listing_date(s: str) -> datetime | None:
    s = s or ""

    # 1) weekday + month date (your existing)
    m = _CO_LISTING_DATE_RE.search(s)
    if m:
        mon = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3))
        month_map = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }
        month = month_map.get(mon, 0)
        if month:
            return _date_guard_not_future(datetime(year, month, day, tzinfo=timezone.utc))

    # 2) ✅ fallback: Month DD, YYYY (no weekday)
    mm = _US_MONTH_DATE_RE.search(s)
    if mm:
        dt = _parse_us_month_date(mm.group(0))
        return _date_guard_not_future(dt) if dt else None

    return None


def _co_drive_download_url(u: str) -> str:
    u = (u or "").strip()
    m = _CO_GDRIVE_ID_RE.search(u)
    if not m:
        return u
    fid = m.group("id")
    # use export=download, but add confirm=t to reduce interstitial issues
    return f"https://drive.google.com/uc?export=download&confirm=t&id={fid}"

def _extract_co_eo_date(pdf_text: str) -> datetime | None:
    if not pdf_text:
        return None

    # normalize whitespace + line breaks (important for "this ninth \n day of ...")
    text = pdf_text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    flat = re.sub(r"\s+", " ", text).strip()

    m = _CO_EO_SEAL_RE.search(flat)
    if not m:
        return None

    day_raw = (m.group("day") or "").strip()
    month_raw = (m.group("month") or "").strip().lower()
    year_raw = (m.group("year") or "").strip()

    day = _co_ordinal_word_to_int(day_raw)
    if not day:
        return None

    year = int(year_raw)
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    month = month_map.get(month_raw, 0)
    if not month:
        return None

    return _date_guard_not_future(datetime(year, month, day, tzinfo=timezone.utc))

def _co_ordinal_word_to_int(s: str) -> int | None:
    """
    Convert ordinal words used in CO EO seals to an int day (1-31).
    Handles:
      - ninth
      - twentieth
      - twenty-eighth / twenty eighth
      - thirty-first / thirty first
    Also accepts numeric strings like "20th".
    """
    if not s:
        return None

    w = s.strip().lower()
    w = w.replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")  # weird hyphens
    w = re.sub(r"[^\w\s-]", "", w)  # strip punctuation
    w = re.sub(r"\s+", " ", w).strip()

    # numeric like "20", "20th"
    m = re.match(r"^(\d{1,2})", w)
    if m:
        d = int(m.group(1))
        return d if 1 <= d <= 31 else None

    # base ordinal map
    base = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
        "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
        "eleventh": 11, "twelfth": 12, "thirteenth": 13, "fourteenth": 14,
        "fifteenth": 15, "sixteenth": 16, "seventeenth": 17, "eighteenth": 18,
        "nineteenth": 19,
        "twentieth": 20,
        "thirtieth": 30,
    }
    if w in base:
        return base[w]

    # handle "twenty-eighth" / "twenty eighth" / "thirty-first"
    parts = re.split(r"[-\s]+", w)
    if not parts:
        return None

    tens = {"twenty": 20, "thirty": 30}
    ones = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
        "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9,
    }

    if len(parts) == 2 and parts[0] in tens and parts[1] in ones:
        d = tens[parts[0]] + ones[parts[1]]
        return d if 1 <= d <= 31 else None

    return None



def _date_from_co_press(html: str) -> datetime | None:
    if not html:
        return None

    text = _strip_html_to_text(html)

    # 1) Colorado's visible date line often includes weekday:
    #    "THURSDAY, DECEMBER 18, 2025"
    m = _CO_LISTING_DATE_RE.search(text)
    if m:
        return _co_parse_listing_date(m.group(0))

    # 2) fallback: plain "December 18, 2025"
    mm = _US_MONTH_DATE_RE.search(text)
    if mm:
        dt = _parse_us_month_date(mm.group(0))
        return _date_guard_not_future(dt) if dt else None

    return None


def _abs_co(u: str) -> str:
    if not u:
        return ""
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u.split("?")[0]
    if not u.startswith("/"):
        u = "/" + u
    return ("https://www.colorado.gov" + u).split("?")[0]

def _canon_co(u: str) -> str:
    u = _abs_co(u)
    if not u:
        return ""
    u = u.split("#")[0].split("?")[0]
    return u.rstrip("/")

def _co_news_page(page: int) -> str:
    # https://www.colorado.gov/governor/news?page=0
    return f"https://www.colorado.gov/governor/news?page={page}"


async def _collect_co_press_release_pairs(
    cx: httpx.AsyncClient,
    *,
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> list[tuple[str, datetime | None]]:
    """
    Crawl Colorado press release listing pages (?page=N), newest -> older.
    Extract (detail_url, published_at) from listing context.
    Stops at stop_at_url (inclusive).
    """
    stop_norm = _canon_co(stop_at_url) if stop_at_url else None
    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for p in range(0, max_pages):
        page_url = _co_news_page(p)
        r = await _get(cx, page_url, headers={"Referer": CO_PUBLIC_PAGES["press_releases"]})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")
        new_count = 0

        for m in href_re.finditer(html):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            u = _canon_co(href)
            if not u:
                continue

            # keep only /governor/news/<slug> detail pages
            if not _CO_PRESS_DETAIL_PATH_RE.match(urlsplit(u).path):
                continue

            if u in seen:
                continue

            # listing date is usually near the link on the listing page
            before = html[:m.start()]
            last_date = None

            for dm in _CO_LISTING_DATE_RE.finditer(before):
                last_date = dm.group(0)

            dt = None
            if last_date:
                dt = _co_parse_listing_date(last_date)

            seen.add(u)
            out.append((u, dt))
            new_count += 1

            if stop_norm and u == stop_norm:
                return out
            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.1)

    return out

# Colorado EO page contains google drive "view" links
_CO_GDRIVE_VIEW_RE = re.compile(r"^https?://drive\.google\.com/file/d/([^/]+)/view", re.I)


async def _collect_co_eo_drive_items(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    limit: int = 5000,
) -> list[tuple[str, str, datetime | None]]:
    """
    Returns [(drive_view_url, title_hint, date_hint)] from a CO EO page.
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    out: list[tuple[str, str, datetime | None]] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        href = clean_url(m.group(1) or "")
        if not href:
            continue

        # normalize absolute
        if href.startswith("/"):
            href = "https://www.colorado.gov" + href
        elif href.startswith("//"):
            href = "https:" + href

        href = href.split("?")[0].strip()

        if not _CO_GDRIVE_VIEW_RE.match(href):
            continue

        if href in seen:
            continue
        seen.add(href)

        # title/date hints from nearby context
        ctx_start = max(m.start() - 250, 0)
        ctx_end = min(m.end() + 400, len(html))
        ctx = _strip_html_to_text(html[ctx_start:ctx_end])

        dt = None
        mm = _US_MONTH_DATE_RE.search(ctx)
        if mm:
            dt = _parse_us_month_date(mm.group(0))
            dt = _date_guard_not_future(dt) if dt else None

        # lightweight title hint: use closest text chunk or fallback later
        title_hint = ""
        # try to capture something like "Executive Order ..." in the context
        tmatch = re.search(r"(?i)\b(executive\s+order[^.\n]{0,140})", ctx)
        if tmatch:
            title_hint = re.sub(r"\s+", " ", tmatch.group(1)).strip()

        out.append((href, title_hint, dt))

        if len(out) >= limit:
            break

    return out


def _parse_dmy_abbr(s: str) -> datetime | None:
    """
    Parses "16 Jan 2026" style dates (as used on VA proclamation list).
    """
    if not s:
        return None
    m = _VA_LISTING_DATE_DMY_RE.search(s)
    if not m:
        return None
    day = int(m.group(1))
    mon = _MONTH_ABBR.get(m.group(2).lower(), 0)
    yr = int(m.group(3))
    if mon <= 0:
        return None
    try:
        return datetime(yr, mon, day, tzinfo=timezone.utc)
    except Exception:
        return None


# EO PDFs often include: "this 12th day of September 2025."
_VA_EO_SEAL_RE = re.compile(
    r'\bthis\s+(\d{1,2})(?:st|nd|rd|th)\s+day\s+of\s+([A-Za-z]+)\s+(20\d{2})\b',
    re.I
)

def _extract_va_eo_date(pdf_text: str) -> datetime | None:
    """
    Extract EO date from the "Under the Seal..." line.
    We'll take the LAST match in the PDF (usually near the end).
    """
    if not pdf_text:
        return None
    matches = list(_VA_EO_SEAL_RE.finditer(pdf_text))
    if not matches:
        return None
    m = matches[-1]
    day = int(m.group(1))
    month_name = (m.group(2) or "").strip().lower()
    year = int(m.group(3))

    # full month name -> month number
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
    }
    mon = month_map.get(month_name, 0)
    if mon <= 0:
        return None
    try:
        return datetime(year, mon, day, tzinfo=timezone.utc)
    except Exception:
        return None

def _abs_ga(u: str) -> str:
    if not u:
        return u
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return "https://gov.georgia.gov" + u

async def _collect_ga_press_release_pairs(
    cx: httpx.AsyncClient,
    *,
    year: int = 2025,
    max_pages: int = 50,
    limit: int = 5000,
) -> list[tuple[str, datetime | None]]:
    """
    Crawl https://gov.georgia.gov/press-releases/2025?page=N
    Extract (detail_url, published_at) from the listing cards.
    """
    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    for p in range(max_pages):
        page_url = f"https://gov.georgia.gov/press-releases/{year}?page={p}"
        r = await _get(cx, page_url, headers={"Referer": GA_PUBLIC_PAGES["press_releases_2025"]})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")

        # find all press release detail hrefs, then search nearby for "Month DD, YYYY"
        href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
        any_new = 0

        for m in href_re.finditer(html):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            u = _abs_ga(href)

            path = urlsplit(u).path
            if not _GA_PRESS_DETAIL_RE.match(path):
                continue

            if u in seen:
                continue

            # look around the link for the listing date text
            start = max(m.start() - 150, 0)
            end = min(m.end() + 500, len(html))
            context = html[start:end]
            context_text = _strip_html_to_text(context)

            # common listing: "December 19, 2025"
            dt = None
            mm = _US_MONTH_DATE_RE.search(context_text)
            if mm:
                dt = _parse_us_month_date(mm.group(0))
                dt = _date_guard_not_future(dt) if dt else None

            # keep only 2025
            if dt and dt.year != year:
                continue

            seen.add(u)
            out.append((u, dt))
            any_new += 1

            if len(out) >= limit:
                return out

        if any_new == 0:
            break

        await asyncio.sleep(0.1)

    return out

async def _collect_va_proclamation_urls_years(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    years: set[int],
    max_urls: int = 5000,
) -> List[str]:
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text
    out: List[str] = []
    seen: set[str] = set()

    for m in _VA_PROC_ROW_RE.finditer(html):
        href = (m.group("href") or "").strip()
        rest = (m.group("rest") or "").strip()

        ym = _VA_YEAR_RE.findall(rest)
        if not ym:
            continue

        yrs = {int(y) for y in ym if y.isdigit()}
        if not (yrs & years):
            continue

        u = _abs_va(href)
        if not u or u in seen:
            continue

        seen.add(u)
        out.append(u)
        if len(out) >= max_urls:
            break

    return out

# ----------------------------
# Small helpers
# ----------------------------

def clean_url(u: str) -> str:
    if not u:
        return u
    return (
        u.strip()
         .replace("“", "")
         .replace("”", "")
         .replace("’", "'")
         .replace("\u00a0", " ")
         .strip(" \t\r\n\"'")
    )

def clean_headers(headers: dict | None) -> dict | None:
    if not headers:
        return headers
    out = {}
    for k, v in headers.items():
        if v is None:
            continue
        v = str(v)
        v = v.replace("“", '"').replace("”", '"').replace("\u00a0", " ")
        v = v.encode("ascii", "ignore").decode("ascii")
        out[str(k)] = v
    return out

def _nz(s: str | None) -> str:
    if not s:
        return ""
    return s.replace("\x00", "").strip()

def _abs_ohio(u: str) -> str:
    if not u:
        return u
    u = u.split("#")[0].strip()

    # protocol-relative
    if u.startswith("//"):
        u = "https:" + u

    # absolute
    if u.startswith("http"):
        return u.split("?")[0]

    # relative
    if not u.startswith("/"):
        u = "/" + u
    return ("https://governor.ohio.gov" + u).split("?")[0]


def _set_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, doseq=True), parts.fragment))

def _abs_az(u: str) -> str:
    if not u:
        return u
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u.split("?")[0]
    if not u.startswith("/"):
        u = "/" + u
    return ("https://azgovernor.gov" + u).split("?")[0]

def _abs_va(u: str) -> str:
    if not u:
        return u
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u.split("?")[0]
    if not u.startswith("/"):
        u = "/" + u
    return ("https://www.governor.virginia.gov" + u).split("?")[0]

def _abs_vt(u: str) -> str:
    if not u:
        return u
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u.split("?")[0]
    if not u.startswith("/"):
        u = "/" + u
    return ("https://governor.vermont.gov" + u).split("?")[0]

def _canon_vt(u: str) -> str:
    u = _abs_vt(u)
    if not u:
        return ""
    u = u.split("#")[0].split("?")[0].rstrip("/")
    return u



def _vt_page(url: str, page: int) -> str:
    # All your VT lists page via ?page=N (including the first page)
    return _set_query_param(url, "page", str(page))


def _extract_first_pdf_link_vt(html: str) -> str:
    """
    VT document pages (EOs/Proclamations) have a PDF link like:
      https://governor.vermont.gov/sites/scott/files/documents/....pdf
    We grab the first matching PDF link.
    """
    if not html:
        return ""
    blob = html.replace("\\/", "/")
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
    for m in href_re.finditer(blob):
        u = _abs_vt(m.group(1))
        if u and _VT_PDF_RE.match(u):
            return u
    return ""


def _date_from_vt_doc_page(html: str) -> datetime | None:
    """
    VT document pages show a date line like 'September 17, 2025' or 'June 1, 2025'.
    We parse the first Month DD, YYYY we see.
    """
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _US_MONTH_DATE_RE.search(text)
    if not m:
        return None
    dt = _parse_us_month_date(m.group(0))
    return _date_guard_not_future(dt) if dt else None


def _extract_urls_dates_from_any_json(obj: object) -> list[tuple[str, datetime | None]]:
    """
    Robust VA news feed extractor:
    - accepts absolute OR relative URLs
    - extracts hrefs embedded inside HTML strings within JSON
    - grabs date-ish fields when available
    """
    out: list[tuple[str, datetime | None]] = []

    def parse_dt(v: object) -> datetime | None:
        if v is None:
            return None

        s = str(v).strip()
        if not s:
            return None

        # YYYYMMDD (e.g., 20251223)
        if re.fullmatch(r"\d{8}", s):
            try:
                return datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
            except Exception:
                pass

        # ISO
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

        # RFC2822
        try:
            return parsedate_to_datetime(s).astimezone(timezone.utc)
        except Exception:
            pass

        # Month DD, YYYY
        dt = _parse_us_month_date(s)
        if dt:
            return dt

        # DMY like "16 Jan 2026"
        dt2 = _parse_dmy_abbr(s)
        if dt2:
            return dt2

        return None

    def looks_like_news_path(s: str) -> bool:
        if not s:
            return False
        s = s.strip()
        return ("/newsroom/news-releases/" in s) and (".htm" in s or ".html" in s)

    def norm_news_url(s: str) -> str:
        s = (s or "").strip()
        # if it's already absolute, normalize
        if s.startswith("http"):
            return _abs_va(s)
        # relative path
        if s.startswith("/"):
            return _abs_va(s)
        return s

    # find hrefs inside any big string blob
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
    path_re = re.compile(r'(/newsroom/news-releases/[^"\s<>]+?\.(?:html?|php))', re.I)

    def scan_string_blob(blob: str):
        if not blob:
            return
        # href="..."
        for m in href_re.finditer(blob):
            u = m.group(1) or ""
            if looks_like_news_path(u):
                out.append((norm_news_url(u), None))

        # plain paths
        for m in path_re.finditer(blob):
            u = m.group(1) or ""
            if looks_like_news_path(u):
                out.append((norm_news_url(u), None))

    def walk(node: object):
        if isinstance(node, dict):
            url_val = None
            dt_val = None

            for k, v in node.items():
                ks = str(k).lower()

                # URL candidates (many feeds use these keys)
                if url_val is None and isinstance(v, str):
                    if ks in ("url", "link", "href", "permalink", "storyurl", "pageurl"):
                        if looks_like_news_path(v):
                            url_val = v
                    # even if key is unknown, still accept if it looks like the path
                    if url_val is None and looks_like_news_path(v):
                        url_val = v

                # date candidates (feeds vary a lot)
                if dt_val is None and ks in (
                    "datecode",
                    "date", "datetime", "published", "published_at", "publishedat",
                    "publishdate", "pubdate", "datepublished", "created", "created_at",
                    "releasedate", "release_date", "updated", "updated_at"
                ):
                    dt_val = parse_dt(v)

                # strings may contain HTML snippets with hrefs
                if isinstance(v, str) and len(v) > 40 and "/newsroom/news-releases/" in v:
                    scan_string_blob(v)

            if url_val:
                out.append((norm_news_url(url_val), _date_guard_not_future(dt_val)))

            for v in node.values():
                walk(v)

        elif isinstance(node, list):
            for it in node:
                walk(it)

        elif isinstance(node, str):
            if "/newsroom/news-releases/" in node:
                scan_string_blob(node)
                if looks_like_news_path(node):
                    out.append((norm_news_url(node), None))

    walk(obj)

    # dedupe preserve order (keep first)
    dedup: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()
    for (u, dt) in out:
        u = (u or "").split("#")[0].strip()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        dedup.append((u, dt))
    return dedup

async def _collect_va_news_urls_from_feed(
    cx: httpx.AsyncClient,
    *,
    feed_url: str,
    max_urls: int = 5000,
    stop_at_url: str | None = None,
    year_only: int | None = 2025,
    max_pages: int = 50,
) -> list[tuple[str, datetime | None]]:
    """
    Uses VA govnewsfeed.php JSON.
    Returns list of (url, published_at) in newest->oldest order (as delivered),
    stopping at stop_at_url (inclusive) and filtering to year_only (if provided).
    Pagination is attempted if response exposes a cursor/next value.
    """
    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()

    next_url = feed_url

    for page in range(max_pages):
        r = await _get(cx, next_url, headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": VA_PUBLIC_PAGES["news_releases"],
        })
        if r.status_code >= 400 or not r.text:
            print("VA NEWS FEED fetch failed:", r.status_code, "url=", next_url)
            break

        try:
            payload = r.json()
        except Exception:
            print("VA NEWS FEED json parse failed")
            break

        pairs = _extract_urls_dates_from_any_json(payload)

        if not pairs:
            head = (r.text or "")[:400]
            print("VA NEWS FEED no pairs extracted; url=", next_url, "status=", r.status_code, "head=", repr(head))
            try:
                if isinstance(payload, dict):
                    print("VA NEWS FEED top keys:", list(payload.keys())[:40])
                elif isinstance(payload, list):
                    print("VA NEWS FEED payload is list; len=", len(payload))
                    if payload and isinstance(payload[0], dict):
                        print("VA NEWS FEED first item keys:", list(payload[0].keys())[:40])
            except Exception:
                pass
            break

        new_count = 0
        for (u, dt) in pairs:
            if not u or u in seen:
                continue

            # Use feed date if present (datecode is usually present once you parse it)
            y = dt.year if dt else _va_year_from_news_url(u)

            if year_only:
                # If we have a date and it's older than the target year, we can stop (feed is newest→oldest)
                if dt and dt.year < year_only:
                    return out

                # If we have a date and it's not the target year, skip it
                if dt and dt.year != year_only:
                    continue

                # If dt is missing AND url has a year, enforce it
                if (dt is None) and (y is not None) and (y != year_only):
                    continue


                # IMPORTANT: if y is None (no year in URL + no dt in feed),
                # DON'T SKIP — keep it and we'll enforce year later from the detail page.
                # (so just fall through)

            seen.add(u)
            out.append((u, dt))
            new_count += 1   # ✅ you were missing this

            if stop_at_url and u == stop_at_url:
                return out
            if len(out) >= max_urls:
                return out

        if new_count == 0:
            break

        # ---- attempt pagination (best effort) ----
        # If the JSON includes something like next/cursor/offset, use it.
        cursor = None
        if isinstance(payload, dict):
            for k in ("next", "nextPage", "next_page", "cursor", "offset", "page"):
                if k in payload:
                    cursor = payload.get(k)
                    break

                if cursor:
                    cur_s = str(cursor).strip()
                    if cur_s.startswith("http"):
                        next_url = cur_s
                    else:
                        parts = urlsplit(feed_url)
                        next_url = urlunsplit((parts.scheme, parts.netloc, parts.path, str(cur_s), ""))
                    continue

                # ✅ fallback pagination attempt: try ?page=N on the same feed URL
                # many simple endpoints support page= / pg= style paging even if not explicit in JSON.
                guessed = _set_query_param(feed_url, "page", str(page + 1))
                if guessed == next_url:
                    break
                next_url = guessed
                continue

        # If no cursor info exposed, we can't page.
        break

    return out

async def _collect_vt_listing_urls(
    cx: httpx.AsyncClient,
    *,
    base_url: str,
    keep_re: re.Pattern,
    max_pages: int,
    limit: int,
    stop_at_url: str | None = None,
    referer: str | None = None,
) -> List[str]:
    """
    Crawl VT listing pages that paginate with ?page=N.
    Scrape hrefs, normalize to absolute, filter via keep_re.
    Stops at stop_at_url (inclusive).
    """
    out: List[str] = []
    seen: set[str] = set()
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for p in range(0, max_pages + 1):
        page_url = _vt_page(base_url, p)
        r = await _get(cx, page_url, headers={"Referer": referer or base_url})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")

        page_found: List[str] = []
        for m in href_re.finditer(html):
            u = _abs_vt(m.group(1))
            if not u:
                continue
            if not keep_re.search(u):
                continue
            page_found.append(u)

        new_count = 0
        for u in page_found:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            new_count += 1

            if stop_at_url and u == stop_at_url:
                return out

            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.1)

    return out


def _extract_links_from_views_ajax_payload(payload: object) -> List[str]:
    """
    Drupal views/ajax returns JSON array of command objects.
    Most useful HTML is usually in cmd['data'] for commands like:
      - insert / replace / append
    We just scrape links from any string fields.
    """
    urls: List[str] = []
    if not isinstance(payload, list):
        return urls

    # collect all string blobs (mostly HTML)
    blobs: List[str] = []
    for cmd in payload:
        if not isinstance(cmd, dict):
            continue
        for k, v in cmd.items():
            if isinstance(v, str) and len(v) > 20:
                blobs.append(v)

    # scrape hrefs
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)
    for blob in blobs:
        for m in href_re.finditer(blob):
            u = _abs_az(m.group(1))
            if u:
                urls.append(u)

    # dedupe preserve order
    out: List[str] = []
    seen: set[str] = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


async def _safe_ai_polish(summary: str, title: str, url: str) -> str:
    if not summary:
        return ""
    try:
        return await ai_polish_summary(summary, title, url)
    except Exception:
        return summary
    
def _extract_view_dom_id(html: str) -> str:
    if not html:
        return ""
    # common: <input type="hidden" name="view_dom_id" value="...">
    m = re.search(r'name=["\']view_dom_id["\']\s+value=["\']([^"\']+)["\']', html, re.I)
    if m:
        return (m.group(1) or "").strip()

    # sometimes in Drupal settings JSON blobs
    m2 = re.search(r'"view_dom_id"\s*:\s*"([^"]+)"', html, re.I)
    if m2:
        return (m2.group(1) or "").strip()

    return ""

def _extract_va_title(html: str) -> str:
    """
    VA pages often have a generic header <h1> ("Governor of Virginia").
    Try meta og:title, then all headings, skipping generic ones.
    """
    if not html:
        return ""

    generic = {"governor of virginia", "home", "newsroom"}

    # og:title
    m = re.search(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html)
    if m:
        t = re.sub(r"\s+", " ", m.group(1)).strip()
        if t and t.lower() not in generic:
            return t

    # collect h1/h2 candidates
    cands: list[str] = []
    for tag in ("h1", "h2"):
        for mh in re.finditer(rf'(?is)<{tag}[^>]*>(.*?)</{tag}>', html):
            t = re.sub(r'(?is)<[^>]+>', " ", mh.group(1))
            t = re.sub(r"\s+", " ", t).strip()
            if not t:
                continue
            if t.lower() in generic:
                continue
            cands.append(t)

    if cands:
        cands.sort(key=len, reverse=True)
        return cands[0]

    # last fallback: <title>
    m2 = re.search(r'(?is)<title[^>]*>(.*?)</title>', html)
    if m2:
        t = re.sub(r'(?is)<[^>]+>', " ", m2.group(1))
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            return t

    return ""

_GENERIC_TITLES = {
    "proclamations",
    "governor of virginia",
    "newsroom",
    "home",
}

def _title_from_va_slug(url: str) -> str:
    """
    url slug -> human title
    e.g. .../religious-freedom-day-3.html -> Religious Freedom Day
    """
    path = urlsplit(url).path.rstrip("/")
    slug = path.rsplit("/", 1)[-1]
    slug = slug.replace(".html", "").replace(".htm", "")
    slug = re.sub(r"-\d+$", "", slug)  # drop trailing -3, -2, etc.
    slug = slug.replace("-", " ").strip()
    slug = re.sub(r"\s+", " ", slug)
    # Title Case but keep small words lower-ish
    words = slug.split()
    if not words:
        return ""
    small = {"and", "or", "the", "a", "an", "of", "to", "in", "for", "on"}
    out = []
    for i, w in enumerate(words):
        lw = w.lower()
        if i != 0 and lw in small:
            out.append(lw)
        else:
            out.append(lw.capitalize())
    return " ".join(out).strip()

def _extract_va_title_by_status(html: str, url: str, status: str) -> str:
    """
    For proclamations: VA pages are generic, so prefer URL slug.
    For others: use existing extractor logic.
    """
    t = _extract_va_title(html) or _extract_h1(html) or ""
    tl = t.strip().lower()

    if status == VA_STATUS_MAP["proclamations"]:
        # if it's generic, derive from slug
        if not t or tl in _GENERIC_TITLES:
            slug_t = _title_from_va_slug(url)
            return slug_t or (t.strip() if t else url)
        return t.strip()

    # news / everything else
    if t and tl not in _GENERIC_TITLES:
        return t.strip()
    return (t.strip() if t else url)


async def _get(
    cx: httpx.AsyncClient,
    url: str,
    tries: int = 3,
    read_timeout: float = 45.0,
    headers: dict | None = None,
) -> httpx.Response:
    last_exc = None
    for i in range(tries):
        try:
            headers = clean_headers(headers)   # ✅ ADD THIS LINE
            r = await cx.get(
                url,
                headers=headers,
                timeout=httpx.Timeout(connect=15.0, read=read_timeout, write=15.0, pool=None),
            )
            if r.status_code < 500 and r.status_code != 429:
                return r
        except (
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
            httpx.ConnectError,
            httpx.RemoteProtocolError,
            httpx.ReadError, 
            httpx.WriteError,
        ) as e:
            last_exc = e
        await asyncio.sleep(0.5 * (2 ** i))

    return httpx.Response(
        599,
        request=httpx.Request("GET", url),
        content=b"",
        headers={"X-Error": str(last_exc) if last_exc else ""},
    )

def _extract_h1(html: str) -> str:
    # og:title first
    m = re.search(r'(?is)<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html)
    if m:
        t = re.sub(r"\s+", " ", m.group(1)).strip()
        if t:
            return t

    # longest h1
    h1s: list[str] = []
    for mh in re.finditer(r'(?is)<h1[^>]*>(.*?)</h1>', html):
        t = re.sub(r'(?is)<[^>]+>', ' ', mh.group(1))
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            h1s.append(t)
    if h1s:
        h1s.sort(key=len, reverse=True)
        return h1s[0]

    # title tag fallback
    m2 = re.search(r'(?is)<title[^>]*>(.*?)</title>', html)
    if m2:
        t = re.sub(r'(?is)<[^>]+>', ' ', m2.group(1))
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            return t

    return ""

_US_MONTH_DATE_RE = re.compile(
    r'\b('
    r'Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May\.?|Jun(?:e)?\.?|'
    r'Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t(?:ember)?)?\.?|Oct(?:ober)?\.?|'
    r'Nov(?:ember)?\.?|Dec(?:ember)?\.?'
    r')\s+\d{1,2},\s+\d{4}\b',
    re.I
)

def _parse_us_month_date(s: str) -> datetime | None:
    s = re.sub(r"\s+", " ", (s or "").strip())
    # remove trailing dots in month abbreviations: "Dec." -> "Dec"
    s = re.sub(r"(?i)\b(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.", r"\1", s)

    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _date_guard_not_future(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    if dt > now + timedelta(days=2):
        return None
    return dt

def _date_from_json_ld(html: str) -> datetime | None:
    if not html:
        return None

    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.S,
    ):
        blob = (m.group(1) or "").strip()
        if not blob:
            continue

        try:
            data = __import__("json").loads(blob)
        except Exception:
            continue

        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue

            # ✅ PRIMARY: datePublished / dateCreated
            dp = node.get("datePublished") or node.get("dateCreated")
            if dp:
                try:
                    dt = datetime.fromisoformat(str(dp).replace("Z", "+00:00")).astimezone(timezone.utc)
                    dt = _date_guard_not_future(dt)
                    if dt:
                        return dt
                except Exception:
                    pass

            # ✅ SECONDARY: dateModified (only if not "today", since that's often untrustworthy)
            dm = node.get("dateModified")
            if dm:
                try:
                    dm_dt = datetime.fromisoformat(str(dm).replace("Z", "+00:00")).astimezone(timezone.utc)
                    dm_dt = _date_guard_not_future(dm_dt)
                    now = datetime.now(timezone.utc)
                    if dm_dt and dm_dt.date() != now.date():
                        return dm_dt
                except Exception:
                    pass

    return None

def _date_from_meta(html: str) -> datetime | None:
    if not html:
        return None
    pats = [
        r'property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
        r'name=["\']publish[-_ ]?date["\'][^>]+content=["\'](.*?)["\']',
        r'itemprop=["\']datePublished["\'][^>]+content=["\'](.*?)["\']',
    ]
    for pat in pats:
        m = re.search(pat, html, re.I)
        if not m:
            continue
        val = (m.group(1) or "").strip()
        if not val:
            continue
        # iso-ish
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass
        # rfc2822-ish
        try:
            return parsedate_to_datetime(val).astimezone(timezone.utc)
        except Exception:
            pass
    return None

def _date_from_ohio_article(html: str, url: str) -> datetime | None:
    dt = _date_from_meta(html) or _date_from_json_ld(html)
    dt = _date_guard_not_future(dt)
    if dt:
        return dt

    text = _strip_html_to_text(html)
    m = _US_MONTH_DATE_RE.search(text)
    if m:
        dt2 = _parse_us_month_date(m.group(0))
        dt2 = _date_guard_not_future(dt2) if dt2 else None
        if dt2:
            return dt2


    # last modified header is a last resort (handled in caller if needed)
    return None


# ----------------------------
# Ohio listing crawl (HTML)
# ----------------------------

def _ohio_detail_link_re(section: str) -> re.Pattern:
    prefix = OH_SECTION_PREFIX[section]  # e.g. "/media/news-and-media/"
    return re.compile(
        r'href=["\'](?P<u>(?:https?://governor\.ohio\.gov)?' + re.escape(prefix) + r'[^"\']+)["\']',
        re.I
    )


async def _collect_ohio_listing_urls(
    cx: httpx.AsyncClient,
    section: str,
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_url: str | None = None,
    referer: str | None = None,
) -> List[str]:
    """
    Tries:
      - base page: /media/<section>
      - then /media/<section>?page=1.. until no new URLs
    Stops early if stop_at_url is encountered (inclusive).
    """
    base = OH_COMPONENT_LISTING[section]         # XHR HTML listing
    prefix = OH_SECTION_PREFIX[section]          # "/media/....../"
    listing_public = OH_PUBLIC_PAGES[section]        # public landing page for this section
    seen: set[str] = set()
    out: List[str] = []

    for p in range(0, max_pages):
        page_url = base if p == 0 else _set_query_param(base, "page", str(p))
        r = await _get(cx, page_url, headers={"Referer": referer} if referer else None)
        if r.status_code >= 400 or not r.text:
            break

        html = r.text
        print("OH LIST PAGE", section, page_url, "len=", len(html))
        if p in (0, 1):
            print("OH LIST PAGE HEAD", section, "p=", p, "head=", repr(html[:200]))

        # Some Ohio pages don’t include <a href="..."> links in server HTML.
        # The detail URLs often appear inside JSON/script as "/media/..." or escaped as "\\/media\\/..."
        page_urls: List[str] = []

        # Normalize escaped slashes from JSON blobs: "\\/media\\/news-and-media\\/..." -> "/media/news-and-media/..."
        html_norm = html.replace("\\/", "/")

        # Look for any occurrences of the section prefix anywhere in the HTML (href OR JSON/script)
        # Example match: /media/news-and-media/some-slug
        pat = re.compile(r"(" + re.escape(prefix) + r"[^\"'\s<>]+)", re.I)

        for blob in (html, html_norm):
            for m in pat.finditer(blob):
                path_like = (m.group(1) or "").strip()
                if not path_like:
                    continue

                abs_u = _abs_ohio(path_like)
                if not abs_u:
                    continue

                # exclude listing root itself
                if abs_u.rstrip("/") == listing_public.rstrip("/"):
                    continue

                # keep only urls still under this section path
                if not urlsplit(abs_u).path.startswith(prefix):
                    continue

                page_urls.append(abs_u)

        # optional debug
        if p == 0:
            print("OH PREFIX", section, "=", prefix)
            print("OH PREFIX COUNT raw:", html.count(prefix), "norm:", html_norm.count(prefix))
            print("OH MATCHES", section, "sample:", page_urls[:5])


        # dedupe + preserve order
        new_count = 0
        for u in page_urls:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            new_count += 1

            if stop_at_url and u == stop_at_url:
                return out  # inclusive stop

            if len(out) >= limit:
                return out

        if new_count == 0:
            # usually means paging param doesn't work OR we reached the end
            break

        await asyncio.sleep(0.15)

    return out

async def _collect_az_views_urls(
    cx: httpx.AsyncClient,
    *,
    kind: str,                  # "press_releases" | "executive_orders"
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> List[str]:
    """
    Collect detail URLs by paging Drupal views/ajax.
    We vary `page` in the form body.
    """

    # -------------------------
    # 1) base_form (same as you had)
    # -------------------------
    if kind == "executive_orders":
        base_form = {
            "view_name": "executive_orders",
            "view_display_id": "panel_pane_2",
            "view_args": "",
            "view_path": "executive-orders",
            "view_base_path": "executiveorder/feed",
            "pager_element": "0",
        }
    elif kind == "press_releases":
        base_form = {
            "view_name": "panopoly_news",
            "view_display_id": "panel_pane_11",
            "view_args": "",
            "view_path": "news-releases",
            "view_base_path": "newsroom/feed",
            "pager_element": "0",
            "type[panopoly_news_article]": "panopoly_news_article",
        }
    else:
        raise ValueError(f"Unknown AZ kind: {kind}")

    # -------------------------
    # 2) ✅ URL filters (defined ONCE)
    # -------------------------
    if kind == "executive_orders":
        keep_re = re.compile(r"^https://azgovernor\.gov/", re.I)

        def is_detail(u: str) -> bool:
            path = urlsplit(u).path.rstrip("/")
            if path == "/executive-orders":
                return False
            return (
                path.startswith("/executive-orders/")
                or "/executive-order" in path
            )
    else:
        keep_re = re.compile(r"^https://azgovernor\.gov/office-arizona-governor/news/", re.I)

        def is_detail(u: str) -> bool:
            return True

    # -------------------------
    # 3) ✅ dom_id fetch ONCE (THIS is what you were missing)
    # -------------------------
    dom_id = ""
    try:
        r0 = await _get(cx, AZ_PUBLIC_PAGES[kind], headers={"Referer": AZ_PUBLIC_PAGES[kind]})
        if r0.status_code < 400 and r0.text:
            dom_id = _extract_view_dom_id(r0.text)
            print("AZ", kind, "view_dom_id =", dom_id)
    except Exception as e:
        print("AZ", kind, "failed to fetch dom_id:", repr(e))
        dom_id = ""

    out: List[str] = []
    seen: set[str] = set()

    # ✅ EO and PR both use page=0 for the newest results
    start_page = 0

    for p in range(start_page, start_page + max_pages):
        form = dict(base_form)
        form["page"] = str(p)

        # ✅ include dom id if found
        if dom_id:
            form["view_dom_id"] = dom_id

        r = await cx.post(
            AZ_VIEWS_AJAX,
            data=form,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": AZ_PUBLIC_PAGES[kind],
            },
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        )
        if r.status_code >= 400 or not r.text:
            break

        try:
            payload = r.json()
        except Exception:
            break

        page_links = _extract_links_from_views_ajax_payload(payload)

        # ✅ debug (EO only)
        if kind == "executive_orders" and p in (start_page, start_page + 1):
            print("AZ EO raw links sample:", page_links[:25])

        new_count = 0
        for u in page_links:
            if not keep_re.search(u):
                continue
            if not is_detail(u):
                continue
            if u in seen:
                continue

            seen.add(u)
            out.append(u)
            new_count += 1

            if stop_at_url and u == stop_at_url:
                return out

            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.15)

    return out

async def _collect_va_static_urls(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    keep_re: re.Pattern,
    max_urls: int = 5000,
    stop_at_url: str | None = None,
) -> List[str]:
    """
    Virginia pages are static lists (no paging). Fetch once, scrape hrefs, filter, stop if needed.
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    out: List[str] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        u = _abs_va(m.group(1))
        if not u:
            continue
        if not keep_re.search(u):
            continue
        if u in seen:
            continue

        seen.add(u)
        out.append(u)

        # inclusive stop
        if stop_at_url and u == stop_at_url:
            break

        if len(out) >= max_urls:
            break

    return out


async def _collect_va_eo_pdf_urls(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    max_urls: int = 5000,
) -> List[str]:
    """
    Executive Actions page exposes EO PDFs like:
      /media/governorvirginiagov/governor-of-virginia/pdf/eo/EO-56.pdf
    We scrape all hrefs and keep only EO PDFs.
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    # EO PDFs only
    keep_re = re.compile(r"/pdf/eo/EO-\d+\.pdf$", re.I)

    out: List[str] = []
    seen: set[str] = set()

    for m in href_re.finditer(html):
        u = _abs_va(m.group(1))
        if not u:
            continue
        if not keep_re.search(urlsplit(u).path):
            continue
        if u in seen:
            continue

        seen.add(u)
        out.append(u)

        if len(out) >= max_urls:
            break

    return out

async def _filter_new_external_ids(conn, source_id: int, urls: list[str]) -> list[str]:
    """
    Return only urls not already present in DB for this source_id.
    DB-side filter so we don't load the entire history.
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



# ----------------------------
# Ohio ingest
# ----------------------------

async def ingest_ohio(limit_each: int = 2000, max_pages_each: int = 300) -> Dict[str, object]:
    """
    Ingest Ohio:
      - News: crawl listing, then only keep items that parse into year=2025 (skip 2026+), stop at cutoff URL
      - Appointments: stop at cutoff URL
      - Executive Orders: ingest all found
    """
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={
                **BROWSER_UA_HEADERS,
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://governor.ohio.gov/",  # default; overridden per request below
            },
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        ) as cx:


            src_news = await get_or_create_source(conn, "Ohio — News", "state_newsroom", OH_PUBLIC_PAGES["news"])
            src_appt = await get_or_create_source(conn, "Ohio — Appointments", "state_appointments", OH_PUBLIC_PAGES["appointments"])
            src_eo   = await get_or_create_source(conn, "Ohio — Executive Orders", "state_executive_orders", OH_PUBLIC_PAGES["executive_orders"])

            news_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_news) or 0
            appt_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_appt) or 0
            eo_existing   = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0

            news_backfill = (news_existing == 0)
            appt_backfill = (appt_existing == 0)
            eo_backfill   = (eo_existing == 0)


            # ----------------------------
            # Cron-safe filtering / backfill mode (single crawl)
            # ----------------------------

            def _effective_crawl_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    return int(max_pages_each or 0), int(limit_each or 0)
                # cron buffer: crawl deeper so new items hidden behind old ones aren't missed
                mp = max(int(max_pages_each or 0), 120)
                lim = max(int(limit_each or 0), 3000)
                return mp, lim

            mp_news, lim_news = _effective_crawl_params(news_backfill)
            mp_appt, lim_appt = _effective_crawl_params(appt_backfill)
            mp_eo,   lim_eo   = _effective_crawl_params(eo_backfill)

            # 1) collect URLs — ONCE, using effective params
            news_urls = await _collect_ohio_listing_urls(
                cx, "news",
                max_pages=mp_news,
                limit=lim_news,
                stop_at_url=OH_NEWS_CUTOFF_URL,
                referer=OH_PUBLIC_PAGES["news"],
            )

            appt_urls = await _collect_ohio_listing_urls(
                cx, "appointments",
                max_pages=mp_appt,
                limit=lim_appt,
                stop_at_url=OH_APPTS_CUTOFF_URL,
                referer=OH_PUBLIC_PAGES["appointments"],
            )

            eo_urls = await _collect_ohio_listing_urls(
                cx, "executive_orders",
                max_pages=mp_eo,
                limit=lim_eo,
                stop_at_url=None,
                referer=OH_PUBLIC_PAGES["executive_orders"],
            )

            # Decide what to PROCESS
            news_new_urls = news_urls[:] if news_backfill else await _filter_new_external_ids(conn, src_news, news_urls)
            appt_new_urls = appt_urls[:] if appt_backfill else await _filter_new_external_ids(conn, src_appt, appt_urls)
            eo_new_urls   = eo_urls[:]   if eo_backfill   else await _filter_new_external_ids(conn, src_eo, eo_urls)

            print(f"OH news mode={'backfill' if news_backfill else 'cron_safe'} new={len(news_new_urls)} seen={len(news_urls)}")
            print(f"OH appt mode={'backfill' if appt_backfill else 'cron_safe'} new={len(appt_new_urls)} seen={len(appt_urls)}")
            print(f"OH eo   mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)}")


            out["news_seen_urls"] = len(news_urls)
            out["appointments_seen_urls"] = len(appt_urls)
            out["executive_orders_seen_urls"] = len(eo_urls)

            out["news_new_urls"] = len(news_new_urls)
            out["appointments_new_urls"] = len(appt_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_urls)

            async def upsert_url(source_id: int, status: str, url: str, enforce_news_year: bool = False) -> bool:
                r = await _get(cx, url)
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = _date_from_ohio_article(html, url)
                if not pub_dt:
                    lm = r.headers.get("Last-Modified")
                    if lm:
                        try:
                            pub_dt = parsedate_to_datetime(lm).astimezone(timezone.utc)
                            pub_dt = _date_guard_not_future(pub_dt)
                        except Exception:
                            pub_dt = None

                # Optional: if somehow we got a wildly future date, skip.
                # (Most of this is already handled by _date_guard_not_future)
                if pub_dt is None:
                    # keep items even if date parsing fails (safer for not missing content)
                    pass

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
                    "ohio",
                    "Ohio Governor",
                    status,
                    pub_dt,
                )
                return True

            upserted = {"news": 0, "appointments": 0, "executive_orders": 0}

            for u in news_new_urls:
                if await upsert_url(src_news, STATUS_MAP["news"], u, enforce_news_year=False):
                    upserted["news"] += 1

            for u in appt_new_urls:
                if await upsert_url(src_appt, STATUS_MAP["appointments"], u, enforce_news_year=False):
                    upserted["appointments"] += 1

            for u in eo_new_urls:
                if await upsert_url(src_eo, STATUS_MAP["executive_orders"], u, enforce_news_year=False):
                    upserted["executive_orders"] += 1

            out["upserted"] = upserted
            return out
        
# ----------------------------
# Arizona Proclamations (goyff.az.gov) helpers
# ----------------------------

_AZ_PROC_KEEP_RE = re.compile(r"^https://goyff\.az\.gov/", re.I)

def _az_proc_page_url(page_num: int) -> str:
    """
    goyff.az.gov uses a weird "page=0,0,N" style.
    Your examples:
      page=0%2C0%2C0, 0%2C0%2C1, 0%2C0%2C2 ...
    """
    return (
        "https://goyff.az.gov/proclamations"
        "?field_featured_categories_tid%5B4%5D=4"
        "&sort_order=DESC&sort_by=created"
        f"&page=0%2C0%2C{page_num}"
    )

def _az_proc_norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = "https://goyff.az.gov" + u
    # drop fragment + normalize trivial trailing slash
    parts = urlsplit(u)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
    return clean.rstrip("/")

async def _collect_az_proclamation_urls(
    cx: httpx.AsyncClient,
    *,
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> List[str]:
    """
    Scrape proclamation detail URLs from listing pages.
    """
    out: List[str] = []
    seen: set[str] = set()

    stop_norm = _az_proc_norm_url(stop_at_url) if stop_at_url else None

    def _az_proc_canon_path(u: str | None) -> str | None:
        uu = _az_proc_norm_url(u) if u else None
        if not uu:
            return None
        pth = urlsplit(uu).path or ""
        # treat /proclamations/<slug> and /<slug> as equivalent
        if pth.startswith("/proclamations/"):
            pth = "/" + pth[len("/proclamations/"):]
        return pth.rstrip("/") or "/"

    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for p in range(0, max_pages):
        page_url = _az_proc_page_url(p)
        r = await _get(cx, page_url, headers={"Referer": AZ_PUBLIC_PAGES["proclamations"]})
        if r.status_code >= 400 or not r.text:
            break

        html = _nz(r.text)

        # 🔍 DEBUG: sanity check listing page fetch
        print("AZ PROC page", p, "html_len=", len(html))
        print("AZ PROC page", p, "count /proclamations/ =", html.count("/proclamations/"))

        page_links: List[str] = []
        for m in href_re.finditer(html):
            raw = (m.group(1) or "").strip()
            u = _az_proc_norm_url(raw)
            if not u:
                continue
            if not _AZ_PROC_KEEP_RE.search(u):
                continue

            # keep only proclamation detail pages (most common is /proclamations/<slug>)
            path = urlsplit(u).path or ""

            # ✅ Accept both:
            #   1) /proclamations/<slug>
            #   2) /<slug>   (e.g., /DVAM2025)
            is_proc_style_1 = path.startswith("/proclamations/") and len([p for p in path.split("/") if p]) >= 2
            # style_2 is only valid if the slug clearly looks like a proclamation slug
            # (these almost always contain a year like 2024/2025/2019, etc.)
            is_proc_style_2 = (
                re.fullmatch(r"/[A-Za-z0-9_-]{4,}", path) is not None
                and re.search(r"(?:19|20)\d{2}", path) is not None
            )

            # exclude obvious non-detail routes
            bad_prefixes = ("/node/", "/user/", "/search", "/sitemap", "/taxonomy")
            if path == "/" or path.startswith(bad_prefixes):
                continue

            # IMPORTANT: do NOT exclude "/proclamations/<slug>" detail pages.
            # Only exclude the listing root itself.
            if path.rstrip("/") == "/proclamations":
                continue

            if not (is_proc_style_1 or is_proc_style_2):
                continue

            page_links.append(u)

        # dedupe + preserve order
        new_count = 0
        for u in page_links:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            new_count += 1

            stop_canon = _az_proc_canon_path(stop_norm) if stop_norm else None
            u_canon = _az_proc_canon_path(u)

            if stop_canon and u_canon and u_canon == stop_canon:
                return out  # inclusive stop

            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.15)

    return out

def _az_proc_date_from_html(html: str) -> datetime | None:
    """
    Prefer the human-readable date near the top (e.g., 'January 1, 2026').
    Fallback: parse the 'DONE at the Capitol ...' line (day word + month + year words).
    """
    if not html:
        return None
    
    # 0) BEST: date near the title (right after <h1>)
    try:
        mh1 = re.search(r"(?is)<h1[^>]*>.*?</h1>", html)
        if mh1:
            nearby_html = html[mh1.end(): mh1.end() + 9000]
            nearby_text = _strip_html_to_text(nearby_html)
            m0 = _US_MONTH_DATE_RE.search(nearby_text or "")
            if m0:
                dt0 = _parse_us_month_date(m0.group(0))
                dt0 = _date_guard_not_future(dt0) if dt0 else None
                if dt0:
                    return dt0
    except Exception:
        pass

    # 1) Top-of-page "Month D, YYYY" (use only early part to avoid grabbing DONE-date first)
    text = _strip_html_to_text(html)
    head = (text or "")[:2500]
    m = _US_MONTH_DATE_RE.search(head)
    if m:
        dt = _parse_us_month_date(m.group(0))
        dt = _date_guard_not_future(dt) if dt else None
        if dt:
            return dt

    # 2) Fallback: "DONE ... on this fifteenth day of December in the year Two Thousand and Twenty-Five ..."
    done_re = re.compile(
        r"DONE\s+at\s+the\s+Capitol.*?on\s+this\s+(?P<dayword>[A-Za-z\-]+)\s+day\s+of\s+"
        r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)\s+"
        r"in\s+the\s+year\s+(?P<yearwords>[A-Za-z\-\s]+?)\s+and\s+of\s+the",
        re.I | re.S,
    )
    m2 = done_re.search(text or "")
    if not m2:
        return None

    dayword = (m2.group("dayword") or "").lower().replace("-", " ").strip()
    month = (m2.group("month") or "").strip()
    yearwords = (m2.group("yearwords") or "").lower().replace("-", " ").strip()

    day_map = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
        "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
        "eleventh": 11, "twelfth": 12, "thirteenth": 13, "fourteenth": 14, "fifteenth": 15,
        "sixteenth": 16, "seventeenth": 17, "eighteenth": 18, "nineteenth": 19, "twentieth": 20,
        "twenty first": 21, "twenty second": 22, "twenty third": 23, "twenty fourth": 24,
        "twenty fifth": 25, "twenty sixth": 26, "twenty seventh": 27, "twenty eighth": 28,
        "twenty ninth": 29, "thirtieth": 30, "thirty first": 31,
    }
    day = day_map.get(dayword)

    def _az_yearwords_to_int(s: str) -> int | None:
        # Handles typical patterns like: "two thousand and twenty five"
        toks = [t for t in re.findall(r"[a-z]+", s) if t != "and"]
        if not toks:
            return None
        base = 0
        i = 0
        num = {
            "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
            "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
            "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
            "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60,
            "seventy": 70, "eighty": 80, "ninety": 90,
        }
        while i < len(toks):
            t = toks[i]
            if t == "thousand":
                if base == 0:
                    base = 1
                base *= 1000
                i += 1
                continue
            # try "twenty five" composition
            if t in ("twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"):
                v = num[t]
                if i + 1 < len(toks) and toks[i + 1] in num and num[toks[i + 1]] < 10:
                    v += num[toks[i + 1]]
                    i += 1
                base += v
                i += 1
                continue
            if t in num:
                base += num[t]
                i += 1
                continue
            i += 1
        return base if base >= 1900 else None

    year = _az_yearwords_to_int(yearwords)

    if not (day and year):
        return None

    try:
        # reuse your US month-date parser for month name -> month number
        dt = _parse_us_month_date(f"{month} {day}, {year}")
        dt = _date_guard_not_future(dt) if dt else None
        return dt
    except Exception:
        return None
        
async def ingest_arizona(limit_each: int = 5000, max_pages_each: int = 300) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={
                **BROWSER_UA_HEADERS,
                "X-Requested-With": "XMLHttpRequest",
            },
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Arizona — Press Releases", "state_newsroom", AZ_PUBLIC_PAGES["press_releases"]
            )
            src_eo = await get_or_create_source(
                conn, "Arizona — Executive Orders", "state_executive_orders", AZ_PUBLIC_PAGES["executive_orders"]
            )
            # ✅ NEW
            src_proc = await get_or_create_source(
                conn, "Arizona — Proclamations", "state_proclamations", AZ_PUBLIC_PAGES["proclamations"]
            )

            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            proc_backfill = (proc_existing == 0)


            # ----------------------------
            # Cron-safe filtering / backfill mode (single crawl)
            # ----------------------------
            def _effective_crawl_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    return int(max_pages_each or 0), int(limit_each or 0)
                # cron buffer: crawl deeper so new items hidden behind old ones aren't missed
                mp = max(int(max_pages_each or 0), 120)
                lim = max(int(limit_each or 0), 3000)
                return mp, lim

            mp_pr, lim_pr = _effective_crawl_params(pr_backfill)
            mp_eo, lim_eo = _effective_crawl_params(eo_backfill)
            mp_proc, lim_proc = _effective_crawl_params(proc_backfill)

            # 1) collect URLs — ONCE, using effective params
            pr_urls = await _collect_az_views_urls(
                cx,
                kind="press_releases",
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=AZ_PRESS_CUTOFF_URL,
            )

            eo_urls = await _collect_az_views_urls(
                cx,
                kind="executive_orders",
                max_pages=mp_eo,
                limit=lim_eo,
                stop_at_url=None,
            )

            proc_urls = await _collect_az_proclamation_urls(
                cx,
                max_pages=mp_proc,
                limit=lim_proc,
                stop_at_url=AZ_PROC_CUTOFF_URL,
            )

            # Decide what to PROCESS (only new on cron)
            pr_new_urls = pr_urls[:] if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            eo_new_urls = eo_urls[:] if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)
            proc_new_urls = proc_urls[:] if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_urls)

            print(f"AZ PR  mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"AZ EO  mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)}")
            print(f"AZ PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_urls)} seen={len(proc_urls)}")

            print("AZ PR sample new:", pr_new_urls[:5])
            print("AZ EO sample new:", eo_new_urls[:5])
            print("AZ PROC sample new:", proc_new_urls[:5])

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_urls)
            out["proclamations_new_urls"] = len(proc_new_urls)

            async def upsert_url(source_id: int, status: str, url: str) -> bool:
                r = await _get(cx, url)
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                pub_dt = _date_guard_not_future(pub_dt)

                # ✅ NEW: AZ proclamations often have a plain date near the title, plus a DONE clause.
                if not pub_dt and "goyff.az.gov" in url:
                    pub_dt = _az_proc_date_from_html(html)

                if not pub_dt:
                    text = _strip_html_to_text(html)

                    # ✅ EO pages: prefer the first date near the top (right under "Executive Order 2025-01")
                    if status == STATUS_MAP["executive_orders"]:
                        head = "\n".join([ln.strip() for ln in text.splitlines() if ln.strip()][:40])
                        m = _US_MONTH_DATE_RE.search(head)
                        if m:
                            dt2 = _parse_us_month_date(m.group(0))
                            pub_dt = _date_guard_not_future(dt2) if dt2 else None

                    # fallback: search entire text
                    if not pub_dt:
                        m = _US_MONTH_DATE_RE.search(text)
                        if m:
                            dt2 = _parse_us_month_date(m.group(0))
                            pub_dt = _date_guard_not_future(dt2) if dt2 else None


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
                    "arizona",
                    "Arizona Governor",
                    status,
                    pub_dt,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}

            for u in pr_new_urls:
                if await upsert_url(src_pr, AZ_STATUS_MAP["press_releases"], u):
                    upserted["press_releases"] += 1

            for u in eo_new_urls:
                if await upsert_url(src_eo, AZ_STATUS_MAP["executive_orders"], u):
                    upserted["executive_orders"] += 1

            for u in proc_new_urls:
                if await upsert_url(src_proc, AZ_STATUS_MAP["proclamations"], u):
                    upserted["proclamations"] += 1


            out["upserted"] = upserted
            return out

async def ingest_virginia(limit_each: int = 5000, max_pages_each: int = 1) -> Dict[str, object]:
    """
    Virginia (cron-safe + backfill-safe):
      - News releases: use govnewsfeed; stop at VA_NEWS_CUTOFF_URL (inclusive). Future-proof (2026+ ok).
      - Proclamations: scrape list once (with stop_at_url as configured).
      - Executive orders: scrape EO PDF links from executive-actions page.
    """
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=15.0, pool=None),
        ) as cx:

            src_news = await get_or_create_source(
                conn, "Virginia — News Releases", "state_newsroom", VA_PUBLIC_PAGES["news_releases"]
            )
            src_proc = await get_or_create_source(
                conn, "Virginia — Proclamations", "state_proclamations", VA_PUBLIC_PAGES["proclamations"]
            )
            src_eo = await get_or_create_source(
                conn, "Virginia — Executive Orders", "state_executive_orders", VA_PUBLIC_PAGES["executive_orders"]
            )

            # --- detect backfill mode per-source ---
            news_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_news) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0
            eo_existing   = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0

            news_backfill = (news_existing == 0)
            proc_backfill = (proc_existing == 0)
            eo_backfill   = (eo_existing == 0)

            # ----------------------------
            # Cron-safe crawl params
            # ----------------------------
            def _effective_crawl_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    return int(max_pages_each or 1), int(limit_each or 0)
                # cron buffer: crawl deeper so new items hidden behind old ones aren't missed
                mp = max(int(max_pages_each or 1), 3)
                lim = max(int(limit_each or 0), 3000)
                return mp, lim

            mp_news, lim_news = _effective_crawl_params(news_backfill)
            mp_proc, lim_proc = _effective_crawl_params(proc_backfill)
            mp_eo,   lim_eo   = _effective_crawl_params(eo_backfill)

            # ----------------------------
            # 1) COLLECT URLs (once)
            # ----------------------------

            # --- VA news via JSON feed (future-proof: no year_only filter) ---
            news_pairs = await _collect_va_news_urls_from_feed(
                cx,
                feed_url=VA_NEWS_FEED_URL,
                max_urls=lim_news,
                stop_at_url=VA_NEWS_CUTOFF_URL,  # ✅ stop at cutoff inclusive
                year_only=None,                  # ✅ FUTURE-PROOF (2026/2027+ allowed)
                max_pages=mp_news,
            )

            # Fallback if feed fails: scrape the listing page for at least 1 latest item
            if not news_pairs:
                try:
                    latest = await _collect_va_news_urls(
                        cx,
                        page_url=VA_PUBLIC_PAGES["news_releases"],
                        max_urls=1,
                        stop_at_url=None,
                    )
                    if latest:
                        news_pairs = [(latest[0], None)]
                except Exception:
                    news_pairs = []

            if not news_pairs:
                # last-resort fallback (kept), but ideally never used
                news_pairs = [(VA_NEWS_LATEST_URL, None)]

            news_urls = [u for (u, _) in news_pairs]
            news_date_map = {u: dt for (u, dt) in news_pairs}

            # --- Proclamations ---
            proc_pairs = await _collect_va_proclamation_urls_with_dates(
                cx,
                page_url=VA_PUBLIC_PAGES["proclamations"],
                years=None,  # ✅ no year filter (future-proof)
                max_urls=lim_proc,
                stop_at_url="https://www.governor.virginia.gov/newsroom/proclamations/proclamation-list/135th-birthday-of-the-united-states-public-health-service-commissioned-corps.html",
            )
            proc_urls = [u for (u, _) in proc_pairs]
            proc_date_map = {u: dt for (u, dt) in proc_pairs}

            # --- Executive Orders PDFs ---
            eo_pdf_urls = await _collect_va_eo_pdf_urls(
                cx,
                page_url=VA_PUBLIC_PAGES["executive_orders"],
                max_urls=lim_eo,
            )

            out["news_releases_seen_urls"] = len(news_urls)
            out["proclamations_seen_urls"] = len(proc_urls)
            out["executive_orders_seen_urls"] = len(eo_pdf_urls)

            # ----------------------------
            # 2) FILTER to only NEW urls (cron-safe) unless backfill
            # ----------------------------
            news_new_urls = news_urls[:] if news_backfill else await _filter_new_external_ids(conn, src_news, news_urls)
            proc_new_urls = proc_urls[:] if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_urls)
            eo_new_urls   = eo_pdf_urls[:] if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_pdf_urls)

            print(f"VA NEWS mode={'backfill' if news_backfill else 'cron_safe'} new={len(news_new_urls)} seen={len(news_urls)}")
            print(f"VA PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_urls)} seen={len(proc_urls)}")
            print(f"VA EO   mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_pdf_urls)}")

            out["news_releases_new_urls"] = len(news_new_urls)
            out["proclamations_new_urls"] = len(proc_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_urls)

            async def upsert_html_url(
                source_id: int,
                status: str,
                url: str,
                jurisdiction: str,
                agency: str,
                forced_published_at: datetime | None = None,
            ) -> bool:
                r = await _get(cx, url)
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_va_title_by_status(html, url, status) or url

                pub_dt = forced_published_at

                if not pub_dt and status == VA_STATUS_MAP["news_releases"]:
                    pub_dt = _date_from_va_news(html, url)

                if not pub_dt:
                    pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                    pub_dt = _date_guard_not_future(pub_dt)

                if not pub_dt:
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None

                # ✅ REMOVED: "enforce 2025-only" (this was blocking 2026+)
                # We rely on VA_NEWS_CUTOFF_URL to control backfill depth.

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
                    jurisdiction,
                    agency,
                    status,
                    pub_dt,
                )
                return True

            async def upsert_pdf_url(
                source_id: int,
                status: str,
                url: str,
                jurisdiction: str,
                agency: str,
                published_at: datetime | None = None,
            ) -> bool:
                r = await _get(cx, url)
                if r.status_code >= 400:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if ("pdf" not in ct) and (not url.lower().endswith(".pdf")):
                    return False

                path = urlsplit(url).path
                fname = (path.rsplit("/", 1)[-1] or "").strip()
                title = (
                    fname.replace(".pdf", "")
                        .replace("_", " ")
                        .replace("-", " ")
                        .strip()
                    or url
                )

                summary = ""
                try:
                    pdf_bytes = r.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        eo_dt = _extract_va_eo_date(pdf_text)
                        if eo_dt:
                            published_at = eo_dt

                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)
                except Exception:
                    summary = ""

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
                    jurisdiction,
                    agency,
                    status,
                    published_at,
                )
                return True

            upserted = {"news_releases": 0, "proclamations": 0, "executive_orders": 0}

            # ✅ Only process NEW urls in cron-safe mode (prevents repolish)
            for u in news_new_urls:
                if await upsert_html_url(
                    src_news,
                    VA_STATUS_MAP["news_releases"],
                    u,
                    "virginia",
                    "Virginia Governor",
                    forced_published_at=news_date_map.get(u),
                ):
                    upserted["news_releases"] += 1

            for u in proc_new_urls:
                if await upsert_html_url(
                    src_proc,
                    VA_STATUS_MAP["proclamations"],
                    u,
                    "virginia",
                    "Virginia Governor",
                    forced_published_at=proc_date_map.get(u),
                ):
                    upserted["proclamations"] += 1

            for u in eo_new_urls:
                if await upsert_pdf_url(
                    src_eo,
                    VA_STATUS_MAP["executive_orders"],
                    u,
                    "virginia",
                    "Virginia Governor",
                    published_at=None,
                ):
                    upserted["executive_orders"] += 1

            out["upserted"] = upserted
            return out

async def ingest_georgia(limit_each: int = 5000, max_pages_each: int = 50) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Georgia — Press Releases", "state_newsroom", GA_PUBLIC_PAGES["press_releases_2025"]
            )
            src_eo = await get_or_create_source(
                conn,
                "Georgia — Executive Orders",
                "state_executive_orders",
                GA_PUBLIC_PAGES["executive_orders_home"],
            )

            # --- detect backfill mode per-source ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)

            # ----------------------------
            # Cron-safe crawl params
            # ----------------------------
            def _effective_crawl_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    return int(max_pages_each or 0), int(limit_each or 0)
                # cron buffer
                mp = max(int(max_pages_each or 0), 20)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_crawl_params(pr_backfill)
            mp_eo, lim_eo = _effective_crawl_params(eo_backfill)

            # ----------------------------
            # Press Releases (future-proof)
            # Crawl current year + previous year, newest first
            # ----------------------------
            now_year = datetime.now(timezone.utc).year
            years_to_crawl = [now_year, now_year - 1]

            pr_pairs: list[tuple[str, datetime | None]] = []
            remaining = lim_pr

            for y in years_to_crawl:
                if remaining <= 0:
                    break
                pairs_y = await _collect_ga_press_release_pairs(
                    cx, year=y, max_pages=mp_pr, limit=remaining
                )
                pr_pairs.extend(pairs_y)
                remaining = lim_pr - len(pr_pairs)

            # dedupe preserve order
            seen_pr: set[str] = set()
            pr_urls: list[str] = []
            pr_date_map: dict[str, datetime | None] = {}
            for (u, dt) in pr_pairs:
                if not u or u in seen_pr:
                    continue
                seen_pr.add(u)
                pr_urls.append(u)
                pr_date_map[u] = dt

            out["press_releases_seen_urls"] = len(pr_urls)

            # ✅ cron-safe filter: only new URLs unless backfill
            pr_new_urls = pr_urls[:] if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)

            print(f"GA PR  mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            out["press_releases_new_urls"] = len(pr_new_urls)

            async def upsert_html_url(
                source_id: int,
                status: str,
                url: str,
                forced_published_at: datetime | None = None,
            ) -> bool:
                r = await _get(cx, url, headers={"Referer": "https://gov.georgia.gov/press-releases"})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = forced_published_at
                if not pub_dt:
                    pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                    pub_dt = _date_guard_not_future(pub_dt)

                if not pub_dt:
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None

                # final fallback: parse from URL path YYYY-MM-DD
                if not pub_dt:
                    pub_dt = _date_from_ga_url(url)

                if not pub_dt:
                    return False

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
                    GA_JURISDICTION,
                    GA_AGENCY,
                    status,
                    pub_dt,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0}

            # ✅ only upsert NEW press releases in cron mode (prevents repolish)
            for u in pr_new_urls:
                if await upsert_html_url(
                    src_pr,
                    GA_STATUS_MAP["press_releases"],
                    u,
                    forced_published_at=pr_date_map.get(u),
                ):
                    upserted["press_releases"] += 1

            # ----------------------------
            # Executive Orders (future-proof; stops at cutoff inclusive)
            # ----------------------------
            year_urls = await _collect_ga_eo_year_urls(cx, GA_PUBLIC_PAGES["executive_orders_home"])

            eo_rows: list[tuple[str, str, str, datetime | None]] = []
            hit_cutoff = False

            for yurl in year_urls:
                rows = await _collect_ga_eo_rows_from_year_page(cx, yurl)
                for (dl, num, desc, pub_dt) in rows:
                    eo_rows.append((dl, num, desc, pub_dt))
                    if _ga_norm_abs(dl) == _ga_norm_abs(GA_EO_CUTOFF_URL):
                        hit_cutoff = True
                        break
                if hit_cutoff:
                    break

            out["executive_orders_seen_urls"] = len(eo_rows)

            eo_urls = [dl for (dl, _, _, _) in eo_rows]

            # ✅ cron-safe filter: only new EO download urls unless backfill
            eo_new_urls = eo_urls[:] if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)
            eo_new_set = set(eo_new_urls)

            print(f"GA EO  mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)}")
            out["executive_orders_new_urls"] = len(eo_new_urls)

            async def upsert_ga_eo_row(
                source_id: int,
                status: str,
                dl_url: str,
                eo_number: str,
                desc: str,
                published_at: datetime | None,
            ) -> bool:
                title = (f"{eo_number} — {desc}".strip(" —")) or eo_number or dl_url

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
                    _nz(dl_url),
                    source_id,
                    _nz(title),
                    _nz(desc),
                    _nz(dl_url),
                    GA_JURISDICTION,
                    GA_AGENCY,
                    status,
                    published_at,
                )
                return True

            # ✅ only upsert NEW EO rows in cron mode
            for (dl, num, desc, pub_dt) in eo_rows:
                if upserted["executive_orders"] >= lim_eo:
                    break
                if dl not in eo_new_set:
                    continue
                if await upsert_ga_eo_row(
                    src_eo,
                    GA_STATUS_MAP["executive_orders"],
                    dl,
                    num,
                    desc,
                    pub_dt,
                ):
                    upserted["executive_orders"] += 1

            out["upserted"] = upserted
            return out
        
async def ingest_hawaii(limit_each: int = 5000, max_pages_each: int = 60) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Hawaii — Press Releases", "state_newsroom", HI_PUBLIC_PAGES["press_releases"]
            )
            src_eo = await get_or_create_source(
                conn, "Hawaii — Executive Orders", "state_executive_orders", HI_PUBLIC_PAGES["executive_orders"]
            )
            src_proc = await get_or_create_source(
                conn, "Hawaii — Proclamations", "state_proclamations", HI_PUBLIC_PAGES["proclamations"]
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            proc_backfill = (proc_existing == 0)

            # --- cron-safe param caps (ignore huge payloads unless backfill) ---
            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe = cap hard (so your "generic run" stays light)
                mp = max(1, min(int(max_pages_each or 0) or 1, 4))      # <= 4 pages
                lim = max(50, min(int(limit_each or 0) or 200, 800))    # <= 800 urls
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_eo, lim_eo = _effective_params(eo_backfill)
            mp_proc, lim_proc = _effective_params(proc_backfill)

            # --- Collect seen URLs (bounded by mode) ---
            pr_urls = await _collect_hi_press_release_urls(
                cx,
                start_url=HI_PUBLIC_PAGES["press_releases"],
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=HI_PRESS_CUTOFF_URL,   # future-proof: stops at fixed 2025 boundary
            )
            pr_urls = [clean_url(u) for u in pr_urls if u]

            # --- Collect seen URLs (bounded by mode) ---
            eo_items = await _collect_hi_pdf_items_from_category(
                cx,
                start_url=HI_PUBLIC_PAGES["executive_orders"],
                max_pages=mp_eo,
                limit=lim_eo,
                stop_at_pdf_url=None,  # all EOs
            )

            proc_items = await _collect_hi_pdf_items_from_category(
                cx,
                start_url=HI_PUBLIC_PAGES["proclamations"],
                max_pages=mp_proc,
                limit=lim_proc,
                stop_at_pdf_url=HI_PROC_CUTOFF_PDF_URL,  # stop at 2025 boundary (inclusive)
            )

            print("HI EO sample:", eo_items[:3])
            print("HI PROC sample:", proc_items[:3])

            out["press_releases_seen_urls"] = len(pr_urls)
            out["executive_orders_seen_urls"] = len(eo_items)
            out["proclamations_seen_urls"] = len(proc_items)

            eo_urls = [clean_url(u) for (u, _t, _dt) in eo_items if u]
            proc_urls = [clean_url(u) for (u, _t, _dt) in proc_items if u]

            # --- Cron-safe filtering (ONLY new external_ids unless backfill) ---
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            eo_new_urls = eo_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)
            proc_new_urls = proc_urls if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_urls)

            # Map back to title + posted date for only-new processing
            eo_map = {clean_url(u): (u, t, dt) for (u, t, dt) in eo_items if u}
            proc_map = {clean_url(u): (u, t, dt) for (u, t, dt) in proc_items if u}

            eo_new_items = [eo_map[u] for u in eo_new_urls if u in eo_map]
            proc_new_items = [proc_map[u] for u in proc_new_urls if u in proc_map]

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_urls)
            out["proclamations_new_urls"] = len(proc_new_urls)

            print(f"HI PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"HI EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)}")
            print(f"HI PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_urls)} seen={len(proc_urls)}")

            # Fast exit if nothing new (prevents re-polishing)
            if not pr_new_urls and not eo_new_items and not proc_new_items:
                out["upserted"] = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}
                return out

            async def upsert_html_url(source_id: int, status: str, url: str) -> bool:
                r = await _get(cx, url, headers={"Referer": HI_PUBLIC_PAGES["press_releases"]})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                pub_dt = _date_guard_not_future(pub_dt)

                if not pub_dt:
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None

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
                    "hawaii",
                    "Hawaii Governor",
                    status,
                    pub_dt,
                )
                return True

            async def upsert_pdf_url(
                source_id: int,
                status: str,
                url: str,
                title_hint: str = "",
                published_at_hint: datetime | None = None,
            ) -> bool:
                r = await _get(cx, url, headers={"Referer": HI_PUBLIC_PAGES["all_newsroom"]})
                if r.status_code >= 400:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if ("pdf" not in ct) and (not url.lower().endswith(".pdf")):
                    return False

                # Title: prefer listing card title
                title = (title_hint or "").strip()
                if not title:
                    path = urlsplit(url).path
                    fname = (path.rsplit("/", 1)[-1] or "").strip()
                    title = (
                        fname.replace(".pdf", "")
                            .replace("_", " ")
                            .replace("-", " ")
                            .strip()
                        or url
                    )

                # Date: prefer "Posted on ..." from listing card, fallback to filename yymmdd...
                published_at = _date_guard_not_future(published_at_hint) or _hi_date_from_pdf_filename(url)
                published_at = _date_guard_not_future(published_at)

                summary = ""
                try:
                    pdf_bytes = r.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, url)
                except Exception:
                    summary = ""

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
                    "hawaii",
                    "Hawaii Governor",
                    status,
                    published_at,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}

            # IMPORTANT: Only process NEW urls in cron_safe mode (prevents repolish)
            for u in pr_new_urls:
                if await upsert_html_url(src_pr, HI_STATUS_MAP["press_releases"], u):
                    upserted["press_releases"] += 1

            for (u, t, dt) in eo_new_items:
                if await upsert_pdf_url(src_eo, HI_STATUS_MAP["executive_orders"], u, t, dt):
                    upserted["executive_orders"] += 1

            for (u, t, dt) in proc_new_items:
                if await upsert_pdf_url(src_proc, HI_STATUS_MAP["proclamations"], u, t, dt):
                    upserted["proclamations"] += 1

            out["upserted"] = upserted
            return out

async def ingest_vermont(limit_each: int = 5000, max_pages_each: int = 30) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Vermont — Press Releases", "state_newsroom", VT_PUBLIC_PAGES["press_releases"]
            )
            src_eo = await get_or_create_source(
                conn, "Vermont — Executive Orders", "state_executive_orders",
                "https://governor.vermont.gov/document-types/executive-orders"
            )
            src_proc = await get_or_create_source(
                conn, "Vermont — Proclamations", "state_proclamations",
                "https://governor.vermont.gov/document-categories/proclamations"
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            proc_backfill = (proc_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe buffers (ignore huge payloads)
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_eo, lim_eo = _effective_params(eo_backfill)
            mp_proc, lim_proc = _effective_params(proc_backfill)


            # ---- Collect listing URLs ----
            pr_urls_raw = await _collect_vt_listing_urls(
                cx,
                base_url=VT_PUBLIC_PAGES["press_releases"],
                keep_re=_VT_KEEP_PRESS_RE,
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=_canon_vt(VT_PRESS_CUTOFF_URL),
                referer=VT_PUBLIC_PAGES["press_releases"],
            )

            # canonicalize + dedupe preserve order
            pr_urls = []
            seen = set()
            for u in pr_urls_raw:
                cu = _canon_vt(u)
                if cu and cu not in seen:
                    seen.add(cu)
                    pr_urls.append(cu)

            eo_doc_urls_raw = await _collect_vt_listing_urls(
                cx,
                base_url="https://governor.vermont.gov/document-types/executive-orders",
                keep_re=_VT_KEEP_DOC_RE,
                max_pages=mp_eo,
                limit=lim_eo,
                stop_at_url=None,
                referer="https://governor.vermont.gov/document-types/executive-orders",
            )

            eo_doc_urls = []
            seen = set()
            for u in eo_doc_urls_raw:
                cu = _canon_vt(u)
                if cu and cu not in seen:
                    seen.add(cu)
                    eo_doc_urls.append(cu)

            proc_doc_urls_raw = await _collect_vt_listing_urls(
                cx,
                base_url="https://governor.vermont.gov/document-categories/proclamations",
                keep_re=_VT_KEEP_DOC_RE,
                max_pages=mp_proc,
                limit=lim_proc,
                stop_at_url=_canon_vt(VT_PROC_CUTOFF_URL),
                referer="https://governor.vermont.gov/document-categories/proclamations",
            )

            proc_doc_urls = []
            seen = set()
            for u in proc_doc_urls_raw:
                cu = _canon_vt(u)
                if cu and cu not in seen:
                    seen.add(cu)
                    proc_doc_urls.append(cu)

            out["press_releases_seen_urls"] = len(pr_urls)
            out["executive_orders_seen_urls"] = len(eo_doc_urls)
            out["proclamations_seen_urls"] = len(proc_doc_urls)

            # ✅ Cron-safe filtering (only new external_ids unless backfill)
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            eo_new_doc_urls = eo_doc_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_doc_urls)
            proc_new_doc_urls = proc_doc_urls if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_doc_urls)

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_doc_urls)
            out["proclamations_new_urls"] = len(proc_new_doc_urls)

            print(f"VT PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"VT EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_doc_urls)} seen={len(eo_doc_urls)}")
            print(f"VT PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_doc_urls)} seen={len(proc_doc_urls)}")

            # ✅ Fast exit: nothing new to ingest
            if not pr_new_urls and not eo_new_doc_urls and not proc_new_doc_urls:
                out["upserted"] = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}
                return out

            async def upsert_press_release(url: str) -> bool:
                r = await _get(cx, url, headers={"Referer": VT_PUBLIC_PAGES["press_releases"]})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                pub_dt = _date_guard_not_future(pub_dt)
                if not pub_dt:
                    # VT press pages usually show a visible "Month DD, YYYY"
                    pub_dt = _date_from_vt_doc_page(html)

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
                    src_pr,
                    _nz(title),
                    _nz(summary),
                    url,
                    "vermont",
                    "Vermont Governor",
                    VT_STATUS_MAP["press_releases"],
                    pub_dt,
                )
                return True

            async def upsert_doc_with_pdf(doc_url: str, status: str, source_id: int, referer: str) -> bool:
                """
                Fetch the VT document page, extract the PDF link, then fetch PDF and summarize text.
                We store the PDF URL as the item URL (so clicking opens the actual doc),
                but we keep external_id = doc_url (stable canonical page).
                """
                r = await _get(cx, doc_url, headers={"Referer": referer})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or doc_url

                pub_dt = _date_from_vt_doc_page(html)

                pdf_url = _extract_first_pdf_link_vt(html)
                if not pdf_url:
                    # If VT ever changes markup, don’t insert broken items
                    return False

                # fetch pdf for summary
                summary = ""
                try:
                    pr = await _get(cx, pdf_url, headers={"Referer": doc_url}, read_timeout=90.0)
                    if pr.status_code < 400:
                        pdf_bytes = pr.content or b""
                        pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                        if pdf_text:
                            summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                            if summary:
                                summary = _soft_normalize_caps(summary)
                                summary = await _safe_ai_polish(summary, title, pdf_url)
                except Exception:
                    summary = ""

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
                    doc_url,            # external_id = canonical doc page
                    source_id,
                    _nz(title),
                    _nz(summary),
                    pdf_url,            # url = actual PDF
                    "vermont",
                    "Vermont Governor",
                    status,
                    pub_dt,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}

            for u in pr_new_urls:
                if await upsert_press_release(u):
                    upserted["press_releases"] += 1

            for u in eo_new_doc_urls:
                if await upsert_doc_with_pdf(
                    u,
                    VT_STATUS_MAP["executive_orders"],
                    src_eo,
                    referer=VT_PUBLIC_PAGES["executive_orders"],
                ):
                    upserted["executive_orders"] += 1

            for u in proc_new_doc_urls:
                if await upsert_doc_with_pdf(
                    u,
                    VT_STATUS_MAP["proclamations"],
                    src_proc,
                    referer=VT_PUBLIC_PAGES["proclamations"],
                ):
                    upserted["proclamations"] += 1


            out["upserted"] = upserted
            return out

# --- ADD THIS WHOLE UTAH BLOCK INTO app/ingest_states2.py ---
# (place it near the other state configs + helpers, before INGESTERS_V2)

# ----------------------------
# Utah config (WordPress paging + single-page EO/Declarations)
# ----------------------------

UT_PUBLIC_PAGES = {
    "news": "https://governor.utah.gov/news/",
    "executive_orders": "https://governor.utah.gov/executive-orders/",
    "declarations": "https://governor.utah.gov/declarations/",
}

# News: newest -> older, stop at this (inclusive). (1st item of 2025)
UT_NEWS_CUTOFF_URL = (
    "https://governor.utah.gov/press/"
    "gov-cox-signs-executive-order-to-streamline-permitting-and-empower-utahns-to-build-a-future-of-abundance/"
)

UT_STATUS_MAP = {
    "news": "news",
    "executive_orders": "executive_order",
    "declarations": "declaration",
}

UT_JURISDICTION = "utah"
UT_AGENCY = "Utah Governor"

# Listing paths we accept from /news/ pages
# Only accept actual article detail pages (Utah uses /press/ for news posts)
_UT_KEEP_NEWS_DETAIL_RE = re.compile(r"^https://governor\.utah\.gov/press/[^#?]+/?$", re.I)

# Google Drive "view" => direct download
_UT_GDRIVE_VIEW_RE = re.compile(r"^https?://drive\.google\.com/file/d/([^/]+)/view", re.I)

def _ut_drive_download_url(view_url: str) -> str:
    m = _UT_GDRIVE_VIEW_RE.match((view_url or "").strip())
    if not m:
        return view_url
    fid = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={fid}"

_UT_GDRIVE_FILE_ID_RE = re.compile(r"^https?://drive\.google\.com/file/d/([^/]+)/", re.I)

def _ut_canon_id(u: str) -> str:
    u = clean_url(u or "")
    u = unquote(u)
    u = u.replace("“", "").replace("”", "").replace("’", "").replace("‘", "").strip()

    m = _UT_GDRIVE_FILE_ID_RE.match(u)
    if m:
        fid = m.group(1)
        return f"https://drive.google.com/file/d/{fid}/view"

    sp = urlsplit(u)
    sp = sp._replace(query="", fragment="")
    return urlunsplit(sp).rstrip("/")


def _ut_news_page(page: int) -> str:
    # https://governor.utah.gov/news/page/2/
    base = UT_PUBLIC_PAGES["news"].rstrip("/") + "/"
    if page <= 1:
        return base
    return f"{base}page/{page}/"

async def _collect_ut_news_urls(
    cx: httpx.AsyncClient,
    *,
    max_pages: int = 400,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> list[str]:
    """
    Crawl /news/page/N/ and extract detail URLs (mostly /press/...).
    Stops at stop_at_url (inclusive).
    """
    out: list[str] = []
    seen: set[str] = set()
    href_re = re.compile(r'href=["\']([^"\']+)["\']', re.I)

    for p in range(1, max_pages + 1):
        page_url = _ut_news_page(p)
        r = await _get(cx, page_url, headers={"Referer": UT_PUBLIC_PAGES["news"]})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")
        page_found: list[str] = []

        for m in href_re.finditer(html):
            u = (m.group(1) or "").split("#")[0].strip()
            if not u:
                continue
            if not u.startswith("http"):
                # only accept same-site relative links
                if u.startswith("/"):
                    u = "https://governor.utah.gov" + u
                else:
                    continue
            u = u.split("?")[0].rstrip("/")

            # 🚫 never treat listing pages as items
            if re.search(r"/news/page/\d+/?$", u, re.I) or u.rstrip("/") == "https://governor.utah.gov/news":
                continue

            if not _UT_KEEP_NEWS_DETAIL_RE.match(u):
                continue

            page_found.append(u)


        new_count = 0
        for u in page_found:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
            new_count += 1

            if stop_at_url and u.rstrip("/") == stop_at_url.rstrip("/"):
                return out
            if len(out) >= limit:
                return out

        if new_count == 0:
            break

        await asyncio.sleep(0.1)

    return out

def _ut_slice_section_by_year(html: str, prefix: str, year: int) -> str:
    if not html:
        return ""

    blob = html.replace("\\/", "/")

    # Start markers (in priority order)
    start_res = [
        re.compile(rf'(?is)\b(?:id|name)\s*=\s*["\']{re.escape(prefix)}[-_]?{year}["\']'),
        re.compile(rf'(?is)\b(?:id|name)\s*=\s*["\']{year}["\']'),
        re.compile(rf'(?is)<h[1-6][^>]*>\s*{year}\s*</h[1-6]>'),
    ]

    m = None
    for sr in start_res:
        m = sr.search(blob)
        if m:
            break
    if not m:
        return ""

    start = m.start()

    # End at next year marker (year headings or id/name anchors)
    end_re = re.compile(
        r'(?is)('
        r'<h[1-6][^>]*>\s*20\d{2}\s*</h[1-6]>'
        r'|\b(?:id|name)\s*=\s*["\'](?:declarations|orders)[-_]?20\d{2}["\']'
        r'|\b(?:id|name)\s*=\s*["\']20\d{2}["\']'
        r')'
    )
    m2 = end_re.search(blob, m.end())
    end = m2.start() if m2 else len(blob)

    return blob[start:end]


_UT_LI_DATE_LINK_RE = re.compile(
    r'(?is)<li[^>]*>\s*(?P<date>[^<]{0,80}?)\s*'
    r'(?:[:\-–]\s*)?'
    r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>'
)

_UT_ANY_A_RE = re.compile(
    r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>'
)

_UT_KEEP_PDF_RE = re.compile(r"^https://governor\.utah\.gov/wp-content/uploads/.*\.pdf$", re.I)
_UT_KEEP_DRIVE_RE = re.compile(r"^https?://drive\.google\.com/file/d/[^/]+/view", re.I)


def _ut_strip_html(s: str) -> str:
    s = re.sub(r'(?is)<[^>]+>', ' ', s or '')
    return re.sub(r"\s+", " ", s).strip()

def _parse_month_year(s: str) -> datetime | None:
    """
    Parses "January 2025" => 2025-01-01 UTC
    """
    s = re.sub(r"\s+", " ", (s or "").strip())
    m = re.match(r'(?i)^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|'
                 r'Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(20\d{2})$', s)
    if not m:
        return None
    mon_name = m.group(1)
    yr = int(m.group(2))
    # Reuse existing month-date parser by fabricating day=1 with same month token
    fake = f"{mon_name} 1, {yr}"
    return _parse_us_month_date(fake)

def _ut_parse_date_prefix(s: str) -> datetime | None:
    """
    Decls sometimes show:
      - "January 2025"
      - "January 1, 2025"
      - "January 26 - February 1, 2025"
    We take the first parseable date in the string.
    """
    if not s:
        return None
    s = _ut_strip_html(s)

    # Try Month DD, YYYY first
    m = _US_MONTH_DATE_RE.search(s)
    if m:
        dt = _parse_us_month_date(m.group(0))
        return _date_guard_not_future(dt) if dt else None

    # Try Month YYYY
    mm = re.search(
        r'\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|'
        r'Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(20\d{2})\b',
        s,
        re.I,
    )
    if mm:
        dt = _parse_month_year(mm.group(0))
        return _date_guard_not_future(dt) if dt else None

    return None

def _ut_abs_url(u: str) -> str:
    if not u:
        return ""
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return "https://governor.utah.gov" + u

async def _collect_ut_list_items_by_year_anchor(
    cx: httpx.AsyncClient,
    *,
    page_url: str,
    anchor_prefix: str,  # unused now, kept so callers don’t change
    years: list[int],
    limit: int = 10000,
) -> list[tuple[str, str, datetime | None]]:
    """
    Utah EO/Declarations pages are basically:
      <h2>2025</h2>
      <ul><li>Jan. 7, 2025: <a href="...pdf">...</a></li> ...</ul>
      <h2>2024</h2> ...

    They don't reliably expose id/name anchors like orders2025, declarations2025.
    So: scan headings + li blocks in order, track current section year, and
    collect PDF/Drive links from <li> blocks.
    """
    r = await _get(cx, page_url, headers={"Referer": page_url})
    if r.status_code >= 400 or not r.text:
        return []

    html = r.text.replace("\\/", "/")

    # Walk headings + list items in document order
    token_re = re.compile(
        r"(?is)(<h[1-6][^>]*>.*?</h[1-6]>|<li[^>]*>.*?</li>)"
    )
    year_re = re.compile(r"\b(20\d{2})\b")

    out: list[tuple[str, str, datetime | None]] = []
    seen: set[str] = set()

    current_section_year: int | None = None
    years_set = set(years or [])

    def _strip_tags(s: str) -> str:
        s = re.sub(r"(?is)<[^>]+>", " ", s or "")
        return re.sub(r"\s+", " ", s).strip()

    for m in token_re.finditer(html):
        chunk = m.group(1) or ""

        # Heading: update current year context
        if chunk.lstrip().lower().startswith("<h"):
            ht = _strip_tags(chunk)
            ym = year_re.search(ht)
            if ym:
                current_section_year = int(ym.group(1))
            continue

        # List item: try to find a link
        li_html = chunk
        a = _UT_ANY_A_RE.search(li_html)
        if not a:
            continue

        href_raw = clean_url(a.group("href") or "")
        if not href_raw:
            continue

        href = href_raw.split("#")[0].strip()
        href = clean_url(href)

        # normalize URL
        if href.startswith("/"):
            href = "https://governor.utah.gov" + href
        elif href.startswith("//"):
            href = "https:" + href

        # keep only Drive view links or direct PDFs
        if not (_UT_KEEP_DRIVE_RE.match(href) or _UT_KEEP_PDF_RE.match(href) or href.lower().endswith(".pdf")):
            continue

        # Filter to requested years if we can infer year
        li_text = _strip_tags(li_html)

        dt = _ut_parse_date_prefix(li_text)
        inferred_year = dt.year if dt else current_section_year

        if years_set:
            # If we have a year, enforce it. If we don't, allow it (rare).
            if inferred_year is not None and inferred_year not in years_set:
                continue

        if href in seen:
            continue
        seen.add(href)

        title = _ut_strip_html(a.group("title") or "").strip()
        if not title:
            # fallback title from filename
            fname = urlsplit(href).path.rsplit("/", 1)[-1]
            title = (
                fname.replace(".pdf", "")
                    .replace("_", " ")
                    .replace("-", " ")
                    .strip()
                or href
            )

        out.append((href, title, dt))
        if len(out) >= limit:
            break

    return out

async def ingest_utah(limit_each: int = 8000, max_pages_each: int = 300) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_news = await get_or_create_source(conn, "Utah — News", "state_newsroom", UT_PUBLIC_PAGES["news"])
            src_eo = await get_or_create_source(conn, "Utah — Executive Orders", "state_executive_orders", UT_PUBLIC_PAGES["executive_orders"])
            src_decl = await get_or_create_source(conn, "Utah — Declarations", "state_declarations", UT_PUBLIC_PAGES["declarations"])

            # --- per-source backfill detection ---
            news_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_news) or 0
            eo_existing   = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            decl_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_decl) or 0

            news_backfill = (news_existing == 0)
            eo_backfill   = (eo_existing == 0)
            decl_backfill = (decl_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe buffers (ignore huge payloads)
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_news, lim_news = _effective_params(news_backfill)
            mp_docs, lim_docs = _effective_params(eo_backfill or decl_backfill)

            # --- 1) NEWS (paged) ---
            news_urls = await _collect_ut_news_urls(
                cx,
                max_pages=mp_news,
                limit=lim_news,
                stop_at_url=UT_NEWS_CUTOFF_URL,
            )
            out["news_seen_urls"] = len(news_urls)

            current_year = datetime.now(timezone.utc).year

            # Backfill = crawl deep. Cron-safe = only last 2 years max.
            if eo_backfill or decl_backfill:
                min_year = 2021
                years_docs = list(range(current_year, min_year - 1, -1))
            else:
                years_docs = [current_year, current_year - 1]  # or just [current_year]


            eo_items = await _collect_ut_list_items_by_year_anchor(
                cx,
                page_url=UT_PUBLIC_PAGES["executive_orders"],
                anchor_prefix="orders",
                years=years_docs,
                limit=lim_docs,
            )

            decl_items = await _collect_ut_list_items_by_year_anchor(
                cx,
                page_url=UT_PUBLIC_PAGES["declarations"],
                anchor_prefix="declarations",
                years=years_docs,
                limit=lim_docs,
            )

            # ✅ ADD THESE TWO LINES (you’re missing them)
            out["executive_orders_seen_urls"] = len(eo_items)
            out["declarations_seen_urls"] = len(decl_items)
            

            # ✅ Cron-safe filtering (only new external_ids unless backfill)
            news_new_urls = news_urls if news_backfill else await _filter_new_external_ids(conn, src_news, news_urls)

            # preserve mapping back to items
            # preserve mapping back to items (keyed by canonical external_id)
            eo_map: dict[str, tuple[str, str, datetime | None]] = {}
            for (u, t, dt) in eo_items:
                canon = _ut_canon_id(u)
                if canon and canon not in eo_map:
                    eo_map[canon] = (u, t, dt)

            decl_map: dict[str, tuple[str, str, datetime | None]] = {}
            for (u, t, dt) in decl_items:
                canon = _ut_canon_id(u)
                if canon and canon not in decl_map:
                    decl_map[canon] = (u, t, dt)

            eo_ids = list(eo_map.keys())
            decl_ids = list(decl_map.keys())

            eo_new_ids = eo_ids if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_ids)
            decl_new_ids = decl_ids if decl_backfill else await _filter_new_external_ids(conn, src_decl, decl_ids)

            eo_new_items = [eo_map[i] for i in eo_new_ids if i in eo_map]
            decl_new_items = [decl_map[i] for i in decl_new_ids if i in decl_map]

            out["news_new_urls"] = len(news_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_items)
            out["declarations_new_urls"] = len(decl_new_items)

            print(f"UT NEWS mode={'backfill' if news_backfill else 'cron_safe'} new={len(news_new_urls)} seen={len(news_urls)}")
            print(f"UT EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_items)} seen={len(eo_items)}")
            print(f"UT DECL mode={'backfill' if decl_backfill else 'cron_safe'} new={len(decl_new_items)} seen={len(decl_items)}")

            # ✅ E) Fast exit if nothing new
            if not news_new_urls and not eo_new_items and not decl_new_items:
                out["upserted"] = {"news": 0, "executive_orders": 0, "declarations": 0}
                return out


            print("UT EO sample:", eo_items[:5])
            print("UT DECL sample:", decl_items[:5])

            async def upsert_html_url(source_id: int, status: str, url: str) -> bool:
                r = await _get(cx, url, headers={"Referer": UT_PUBLIC_PAGES["news"]})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                pub_dt = _date_guard_not_future(pub_dt)
                if not pub_dt:
                    # Many UT press pages display e.g. "December 17, 2025"
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None

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
                    UT_JURISDICTION,
                    UT_AGENCY,
                    status,
                    pub_dt,
                )
                return True

            async def upsert_doc_url(
                source_id: int,
                status: str,
                doc_url: str,
                title_hint: str,
                published_at_hint: datetime | None,
            ) -> bool:
                """
                Store doc_url as the item url.
                If it's a Drive "view" URL, fetch the download URL for PDF text extraction.
                """
                doc_url = clean_url(doc_url)
                fetch_url = doc_url
                if _UT_GDRIVE_VIEW_RE.match(doc_url):
                    fetch_url = clean_url(_ut_drive_download_url(doc_url))   # ✅ wrap
                else:
                    fetch_url = clean_url(fetch_url)   # ✅ keep consistent

                r = await _get(
                    cx,
                    fetch_url,
                    headers={
                        "Referer": clean_url(doc_url),   # ✅ sanitize header value
                    },
                    read_timeout=120.0
                )
                if r.status_code >= 400:
                    return False

                # Accept PDF bytes OR a URL ending with .pdf (some servers mislabel ct)
                ct = (r.headers.get("Content-Type") or "").lower()
                is_pdfish = (
                    ("pdf" in ct)
                    or ("octet-stream" in ct)
                    or fetch_url.lower().endswith(".pdf")
                    or doc_url.lower().endswith(".pdf")
                )
                if not is_pdfish:
                    return False

                title = (title_hint or "").strip()
                if not title:
                    if _UT_GDRIVE_VIEW_RE.match(doc_url):
                        # Drive path is /file/d/<id>/view -> filename fallback becomes useless
                        # Pick something clean; you can also branch by status if you want.
                        if status == UT_STATUS_MAP["executive_orders"]:
                            title = "Executive Order"
                        elif status == UT_STATUS_MAP["declarations"]:
                            title = "Declaration"
                        else:
                            title = "Document"
                    else:
                        # fallback title from filename
                        path = urlsplit(doc_url).path
                        fname = (path.rsplit("/", 1)[-1] or "").strip()
                        title = (
                            fname.replace(".pdf", "")
                                .replace("_", " ")
                                .replace("-", " ")
                                .strip()
                            or doc_url
                        )

                published_at = _date_guard_not_future(published_at_hint)

                summary = ""
                try:
                    pdf_bytes = r.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, doc_url)
                except Exception:
                    summary = ""

                external_id = _ut_canon_id(doc_url)

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
                    external_id,      # ✅ canonical external_id
                    source_id,
                    _nz(title),
                    _nz(summary),
                    doc_url,          # ✅ keep original doc URL for user
                    UT_JURISDICTION,
                    UT_AGENCY,
                    status,
                    published_at,
                )
                return True

            upserted = {"news": 0, "executive_orders": 0, "declarations": 0}

            # NEWS
            # NEWS
            for u in news_new_urls:
                if await upsert_html_url(src_news, UT_STATUS_MAP["news"], u):
                    upserted["news"] += 1

            # EOs
            for (u, t, dt) in eo_new_items:
                if await upsert_doc_url(src_eo, UT_STATUS_MAP["executive_orders"], u, t, dt):
                    upserted["executive_orders"] += 1

            # Declarations
            for (u, t, dt) in decl_new_items:
                if await upsert_doc_url(src_decl, UT_STATUS_MAP["declarations"], u, t, dt):
                    upserted["declarations"] += 1

            out["upserted"] = upserted
            return out
        
async def ingest_new_jersey(limit_each: int = 20000, max_pages_each: int = 1) -> Dict[str, object]:
    """
    New Jersey:
      - Press Releases: ingest newest -> back to first 2025 item (inclusive),
        but also include any future years (2026+) automatically.
      - Executive Orders: ingest EO PDFs where Date Issued year is 2024 or 2025.
      - Administrative Orders: ingest ALL AO PDFs from the archive.
    """
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "New Jersey — Press Releases", "state_newsroom", NJ_PUBLIC_PAGES["press_releases"]
            )
            src_eo = await get_or_create_source(
                conn, "New Jersey — Executive Orders", "state_executive_orders", NJ_PUBLIC_PAGES["executive_orders"]
            )
            src_ao = await get_or_create_source(
                conn, "New Jersey — Administrative Orders", "state_administrative_orders", NJ_PUBLIC_PAGES["administrative_orders"]
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            ao_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_ao) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            ao_backfill = (ao_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    return int(max_pages_each or 0), int(limit_each or 0)
                # cron-safe buffers
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_eo, lim_eo = _effective_params(eo_backfill)
            mp_ao, lim_ao = _effective_params(ao_backfill)


            # ---- Press Releases (future years + 2025 until stop url) ----
            pr_pairs = await _collect_nj_press_release_pairs(
                cx,
                year_min=NJ_PRESS_YEAR_MIN,
                cutoff_url=NJ_PRESS_CUTOFF_URL,
                limit=lim_pr,
            )
            pr_urls = [u for (u, _) in pr_pairs]
            pr_date_map = {u: dt for (u, dt) in pr_pairs}

            # ---- Executive Orders ----
            # A) Current governor (rolling years, keeps cron light)
            eo_page_url = await _nj_find_latest_governor_eo_page(cx)

            now_year = datetime.now(timezone.utc).year
            eo_years = set(range(max(NJ_EO_YEAR_MIN, now_year - (NJ_EO_YEARS_ROLLING - 1)), now_year + 1))

            eo_pairs_current = await _collect_nj_eo_pdf_pairs_2024_2025_anygov(
                cx,
                page_url=eo_page_url,
                years=eo_years,
                limit=lim_eo,
            )

            # B) Murphy legacy (EO-415 down to EO-350 inclusive)
            eo_pairs_murphy = await _collect_nj_eo_pdf_pairs_until_cutoff(
                cx,
                page_url=NJ_EO_MURPHY_PAGE,
                cutoff_pdf_url=NJ_EO_MURPHY_CUTOFF_PDF,
                limit=lim_eo,
            )

            # Merge + dedupe (prefer date if available)
            merged: dict[str, datetime | None] = {}
            for (u, dt) in (eo_pairs_current + eo_pairs_murphy):
                if u not in merged or (merged[u] is None and dt is not None):
                    merged[u] = dt

            eo_pairs = [(u, merged[u]) for u in merged.keys()]
            eo_urls = [u for (u, _) in eo_pairs]
            eo_date_map = {u: dt for (u, dt) in eo_pairs}

            # ---- Administrative Orders (all PDFs) ----
            ao_urls = await _collect_nj_ao_pdf_urls(
                cx,
                page_url=NJ_PUBLIC_PAGES["administrative_orders"],
                limit=lim_ao,
            )

            out["press_releases_seen_urls"] = len(pr_urls)
            out["executive_orders_seen_urls"] = len(eo_urls)
            out["administrative_orders_seen_urls"] = len(ao_urls)

            # ✅ Cron-safe filtering (only process new URLs unless backfill)
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            eo_new_urls = eo_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)
            ao_new_urls = ao_urls if ao_backfill else await _filter_new_external_ids(conn, src_ao, ao_urls)

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["executive_orders_new_urls"] = len(eo_new_urls)
            out["administrative_orders_new_urls"] = len(ao_new_urls)

            print(f"NJ PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"NJ EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)} years={sorted(eo_years)}")
            print(f"NJ AO mode={'backfill' if ao_backfill else 'cron_safe'} new={len(ao_new_urls)} seen={len(ao_urls)}")

            async def upsert_html_url(source_id: int, status: str, url: str, forced_published_at: datetime | None) -> bool:
                r = await _get(cx, url, headers={"Referer": NJ_PUBLIC_PAGES["press_releases"]})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)

                # ✅ NJ title strategy:
                # 1) pull real headline from NJ page structure
                # 2) if missing, fall back to _extract_h1 BUT only if not generic
                # 3) final fallback: use URL (since NJ URL has no slug)
                title = _extract_nj_press_title(html)

                # ✅ if extraction returned a chrome/boilerplate title, treat as missing so fallbacks run
                if title and _is_generic_nj_title(title):
                    title = ""

                if not title:
                    t2 = _extract_h1(html)
                    if t2 and not _is_generic_nj_title(t2):
                        title = t2

                if not title:
                    # last resort: don't store URL as a title unless absolutely necessary
                    title = _nj_title_from_url(url) or "New Jersey Press Release"

                pub_dt = forced_published_at
                if not pub_dt:
                    # fallback for safety (some NJ pages may include meta or visible dates)
                    pub_dt = _date_from_meta(html) or _date_from_json_ld(html)
                    pub_dt = _date_guard_not_future(pub_dt)
                if not pub_dt:
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None

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
                    NJ_JURISDICTION,
                    NJ_AGENCY,
                    status,
                    pub_dt,
                )
                return True

            async def upsert_pdf_url(
                source_id: int,
                status: str,
                url: str,
                published_at_hint: datetime | None,
                referer: str,
            ) -> bool:
                r = await _get(cx, url, headers={"Referer": referer}, read_timeout=120.0)
                if r.status_code >= 400:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if ("pdf" not in ct) and (not url.lower().endswith(".pdf")):
                    return False

                path = urlsplit(url).path
                fname = (path.rsplit("/", 1)[-1] or "").strip()
                title = (
                    fname.replace(".pdf", "")
                        .replace("_", " ")
                        .replace("-", " ")
                        .strip()
                    or url
                )

                published_at = _date_guard_not_future(published_at_hint)

                pdf_bytes = r.content or b""
                pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))

                # ✅ NJ AO published_at fallback from PDF text (isolated so it can't kill summary)
                if (not published_at) and (status == NJ_STATUS_MAP["administrative_orders"]) and pdf_text:
                    try:
                        published_at = _date_guard_not_future(_nj_ao_published_at_from_text(pdf_text))
                    except Exception:
                        pass

                # ✅ summary generation (separate try)
                summary = ""
                try:
                    if pdf_text and len(pdf_text.strip()) >= 80:
                        summary = summarize_text(pdf_text, max_sentences=2, max_chars=700) or ""

                    if not summary and pdf_text:
                        lines = [ln.strip() for ln in re.split(r"\n+", pdf_text) if len(ln.strip()) >= 40]
                        if lines:
                            summary = " ".join(lines[:3])[:700]

                    if summary:
                        summary = _soft_normalize_caps(summary)
                        summary = await _safe_ai_polish(summary, title, url)

                except Exception:
                    summary = ""


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
                    NJ_JURISDICTION,
                    NJ_AGENCY,
                    status,
                    published_at,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0, "administrative_orders": 0}

            # Press releases
            for u in pr_new_urls:
                try:
                    ok = await upsert_html_url(
                        src_pr,
                        NJ_STATUS_MAP["press_releases"],
                        u,
                        forced_published_at=pr_date_map.get(u),
                    )
                except Exception as e:
                    print("NJ press release failed:", u, "err:", repr(e))
                    ok = False

                if ok:
                    upserted["press_releases"] += 1

            # Executive Orders (2024/2025 only)
            for u in eo_new_urls:
                referer = NJ_EO_MURPHY_PAGE if "/056murphy/" in u else eo_page_url
                if await upsert_pdf_url(
                    src_eo,
                    NJ_STATUS_MAP["executive_orders"],
                    u,
                    published_at_hint=eo_date_map.get(u),
                    referer=referer,
                ):
                    upserted["executive_orders"] += 1

            # Administrative Orders (all)
            for u in ao_new_urls:
                if await upsert_pdf_url(
                    src_ao,
                    NJ_STATUS_MAP["administrative_orders"],
                    u,
                    published_at_hint=None,
                    referer=NJ_PUBLIC_PAGES["administrative_orders"],
                ):
                    upserted["administrative_orders"] += 1

            out["upserted"] = upserted
            return out
        
async def ingest_colorado(limit_each: int = 5000, max_pages_each: int = 300) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Colorado — Press Releases", "state_newsroom", CO_PUBLIC_PAGES["press_releases"]
            )
            src_eo = await get_or_create_source(
                conn, "Colorado — Executive Orders", "state_executive_orders", "https://www.colorado.gov/governor"
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe buffers (ignore huge payloads)
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_eo, lim_eo = _effective_params(eo_backfill)


            # ---- Press Releases ----
            pr_pairs = await _collect_co_press_release_pairs(
                cx,
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=CO_PRESS_CUTOFF_URL,
            )
            
            pr_urls_raw = [u for (u, _) in pr_pairs]
            pr_date_map_raw = {u: dt for (u, dt) in pr_pairs}

            # ✅ canonicalize + dedupe (preserve order)
            pr_urls = []
            seen_pr: set[str] = set()
            for x in pr_urls_raw:
                cu = _canon_co(x)
                if not cu or cu in seen_pr:
                    continue
                seen_pr.add(cu)
                pr_urls.append(cu)

            # ✅ rebuild date map keyed by canonical URL
            pr_date_map = {}
            for raw_u, dt in pr_pairs:
                cu = _canon_co(raw_u)
                if cu and cu not in pr_date_map:
                    pr_date_map[cu] = dt

            # ✅ Cron-safe filtering
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)

            out["press_releases_seen_urls"] = len(pr_urls)
            out["press_releases_new_urls"] = len(pr_new_urls)

            print(f"CO PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")

            # ---- Executive Orders (dynamic year pages) ----
            current_year = datetime.now(timezone.utc).year

            eo_items: list[tuple[str, str, datetime | None, int]] = []

            # Try from current year down to 2024 (inclusive)
            for y in range(current_year, 2023, -1):
                page_url = _co_eo_year_page(y)
                items_y = await _collect_co_eo_drive_items(cx, page_url=page_url, limit=lim_eo)
                if items_y:
                    eo_items.extend([(u, t, dt, y) for (u, t, dt) in items_y])

            out["executive_orders_seen_urls"] = len(eo_items)

            # ✅ Canonicalize EO external_ids (drive view URLs) and filter new unless backfill
            eo_urls: list[str] = []
            for (u, _, _, _) in eo_items:
                cu = clean_url(u)
                cu = cu.split("#")[0].split("?")[0].rstrip("/")
                if cu:
                    eo_urls.append(cu)

            # dedupe preserve order
            eo_urls_dedup: list[str] = []
            seen_eo: set[str] = set()
            for cu in eo_urls:
                if cu in seen_eo:
                    continue
                seen_eo.add(cu)
                eo_urls_dedup.append(cu)
            eo_urls = eo_urls_dedup

            eo_new_urls = eo_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)

            out["executive_orders_new_urls"] = len(eo_new_urls)
            print(f"CO EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_urls)} seen={len(eo_urls)}")

            eo_new_set = set(eo_new_urls)

            # Only keep EO items whose canonical view_url is new
            eo_new_items = []
            for (u, t, dt, y) in eo_items:
                cu = clean_url(u).split("#")[0].split("?")[0].rstrip("/")
                if cu in eo_new_set:
                    eo_new_items.append((u, t, dt, y))
            
            # ✅ Fast exit: nothing new to ingest
            if not pr_new_urls and not eo_new_items:
                out["upserted"] = {"press_releases": 0, "executive_orders": 0}
                return out

            async def upsert_html_url(source_id: int, status: str, url: str, forced_published_at: datetime | None = None,) -> bool:
                r = await _get(cx, url, headers={"Referer": CO_PUBLIC_PAGES["press_releases"]})
                if r.status_code >= 400 or not r.text:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if "html" not in ct:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = forced_published_at

                if not pub_dt:
                    pub_dt = (
                        _date_from_meta(html)
                        or _date_from_json_ld(html)
                        or _date_from_co_press(html)
                    )
                    pub_dt = _date_guard_not_future(pub_dt)

                if not pub_dt:
                    text = _strip_html_to_text(html)
                    m = _US_MONTH_DATE_RE.search(text)
                    if m:
                        dt2 = _parse_us_month_date(m.group(0))
                        pub_dt = _date_guard_not_future(dt2) if dt2 else None


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
                    CO_JURISDICTION,
                    CO_AGENCY,
                    status,
                    pub_dt,
                )
                return True

            async def upsert_drive_pdf(
                source_id: int,
                status: str,
                view_url: str,
                title_hint: str,
                date_hint: datetime | None,
                page_year: int,
            ) -> bool:
                view_url = clean_url(view_url)
                fetch_url = _co_drive_download_url(view_url)

                r = await _get(
                    cx,
                    fetch_url,
                    headers={"Referer": view_url},
                    read_timeout=120.0,
                )
                if r.status_code >= 400:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                is_pdfish = ("pdf" in ct) or ("octet-stream" in ct) or fetch_url.lower().endswith(".pdf")
                if not is_pdfish:
                    return False

                title = (title_hint or "").strip()
                if not title:
                    title = "Colorado Executive Order"

                published_at = _date_guard_not_future(date_hint)

                # ✅ if we couldn't parse anything, fall back to the page year
                if not published_at:
                    published_at = datetime(page_year, 1, 1, tzinfo=timezone.utc)

                summary = ""
                try:
                    pdf_bytes = r.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        # ✅ extract EO date from signed PDF text
                        eo_dt = _extract_co_eo_date(pdf_text)

                        if eo_dt:
                            print("CO EO date extracted:", eo_dt.date(), "url=", view_url)
                        else:
                            print("CO EO date NOT found, fallback page_year=", page_year, "url=", view_url)

                        if eo_dt:
                            published_at = eo_dt

                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, view_url)
                except Exception:
                    summary = ""

                # ✅ allow EO years >= 2024 (future-proof)
                if not published_at or published_at.year < 2024:
                    return False


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
                    view_url,          # external_id stable = drive view link
                    source_id,
                    _nz(title),
                    _nz(summary),
                    view_url,          # url user clicks
                    CO_JURISDICTION,
                    CO_AGENCY,
                    status,
                    published_at,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0}

            for u in pr_new_urls:
                if await upsert_html_url(
                    src_pr,
                    CO_STATUS_MAP["press_releases"],
                    u,
                    forced_published_at=pr_date_map.get(u),
                ):
                    upserted["press_releases"] += 1

            for (u, t, dt, y) in eo_new_items:
                if await upsert_drive_pdf(src_eo, CO_STATUS_MAP["executive_orders"], u, t, dt, y):
                    upserted["executive_orders"] += 1

            out["upserted"] = upserted
            return out
        
def _date_from_us_month_text(html: str) -> datetime | None:
    """
    Find a visible 'Month DD, YYYY' date anywhere in the page's readable text.
    Useful fallback when meta/json-ld are missing.
    """
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _US_MONTH_DATE_RE.search(text)
    if not m:
        return None
    dt = _parse_us_month_date(m.group(0))
    return _date_guard_not_future(dt) if dt else None

_AK_DATED_LINE_RE = re.compile(
    r'\bDATED\s+this\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+'
    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b',
    re.I
)

def _date_from_ak_dated_line(html: str) -> datetime | None:
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _AK_DATED_LINE_RE.search(text)
    if not m:
        return None

    day = int(m.group(1))
    month_name = m.group(2).title()
    year = int(m.group(3))

    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }

    try:
        dt = datetime(year, month_map[month_name], day, tzinfo=timezone.utc)
    except Exception:
        return None

    return _date_guard_not_future(dt)
        
# ----------------------------
# Alaska config
# ----------------------------

AK_PUBLIC_PAGES = {
    "press_releases": "https://gov.alaska.gov/newsroom/",
    "proclamations": "https://gov.alaska.gov/proclamations/",
    "administrative_orders": "https://gov.alaska.gov/administrative-orders/",
}

AK_STATUS_MAP = {
    "press_releases": "press_release",
    "proclamations": "proclamation",
    "administrative_orders": "administrative_order",
}

AK_JURISDICTION = "alaska"
AK_AGENCY = "Alaska Governor"

# stop at FIRST item of 2025 (inclusive)
AK_PRESS_CUTOFF_URL = "https://gov.alaska.gov/governor-dunleavy-orders-flags-to-fly-full-staff-on-inauguration-day-2/"
AK_PROC_CUTOFF_URL  = "https://gov.alaska.gov/mentoring-month-5/"
AK_AO_CUTOFF_URL    = "https://gov.alaska.gov/admin-orders/administrative-order-no-352/"

_AK_PRESS_DETAIL_RE = re.compile(
    r"^https://gov\.alaska\.gov/(?!newsroom(?:/|$))(?!wp-)(?!category/)(?!tag/)(?!author/)(?!page/)[^/?#]+/?$",
    re.I
)

_AK_ET_DETAIL_RE = re.compile(r"^https://gov\.alaska\.gov/(?!wp-)(?!page/)(?!tag/)(?!category/)(?!author/).+/?$", re.I)

_AK_ELEMENTOR_TITLE_HREF_RE = re.compile(
    r'(?:'
    r'elementor-post__title[^>]*>\s*<a[^>]+href=["\']([^"\']+)["\']'  # <hX class="elementor-post__title"><a href=...>
    r'|'
    r'<a[^>]+class=["\'][^"\']*(?:elementor-post__title-link|elementor-post__title)[^"\']*["\'][^>]*href=["\']([^"\']+)["\']'  # <a class="elementor-post__title-link" href=...>
    r')',
    re.I
)

_AK_ELEMENTOR_CARD_HREF_RE = re.compile(
    r'<a[^>]+class=["\'][^"\']*(?:elementor-post__thumbnail__link|elementor-post__read-more)[^"\']*["\'][^>]*href=["\']([^"\']+)["\']',
    re.I
)

_AK_DIVI_ENTRY_TITLE_HREF_RE = re.compile(
    r'class=["\']entry-title["\'][^>]*>\s*<a[^>]+href=["\']([^"\']+)["\']',
    re.I
)

_AK_REL_BOOKMARK_HREF_RE = re.compile(
    r'<a[^>]+href=["\']([^"\']+)["\'][^>]*\brel=["\']bookmark["\']'
    r'|<a[^>]+\brel=["\']bookmark["\'][^>]*href=["\']([^"\']+)["\']',
    re.I
)

_AK_GARBAGE_PREFIXES = (
    "/contact/",
    "/services/",
    "/video-archive/",
    "/meet-",
    "/meet-the-",
    "/first-lady-",  # optional safety net
)

_AK_ANY_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)

def _ak_is_garbage_path(path: str) -> bool:
    p = (path or "").lower()
    return any(p.startswith(x) for x in _AK_GARBAGE_PREFIXES)

def _abs_ak(u: str) -> str:
    if not u:
        return ""
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return "https://gov.alaska.gov" + u

def _canon_ak(u: str) -> str:
    """
    Canonical Alaska URL:
    - absolute
    - no fragment/query
    - no trailing slash (so DB comparisons are stable)
    """
    u = _abs_ak(u)
    if not u:
        return ""
    u = u.split("#")[0]
    u = u.split("?")[0]
    return u.rstrip("/")


def _ak_press_page(page: int) -> str:
    # page 1 is the base listing
    if page <= 1:
        return "https://gov.alaska.gov/newsroom/"
    # page 2+ is the elementor pagination endpoint
    return f"https://gov.alaska.gov/newsroom/page/{page}/?el_dbe_page"

_AK_NONCE_RE = re.compile(
    r'(?:name=["\']el_dbe_nonce["\']\s+value=["\']([0-9A-Za-z_-]+)["\']'
    r'|el_dbe_nonce["\']?\s*[:=]\s*["\']([0-9A-Za-z_-]+)["\'])',
    re.I
)


def _ak_extract_el_dbe_nonce(html: str) -> str | None:
    if not html:
        return None
    m = _AK_NONCE_RE.search(html)
    if not m:
        return None
    return (m.group(1) or m.group(2))

async def _collect_ak_press_release_urls(
    cx: httpx.AsyncClient,
    *,
    max_pages: int = 300,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> list[str]:

    out: list[str] = []
    seen: set[str] = set()
    stop_norm = _canon_ak(stop_at_url) if stop_at_url else None

    def _extract_urls_from_html(html: str) -> int:
        nonlocal out, seen
        new_count = 0

        # ✅ Divi newsroom cards
        matches = list(_AK_DIVI_ENTRY_TITLE_HREF_RE.finditer(html))
        if not matches:
            matches = list(_AK_REL_BOOKMARK_HREF_RE.finditer(html))

        hrefs: list[str] = []
        if matches:
            for m in matches:
                href = m.group(1) or (m.group(2) if m.lastindex and m.lastindex >= 2 else None)
                if href:
                    hrefs.append(href)
        else:
            hrefs = [m.group(1) for m in _AK_ANY_HREF_RE.finditer(html)]

        for href in hrefs:
            u = _canon_ak(href)
            if not u:
                continue

            if not u.startswith("https://gov.alaska.gov/"):
                continue

            sp = urlsplit(u)

            if _ak_is_garbage_path(sp.path):
                continue

            # skip obvious non-detail junk
            if sp.path in ("/newsroom", "/newsroom/"):
                continue
            if sp.path.startswith("/newsroom/page/"):
                continue
            if sp.path.startswith(("/tag/", "/category/", "/author/")):
                continue
            if sp.path.startswith(("/admin-orders/", "/administrative-orders/", "/proclamations/")):
                continue

            # ✅ Press releases are root slug pages
            if not _AK_PRESS_DETAIL_RE.match(u):
                continue

            u_norm = u  # already canonical

            # stop-at (include cutoff itself)
            if stop_norm and u_norm == stop_norm:
                if u not in seen:
                    seen.add(u)
                    out.append(u)
                return 999999

            if u in seen:
                continue

            seen.add(u)
            out.append(u)
            new_count += 1

            if len(out) >= limit:
                return 999999

        return new_count

    for page in range(1, max_pages + 1):
        # ✅ GET pagination (no nonce, no POST)
        page_url = "https://gov.alaska.gov/newsroom/" if page == 1 else f"https://gov.alaska.gov/newsroom/page/{page}/"
        r = await _get(cx, page_url, headers={"Referer": "https://gov.alaska.gov/newsroom/"})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")

        # ✅ TEMP DEBUG (optional)
        if page in (1, 2):
            print("page:", page, "url:", page_url, "len(html):", len(html))
            print("divi-title-matches:", len(list(_AK_DIVI_ENTRY_TITLE_HREF_RE.finditer(html))))
            print("bookmark-matches:", len(list(_AK_REL_BOOKMARK_HREF_RE.finditer(html))))
            print("any-hrefs:", len(list(_AK_ANY_HREF_RE.finditer(html))))

        new_count = _extract_urls_from_html(html)

        if new_count == 999999:
            return out
        if new_count == 0:
            break

        await asyncio.sleep(0.12)

    return out

def _ak_et_blog_page(base: str, page: int) -> str:
    if page <= 1:
        return base
    return f"{base}page/{page}/?et_blog"

async def _collect_ak_et_blog_urls(
    cx: httpx.AsyncClient,
    *,
    base_url: str,
    limit: int = 5000,
    max_pages: int = 500,
    stop_at_url: str | None = None,
) -> list[str]:
    
    # ✅ ADD THIS RIGHT HERE
    base_url = (base_url or "").strip()
    if not base_url.endswith("/"):
        base_url += "/"

    out: list[str] = []
    seen: set[str] = set()
    href_re = _AK_DIVI_ENTRY_TITLE_HREF_RE

    stop_norm = _canon_ak(stop_at_url) if stop_at_url else None

    for p in range(1, max_pages + 1):
        page_url = _ak_et_blog_page(base_url, p)
        r = await _get(cx, page_url, headers={"Referer": base_url})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")
        page_found = 0

        matches = list(_AK_DIVI_ENTRY_TITLE_HREF_RE.finditer(html))
        if not matches:
            matches = list(_AK_REL_BOOKMARK_HREF_RE.finditer(html))

        for m in matches:
            href = m.group(1) or m.group(2)
            u = _canon_ak(href)
            if not u:
                continue
            if not _AK_ET_DETAIL_RE.match(u):
                continue
            if not u.startswith("https://gov.alaska.gov/"):
                continue

            sp = urlsplit(u)

            if _ak_is_garbage_path(sp.path):
                continue

            # skip listing/pagination links (we only want post/detail pages)
            if "et_blog" in (sp.query or ""):
                continue
            if sp.path.startswith(("/proclamations/page/", "/administrative-orders/page/")):
                continue
            if sp.path.rstrip("/") in ("/proclamations", "/administrative-orders"):
                continue

            u_norm = u  # already canonical

            # stop_at_url robust match (include it)
            if stop_norm and u_norm == stop_norm:
                if u not in seen:
                    seen.add(u)
                    out.append(u)
                return out
            
            if u in seen:
                continue

            seen.add(u)
            out.append(u)
            page_found += 1

            if len(out) >= limit:
                return out

        if page_found == 0:
            break

        await asyncio.sleep(0.15)

    return out

async def ingest_alaska(limit_each: int = 5000, max_pages_each: int = 300) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn, "Alaska — Press Releases", "state_newsroom", AK_PUBLIC_PAGES["press_releases"]
            )
            src_proc = await get_or_create_source(
                conn, "Alaska — Proclamations", "state_proclamations", AK_PUBLIC_PAGES["proclamations"]
            )
            src_ao = await get_or_create_source(
                conn, "Alaska — Administrative Orders", "state_administrative_orders", AK_PUBLIC_PAGES["administrative_orders"]
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0
            ao_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_ao) or 0

            pr_backfill = (pr_existing == 0)
            proc_backfill = (proc_existing == 0)
            ao_backfill = (ao_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe buffers (ignore huge payloads)
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_proc, lim_proc = _effective_params(proc_backfill)
            mp_ao, lim_ao = _effective_params(ao_backfill)


            # ---- Collect URLs ----
            pr_urls = await _collect_ak_press_release_urls(
                cx,
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=AK_PRESS_CUTOFF_URL,
            )

            proc_urls = await _collect_ak_et_blog_urls(
                cx,
                base_url=AK_PUBLIC_PAGES["proclamations"],
                limit=lim_proc,
                max_pages=mp_proc,
                stop_at_url=AK_PROC_CUTOFF_URL,
            )

            ao_urls = await _collect_ak_et_blog_urls(
                cx,
                base_url=AK_PUBLIC_PAGES["administrative_orders"],
                limit=lim_ao,
                max_pages=mp_ao,
                stop_at_url=AK_AO_CUTOFF_URL,
            )

            # ✅ ensure canonical URLs everywhere (extra safety)
            pr_urls = [u for u in (_canon_ak(x) for x in pr_urls) if u]
            proc_urls = [u for u in (_canon_ak(x) for x in proc_urls) if u]
            ao_urls = [u for u in (_canon_ak(x) for x in ao_urls) if u]

            # 🔍 DEBUG: check cross-source overlap (run once)
            overlap_pr_proc = len(set(pr_urls) & set(proc_urls))
            overlap_pr_ao = len(set(pr_urls) & set(ao_urls))
            print("AK overlap PR∩PROC:", overlap_pr_proc, "PR∩AO:", overlap_pr_ao)


            # ✅ prevent cross-source URL overlap (keeps source_id stable across runs)
            proc_set = set(proc_urls)
            ao_set = set(ao_urls)

            pr_before = len(pr_urls)
            pr_urls = [u for u in pr_urls if u not in proc_set and u not in ao_set]
            out["press_releases_removed_overlap"] = pr_before - len(pr_urls)

            out["press_releases_seen_urls"] = len(pr_urls)
            out["proclamations_seen_urls"] = len(proc_urls)
            out["administrative_orders_seen_urls"] = len(ao_urls)

            # ✅ Cron-safe filtering (only process new URLs unless backfill)
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            proc_new_urls = proc_urls if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_urls)
            ao_new_urls = ao_urls if ao_backfill else await _filter_new_external_ids(conn, src_ao, ao_urls)

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["proclamations_new_urls"] = len(proc_new_urls)
            out["administrative_orders_new_urls"] = len(ao_new_urls)

            print(f"AK PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"AK PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_urls)} seen={len(proc_urls)}")
            print(f"AK AO mode={'backfill' if ao_backfill else 'cron_safe'} new={len(ao_new_urls)} seen={len(ao_urls)}")

            # ✅ Fast exit: nothing new to ingest
            if not pr_new_urls and not proc_new_urls and not ao_new_urls:
                out["upserted"] = {
                    "press_releases": 0,
                    "proclamations": 0,
                    "administrative_orders": 0,
                }
                return out

            async def upsert_html(source_id: int, status: str, url: str) -> bool:
                url = _canon_ak(url)
                if not url:
                    return False

                referer = (
                    AK_PUBLIC_PAGES["press_releases"]
                    if status == AK_STATUS_MAP["press_releases"]
                    else AK_PUBLIC_PAGES["proclamations"]
                    if status == AK_STATUS_MAP["proclamations"]
                    else AK_PUBLIC_PAGES["administrative_orders"]
                )

                r = await _get(cx, url, headers={"Referer": referer})
                if r.status_code >= 400 or not r.text:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                pub_dt = (
                    _date_from_meta(html)
                    or _date_from_json_ld(html)
                    or _date_from_ak_dated_line(html)   # <-- ADD THIS LINE
                    or _date_from_us_month_text(html)
                )

                pub_dt = _date_guard_not_future(pub_dt)

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
                    AK_JURISDICTION,
                    AK_AGENCY,
                    status,
                    pub_dt,
                )
                return True

            upserted = {"press_releases": 0, "proclamations": 0, "administrative_orders": 0}

            for u in pr_new_urls:
                if await upsert_html(src_pr, AK_STATUS_MAP["press_releases"], u):
                    upserted["press_releases"] += 1

            for u in proc_new_urls:
                if await upsert_html(src_proc, AK_STATUS_MAP["proclamations"], u):
                    upserted["proclamations"] += 1

            for u in ao_new_urls:
                if await upsert_html(src_ao, AK_STATUS_MAP["administrative_orders"], u):
                    upserted["administrative_orders"] += 1


            out["upserted"] = upserted
            return out
        
# ----------------------------
# Maryland helpers + ingester
# ----------------------------

MD_PUBLIC_PAGES = {
    "press_releases": "https://governor.maryland.gov/news/press/Pages/default.aspx",
    "executive_orders": "https://governor.maryland.gov/news/Pages/executive-orders.aspx",
    "proclamations": "https://governor.maryland.gov/news/Pages/Proclamations.aspx",
}

MD_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

MD_JURISDICTION = "maryland"
MD_AGENCY = "Maryland Governor"

# ---- Cutoffs (inclusive) ----
MD_PRESS_CUTOFF_URL = "https://governor.maryland.gov/news/press/pages/governor-moore%e2%80%99s-statement-on-tentative-labor-agreements-reached-between-the-state-of-maryland-and-state-government-major-e.aspx"

MD_EO_CUTOFF_PDF_URL = "https://governor.maryland.gov/Lists/ExecutiveOrders/Attachments/28/EO%2001.0.1.2024.01%20The%20Longevity%20Ready%20Maryland%20Initiative%20A%20Multisector%20Plan%20for%20Longevity_Accessible.pdf"

MD_PROC_CUTOFF_PDF_URL = "https://governor.maryland.gov/Lists/Proclamations/Attachments/29/Purple%20Friday%20V1.pdf"


_MD_PRESS_DETAIL_RE = re.compile(
    r"^https://governor\.maryland\.gov/news/press/pages/[^?#]+\.aspx$",
    re.I,
)

_MD_ANY_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)

# Press detail pages show: "Published: 1/3/2025"
_MD_PUBLISHED_MDY_RE = re.compile(r"\bPublished:\s*(\d{1,2}/\d{1,2}/\d{4})\b", re.I)

# EO list uses mm/dd/yyyy in first column
_MD_MDY_SLASH_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")

# Proc list often uses "June 06, 2025"
_MD_MONTH_DAY_YEAR_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.I,
)

# Match a whole table row, then capture (date, pdf href) from the same row
_MD_EO_ROW_DATE_PDF_RE = re.compile(
    r"<tr[^>]*>.*?"
    r"<td[^>]*>\s*(\d{1,2}/\d{1,2}/\d{4})\s*</td>.*?"
    r'href=["\']([^"\']+\.pdf)["\']'
    r".*?</tr>",
    re.I | re.S,
)

_MD_PROC_ROW_DATE_PDF_RE = re.compile(
    r"<tr[^>]*>.*?"
    r"<td[^>]*>\s*("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+(\d{1,2}),\s*(\d{4})\s*</td>.*?"
    r'href=["\']([^"\']+\.pdf)["\']'
    r".*?</tr>",
    re.I | re.S,
)


def _md_abs(u: str) -> str:
    if not u:
        return ""
    u = u.split("#")[0].strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("http"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return "https://governor.maryland.gov" + u


def _md_parse_mdy_slash(s: str) -> datetime | None:
    m = _MD_MDY_SLASH_RE.search(s or "")
    if not m:
        return None
    mm = int(m.group(1))
    dd = int(m.group(2))
    yy = int(m.group(3))
    try:
        return datetime(yy, mm, dd, tzinfo=timezone.utc)
    except Exception:
        return None


def _md_parse_month_day_year(s: str) -> datetime | None:
    m = _MD_MONTH_DAY_YEAR_RE.search(s or "")
    if not m:
        return None
    month_name = m.group(1).title()
    day = int(m.group(2))
    year = int(m.group(3))

    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
    try:
        return datetime(year, month_map[month_name], day, tzinfo=timezone.utc)
    except Exception:
        return None


def _date_from_md_published_line(html: str) -> datetime | None:
    if not html:
        return None
    text = _strip_html_to_text(html)
    m = _MD_PUBLISHED_MDY_RE.search(text)
    if not m:
        return None
    dt = _md_parse_mdy_slash(m.group(1))
    return _date_guard_not_future(dt) if dt else None


def _md_page(base: str, page: int) -> str:
    if page <= 1:
        return base
    return f"{base}?page={page}"


async def _collect_md_press_release_urls(
    cx: httpx.AsyncClient,
    *,
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_url: str | None = None,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    stop_norm = stop_at_url.rstrip("/").lower() if stop_at_url else None

    base = MD_PUBLIC_PAGES["press_releases"]

    for p in range(1, max_pages + 1):
        page_url = _md_page(base, p)
        r = await _get(cx, page_url, headers={"Referer": base})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")
        hrefs = [m.group(1) for m in _MD_ANY_HREF_RE.finditer(html)]

        page_found = 0

        for href in hrefs:
            u = _md_abs(href)

            # only press release detail pages
            if not _MD_PRESS_DETAIL_RE.match(u):
                continue

            u_norm = u.rstrip("/").lower()

            if stop_norm and u_norm == stop_norm:
                if u not in seen:
                    seen.add(u)
                    out.append(u)
                return out

            if u in seen:
                continue

            seen.add(u)
            out.append(u)
            page_found += 1

            if len(out) >= limit:
                return out

        if page_found == 0:
            break

        await asyncio.sleep(0.12)

    return out


async def _collect_md_pdf_links_with_dates(
    cx: httpx.AsyncClient,
    *,
    base_url: str,
    max_pages: int = 200,
    limit: int = 5000,
    stop_at_pdf_url: str | None = None,
    mode: str,
) -> list[tuple[str, datetime | None]]:
    """
    mode:
      - "eo": date in mm/dd/yyyy, pdf links under /Lists/ExecutiveOrders/Attachments/
      - "proc": date like 'June 06, 2025', pdf links under /Lists/Proclamations/Attachments/
    """
    out: list[tuple[str, datetime | None]] = []
    seen: set[str] = set()
    stop_norm = stop_at_pdf_url.rstrip("/").lower() if stop_at_pdf_url else None

    row_re = _MD_EO_ROW_DATE_PDF_RE if mode == "eo" else _MD_PROC_ROW_DATE_PDF_RE

    for p in range(1, max_pages + 1):
        page_url = _md_page(base_url, p)
        r = await _get(cx, page_url, headers={"Referer": base_url})
        if r.status_code >= 400 or not r.text:
            break

        html = r.text.replace("\\/", "/")

        page_found = 0
        for m in row_re.finditer(html):
            if mode == "eo":
                date_str = m.group(1)              # mm/dd/yyyy
                href = m.group(2)                  # pdf href
                dt = _md_parse_mdy_slash(date_str)
            else:
                # month name, day, year, then pdf href
                month_name = (m.group(1) or "").title()
                day = m.group(2)
                year = m.group(3)
                href = m.group(4)
                dt = _md_parse_month_day_year(f"{month_name} {day}, {year}")

            pdf_url = _md_abs(href)

            # extra safety: ensure we stay in the right list
            low = pdf_url.lower()
            if mode == "eo" and "/lists/executiveorders/attachments/" not in low:
                continue
            if mode == "proc" and "/lists/proclamations/attachments/" not in low:
                continue

            if pdf_url in seen:
                continue

            dt = _date_guard_not_future(dt)

            pdf_norm = pdf_url.rstrip("/").lower()
            if stop_norm and pdf_norm == stop_norm:
                seen.add(pdf_url)
                out.append((pdf_url, dt))
                return out

            seen.add(pdf_url)
            out.append((pdf_url, dt))
            page_found += 1

            if len(out) >= limit:
                return out

        if page_found == 0:
            break

        await asyncio.sleep(0.12)

    return out

async def ingest_maryland(limit_each: int = 5000, max_pages_each: int = 250) -> Dict[str, object]:
    out: Dict[str, object] = {}

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn,
                "Maryland — Press Releases",
                "state_newsroom",
                MD_PUBLIC_PAGES["press_releases"],
            )
            src_eo = await get_or_create_source(
                conn, "Maryland — Executive Orders", "state_executive_orders",
                "https://governor.maryland.gov/news/Pages/executive-orders.aspx"
            )
            src_proc = await get_or_create_source(
                conn, "Maryland — Proclamations", "state_proclamations",
                "https://governor.maryland.gov/news/Pages/Proclamations.aspx"
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            proc_backfill = (proc_existing == 0)

            def _effective_params(is_backfill: bool) -> tuple[int, int]:
                if is_backfill:
                    # backfill = honor user-provided payload
                    return int(max_pages_each or 0), int(limit_each or 0)

                # cron-safe buffers (ignore huge payloads)
                mp = max(int(max_pages_each or 0), 1)
                lim = max(int(limit_each or 0), 2000)
                return mp, lim

            mp_pr, lim_pr = _effective_params(pr_backfill)
            mp_eo, lim_eo = _effective_params(eo_backfill)
            mp_proc, lim_proc = _effective_params(proc_backfill)


            # ---- Collect URLs ----
            pr_urls = await _collect_md_press_release_urls(
                cx,
                max_pages=mp_pr,
                limit=lim_pr,
                stop_at_url=MD_PRESS_CUTOFF_URL,
            )

            eo_pdfs = await _collect_md_pdf_links_with_dates(
                cx,
                base_url=MD_PUBLIC_PAGES["executive_orders"],
                max_pages=mp_eo,
                limit=lim_eo,
                stop_at_pdf_url=MD_EO_CUTOFF_PDF_URL,
                mode="eo",
            )

            proc_pdfs = await _collect_md_pdf_links_with_dates(
                cx,
                base_url=MD_PUBLIC_PAGES["proclamations"],
                max_pages=mp_proc,
                limit=lim_proc,
                stop_at_pdf_url=MD_PROC_CUTOFF_PDF_URL,
                mode="proc",
            )

            out["press_releases_seen_urls"] = len(pr_urls)
            out["executive_orders_seen_pdfs"] = len(eo_pdfs)
            out["proclamations_seen_pdfs"] = len(proc_pdfs)

            # ✅ Cron-safe filtering (only new external_ids unless backfill)
            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)

            eo_pdf_urls = [u for (u, _) in eo_pdfs]
            proc_pdf_urls = [u for (u, _) in proc_pdfs]

            eo_new_urls = eo_pdf_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_pdf_urls)
            proc_new_urls = proc_pdf_urls if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_pdf_urls)

            # rebuild pairs preserving original order + dates
            eo_dt_map = {u: dt for (u, dt) in eo_pdfs}
            proc_dt_map = {u: dt for (u, dt) in proc_pdfs}

            eo_new_pdfs = [(u, eo_dt_map.get(u)) for u in eo_new_urls]
            proc_new_pdfs = [(u, proc_dt_map.get(u)) for u in proc_new_urls]

            out["press_releases_new_urls"] = len(pr_new_urls)
            out["executive_orders_new_pdfs"] = len(eo_new_pdfs)
            out["proclamations_new_pdfs"] = len(proc_new_pdfs)

            print(f"MD PR mode={'backfill' if pr_backfill else 'cron_safe'} new={len(pr_new_urls)} seen={len(pr_urls)}")
            print(f"MD EO mode={'backfill' if eo_backfill else 'cron_safe'} new={len(eo_new_pdfs)} seen={len(eo_pdfs)}")
            print(f"MD PROC mode={'backfill' if proc_backfill else 'cron_safe'} new={len(proc_new_pdfs)} seen={len(proc_pdfs)}")

            if not pr_new_urls and not eo_new_pdfs and not proc_new_pdfs:
                out["upserted"] = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}
                return out

            def _md_title_from_pdf_url(pdf_url: str) -> str:
                try:
                    from urllib.parse import unquote, urlsplit
                    name = unquote(urlsplit(pdf_url).path.split("/")[-1])
                    name = re.sub(r"\.pdf$", "", name, flags=re.I)
                    name = name.replace("_", " ").replace("-", " ").strip()
                    return name or pdf_url
                except Exception:
                    return pdf_url

            async def upsert_md_pdf(
                *,
                source_id: int,
                status: str,
                pdf_url: str,
                published_at: datetime | None,
                referer: str,
            ) -> bool:
                r = await _get(cx, pdf_url, headers={"Referer": referer})
                if r.status_code >= 400:
                    return False

                ct = (r.headers.get("Content-Type") or "").lower()
                if ("pdf" not in ct) and (not pdf_url.lower().endswith(".pdf")):
                    return False

                title = _md_title_from_pdf_url(pdf_url)
                published_at = _date_guard_not_future(published_at)

                summary = ""
                try:
                    pdf_bytes = r.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            # optional polish (safe + cheap-ish)
                            summary = await _safe_ai_polish(summary, title, pdf_url)
                except Exception:
                    summary = ""

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
                    MD_JURISDICTION,
                    MD_AGENCY,
                    status,
                    published_at,
                )
                return True

            # ---- Upserts ----
            async def upsert_md_press(url: str) -> bool:
                r = await _get(cx, url)
                if r.status_code >= 400 or not r.text:
                    return False

                html = _nz(r.text)
                title = _extract_h1(html) or url

                # Maryland press release pages reliably show "Published: M/D/YYYY"
                pub_dt = (
                    _date_from_meta(html)
                    or _date_from_json_ld(html)
                    or _date_from_md_published_line(html)
                    or _date_from_us_month_text(html)  # fallback if any
                )
                pub_dt = _date_guard_not_future(pub_dt)

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
                    src_pr,
                    _nz(title),
                    _nz(summary),
                    url,
                    MD_JURISDICTION,
                    MD_AGENCY,
                    MD_STATUS_MAP["press_releases"],
                    pub_dt,
                )
                return True

            upserted = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}

            for u in pr_new_urls:
                if await upsert_md_press(u):
                    upserted["press_releases"] += 1

            for pdf_url, dt in eo_new_pdfs:
                ok = await upsert_md_pdf(
                    source_id=src_eo,
                    status=MD_STATUS_MAP["executive_orders"],
                    pdf_url=pdf_url,
                    published_at=dt,
                    referer=MD_PUBLIC_PAGES["executive_orders"],
                )
                if ok:
                    upserted["executive_orders"] += 1

            for pdf_url, dt in proc_new_pdfs:
                ok = await upsert_md_pdf(
                    source_id=src_proc,
                    status=MD_STATUS_MAP["proclamations"],
                    pdf_url=pdf_url,
                    published_at=dt,
                    referer=MD_PUBLIC_PAGES["proclamations"],
                )
                if ok:
                    upserted["proclamations"] += 1

            out["upserted"] = upserted
            return out

# ----------------------------
# Minnesota helpers + ingester
# ----------------------------

MN_PUBLIC_PAGES = {
    "press_releases": "https://mn.gov/governor/newsroom/press-releases/",
    "executive_orders": "https://mn.gov/governor/newsroom/executive-orders/",
    "proclamations": "https://mn.gov/governor/newsroom/proclamations/",
}

MN_STATUS_MAP = {
    "press_releases": "press_release",
    "executive_orders": "executive_order",
    "proclamations": "proclamation",
}

MN_JURISDICTION = "minnesota"
MN_AGENCY = "Minnesota Governor"

# --- Cutoffs (inclusive) ---
MN_PRESS_CUTOFF_DETAIL = "https://mn.gov/governor/newsroom/press-releases/#/detail/appId/1/id/663457"
MN_PRESS_CUTOFF_ID = "663457"  # first item of 2025

MN_EO_CUTOFF_PDF_URL = "https://mn.gov/governor/assets/EO%2024-%2001%20Continuity%20of%20Operations_tcm1055-607112.pdf"
MN_PROC_CUTOFF_PDF_URL = "https://mn.gov/governor/assets/01.01.24%20Cervical%20Cancer%20Prevention%20and%20Awareness%20Month_Signed_tcm1055-608511.pdf"

MN_LIST = {
    "press_releases": "https://mn.gov/governor/rest/list/Newsroom?id=1055&nav=Date,Category,Tag&sort=Date,descending",
    "executive_orders": "https://mn.gov/governor/rest/list/Executive%20Orders?id=1055&nav=Date,Category,Tag&sort=Date,descending",
    "proclamations": "https://mn.gov/governor/rest/list/Proclamations?id=1055&nav=Date,Category,Tag&sort=Date,descending",
}

MN_WARMED_REFERERS: set[str] = set()


def _mn_norm(u: str) -> str:
    return (u or "").strip().replace("\\/", "/").rstrip("/").lower()

def _mn_extract_id_from_url(u: str) -> str | None:
    """
    Handles:
      - ...#/detail/appId/1/id/663457
      - ...index.jsp?id=1055-663457
      - ...?id=1055-663457
    """
    u = u or ""
    m = re.search(r"/id/(\d+)\b", u)
    if m:
        return m.group(1)

    m = re.search(r"[?&]id=(?:1055-)?(\d+)\b", u)
    if m:
        return m.group(1)

    m = re.search(r"tcm1055-(\d+)\.pdf\b", u, re.I)
    if m:
        return m.group(1)

    return None

    
def _mn_hash_to_detail_url(u: str) -> str:
    u = u or ""
    id_ = _mn_extract_id_from_url(u)
    if not id_:
        return u
    # this is the real server-rendered detail endpoint used by MN
    return f"https://mn.gov/governor/newsroom/press-releases/index.jsp?id=1055-{id_}"

def _mn_parse_any_date(v) -> datetime | None:
    """
    MN list JSON often includes a date-ish field. We try:
      - RFC-ish strings
      - ISO strings
      - plain 'January 2, 2025' style
    """
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None

    # Try email/RFC style
    try:
        dt = parsedate_to_datetime(s)
        if dt:
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return _date_guard_not_future(dt.astimezone(timezone.utc))
    except Exception:
        pass

    # Try ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return _date_guard_not_future(dt.astimezone(timezone.utc))
    except Exception:
        pass

    # Try "January 2, 2025"
    try:
        dt = _date_from_us_month_text(s)
        return _date_guard_not_future(dt)
    except Exception:
        return None


def _mn_find_first_url(obj, *, prefer_pdf: bool = False) -> str | None:
    """
    Walks dict/list and returns first URL-like string.
    If prefer_pdf=True, returns first .pdf URL if found, else any URL.
    """
    if obj is None:
        return None

    def normalize_url(s: str) -> str | None:
        s = (s or "").strip()
        if not s:
            return None
        if s.startswith("http://") or s.startswith("https://"):
            return s
        if s.startswith("/"):
            return "https://mn.gov" + s
        return None


    best_any = None

    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if isinstance(v, str):
                    sv = normalize_url(v)
                    if sv:
                        if prefer_pdf and sv.lower().endswith(".pdf"):
                            return sv
                        if best_any is None:
                            best_any = sv
                elif isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)
                elif isinstance(v, str):
                    sv = normalize_url(v)
                    if sv:
                        if prefer_pdf and sv.lower().endswith(".pdf"):
                            return sv
                        if best_any is None:
                            best_any = sv

    return best_any


def _mn_pick_title(obj) -> str:
    """
    Try common keys used by Tridion-ish APIs.
    """
    if not isinstance(obj, dict):
        return ""
    for k in ("title", "Title", "headline", "Headline", "name", "Name", "label", "Label"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _strip_html(s: str) -> str:
    if not s:
        return ""
    # Remove script/style blocks first
    s = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", s)
    # Remove all tags
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    # Decode a few common entities (minimal)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&quot;", '"')
    # Collapse whitespace
    return re.sub(r"\s+", " ", s).strip()


def _mn_pick_date(obj) -> datetime | None:
    if not isinstance(obj, dict):
        return None
    for k in (
        "date", "Date", "publishDate", "PublishDate", "publishedDate", "PublishedDate",
        "effectiveDate", "EffectiveDate", "displayDate", "DisplayDate",
        "pubDate", "PubDate", "created", "Created", "lastModified", "LastModified",
    ):
        v = obj.get(k)
        dt = _mn_parse_any_date(v)
        if dt:
            return dt
    return None

# ✅ ADD THESE TWO HELPERS HERE (right before _mn_fetch_list_page)
def _mn_is_radware_html(text: str) -> bool:
    t = (text or "").lower()
    return ("radware bot manager captcha" in t) or ("__uzdbm" in t)

def _mn_salvage_looks_valid(base_url: str, salvaged: list[dict]) -> bool:
    """
    Guardrail: only accept salvaged records if they resemble MN's real list records.
    Prevents garbage 'list' extracted from Radware HTML from being treated as success.
    """
    if not salvaged:
        return False

    # Press releases (Newsroom): should have numeric-ish "id"
    if "Newsroom" in base_url:
        ok = 0
        for x in salvaged[:20]:
            rid = str((x or {}).get("id") or "").strip()
            if rid.isdigit():
                ok += 1
        return ok >= 5  # at least 5 real-looking items

    # Proclamations / Executive Orders: should contain some URL that looks like a PDF
    if ("Proclamations" in base_url) or ("Executive%20Orders" in base_url):
        ok = 0
        for x in salvaged[:30]:
            u = _mn_find_first_url(x, prefer_pdf=True) or _mn_find_first_url(x, prefer_pdf=False) or ""
            u2 = (u or "").lower()
            if u2.endswith(".pdf") or ("tcm1055-" in u2 and ".pdf" in u2):
                ok += 1
        return ok >= 5

    # default conservative
    return False


def _mn_jina(url: str) -> str:
    # jina expects https://r.jina.ai/http(s)://...
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://"):]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://"):]
    return "https://r.jina.ai/https://" + url

def _mn_extract_json_from_jina(text: str) -> str | None:
    """
    Extract the FIRST complete JSON object/array from jina output using bracket matching.
    This is robust against wrappers + extra braces + truncation.
    """
    if not text:
        return None

    marker = "Markdown Content:"
    idx = text.find(marker)
    candidate = text[idx + len(marker):].strip() if idx != -1 else text.strip()

    # find first '{' or '['
    start_obj = candidate.find("{")
    start_arr = candidate.find("[")
    if start_obj == -1 and start_arr == -1:
        return None

    start = start_obj if (start_arr == -1 or (start_obj != -1 and start_obj < start_arr)) else start_arr
    opening = candidate[start]
    closing = "}" if opening == "{" else "]"

    in_str = False
    esc = False
    depth = 0

    for i in range(start, len(candidate)):
        ch = candidate[i]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == opening:
            depth += 1
        elif ch == closing:
            depth -= 1
            if depth == 0:
                return candidate[start:i + 1]

    # truncated JSON (no full closing bracket found)
    return candidate[start:]

def _mn_salvage_list_items(extracted: str) -> list[dict]:
    """
    If extracted JSON is truncated/invalid, try to salvage individual item objects
    inside the top-level "list":[ ... ] array by brace-matching.
    Returns list[dict] (possibly partial).
    """
    if not extracted:
        return []

    # Find the start of the list array
    m = re.search(r'"list"\s*:\s*\[', extracted)
    if not m:
        return []

    i = m.end()  # position right after '['
    n = len(extracted)

    items: list[dict] = []

    in_str = False
    esc = False
    depth = 0
    obj_start = -1

    while i < n:
        ch = extracted[i]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            i += 1
            continue

        # not in string
        if ch == '"':
            in_str = True
            i += 1
            continue

        if ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
            i += 1
            continue

        if ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and obj_start != -1:
                    obj_txt = extracted[obj_start : i + 1]
                    try:
                        items.append(json.loads(obj_txt, strict=False))
                    except Exception:
                        pass
                    obj_start = -1
            i += 1
            continue

        # End of list
        if ch == "]" and depth == 0:
            break

        i += 1

    return items

async def _mn_fetch_list_page(
    cx: httpx.AsyncClient,
    base_url: str,
    *,
    page_num: int,
    page_size: int = 10,
    referer: str = "https://mn.gov/governor/newsroom/",
) -> dict | None:
    """
    Calls: base_url + &page={page_num},{page_size}
    If Radware CAPTCHA HTML is returned, retry via jina proxy.
    """
    url = f"{base_url}&page={page_num},{page_size}"

    # --- Warm Radware cookies once per referer (critical for press + proclamations) ---
    if referer and referer not in MN_WARMED_REFERERS:
        try:
            await _get(
                cx,
                referer,
                headers={**BROWSER_UA_HEADERS, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                read_timeout=45.0,
            )
        except Exception:
            pass
        MN_WARMED_REFERERS.add(referer)

    headers = {
        **BROWSER_UA_HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Referer": referer,
        "Origin": "https://mn.gov",
        "X-Requested-With": "XMLHttpRequest",
    }

    r = await _get(cx, url, headers=headers, read_timeout=60.0)

    ct = (r.headers.get("content-type") or "")
    body = r.text or ""

    # If we got blocked (HTML captcha), retry via jina
    blocked = (
        (r.status_code in (403, 429))
        or ("radware bot manager captcha" in body.lower())
        or ("__uzdbm" in body.lower())
        or ("text/html" in ct.lower())
    )

    if blocked:
        jr = await _get(
            cx,
            _mn_jina(url),
            headers={**BROWSER_UA_HEADERS, "Accept": "application/json, text/plain, */*"},
            read_timeout=60.0,
        )
        jtext = jr.text or ""
        if _mn_is_radware_html(jtext):
            print("MN jina still captcha. Preview:", jtext[:200])

            # ✅ LAST-RESORT: real browser fetch (works for EO + PR when Radware blocks)
            pw_payload = await _mn_fetch_json_via_playwright(url, referer)
            if pw_payload:
                return pw_payload
            print("MN Playwright fallback failed:", url)
            return None
        extracted = _mn_extract_json_from_jina(jtext)
        if not extracted:
            print("MN jina extract failed. Preview:", jtext[:200])

            pw_payload = await _mn_fetch_json_via_playwright(url, referer)
            if pw_payload:
                return pw_payload
            print("MN Playwright fallback failed:", url)
            return None

        # ✅ First try normal JSON parse (tolerant)
        try:
            return json.loads(extracted, strict=False)
        except Exception:
            salvaged = _mn_salvage_list_items(extracted)
            if salvaged:
                print("MN jina JSON parse failed; salvaged items:", len(salvaged))

                # If salvage doesn't look like MN records, treat as blocked and DO NOT return it.
                if not _mn_salvage_looks_valid(base_url, salvaged):
                    pw_payload = await _mn_fetch_json_via_playwright(url, referer)
                    if pw_payload:
                        return pw_payload
                    print("MN Playwright fallback failed:", url)
                    return None

                # If salvage is suspiciously small for a page, prefer Playwright.
                if len(salvaged) < max(10, int(page_size * 0.85)):
                    pw_payload = await _mn_fetch_json_via_playwright(url, referer)
                    if pw_payload:
                        return pw_payload
                    print("MN Playwright fallback failed:", url)

                return {"list": salvaged}

            print("MN jina JSON parse failed. Extract preview:", extracted[:200])

            # ✅ LAST-RESORT: real browser fetch
            pw_payload = await _mn_fetch_json_via_playwright(url, referer)
            if pw_payload:
                return pw_payload
            print("MN Playwright fallback failed:", url)
            return None

    # Normal path
    if r.status_code >= 400:
        print("MN LIST HTTP", r.status_code, "Preview:", body[:200])
        return None

    try:
        return r.json()
    except Exception:
        print("MN LIST JSON parse failed. CT:", ct, "Preview:", body[:200])
        return None

def _mn_extract_records(payload) -> list[dict]:
    """
    Prefer payload["list"] when it looks like real items.
    Fallback to recursive search, but only accept lists that look like records.
    """
    if payload is None:
        return []

    # 1) Prefer top-level "list" (MN uses this)
    if isinstance(payload, dict):
        lst = payload.get("list")
        if isinstance(lst, list) and lst:
            if any(isinstance(x, dict) and ("id" in x or "Title" in x or "Link" in x) for x in lst):
                return [x for x in lst if isinstance(x, dict)]

    # 2) Fallback: find the first list-of-dicts that looks like records
    found: list[dict] = []

    def looks_like_records(li: list) -> bool:
        if not li or not all(isinstance(i, dict) for i in li):
            return False
        # reject nav facets like [{"label":..., "count":...}]
        if all(set(i.keys()).issubset({"label", "count"}) for i in li):
            return False
        return any(("id" in i) or ("Title" in i) or ("Link" in i) for i in li)

    def walk(x):
        nonlocal found
        if found:
            return
        if isinstance(x, list):
            if looks_like_records(x):
                found = x
                return
            for i in x:
                walk(i)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)

    walk(payload)
    return found



def _mn_map_record(kind: str, rec: dict) -> dict:
    title = _mn_pick_title(rec) or ""
    dt = _mn_pick_date(rec)

    if kind == "press_releases":
        rid = str(rec.get("id") or "").strip()
        # Stable canonical detail URL (don’t fetch it)
        link = f"https://mn.gov/governor/newsroom/press-releases/index.jsp?id=1055-{rid}" if rid.isdigit() else (_mn_find_first_url(rec) or "")

        short = (rec.get("ShortDescription") or rec.get("Subtitle") or rec.get("shortDescription") or "").strip()
        body  = (rec.get("BodyText") or rec.get("bodyText") or "").strip()

        return {
            "id": rid,
            "title": title,
            "link": link,
            "published_at": dt,
            "short": short,
            "body": body,
        }

    # eo/proc: must grab the PDF (prefer_pdf=True)
    pdf = _mn_find_first_url(rec, prefer_pdf=True) or _mn_find_first_url(rec, prefer_pdf=False) or ""
    return {"title": title, "pdf_url": pdf, "published_at": dt}

def _mn_abs(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("/"):
        return "https://mn.gov" + u
    return u

def _mn_strip_tags(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = _html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()

def _mn_scrape_pdf_links_from_public_html(html: str) -> list[dict]:
    """
    Scrape PDF links + anchor text from MN public EO/Proclamation pages.
    Returns list of dicts shaped like _mn_map_record output for pdf kinds:
      {"title": "...", "pdf_url": "...", "published_at": None}
    """
    if not html:
        return []

    out: list[dict] = []
    seen: set[str] = set()

    # Capture <a href="...pdf">Title</a>
    for m in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+\.pdf[^"\']*)["\'][^>]*>(.*?)</a>', html):
        href = _mn_abs(m.group(1))
        title = _mn_strip_tags(m.group(2))

        href_norm = _mn_norm(href)
        if not href or href_norm in seen:
            continue
        seen.add(href_norm)

        out.append({"title": title or href, "pdf_url": href, "published_at": None})

    return out

async def _mn_try_public_html_fallback(
    cx: httpx.AsyncClient,
    *,
    kind: str,
    cutoff_pdf_url: str,
) -> list[dict]:
    """
    If JSON is blocked on newest pages, scrape newest PDFs from the public page.
    """
    page_url = MN_PUBLIC_PAGES.get(kind) or ""
    if not page_url:
        return []

    r = await _get(
        cx,
        page_url,
        headers={**BROWSER_UA_HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"},
        read_timeout=60.0,
    )
    if r.status_code >= 400:
        return []

    items = _mn_scrape_pdf_links_from_public_html(r.text or "")

    # Apply the same cutoff semantics (inclusive) by stopping once we hit cutoff in this list
    trimmed: list[dict] = []
    for it in items:
        trimmed.append(it)
        if _mn_norm(it.get("pdf_url", "")) == _mn_norm(cutoff_pdf_url):
            break

    return trimmed

async def _mn_fetch_json_via_playwright(url: str, referer: str) -> dict | None:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(
                extra_http_headers=clean_headers(BROWSER_UA_HEADERS),
                java_script_enabled=True,
            )
            page = await ctx.new_page()

            # Optional: block heavy resources (faster + less likely to hang)
            await page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "font", "media")
                else route.continue_(),
            )

            # Warm & allow Radware JS/cookies to settle
            await page.goto(referer, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(1500)

            # Fetch from within the page so cookies/credentials apply like a real browser
            result = await page.evaluate(
                """async (u) => {
                    const r = await fetch(u, { credentials: 'include' });
                    const ct = r.headers.get('content-type') || '';
                    const text = await r.text();
                    return { status: r.status, ct, text };
                }""",
                url,
            )

            await browser.close()

            if not result:
                return None

            status = result.get("status")
            ct = (result.get("ct") or "").lower()
            text = result.get("text") or ""

            if status != 200:
                return None

            # If still HTML/captcha, bail
            if _mn_is_radware_html(text) or "<html" in (text[:200].lower()):
                return None

            # Sometimes CT is weird; still attempt JSON
            return json.loads(text, strict=False)

    except Exception as e:
        print("MN Playwright exception:", type(e).__name__, str(e)[:200])
        return None
    
async def _mn_count_until_cutoff(
    cx: httpx.AsyncClient,
    *,
    kind: str,
    page_size: int,
    max_pages: int = 9999,
) -> int:
    """
    Count how many items exist from newest down to cutoff (inclusive).
    This ignores cron_safe caps and ignores DB.
    """
    base = MN_LIST[kind]
    referer = MN_PUBLIC_PAGES[kind]

    total = 0
    fail_streak = 0
    max_fail = 10

    for page_num in range(1, max_pages + 1):
        payload = await _mn_fetch_list_page(
            cx,
            base,
            page_num=page_num,
            page_size=page_size,
            referer=referer,
        )

        if not payload:
            fail_streak += 1
            if fail_streak >= max_fail:
                break
            continue

        fail_streak = 0
        recs = _mn_extract_records(payload)
        if not recs:
            fail_streak += 1
            if fail_streak >= max_fail:
                break
            continue

        for rec in recs:
            total += 1

            if kind == "press_releases":
                rid = str((rec or {}).get("id") or "").strip()
                if rid == MN_PRESS_CUTOFF_ID:
                    return total

            # for pdf kinds we still need mapping to find pdf_url
            item = _mn_map_record(kind, rec)

            if kind == "proclamations":
                if _mn_norm(item.get("pdf_url", "")) == _mn_norm(MN_PROC_CUTOFF_PDF_URL):
                    return total
            elif kind == "executive_orders":
                if _mn_norm(item.get("pdf_url", "")) == _mn_norm(MN_EO_CUTOFF_PDF_URL):
                    return total

    return total


async def ingest_minnesota(limit_each: int = 5000, max_pages_each: int = 9999) -> Dict[str, object]:
    """
    Minnesota uses a JSON backend for press releases / executive orders / proclamations.
    Use the /governor/rest/list/* endpoints (page=N,10) and apply cutoffs.
    """
    out: Dict[str, object] = {}
    MN_WARMED_REFERERS.clear()

    async with connection() as conn:
        async with httpx.AsyncClient(
            headers={**BROWSER_UA_HEADERS},
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15.0, read=75.0, write=15.0, pool=None),
        ) as cx:

            src_pr = await get_or_create_source(
                conn,
                "Minnesota — Press Releases",
                "state_newsroom",
                MN_PUBLIC_PAGES["press_releases"],
            )
            src_eo = await get_or_create_source(
                conn,
                "Minnesota — Executive Orders",
                "state_executive_orders",
                MN_PUBLIC_PAGES["executive_orders"],
            )
            src_proc = await get_or_create_source(
                conn,
                "Minnesota — Proclamations",
                "state_proclamations",
                MN_PUBLIC_PAGES["proclamations"],
            )

            # --- per-source backfill detection ---
            pr_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_pr) or 0
            eo_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_eo) or 0
            proc_existing = await conn.fetchval("select count(*) from items where source_id = $1", src_proc) or 0

            pr_backfill = (pr_existing == 0)
            eo_backfill = (eo_existing == 0)
            proc_backfill = (proc_existing == 0)

            # --- cron-safe param caps (ignore huge payloads unless backfill) ---
            def _effective_params(is_backfill: bool, mp: int, lim: int) -> tuple[int, int]:
                if is_backfill:
                    return int(mp or 0) or 1, int(lim or 0) or 5000

                # cron-safe caps: keep these conservative (MN endpoints can be heavy)
                mp2 = max(1, min(int(mp or 0) or 1, 3))          # <= 3 pages
                lim2 = max(50, min(int(lim or 0) or 200, 500))   # <= 500 items
                return mp2, lim2

            mp_pr, lim_pr = _effective_params(pr_backfill, max_pages_each, limit_each)
            mp_eo, lim_eo = _effective_params(eo_backfill, max_pages_each, limit_each)
            mp_proc, lim_proc = _effective_params(proc_backfill, max_pages_each, limit_each)

            # For "seen_total": DO NOT crawl MN in normal runs (Radware trigger).
            # Only allow deep counting when you explicitly force it AND you're in backfill mode.
            do_count = (os.getenv("MN_COUNT_TOTALS", "0") == "1") and (pr_backfill or eo_backfill or proc_backfill)

            pr_total_to_cutoff = None
            proc_total_to_cutoff = None
            eo_total_to_cutoff = None

            if do_count:
                pr_total_to_cutoff = await _mn_count_until_cutoff(cx, kind="press_releases", page_size=50, max_pages=200)
                proc_total_to_cutoff = await _mn_count_until_cutoff(cx, kind="proclamations", page_size=50, max_pages=200)
                eo_total_to_cutoff = await _mn_count_until_cutoff(cx, kind="executive_orders", page_size=10, max_pages=200)

            # ---- Collect via JSON list endpoints ----
            async def collect_kind(kind: str, *, max_pages: int, limit: int) -> list[dict]:
                max_fail = 6 if kind in ("press_releases", "proclamations") else 8
                base = MN_LIST[kind]
                all_items: list[dict] = []
                fail_streak = 0

                # ✅ NEW: if newest JSON pages are blocked, scrape the public page for newest PDFs (EO/PROC only)
                did_public_fallback = False

                for page_num in range(1, max_pages + 1):
                    page_size = 50 if kind in ("press_releases", "proclamations") else 10
                    payload = await _mn_fetch_list_page(
                        cx,
                        base,
                        page_num=page_num,
                        page_size=page_size,
                        referer=MN_PUBLIC_PAGES[kind],
                    )

                    if not payload:
                        # ✅ if page 1 is blocked and this is EO/PROC, pull newest from public HTML once
                        if (
                            page_num == 1
                            and (kind in ("executive_orders", "proclamations"))
                            and (not did_public_fallback)
                        ):
                            did_public_fallback = True
                            cutoff = MN_EO_CUTOFF_PDF_URL if kind == "executive_orders" else MN_PROC_CUTOFF_PDF_URL
                            fallback_items = await _mn_try_public_html_fallback(cx, kind=kind, cutoff_pdf_url=cutoff)

                            # prepend newest fallback items (dedupe by pdf_url)
                            fb_seen = {_mn_norm(x.get("pdf_url", "")) for x in all_items}
                            for it in fallback_items:
                                nu = _mn_norm(it.get("pdf_url", ""))
                                if nu and nu not in fb_seen:
                                    all_items.append(it)
                                    fb_seen.add(nu)

                                    # honor cutoffs (inclusive)
                                    if kind == "executive_orders" and _mn_norm(it.get("pdf_url", "")) == _mn_norm(MN_EO_CUTOFF_PDF_URL):
                                        return all_items
                                    if kind == "proclamations" and _mn_norm(it.get("pdf_url", "")) == _mn_norm(MN_PROC_CUTOFF_PDF_URL):
                                        return all_items

                                if len(all_items) >= limit:
                                    return all_items

                        fail_streak += 1
                        if fail_streak >= max_fail:
                            break
                        continue
                    else:
                        fail_streak = 0

                    recs = _mn_extract_records(payload)
                    if not recs:
                        fail_streak += 1
                        if fail_streak >= max_fail:
                            break
                        continue
                    else:
                        fail_streak = 0

                    for rec in recs:
                        # raw id is the most reliable cutoff check (survives partial payloads/salvage)
                        rid = None
                        if kind == "press_releases":
                            rid = str((rec or {}).get("id") or "").strip()

                        item = _mn_map_record(kind, rec)
                        all_items.append(item)

                        # stop conditions (inclusive)
                        if kind == "press_releases":
                            if rid == MN_PRESS_CUTOFF_ID:
                                return all_items
                        elif kind == "executive_orders":
                            if _mn_norm(item.get("pdf_url", "")) == _mn_norm(MN_EO_CUTOFF_PDF_URL):
                                return all_items
                        elif kind == "proclamations":
                            if _mn_norm(item.get("pdf_url", "")) == _mn_norm(MN_PROC_CUTOFF_PDF_URL):
                                return all_items

                    if len(all_items) >= limit:
                        return all_items

                return all_items

            pr_items = await collect_kind("press_releases", max_pages=mp_pr, limit=lim_pr)
            eo_items = await collect_kind("executive_orders", max_pages=mp_eo, limit=lim_eo)
            proc_items = await collect_kind("proclamations", max_pages=mp_proc, limit=lim_proc)

            out["press_releases_seen"] = len(pr_items)
            out["executive_orders_seen"] = len(eo_items)
            out["proclamations_seen"] = len(proc_items)

            # --- Cron-safe filtering: only process NEW items unless backfill ---
            pr_urls = [clean_url(it.get("link") or "") for it in pr_items if it.get("link")]
            eo_urls = [clean_url(it.get("pdf_url") or "") for it in eo_items if it.get("pdf_url")]
            proc_urls = [clean_url(it.get("pdf_url") or "") for it in proc_items if it.get("pdf_url")]

            pr_new_urls = pr_urls if pr_backfill else await _filter_new_external_ids(conn, src_pr, pr_urls)
            eo_new_urls = eo_urls if eo_backfill else await _filter_new_external_ids(conn, src_eo, eo_urls)
            proc_new_urls = proc_urls if proc_backfill else await _filter_new_external_ids(conn, src_proc, proc_urls)

            pr_map = {clean_url(it.get("link") or ""): it for it in pr_items if it.get("link")}
            eo_map = {clean_url(it.get("pdf_url") or ""): it for it in eo_items if it.get("pdf_url")}
            proc_map = {clean_url(it.get("pdf_url") or ""): it for it in proc_items if it.get("pdf_url")}

            pr_new_items = [pr_map[u] for u in pr_new_urls if u in pr_map]
            eo_new_items = [eo_map[u] for u in eo_new_urls if u in eo_map]
            proc_new_items = [proc_map[u] for u in proc_new_urls if u in proc_map]

            out["press_releases_new"] = len(pr_new_items)
            out["executive_orders_new"] = len(eo_new_items)
            out["proclamations_new"] = len(proc_new_items)

            # seen_total definition:
            # - in cron_safe mode: DB count is the reliable "total we have"
            # - in backfill mode: prefer computed "to cutoff" (if you enabled MN_COUNT_TOTALS), else fall back to DB count
            if pr_backfill:
                seen_label_pr = pr_total_to_cutoff if pr_total_to_cutoff is not None else pr_existing
            else:
                seen_label_pr = pr_existing

            if eo_backfill:
                seen_label_eo = eo_total_to_cutoff if eo_total_to_cutoff is not None else eo_existing
            else:
                seen_label_eo = eo_existing

            if proc_backfill:
                seen_label_proc = proc_total_to_cutoff if proc_total_to_cutoff is not None else proc_existing
            else:
                seen_label_proc = proc_existing

            print(
                f"MN PR mode={'backfill' if pr_backfill else 'cron_safe'} "
                f"new={len(pr_new_items)} fetched={len(pr_items)} seen_total={seen_label_pr}"
            )
            print(
                f"MN EO mode={'backfill' if eo_backfill else 'cron_safe'} "
                f"new={len(eo_new_items)} fetched={len(eo_items)} seen_total={seen_label_eo}"
            )
            print(
                f"MN PROC mode={'backfill' if proc_backfill else 'cron_safe'} "
                f"new={len(proc_new_items)} fetched={len(proc_items)} seen_total={seen_label_proc}"
            )

            # Fast exit: prevents repolish when nothing new
            if (not pr_new_items) and (not eo_new_items) and (not proc_new_items):
                out["upserted"] = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}
                return out

            # ---- Upserts ----
            def _mn_text_from_body(s: str) -> str:
                # BodyText may contain HTML-ish markup
                return _nz(_strip_html(s)) if s else ""

            async def upsert_mn_press_item(it: dict) -> bool:
                detail_url = _nz(it.get("link"))
                if not detail_url:
                    return False

                # Guard: skip Radware poison values if they ever sneak in
                bad = "we apologize for the inconvenience" in (it.get("title","").lower())
                if bad:
                    return False

                title = _nz(it.get("title")) or detail_url
                pub_dt = _date_guard_not_future(it.get("published_at"))

                short = _nz(it.get("short"))
                body_txt = _mn_text_from_body(it.get("body"))
                raw_for_summary = short or body_txt

                if not raw_for_summary:
                    summary = ""
                else:
                    summary = summarize_text(raw_for_summary, max_sentences=2, max_chars=700)
                    if summary:
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
                    detail_url,              # keep SAME external_id so it overwrites old bad rows
                    src_pr,
                    title,
                    _nz(summary),
                    detail_url,
                    MN_JURISDICTION,
                    MN_AGENCY,
                    MN_STATUS_MAP["press_releases"],
                    pub_dt,
                )
                return True

            
            async def upsert_mn_pdf(
                *,
                source_id: int,
                status: str,
                pdf_url: str,
                published_at: datetime | None,
                referer: str,
                title: str | None = None,
            ) -> bool:
                
                if not pdf_url:
                    return False

                title = _nz(title) or pdf_url

                # Pull text from PDF for summary
                summary = ""
                try:
                    pr = await _get(cx, pdf_url, headers={"Referer": referer}, read_timeout=90.0)
                    if pr.status_code >= 400:
                        return False

                    pdf_bytes = pr.content or b""
                    pdf_text = _nz(_extract_pdf_text_from_bytes(pdf_bytes))
                    if pdf_text:
                        summary = summarize_text(pdf_text, max_sentences=3, max_chars=700)
                        if summary:
                            summary = _soft_normalize_caps(summary)
                            summary = await _safe_ai_polish(summary, title, pdf_url)
                except Exception:
                    summary = ""

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
                    pdf_url,  # external_id
                    source_id,
                    _nz(title),
                    _nz(summary),
                    pdf_url,
                    MN_JURISDICTION,
                    MN_AGENCY,
                    status,
                    _date_guard_not_future(published_at),
                )
                return True


            upserted = {"press_releases": 0, "executive_orders": 0, "proclamations": 0}

            for it in pr_new_items:
                if await upsert_mn_press_item(it):
                    upserted["press_releases"] += 1

            for it in eo_new_items:
                ok = await upsert_mn_pdf(
                    source_id=src_eo,
                    status=MN_STATUS_MAP["executive_orders"],
                    pdf_url=it.get("pdf_url") or "",
                    published_at=it.get("published_at"),
                    referer=MN_PUBLIC_PAGES["executive_orders"],
                    title=it.get("title"),
                )
                if ok:
                    upserted["executive_orders"] += 1

            for it in proc_new_items:
                ok = await upsert_mn_pdf(
                    source_id=src_proc,
                    status=MN_STATUS_MAP["proclamations"],
                    pdf_url=it.get("pdf_url") or "",
                    published_at=it.get("published_at"),
                    referer=MN_PUBLIC_PAGES["proclamations"],
                    title=it.get("title"),
                )
                if ok:
                    upserted["proclamations"] += 1

            out["upserted"] = upserted
            return out


# --- ADD THIS LINE INSIDE INGESTERS_V2 AT THE BOTTOM ---
# "utah": ingest_utah,


# ----------------------------
# Registry of v2 ingesters
# ----------------------------

INGESTERS_V2 = {
    "ohio": ingest_ohio,
    "arizona": ingest_arizona,
    "virginia": ingest_virginia,
    "georgia": ingest_georgia,
    "hawaii": ingest_hawaii,
    "vermont": ingest_vermont,
    "utah": ingest_utah,
    "new_jersey": ingest_new_jersey,
    "new-jersey": ingest_new_jersey,  # alias
    "colorado": ingest_colorado,
    "alaska": ingest_alaska,
    "ak": ingest_alaska,  # optional alias
    "maryland": ingest_maryland,
    "md": ingest_maryland,  # optional alias
    "minnesota": ingest_minnesota,
    "mn": ingest_minnesota,  # optional alias
}