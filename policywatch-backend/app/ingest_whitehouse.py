from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import re
import html as ihtml
from hashlib import sha1
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit
from .summarize import summarize_extractive, summarize_text, _strip_html_to_text
from .ai_summarizer import ai_polish_summary

import httpx

from .db import connection
from .ingest_rss import upsert_items_from_rows  # keep
from .summarize import summarize_items_needing_help, force_repolish_white_house_batch


# =========================
# Configuration
# =========================

BROWSER_UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://www.whitehouse.gov/",
    # IMPORTANT: many WH pages vary by Accept; prefer HTML here
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}


# Only the 7 categories you requested (and EXACT listing roots)
SECTION_SPECS = [
    # listing label, listing base, status, source name, source kind, base_url
    ("articles", "https://www.whitehouse.gov/articles/", "press_release",
     "White House — Articles", "wh_articles", "https://www.whitehouse.gov/articles/"),

    ("executive_orders", "https://www.whitehouse.gov/presidential-actions/executive-orders/", "executive_order",
     "White House — Executive Orders", "wh_executive_orders", "https://www.whitehouse.gov/presidential-actions/executive-orders/"),

    ("proclamations", "https://www.whitehouse.gov/presidential-actions/proclamations/", "proclamation",
     "White House — Proclamations", "wh_proclamations", "https://www.whitehouse.gov/presidential-actions/proclamations/"),

    ("memoranda", "https://www.whitehouse.gov/presidential-actions/presidential-memoranda/", "memorandum",
     "White House — Presidential Memoranda", "wh_memoranda", "https://www.whitehouse.gov/presidential-actions/presidential-memoranda/"),

    ("fact_sheets", "https://www.whitehouse.gov/fact-sheets/", "fact_sheet",
     "White House — Fact Sheets", "wh_fact_sheets", "https://www.whitehouse.gov/fact-sheets/"),

    ("research", "https://www.whitehouse.gov/research/", "research",
     "White House — Research", "wh_research", "https://www.whitehouse.gov/research/"),

    ("briefings_statements", "https://www.whitehouse.gov/briefings-statements/", "briefings_statements",
     "White House — Briefings & Statements", "wh_briefings_statements", "https://www.whitehouse.gov/briefings-statements/"),
]

# We only accept *post* URLs that include /YYYY/MM/slug/ (this drops garbage listing pages)
POST_URL_DATE_RE = re.compile(
    r"/(19|20)\d{2}/(0[1-9]|1[0-2])(?:/(0[1-9]|[12]\d|3[01]))?/[a-z0-9-]+/?$",
    re.IGNORECASE
)

# Hard safety so you never accidentally crawl forever if WH changes behavior
MAX_PAGES_SAFETY = 500

# =========================
# Helpers
# =========================

def _norm_abs(u: str) -> str:
    """Normalize URL: drop query/fragment; keep path and scheme/host."""
    parts = urlsplit(u)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _dedupe_keep_order_pairs(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    seen = set()
    out: List[Tuple[str, str]] = []
    for url, status in items:
        if url in seen:
            continue
        seen.add(url)
        out.append((url, status))
    return out

def _dedupe_keep_order_triples(items: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    seen = set()
    out: List[Tuple[str, str, str]] = []
    for url, status, src_name in items:
        if url in seen:
            continue
        seen.add(url)
        out.append((url, status, src_name))
    return out


def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = ihtml.unescape(s)
    s = s.replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _is_post_url(u: str) -> bool:
    try:
        path = urlsplit(u).path or ""
        # reject /page/N/ listings and section roots and other garbage
        if re.search(r"/page/\d+/?$", path):
            return False
        return bool(POST_URL_DATE_RE.search(path))
    except Exception:
        return False

def _page_url(base: str, page: int) -> str:
    if page == 1:
        return base
    return f"{base.rstrip('/')}/page/{page}/"

def _infer_status_from_url(u: str) -> str:
    path = (urlsplit(u).path or "").lower()
    # More specific first:
    if "/presidential-actions/executive-orders/" in path:
        return "executive_order"
    if "/presidential-actions/proclamations/" in path:
        return "proclamation"
    if "/presidential-actions/presidential-memoranda/" in path:
        return "memorandum"
    if path.startswith("/fact-sheets/"):
        return "fact_sheet"
    if path.startswith("/research/"):
        return "research"
    if path.startswith("/briefings-statements/"):
        return "briefings_statements"
    if path.startswith("/articles/"):
        return "press_release"
    return "notice"

# =========================
# Robust HTML extraction
# =========================

def _extract_from_html(html_text: str) -> Tuple[str, str]:
    """
    Returns (title, main_text).
    Uses BeautifulSoup if available; falls back to regex.
    """
    title = ""
    main = ""

    # --- Try BeautifulSoup (preferred) ---
    try:
        from bs4 import BeautifulSoup  # type: ignore

        soup = BeautifulSoup(html_text, "html.parser")

        # Remove obvious non-content
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        # Title: try og:title then h1 then <title>
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = og["content"].strip()

        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(" ", strip=True)

        if not title and soup.title and soup.title.string:
            title = soup.title.string.strip()

        # Main content container: try common WP structures
        selectors = [
            "article",
            "main",
            "div.entry-content",
            "div.wp-block-post-content",
            "div.page-content",
            "div.content",
        ]

        container = None
        for sel in selectors:
            container = soup.select_one(sel)
            if container:
                break
        if not container:
            container = soup.body or soup

        # Extract paragraphs/lists/headings text in order
        chunks: List[str] = []
        for el in container.find_all(["p", "li", "h2", "h3", "blockquote"], recursive=True):
            txt = el.get_text(" ", strip=True)
            txt = _clean_text(txt)
            if not txt:
                continue
            # Avoid nav/footer boilerplate
            if len(txt) < 20 and txt.lower() in {"skip to content", "menu"}:
                continue
            chunks.append(txt)

        main = _clean_text(" ".join(chunks))

        # If we got almost nothing, fallback to meta description
        if len(main) < 200:
            md = soup.find("meta", attrs={"name": "description"})
            if md and md.get("content"):
                main = _clean_text(md["content"])

        return _clean_text(title), _clean_text(main)

    except Exception:
        pass

    # --- Regex fallback ---
    m = re.search(r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']', html_text, re.I)
    if m:
        title = _clean_text(m.group(1))

    if not title:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, re.I | re.S)
        if m:
            title = _clean_text(re.sub(r"<.*?>", " ", m.group(1)))

    if not title:
        m = re.search(r"<title>(.*?)</title>", html_text, re.I | re.S)
        if m:
            title = _clean_text(m.group(1))

    # Grab a bunch of <p> text (not only first!)
    ps = re.findall(r"<p[^>]*>(.*?)</p>", html_text, re.I | re.S)
    main = _clean_text(" ".join(_clean_text(re.sub(r"<.*?>", " ", p)) for p in ps[:40]))

    return _clean_text(title), _clean_text(main)


def _extract_published_dt(url: str, html_text: str) -> Optional[datetime]:
    """
    Best-effort published date:
    - article:published_time meta
    - <time datetime=...>
    - JSON-LD datePublished
    - URL /YYYY/MM/
    """
    # meta article:published_time
    m = re.search(
        r'<meta\s+property=["\']article:published_time["\']\s+content=["\']([^"\']+)["\']',
        html_text, re.I
    )
    if m:
        try:
            return datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

    # time datetime
    m = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

    # JSON-LD datePublished
    m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html_text, re.I)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

    # URL /YYYY/MM/
    m = re.search(r"/((19|20)\d{2})/(0[1-9]|1[0-2])/", url)
    if m:
        try:
            y = int(m.group(1))
            mo = int(m.group(3))
            return datetime(y, mo, 1, tzinfo=timezone.utc)
        except Exception:
            pass

    return None


# =========================
# Research PDF extraction (optional)
# =========================

def _find_pdf_url(html_text: str) -> Optional[str]:
    # common patterns: iframe src=..., a href=..., embedded pdf viewer
    m = re.search(r'(https://www\.whitehouse\.gov[^"\']+\.pdf)', html_text, re.I)
    if m:
        return _norm_abs(m.group(1))
    return None

async def _extract_pdf_text(cx: httpx.AsyncClient, pdf_url: str, max_chars: int = 20000) -> str:
    """
    Best-effort PDF text extraction.
    Uses pypdf if installed. If not available, returns "".
    """
    try:
        r = await cx.get(pdf_url)
        if r.status_code >= 400 or not r.content:
            return ""
        data = r.content
    except Exception:
        return ""

    # Try pypdf first
    try:
        from pypdf import PdfReader  # type: ignore
        import io
        reader = PdfReader(io.BytesIO(data))
        chunks: List[str] = []
        for page in reader.pages[:20]:
            t = page.extract_text() or ""
            t = _clean_text(t)
            if t:
                chunks.append(t)
        text = _clean_text(" ".join(chunks))
        return text[:max_chars]
    except Exception:
        return ""


# =========================
# Listing crawler
# =========================

LISTING_HREF_PAT = re.compile(
    r'href=["\'](?P<u>(?:https://www\.whitehouse\.gov)?/[^"\']+)["\']',
    re.I
)

async def _crawl_listing_newest_to_oldest(
    cx: httpx.AsyncClient,
    listing_base: str,
    existing_urls: set[str],
    forced_status: str,
    forced_source_name: str,
) -> List[Tuple[str, str, str]]:
    collected: List[Tuple[str, str, str]] = []
    seen_posts: set[str] = set()

    for page in range(1, MAX_PAGES_SAFETY + 1):
        url = _page_url(listing_base, page)
        r = await cx.get(url)
        print("[WH][list]", listing_base, "GET", url, "->", r.status_code)

        if page > 1 and r.status_code >= 400:
            break
        if r.status_code >= 400 or not r.text:
            break

        text = r.text
        page_posts: List[str] = []

        for m in LISTING_HREF_PAT.finditer(text):
            u = m.group("u")
            if u.startswith("/"):
                u = "https://www.whitehouse.gov" + u
            u = _norm_abs(u)

            if not _is_post_url(u):
                continue

            if u not in seen_posts:
                seen_posts.add(u)
                page_posts.append(u)

        if not page_posts:
            break

        for u in page_posts:
            if u in existing_urls:
                if collected:
                    return collected
                continue
            collected.append((u, forced_status, forced_source_name))

        if collected and all(u in existing_urls for u in page_posts):
            break

    return collected


# =========================
# Ingest
# =========================

async def ingest_white_house() -> dict:
    inserted = 0

    async with connection() as conn:
        # Source row
        # Create/get 7 sources (one per section)
        source_ids_by_name: dict[str, int] = {}
        for _, _, _, src_name, src_kind, base_url in SECTION_SPECS:
            row = await conn.fetchrow(
                """
                insert into sources(name, kind, base_url)
                values($1,$2,$3)
                on conflict (name) do update
                set kind=excluded.kind, base_url=excluded.base_url
                returning id
                """,
                src_name, src_kind, base_url
            )
            source_ids_by_name[src_name] = row["id"]

        # Existing URLs across ALL 7 sources (prevents reprocessing)
        source_ids = list(source_ids_by_name.values())
        existing = await conn.fetch(
            """
            select url from items
            where source_id = any($1::uuid[])
            """,
            source_ids
        )
        existing_urls = {(_norm_abs(r["url"]) if r["url"] else "") for r in existing if r["url"]}

        async with httpx.AsyncClient(timeout=30.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
            all_new_links: List[Tuple[str, str, str]] = []

            for _, listing_base, forced_status, forced_source_name, _, _ in SECTION_SPECS:
                new_links = await _crawl_listing_newest_to_oldest(
                    cx, listing_base, existing_urls, forced_status, forced_source_name
                )
                all_new_links.extend(new_links)

            all_new_links = _dedupe_keep_order_triples(all_new_links)
            print(f"[WH] new links discovered: {len(all_new_links)}")

            if not all_new_links:
                return {
                    "upserted": 0,
                    "feeds": 1,
                    "enriched": 0,
                    "summarized": 0,
                    "repolished": 0,
                }

            rows: List[Dict[str, Any]] = []
            new_external_ids: list[str] = []

            for idx, (link, status, src_name) in enumerate(all_new_links, start=1):
                source_id = source_ids_by_name.get(src_name)
                if not source_id:
                    continue
                if idx == 1 or idx % 25 == 0:
                    print(f"[WH] fetching {idx}/{len(all_new_links)}: {link}")

                try:
                    r = await cx.get(link)
                    if r.status_code >= 400 or not r.text:
                        continue
                    html_text = r.text

                    title, main_text = _extract_from_html(html_text)
                    pub_dt = _extract_published_dt(link, html_text)

                    # Research: try PDF text extraction and append to main_text if helpful
                    if status == "research":
                        pdf_url = _find_pdf_url(html_text)
                        if pdf_url:
                            pdf_text = await _extract_pdf_text(cx, pdf_url)
                            # If PDF extraction succeeds, prefer it (but keep some HTML too)
                            if len(pdf_text) > 800:
                                main_text = pdf_text

                    # ✅ Make an actual short summary (EO-aware)
                    if status == "executive_order":
                        # EO-aware extractive summary from HTML (removes “By the authority vested…” etc.)
                        summary = summarize_extractive(title or "", link, html_text, max_sentences=2, max_chars=650)
                    else:
                        # regular: TextRank on cleaned text
                        plain = _strip_html_to_text(html_text)
                        summary = summarize_text(plain, max_sentences=3, max_chars=650)

                    # fallback if summarize gives nothing
                    if not summary:
                        summary = (main_text[:650] if main_text else "")

                    if not title:
                        # fallback from slug
                        slug = link.rstrip("/").rsplit("/", 1)[-1]
                        title = slug.replace("-", " ").title()

                    title = _clean_text(title)
                    summary = _clean_text(summary)

                    ext = sha1(f"{link}::{title}".encode("utf-8")).hexdigest()
                    new_external_ids.append(ext)

                    rows.append({
                        "external_id": ext,
                        "source_id": source_id,
                        "title": title,
                        "summary": summary,
                        "url": link,
                        "jurisdiction": "federal",
                        "agency": "White House",
                        "topic": [],
                        "status": status,
                        "published_at": pub_dt,
                        "raw": {"link": link, "src": "listing_crawl"},
                    })

                except Exception:
                    continue

            inserted += await upsert_items_from_rows(conn, rows)

            summed = await _polish_new_whitehouse_items(conn, new_external_ids, batch=50)
            forced = 0

    return {
        "upserted": inserted,
        "feeds": 1,
        "summarized": summed,
        "repolished": forced,
        "note": "listing-only ingest; AI-polish only new items into ai_summary; old items untouched",
    }

async def _polish_new_whitehouse_items(
    conn,
    new_external_ids: list[str],
    batch: int = 50,
) -> int:
    """
    AI-polish ONLY the newly discovered items (by external_id).
    Idempotent: if ai_summary already exists, it won't repolish.
    Stores polished result in ai_summary (keeps original summary intact).
    """
    if not new_external_ids:
        return 0

    total = 0

    # process in chunks so the query stays small
    for i in range(0, len(new_external_ids), batch):
        chunk = new_external_ids[i : i + batch]

        rows = await conn.fetch(
            """
            select id, external_id, url, title, summary
            from items
            where external_id = any($1::text[])
              and (ai_summary is null or length(trim(ai_summary)) = 0)
            """,
            chunk,
        )

        updates: list[tuple[str, str]] = []
        for r in rows:
            draft = (r["summary"] or "").strip()
            if not draft:
                continue

            polished = await ai_polish_summary(draft, r["title"] or "", r["url"] or "")
            if polished and polished.strip():
                updates.append((polished.strip(), r["id"]))  # update by uuid id

        if updates:
            await conn.executemany(
                """
                update items
                set ai_summary = $1,
                    ai_model = $2,
                    ai_status = $3,
                    ai_created_at = now()
                where id = $4
                """,
                [(p, "openai:gpt-4.1-mini", "ok", item_id) for (p, item_id) in updates],
            )
            total += len(updates)

    return total
