import hashlib
from typing import Dict, Any, List, Optional
from .utils import parse_rss_date

UA = {"User-Agent": "policywatch/0.1 (portfolio project)"}

def _hash_external_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

async def fetch_rss(url: str) -> Dict[str, Any]:
    import httpx, feedparser
    async with httpx.AsyncClient(timeout=20.0, headers=UA) as cx:
        r = await cx.get(url)
        r.raise_for_status()
        return feedparser.parse(r.content)   # ← use .content (bytes) to be safe

def map_rss_to_rows(
    parsed_feed: Dict[str, Any],
    *,
    source_id: str,
    jurisdiction: str,
    agency: str,
    default_status: str = "notice",
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    entries = parsed_feed.get("entries") or []
    for e in entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not title or not link:
            continue

        # dates in feeds can be `published`, `updated`, or absent
        pub = parse_rss_date(
            e.get("published")
            or e.get("updated")
            or e.get("published_parsed")  # feedparser may give struct time; parse_rss_date copes
        )

        # some feeds put body in summary/detail/content
        summary = None
        if e.get("summary"):
            summary = e["summary"]
        elif e.get("description"):
            summary = e["description"]
        elif e.get("content") and isinstance(e["content"], list) and e["content"]:
            summary = e["content"][0].get("value")

        # stable external_id: hash(link + title)
        ext = _hash_external_id(f"{link}::{title}")

        rows.append({
            "external_id": ext,
            "source_id": source_id,
            "title": title,
            "summary": summary or "",
            "url": link,
            "jurisdiction": jurisdiction,
            "agency": agency,
            "topic": [],
            "status": default_status,
            "published_at": pub,   # may be None; schema allows null
            "raw": e,
        })
    return rows

async def upsert_items_from_rows(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    import json
    sql = """
        insert into items (
            external_id, source_id, title, summary, url,
            jurisdiction, agency, topic, status, published_at, raw
        ) values (
            $1,$2,$3,$4,$5,
            $6,$7,$8::text[],$9,$10::timestamptz,$11::jsonb
        )
        on conflict (external_id) do update set
            source_id=excluded.source_id,
            title=excluded.title,
            summary=excluded.summary,
            url=excluded.url,
            jurisdiction=excluded.jurisdiction,
            agency=excluded.agency,
            topic=excluded.topic,
            status=excluded.status,
            published_at=excluded.published_at,
            raw=excluded.raw
    """
    values = [(
        r["external_id"], r["source_id"], r["title"], r.get("summary") or "", r["url"],
        r["jurisdiction"], r.get("agency"),
        r.get("topic", []),
        r.get("status"),
        r.get("published_at"),
        json.dumps(r["raw"]),
    ) for r in rows]
    await conn.executemany(sql, values)
    return len(rows)
