import httpx
from typing import List, Dict, Any
from .utils import map_fr_status, parse_pub_date
import json     

FR_BASE = "https://www.federalregister.gov/api/v1/documents.json"
UA = {"User-Agent": "policywatch/0.1 (portfolio project)"}


async def fetch_federal_register(since_iso: str) -> List[Dict[str, Any]]:
    page = 1
    out = []
    async with httpx.AsyncClient(timeout=20.0, headers=UA) as client:
        while True:
            r = await client.get(FR_BASE, params={
                "per_page": 100,
                "page": page,
                "order": "newest",
                "fields[]": [
                    "document_number","title","type","agency_names",
                    "html_url","publication_date","abstract"
                ],
                "conditions[publication_date][gte]": since_iso
            })
            r.raise_for_status()
            j = r.json()
            out.extend(j.get("results", []))
            total_pages = j.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1
    return out


async def map_fr_to_rows(raw_docs: List[Dict[str, Any]], source_id) -> List[Dict[str, Any]]:
    rows = []
    for d in raw_docs:
        rows.append({
            "external_id": d.get("document_number"),
            "source_id": source_id,
            "title": d.get("title"),
            "summary": d.get("abstract"),
            "url": d.get("html_url"),
            "jurisdiction": "federal",
            "agency": ", ".join(d.get("agency_names") or []),
            "topic": [],
            "status": map_fr_status(d.get("type")),
            "published_at": parse_pub_date(d.get("publication_date")),
            "raw": d,
        })
    return rows

async def upsert_items(conn, rows):
    if not rows:
        return 0
    sql = """
        insert into items (
            external_id, source_id, title, summary, url,
            jurisdiction, agency, topic, status, published_at, raw
        ) values (
            $1,$2,$3,$4,$5,
            $6,$7,$8::text[],$9,$10::timestamptz,$11::jsonb
        ) on conflict (external_id) do update set
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
        r["external_id"], r["source_id"], r["title"], r.get("summary"), r["url"],
        r["jurisdiction"], r.get("agency"),
        r.get("topic", []),           # list[str] -> text[]
        r.get("status"),
        r.get("published_at"),        # tz-aware datetime -> timestamptz
        json.dumps(r["raw"]),         # ← dict -> JSON string for ::jsonb
    ) for r in rows]
    await conn.executemany(sql, values)
    return len(rows)

async def get_or_create_source(conn, name: str, kind: str, base_url: str):
    existing = await conn.fetchrow("select id from sources where name=$1", name)
    if existing:
        return existing[0]
    row = await conn.fetchrow(
        "insert into sources(name,kind,base_url) values($1,$2,$3) returning id",
        name, kind, base_url
    )
    return row[0]