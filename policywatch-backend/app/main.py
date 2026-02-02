import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from pathlib import Path
from .ingest_states3 import INGESTERS_V3
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)
from typing import Dict, Any
from fastapi import FastAPI, Depends, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Query
from pydantic import BaseModel
import json
from .ingest_whitehouse import ingest_white_house
from .ingest_ofac import ingest_ofac_recent_actions
from .ingest_states import ingest_state_executive_orders, ingest_state_newsroom
from .summarize import summarize_items_needing_help  # add near the top with other imports
from .ingest_states2 import INGESTERS_V2
from .ai_impact import score_item_impact
from .auth import get_user_id_from_auth
from .supabase_admin import admin_delete_user
from pydantic import BaseModel, Field

# helper for raw → abstract
def _normalize_ai_impact(v):
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None

def _extract_abstract(row) -> str:
    raw = row["raw"]
    raw_json = None
    if isinstance(raw, dict):
        raw_json = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            raw_json = json.loads(raw)
        except Exception:
            raw_json = None

    abstract = (raw_json or {}).get("abstract") if raw_json else None
    if abstract and isinstance(abstract, str):
        return abstract
    # fallback to summary
    return (row["summary"] or "")

def _normalize_jurisdiction(s: Optional[str]) -> Optional[str]:
    """
    Normalizes user-provided jurisdiction filters so things like:
      - "nm", "new_mexico", "new-mexico" -> "new mexico"
      - "nc", "north_carolina", "north-carolina" -> "north carolina"
    match your DB values after your SQL-side normalization.

    NOTE: This returns lowercase full names (with spaces).
    Your SQL already lower()'s and strips spaces/-/_ on both sides, so this is safe.
    """
    if not s:
        return None

    t = s.strip().lower()
    t = t.replace("\u00a0", " ")  # NBSP -> space

    alias = {
        # Federal bucket
        "federal": "federal",
        "fed": "federal",

        # White House inputs should normalize to the jurisdiction value used in DB
        "white-house": "federal",
        "white_house": "federal",
        "white house": "federal",
        "wh": "federal",

        # States in your SOURCE_MAP / SOURCE_KEY_TO_NAMES
        "fl": "florida",
        "florida": "florida",

        "tx": "texas",
        "texas": "texas",

        "ny": "new york",
        "new york": "new york",
        "new-york": "new york",
        "new_york": "new york",

        "pa": "pennsylvania",
        "pennsylvania": "pennsylvania",

        "il": "illinois",
        "illinois": "illinois",

        "ma": "massachusetts",
        "massachusetts": "massachusetts",

        "wa": "washington",
        "washington": "washington",

        "ca": "california",
        "california": "california",

        "oh": "ohio",
        "ohio": "ohio",

        "az": "arizona",
        "arizona": "arizona",

        "va": "virginia",
        "virginia": "virginia",

        "ga": "georgia",
        "georgia": "georgia",

        "hi": "hawaii",
        "hawaii": "hawaii",

        "vt": "vermont",
        "vermont": "vermont",

        "ut": "utah",
        "utah": "utah",

        "nj": "new jersey",
        "new jersey": "new jersey",
        "new-jersey": "new jersey",
        "new_jersey": "new jersey",

        "co": "colorado",
        "colorado": "colorado",

        "ak": "alaska",
        "alaska": "alaska",

        "md": "maryland",
        "maryland": "maryland",

        "mn": "minnesota",
        "minnesota": "minnesota",

        "mi": "michigan",
        "michigan": "michigan",

        "tn": "tennessee",
        "tennessee": "tennessee",

        "nc": "north carolina",
        "north carolina": "north carolina",
        "north-carolina": "north carolina",
        "north_carolina": "north carolina",

        "sc": "south carolina",
        "south carolina": "south carolina",
        "south-carolina": "south carolina",
        "south_carolina": "south carolina",

        "or": "oregon",
        "oregon": "oregon",

        "nv": "nevada",
        "nevada": "nevada",

        "wi": "wisconsin",
        "wisconsin": "wisconsin",

        "ia": "iowa",
        "iowa": "iowa",

        "mo": "missouri",
        "missouri": "missouri",

        "ks": "kansas",
        "kansas": "kansas",

        "nm": "new mexico",
        "new mexico": "new mexico",
        "new-mexico": "new mexico",
        "new_mexico": "new mexico",
    }

    return alias.get(t, t)



DATABASE_URL = os.getenv("DATABASE_URL")

from .ai_cloudflare import cf_summarize

from .db import init_pool, connection
from .ingest_federal_register import (
    fetch_federal_register, map_fr_to_rows, upsert_items, get_or_create_source
)

CRON_KEY = os.getenv("CRON_KEY")

app = FastAPI(title="PolicyWatch API", version="0.1.0")

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def _startup():
    await init_pool()

@app.get("/health")
async def health():
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}

@app.get("/")
async def root():
    return {"service": "PolicyWatch API", "status": "ok"}

@app.head("/")
async def root_head():
    return Response(status_code=200)


# -----------------------
# Ingest: Federal Register
# -----------------------
class IngestPayload(BaseModel):
    since_hours: Optional[int] = 24

async def _require_cron(request: Request):
    if not CRON_KEY:
        raise HTTPException(status_code=500, detail="CRON_KEY not set")
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {CRON_KEY}":
        raise HTTPException(status_code=403, detail="Forbidden")

@app.post("/ingest/federal-register", dependencies=[Depends(_require_cron)])
async def ingest_fr(payload: IngestPayload):
    since_dt = datetime.now(timezone.utc) - timedelta(hours=payload.since_hours or 24)
    since_iso = since_dt.date().isoformat()  # FR filters by date
    raw_docs = await fetch_federal_register(since_iso)

    summarized = 0
    async with connection() as conn:
        source_id = await get_or_create_source(
            conn, "Federal Register", "federal_reg", "https://www.federalregister.gov"
        )
        rows = await map_fr_to_rows(raw_docs, source_id)
        upserted = await upsert_items(conn, rows)

        # Summarize newest 20 items using the SAME conn
        try:
            rows2 = await conn.fetch(
                """
                select id, title, summary, raw
                from items
                where ai_summary is null
                order by fetched_at desc
                limit 20
                """
            )
            for r in rows2:
                abstract = _extract_abstract(r)
                if not abstract:
                    continue
                s = await cf_summarize(r["title"], abstract)
                await conn.execute(
                    "update items set ai_summary=$1, ai_model=$2, ai_created_at=now(), ai_status=$3 where id=$4",
                    s, "cf:llama-3.1-8b", "ok", r["id"]
                )
                summarized += 1
        except Exception:
            # keep ingest resilient; rely on batch endpoint/cron
            pass

    return {"ingested": len(raw_docs), "upserted": upserted, "summarized": summarized}

# --------------
# Query / Search
# --------------
@app.get("/items")
async def list_items(
    q: Optional[str] = None,
    topic: Optional[List[str]] = None,
    jurisdiction: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,  # YYYY-MM-DD
    date_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    page = max(1, page)
    page_size = max(1, min(page_size, 100))

    where = []
    params = []

    if q:
        where.append("(title ilike $%d or summary ilike $%d)" % (len(params)+1, len(params)+2))
        params.extend([f"%{q}%", f"%{q}%"])
    if topic:
        where.append("topic && $%d" % (len(params)+1))
        params.append(topic)
    if jurisdiction:
        where.append("jurisdiction = $%d" % (len(params)+1))
        params.append(jurisdiction)
    if status:
        where.append("status = $%d" % (len(params)+1))
        params.append(status)
    if date_from:
        where.append("published_at >= $%d" % (len(params)+1))
        params.append(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc))
    if date_to:
        where.append("published_at < $%d" % (len(params)+1))
        params.append(datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) + timedelta(days=1))

    where_sql = (" where " + " and ".join(where)) if where else ""
    offset = (page - 1) * page_size

    sql = f"""
        select id, external_id, title, summary, url, jurisdiction, agency, topic, status, published_at, ai_summary, ai_model, ai_status, ai_created_at
        from items
        {where_sql}
        order by published_at desc nulls last
        limit $%d offset $%d
    """ % (len(params)+1, len(params)+2)

    count_sql = f"select count(*) as c from items {where_sql}"

    async with connection() as conn:
        total = await conn.fetchval(count_sql, *params)
        rows = await conn.fetch(sql, *params, page_size, offset)

    return {
        "page": page,
        "page_size": page_size,
        "total": int(total or 0),
        "items": [dict(r) for r in rows],
    }

@app.post("/ai/summarize/{item_id}")
async def ai_summarize_item(item_id: str, force: bool = False):
    async with connection() as conn:
        row = await conn.fetchrow(
            "select id, title, summary, raw, ai_summary from items where id=$1", item_id
        )
        if not row:
            raise HTTPException(404, "Not found")

        if row["ai_summary"] and not force:
            return {"status": "cached", "ai_summary": row["ai_summary"]}

        title = row["title"]
        abstract = _extract_abstract(row)

        if not abstract:
            await conn.execute(
                "update items set ai_status=$1 where id=$2", "skipped", row["id"]
            )
            return {"status": "skipped", "reason": "no abstract/summary"}

        ai_sum = await cf_summarize(title, abstract)
        await conn.execute(
            "update items set ai_summary=$1, ai_model=$2, ai_created_at=now(), ai_status=$3 where id=$4",
            ai_sum, "cf:llama-3.1-8b", "ok", row["id"]
        )
        return {"status":"ok","ai_summary": ai_sum}


class BatchPayload(BaseModel):
    max_items: int = 50
    hours: Optional[int] = 24      # <= None or <=0 will disable the time filter
    force: bool = False
    source: Optional[str] = None   # e.g. "White House — News & Actions"

@app.post("/ai/enrich/batch", dependencies=[Depends(_require_cron)])
async def ai_enrich_batch(payload: BatchPayload):
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select i.id, i.title, i.summary, i.raw
            from items i
            join sources s on s.id = i.source_id
            where (i.ai_summary is null or $1::bool)
              and ( $2::int is null or $2::int <= 0
                    or coalesce(i.published_at, i.fetched_at) >= (now() - make_interval(hours => $2::int)) )
              AND (
                $3::text IS NULL OR
                replace(replace(replace(replace(lower(s.name), ' ', ''), chr(160), ''), '—', ''), '-', '') ILIKE
                ('%' || replace(replace(replace(replace(lower($3), ' ', ''), chr(160), ''), '—', ''), '-', '') || '%')
              )
            order by coalesce(i.published_at, i.fetched_at) desc nulls last
            limit $4::int
            """,
            payload.force, payload.hours, payload.source, payload.max_items
        )
        done = 0
        for r in rows:
            title = r["title"]
            abstract = _extract_abstract(r)
            if not abstract:
                await conn.execute(
                    "update items set ai_status=$1 where id=$2", "skipped", r["id"]
                )
                continue
            try:
                ai_sum = await cf_summarize(title, abstract)
                await conn.execute(
                    "update items set ai_summary=$1, ai_model=$2, ai_created_at=now(), ai_status=$3 where id=$4",
                    ai_sum, "cf:llama-3.1-8b", "ok", r["id"]
                )
                done += 1
            except Exception as e:
                await conn.execute(
                    "update items set ai_status=$1 where id=$2", f"error:{type(e).__name__}", r["id"]
                )
        return {"processed": len(rows), "summarized": done}
    
@app.post("/ingest/white-house", dependencies=[Depends(_require_cron)])
async def ingest_wh():
    return await ingest_white_house()

@app.post("/ingest/ofac", dependencies=[Depends(_require_cron)])
async def ingest_ofac():
    return await ingest_ofac_recent_actions()

class StatesPayload(BaseModel):
    states: Optional[List[str]] = None  # if omitted, ingest all configured
    max_pages: int | None = 1           # how many pages of each listing to crawl
    limit: int | None = 40              # max total URLs per state

@app.post("/ingest/states", dependencies=[Depends(_require_cron)])
async def ingest_states(payload: StatesPayload):
    return await ingest_state_executive_orders(payload.states)

@app.post("/ingest/states/newsroom", dependencies=[Depends(_require_cron)])
async def ingest_states_newsroom(payload: StatesPayload):
    return await ingest_state_newsroom(
        payload.states,
        max_pages=payload.max_pages or 1,
        limit=payload.limit or 40,
    )

@app.post("/summaries/backfill", dependencies=[Depends(_require_cron)])
async def backfill_summaries(source: str | None = None, limit: int = 100):
    n = await summarize_items_needing_help(source_name=source, limit=limit)
    return {"updated": n}

# -----------------------
# Frontend feed endpoint
# -----------------------

SOURCE_MAP = {
    "white-house": "White House — News & Actions",
    "florida": "Florida — Newsroom",
    "texas": "Texas — Newsroom",
    "new-york": "New York — Newsroom",
    "pennsylvania": "Pennsylvania — Newsroom",
    "illinois": "Illinois — Newsroom",
    "massachusetts": "Massachusetts — Newsroom",
    "washington": "Washington — Newsroom",
    "california": "California — Newsroom",
    "ohio": "Ohio — News",
    "arizona": "Arizona — Press Releases",
    "virginia": "Virginia — News Releases",

    # ✅ add this (string value doesn’t matter much if you don’t use SOURCE_MAP elsewhere)
    "georgia": "Georgia — Press Releases",
    "hawaii": "Hawaii — Press Releases",
    "vermont": "Vermont — Press Releases",
    "utah": "Utah — News", 
    "new-jersey": "New Jersey — Press Releases",
    "colorado": "Colorado — Press Releases",
    "alaska": "Alaska — Press Releases",
    "ak": "Alaska — Press Releases",  # optional alias
    "maryland": "Maryland — Press Releases",
    "md": "Maryland — Press Releases",  # optional alias
    "minnesota": "Minnesota — Press Releases",
    "mn": "Minnesota — Press Releases",  # optional alias
    "michigan": "Michigan — Press Releases",
    "mi": "Michigan — Press Releases",  # optional alias
    "tennessee": "Tennessee — Press Releases (F&A)",
    "tn": "Tennessee — Press Releases (F&A)",  # optional alias
    "north_carolina": "North Carolina — Press Releases (nc.gov)",
    "nc": "North Carolina — Press Releases (nc.gov)",  # optional alias
    "south_carolina": "South Carolina — News",
    "sc": "South Carolina — News",
    "oregon": "Oregon — Newsroom (Featured Feed)",
    "or": "Oregon — Newsroom (Featured Feed)",
    "nevada": "Nevada — Press Releases",
    "nv": "Nevada — Press Releases",  # optional alias
    "wisconsin": "Wisconsin — Press Releases",
    "wi": "Wisconsin — Press Releases",  # optional alias
    "iowa": "Iowa — Press Releases",
    "ia": "Iowa — Press Releases",
    "missouri": "Missouri — Press Releases",
    "mo": "Missouri — Press Releases",
    "kansas": "Kansas — Press Releases",
    "ks": "Kansas — Press Releases",
    "new_mexico": "New Mexico — Latest News (nm.gov)",
    "nm": "New Mexico — Latest News (nm.gov)",
}

# map frontend source keys → sources.name in DB
SOURCE_KEY_TO_NAMES = {
    "white-house": [
    "White House — Articles",
    "White House — Executive Orders",
    "White House — Proclamations",
    "White House — Presidential Memoranda",
    "White House — Fact Sheets",
    "White House — Research",
    "White House — Briefings & Statements",
    ],
    "florida":        ["Florida — Newsroom"],
    "texas":          ["Texas — Newsroom"],
    "new-york":       ["New York — Newsroom"],
    "pennsylvania":   ["Pennsylvania — Newsroom"],
    "illinois":       ["Illinois — Newsroom"],
    "massachusetts": [
        "Massachusetts — Newsroom",
        "Massachusetts — Executive Orders",
    ],
    "washington": [
        "Washington — Newsroom",
        "Washington — Executive Orders",  # ✅ add this
        "Washington — Proclamations",
    ],
    "california":     ["California — Newsroom"],

    "ohio": ["Ohio — News", "Ohio — Appointments", "Ohio — Executive Orders"],
    "arizona": ["Arizona — Press Releases", "Arizona — Executive Orders", "Arizona — Proclamations"],
    "virginia": ["Virginia — News Releases", "Virginia — Proclamations", "Virginia — Executive Orders"],
    "georgia": [
        "Georgia — Press Releases",
        "Georgia — Executive Orders",
    ],
    "hawaii": [
        "Hawaii — Press Releases",
        "Hawaii — Executive Orders",
        "Hawaii — Proclamations",
    ],
    "vermont": [
    "Vermont — Press Releases",
    "Vermont — Executive Orders",
    "Vermont — Proclamations",
    ],
    "utah": [
        "Utah — News",
        "Utah — Executive Orders",
        "Utah — Declarations",
    ],
    "new-jersey": [
        "New Jersey — Press Releases",
        "New Jersey — Executive Orders",
        "New Jersey — Administrative Orders",
    ],
    "new_jersey": [
        "New Jersey — Press Releases",
        "New Jersey — Executive Orders",
        "New Jersey — Administrative Orders",
    ],
    # ✅ Colorado
    "colorado": [
        "Colorado — Press Releases",
        "Colorado — Executive Orders",
    ],
    # ✅ Alaska
    "alaska": [
        "Alaska — Press Releases",
        "Alaska — Proclamations",
        "Alaska — Administrative Orders",
    ],
    "ak": [  # optional alias
        "Alaska — Press Releases",
        "Alaska — Proclamations",
        "Alaska — Administrative Orders",
    ],
    "maryland": [
        "Maryland — Press Releases",
        "Maryland — Executive Orders",
        "Maryland — Proclamations",
    ],
    "md": [
        "Maryland — Press Releases",
        "Maryland — Executive Orders",
        "Maryland — Proclamations",
    ],
    "minnesota": [
        "Minnesota — Press Releases",
        "Minnesota — Executive Orders",
        "Minnesota — Proclamations",
    ],
    "mn": [
        "Minnesota — Press Releases",
        "Minnesota — Executive Orders",
        "Minnesota — Proclamations",
    ],
    "michigan": [
        "Michigan — Press Releases",
        "Michigan — Proclamations",
        "Michigan — State Orders & Directives",
    ],
    "mi": [  # optional alias
        "Michigan — Press Releases",
        "Michigan — Proclamations",
        "Michigan — State Orders & Directives",
    ],
    "tennessee": [
        "Tennessee — Press Releases (F&A)",
        "Tennessee — Executive Orders (Governor Bill Lee)",
        "Tennessee — Proclamations",
    ],
    "tn": [
        "Tennessee — Press Releases (F&A)",
        "Tennessee — Executive Orders (Governor Bill Lee)",
        "Tennessee — Proclamations",
    ],
    "north_carolina": [
        "North Carolina — Press Releases (nc.gov)",
        "North Carolina — Executive Orders",
        "North Carolina — Proclamations",
    ],
    "nc": [
        "North Carolina — Press Releases (nc.gov)",
        "North Carolina — Executive Orders",
        "North Carolina — Proclamations",
    ],
    "south_carolina": [
        "South Carolina — News",
        "South Carolina — Executive Orders",
    ],
    "sc": [
        "South Carolina — News",
        "South Carolina — Executive Orders",
    ],
    "oregon": [
        "Oregon — Newsroom (Featured Feed)",
        "Oregon — Executive Orders",
    ],
    "or": [
        "Oregon — Newsroom (Featured Feed)",
        "Oregon — Executive Orders",
    ],
    "nevada": [
        "Nevada — Press Releases",
        "Nevada — Executive Orders",
        "Nevada — Proclamations",
    ],
    "nv": [
        "Nevada — Press Releases",
        "Nevada — Executive Orders",
        "Nevada — Proclamations",
    ],
    "wisconsin": [
        "Wisconsin — Press Releases",
        "Wisconsin — Executive Orders",
        "Wisconsin — Proclamations",
    ],
    "wi": [
        "Wisconsin — Press Releases",
        "Wisconsin — Executive Orders",
        "Wisconsin — Proclamations",
    ],
    "iowa": [
        "Iowa — Press Releases",
        "Iowa — Executive Orders",
        "Iowa — Disaster Proclamations",
    ],
    "ia": [
        "Iowa — Press Releases",
        "Iowa — Executive Orders",
        "Iowa — Disaster Proclamations",
    ],
    "missouri": [
        "Missouri — Press Releases",
        "Missouri — Executive Orders",
        "Missouri — Proclamations",
    ],
    "mo": [
        "Missouri — Press Releases",
        "Missouri — Executive Orders",
        "Missouri — Proclamations",
    ],
    "kansas": [
        "Kansas — Press Releases",
        "Kansas — Executive Orders",
    ],
    "ks": [
        "Kansas — Press Releases",
        "Kansas — Executive Orders",
    ],
    "new_mexico": [
        "New Mexico — Latest News (nm.gov)",
        "New Mexico — Executive Orders",
    ],
    "nm": [
        "New Mexico — Latest News (nm.gov)",
        "New Mexico — Executive Orders",
    ],
}

# --- Aliases (hyphenated keys used by frontend/whats-new ordered_keys) ---
if "north_carolina" in SOURCE_KEY_TO_NAMES and "north-carolina" not in SOURCE_KEY_TO_NAMES:
    SOURCE_KEY_TO_NAMES["north-carolina"] = SOURCE_KEY_TO_NAMES["north_carolina"]

if "south_carolina" in SOURCE_KEY_TO_NAMES and "south-carolina" not in SOURCE_KEY_TO_NAMES:
    SOURCE_KEY_TO_NAMES["south-carolina"] = SOURCE_KEY_TO_NAMES["south_carolina"]

if "new_mexico" in SOURCE_KEY_TO_NAMES and "new-mexico" not in SOURCE_KEY_TO_NAMES:
    SOURCE_KEY_TO_NAMES["new-mexico"] = SOURCE_KEY_TO_NAMES["new_mexico"]


@app.get("/frontend/items")
async def frontend_items(
    source: str = Query(..., description="Source key, e.g. white-house"),
    page: int = Query(1, ge=1),
    page_size: int = Query(40, ge=1, le=100),

    # ✅ NEW filters
    sort: str = Query("desc", regex="^(asc|desc)$"),  # asc=oldest→latest, desc=latest→oldest
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),  # YYYY-MM-DD
    date_to: Optional[str] = Query(None),    # YYYY-MM-DD
):
    raw = (source or "").strip()
    key = raw.lower()

    # variants to try (handles "North Carolina", "north-carolina", "north_carolina")
    candidates = []
    candidates.append(key)
    candidates.append(key.replace(" ", "-"))
    candidates.append(key.replace(" ", "_"))
    candidates.append(key.replace("-", "_"))
    candidates.append(key.replace("_", "-"))

    src_names = None
    used = None
    for k in candidates:
        if k in SOURCE_KEY_TO_NAMES:
            src_names = SOURCE_KEY_TO_NAMES[k]
            used = k
            break

    if not src_names:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source: {source}. Tried: {candidates}",
        )

    offset = (page - 1) * page_size

    # ✅ dynamic WHERE + params
    where = ["s.name = any($1::text[])"]
    params = [src_names]

    if status and status.strip():
        where.append(f"i.status = ${len(params) + 1}")
        params.append(status.strip())

    # date filters apply to the same timestamp you show in feed:
    # coalesce(published_at, fetched_at)
    if date_from:
        dt_from = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        where.append(f"coalesce(i.published_at, i.fetched_at) >= ${len(params) + 1}")
        params.append(dt_from)

    if date_to:
        # include the whole "date_to" day by adding 1 day and using <
        dt_to = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) + timedelta(days=1)
        where.append(f"coalesce(i.published_at, i.fetched_at) < ${len(params) + 1}")
        params.append(dt_to)

    where_sql = " where " + " and ".join(where)
    order_sql = "asc" if sort == "asc" else "desc"

    print("frontend_items", {
        "source": source, "used": used, "page": page, "page_size": page_size,
        "sort": sort, "status": status, "date_from": date_from, "date_to": date_to
    })

    async with connection() as conn:
        total = await conn.fetchval(
            f"""
            select count(*)
            from items i
            join sources s on s.id = i.source_id
            {where_sql}
            """,
            *params,
        )

        rows = await conn.fetch(
            f"""
            select
                i.external_id as id,
                i.title,
                coalesce(i.ai_summary, i.summary) as summary,
                i.url,
                s.name as source_name,
                i.jurisdiction,
                i.status,
                i.categories,
                i.ai_impact_score,
                i.ai_impact,
                i.ai_impact_status,
                coalesce(i.published_at, i.fetched_at) as published_at
            from items i
            join sources s on s.id = i.source_id
            {where_sql}
            order by coalesce(i.published_at, i.fetched_at) {order_sql} nulls last
            limit ${len(params) + 1} offset ${len(params) + 2}
            """,
            *params,
            page_size,
            offset,
        )

    # after fetching rows...

    items = []
    for r in rows:
        d = dict(r)
        d["ai_impact"] = _normalize_ai_impact(d.get("ai_impact"))
        items.append(d)

    return {
        "source": used,
        "page": page,
        "page_size": page_size,
        "total": int(total or 0),
        "items": items,
    }

@app.get("/frontend/statuses")
async def frontend_statuses(
    source: str = Query(..., description="Source key, e.g. ohio"),
):
    raw = (source or "").strip()
    key = raw.lower()

    candidates = []
    candidates.append(key)
    candidates.append(key.replace(" ", "-"))
    candidates.append(key.replace(" ", "_"))
    candidates.append(key.replace("-", "_"))
    candidates.append(key.replace("_", "-"))

    src_names = None
    used = None
    for k in candidates:
        if k in SOURCE_KEY_TO_NAMES:
            src_names = SOURCE_KEY_TO_NAMES[k]
            used = k
            break

    if not src_names:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source: {source}. Tried: {candidates}",
        )

    async with connection() as conn:
        rows = await conn.fetch(
            """
            select distinct i.status
            from items i
            join sources s on s.id = i.source_id
            where s.name = any($1::text[])
              and i.status is not null
              and length(trim(i.status)) > 0
            order by i.status asc
            """,
            src_names,
        )

    return {
        "source": used,
        "statuses": [r["status"] for r in rows],
    }

@app.get("/frontend/whats-new")
async def frontend_whats_new():
    # ✅ Order you want the slideshow to rotate through
    # (keep consistent with your tabs list)
    ordered_keys = [
        "white-house",
        "florida", "texas", "new-york", "pennsylvania", "illinois", "massachusetts",
        "washington", "california",
        "utah", "ohio", "vermont", "arizona", "virginia", "georgia", "hawaii",
        "alaska", "new-jersey", "maryland", "colorado", "minnesota", "oregon", "michigan",
        "north-carolina", "wisconsin", "nevada", "tennessee",
        "south-carolina", "iowa", "missouri", "kansas", "new-mexico",
    ]

    # filter to only those actually configured
    keys = [k for k in ordered_keys if k in SOURCE_KEY_TO_NAMES]

    out: List[Dict[str, Any]] = []

    async with connection() as conn:
        for key in keys:
            src_names = SOURCE_KEY_TO_NAMES.get(key)
            if not src_names:
                continue

            row = await conn.fetchrow(
                """
                select
                    i.external_id as id,
                    i.title,
                    coalesce(i.ai_summary, i.summary) as summary,
                    i.url,
                    s.name as source_name,
                    i.jurisdiction,
                    i.status,
                    i.categories,
                    i.ai_impact_score,
                    i.ai_impact,
                    i.ai_impact_status,
                    coalesce(i.published_at, i.fetched_at) as published_at
                from items i
                join sources s on s.id = i.source_id
                where s.name = any($1::text[])
                order by coalesce(i.published_at, i.fetched_at) desc nulls last
                limit 1
                """,
                src_names,
            )

            if row:
                d = dict(row)
                d["ai_impact"] = _normalize_ai_impact(d.get("ai_impact"))
                out.append({"source_key": key, "item": d})
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(out),
        "items": out,
    }

@app.post("/ingest/states2/{state_key}", dependencies=[Depends(_require_cron)])
async def ingest_states2(state_key: str, limit_each: int = 5000, max_pages_each: int = 500):
    key = (state_key or "").strip().lower()

    fn = INGESTERS_V2.get(key)
    if not fn:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown states2 ingester: {key}. Available: {sorted(INGESTERS_V2.keys())}",
        )

    return await fn(limit_each=limit_each, max_pages_each=max_pages_each)

@app.post("/ingest/states3/{state_key}", dependencies=[Depends(_require_cron)])
async def ingest_states3(state_key: str, limit_each: int = 5000, max_pages_each: int = 500):
    key = (state_key or "").strip().lower()

    fn = INGESTERS_V3.get(key)
    if not fn:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown states3 ingester: {key}. Available: {sorted(INGESTERS_V3.keys())}",
        )

    return await fn(limit_each=limit_each, max_pages_each=max_pages_each)

class ImpactBatchPayload(BaseModel):
    # cron-safe defaults
    max_items: int = 200
    hours: Optional[int] = 24   # last 24h by default
    force: bool = False         # never rescore unless explicitly asked
    source: Optional[str] = None
    jurisdiction: Optional[str] = None

@app.post("/ai/impact/batch", dependencies=[Depends(_require_cron)])
async def ai_impact_batch(payload: ImpactBatchPayload):
    """
    Compute ai_impact_score + ai_impact for items that already have ai_summary.
    Rules:
      - if ai_summary is null => skip (leave impact null)
      - default => score only where ai_impact_score is null
      - force=True => rescore everything in scope
    """
    async with connection() as conn:
        payload_j = _normalize_jurisdiction(payload.jurisdiction)
        rows = await conn.fetch(
            """
            select
                i.id,
                i.title,
                i.url,
                coalesce(nullif(trim(i.ai_summary), ''), nullif(trim(i.summary), '')) as summary_text
            from items i
            join sources s on s.id = i.source_id
            where coalesce(nullif(trim(i.ai_summary), ''), nullif(trim(i.summary), '')) is not null
            and (
                $1::bool
                or i.ai_impact_created_at is null
            )
            and (
                $2::int is null or $2::int <= 0
                or coalesce(i.published_at, i.fetched_at) >= (now() - make_interval(hours => $2::int))
            )
            AND (
                $3::text IS NULL OR
                replace(replace(replace(replace(lower(s.name), ' ', ''), chr(160), ''), '—', ''), '-', '') ILIKE
                ('%' || replace(replace(replace(replace(lower($3), ' ', ''), chr(160), ''), '—', ''), '-', '') || '%')
            )
            AND (
                $5::text IS NULL OR
                replace(replace(replace(lower(coalesce(i.jurisdiction,'')), ' ', ''), '-', ''), '_', '')
                =
                replace(replace(replace(lower($5::text), ' ', ''), '-', ''), '_', '')
            )
            order by coalesce(i.published_at, i.fetched_at) desc nulls last
            limit $4::int
            """,
            payload.force,          # $1
            payload.hours,          # $2
            payload.source,         # $3
            payload.max_items,      # $4
            payload_j,              # $5
        )

        processed = 0
        scored = 0
        errored = 0

        for r in rows:
            processed += 1
            title = r["title"] or ""
            url = r["url"] or ""
            summary_text = r["summary_text"] or ""

            try:
                res = await score_item_impact(title=title, url=url, summary_text=summary_text)

                # if res is None => budget exceeded or no key; mark as skipped
                if res is None:
                    await conn.execute(
                        """
                        update items
                        set ai_impact_status=$1
                        where id=$2
                        and ai_impact_created_at is null
                        """,
                        "skipped", r["id"]
                    )
                    continue

                await conn.execute(
                    """
                    update items
                    set ai_impact_score=$1,
                        ai_impact=$2::jsonb,
                        ai_impact_model=$3,
                        ai_impact_created_at=now(),
                        ai_impact_status=$4
                    where id=$5
                    """,
                    res.score,
                    json.dumps(res.impact),
                    res.model,
                    "ok" if res.impact.get("overall_why") != "error generating impact" else "error",
                    r["id"],
                )
                scored += 1

            except Exception as e:
                errored += 1
                await conn.execute(
                    """
                    update items
                    set ai_impact_status=$1
                    where id=$2
                    and ai_impact_created_at is null
                    """,
                    f"error:{type(e).__name__}", r["id"]
                )
        return {
            "processed": processed,
            "scored": scored,
            "errors": errored,
        }

@app.get("/me")
async def me(user_id: str = Depends(get_user_id_from_auth)):
    # returns the user's profile row (from your DB)
    async with connection() as conn:
        row = await conn.fetchrow(
            """
            select id, email, first_name, last_name, full_name, avatar_url, provider, created_at, updated_at
            from user_profiles
            where id = $1
            """,
            user_id,
        )
    return {"user_id": user_id, "profile": dict(row) if row else None}


@app.delete("/me")
async def delete_me(user_id: str = Depends(get_user_id_from_auth)):
    """
    Deletes:
      1) Any user-owned rows you choose (alerts/preferences/deliveries/etc)
      2) The auth user via Supabase Admin API (service role)
         -> because user_profiles references auth.users with ON DELETE CASCADE,
            profile will be removed automatically.
    """

    async with connection() as conn:
        # OPTIONAL (future): clean up your app tables that reference user_id
        # await conn.execute("delete from deliveries where user_id=$1", user_id)
        # await conn.execute("delete from alerts where user_id=$1", user_id)

        # NOTE: don't delete user_profiles manually if you're relying on cascade.

        pass

    # delete the auth user (this triggers cascade for user_profiles)
    await admin_delete_user(user_id)

    return {"ok": True, "deleted_user_id": user_id}

class PreferencesIn(BaseModel):
    sources: List[str] = Field(default_factory=list)

class AlertIn(BaseModel):
    source_key: str
    statuses: List[str] = Field(default_factory=list)
    categories: List[str] = Field(default_factory=list)
    enabled: bool = True
    muted: bool = False

class AlertOut(BaseModel):
    id: str
    source_key: str
    statuses: List[str]
    categories: List[str]
    enabled: bool
    muted: bool
    created_at: str

def _normalize_source_key(k: str) -> str:
    raw = (k or "").strip().lower()
    cands = [raw, raw.replace(" ", "-"), raw.replace(" ", "_"), raw.replace("-", "_"), raw.replace("_", "-")]
    for cand in cands:
        if cand in SOURCE_KEY_TO_NAMES:
            return cand
    raise HTTPException(400, f"Unknown source_key: {k}")

def _clean_list(xs: List[str]) -> List[str]:
    out = []
    for x in xs or []:
        x = (x or "").strip()
        if x:
            out.append(x)
    # stable de-dupe
    seen = set()
    uniq = []
    for x in out:
        if x.lower() in seen: 
            continue
        seen.add(x.lower())
        uniq.append(x)
    return uniq

@app.get("/me/preferences")
async def get_preferences(user_id: str = Depends(get_user_id_from_auth)):
    async with connection() as conn:
        row = await conn.fetchrow(
            "select user_id, sources, created_at, updated_at from user_preferences where user_id=$1",
            user_id,
        )
    return {"user_id": user_id, "preferences": dict(row) if row else None}

@app.put("/me/preferences")
async def put_preferences(payload: PreferencesIn, user_id: str = Depends(get_user_id_from_auth)):
    # validate + normalize
    sources = [_normalize_source_key(s) for s in _clean_list(payload.sources)]
    async with connection() as conn:
        await conn.execute(
            """
            insert into user_preferences(user_id, sources)
            values ($1, $2::text[])
            on conflict (user_id)
            do update set sources=excluded.sources, updated_at=now()
            """,
            user_id, sources,
        )
        row = await conn.fetchrow(
            "select user_id, sources, created_at, updated_at from user_preferences where user_id=$1",
            user_id,
        )
    return {"ok": True, "preferences": dict(row)}

@app.get("/me/alerts")
async def list_alerts(user_id: str = Depends(get_user_id_from_auth)):
    async with connection() as conn:
        rows = await conn.fetch(
            """
            select id, source_key, statuses, categories, enabled, muted, created_at
            from alerts
            where user_id=$1
            order by created_at desc
            """,
            user_id,
        )
    return {"alerts": [dict(r) for r in rows]}

@app.post("/me/alerts")
async def create_alert(payload: AlertIn, user_id: str = Depends(get_user_id_from_auth)):
    source_key = _normalize_source_key(payload.source_key)
    statuses = _clean_list(payload.statuses)
    categories = _clean_list(payload.categories)

    async with connection() as conn:
        row = await conn.fetchrow(
            """
            insert into alerts(user_id, source_key, statuses, categories, enabled, muted)
            values ($1, $2, $3::text[], $4::text[], $5, $6)
            returning id, source_key, statuses, categories, enabled, muted, created_at
            """,
            user_id, source_key, statuses, categories, payload.enabled, payload.muted
        )
    return {"ok": True, "alert": dict(row)}

@app.put("/me/alerts/{alert_id}")
async def update_alert(alert_id: str, payload: AlertIn, user_id: str = Depends(get_user_id_from_auth)):
    source_key = _normalize_source_key(payload.source_key)
    statuses = _clean_list(payload.statuses)
    categories = _clean_list(payload.categories)

    async with connection() as conn:
        row = await conn.fetchrow(
            """
            update alerts
            set source_key=$1, statuses=$2::text[], categories=$3::text[], enabled=$4, muted=$5
            where id=$6 and user_id=$7
            returning id, source_key, statuses, categories, enabled, muted, created_at
            """,
            source_key, statuses, categories, payload.enabled, payload.muted, alert_id, user_id
        )
    if not row:
        raise HTTPException(404, "Alert not found")
    return {"ok": True, "alert": dict(row)}

@app.delete("/me/alerts/{alert_id}")
async def delete_alert(alert_id: str, user_id: str = Depends(get_user_id_from_auth)):
    async with connection() as conn:
        n = await conn.execute("delete from alerts where id=$1 and user_id=$2", alert_id, user_id)
    # asyncpg returns like "DELETE 1"
    return {"ok": True, "deleted": n}

@app.get("/me/alerts/poll")
async def poll_alerts(user_id: str = Depends(get_user_id_from_auth)):
    """
    For each enabled+not-muted alert:
      - find the latest matching item
      - if we have no delivery row OR delivery is un-acked => return it (so UI can show toast)
      - if latest is already acked => return nothing for that alert
    """
    async with connection() as conn:
        alerts = await conn.fetch(
            """
            select id, source_key, statuses, categories, enabled, muted
            from alerts
            where user_id=$1 and enabled=true and muted=false
            order by created_at desc
            """,
            user_id,
        )

        out = []
        for a in alerts:
            src_names = SOURCE_KEY_TO_NAMES.get(a["source_key"]) or []
            if not src_names:
                continue

            # optional filters
            statuses = a["statuses"] or []
            categories = a["categories"] or []

            # build where + params (asyncpg positional)
            where = ["s.name = any($1::text[])"]
            params = [src_names]

            if statuses:
                where.append(f"i.status = any(${len(params)+1}::text[])")
                params.append(statuses)

            if categories:
                # items.categories is _text in your schema; overlap operator works
                where.append(f"coalesce(i.categories, '{{}}'::text[]) && ${len(params)+1}::text[]")
                params.append(categories)

            where_sql = " and ".join(where)

            item = await conn.fetchrow(
                f"""
                select
                  i.id as item_uuid,
                  i.external_id as id,
                  i.title,
                  coalesce(i.ai_summary, i.summary) as summary,
                  i.url,
                  s.name as source_name,
                  i.jurisdiction,
                  i.status,
                  i.categories,
                  i.ai_impact_score,
                  i.ai_impact,
                  i.ai_impact_status,
                  coalesce(i.published_at, i.fetched_at) as published_at
                from items i
                join sources s on s.id = i.source_id
                where {where_sql}
                order by coalesce(i.published_at, i.fetched_at) desc nulls last
                limit 1
                """,
                *params,
            )
            if not item:
                continue

            # do we already have a delivery row for this alert+item?
            delivery = await conn.fetchrow(
                """
                select id, delivered_at, acknowledged_at
                from deliveries
                where alert_id=$1 and item_id=$2
                limit 1
                """,
                a["id"], item["item_uuid"],
            )

            if not delivery:
                # create delivery as "shown" but NOT acknowledged yet
                delivery_id = await conn.fetchval(
                    """
                    insert into deliveries(alert_id, item_id, delivered_at, channel)
                    values ($1, $2, now(), $3)
                    returning id
                    """,
                    a["id"], item["item_uuid"], "web",
                )
                delivery = {"id": delivery_id, "delivered_at": datetime.now(timezone.utc), "acknowledged_at": None}

            # if already acknowledged, don't notify again
            if delivery["acknowledged_at"] is not None:
                continue

            item_d = dict(item)
            item_d["ai_impact"] = _normalize_ai_impact(item_d.get("ai_impact"))

            out.append({
                "alert": {
                    "id": str(a["id"]),
                    "source_key": a["source_key"],
                    "statuses": statuses,
                    "categories": categories,
                    "muted": bool(a["muted"]),
                    "enabled": bool(a["enabled"]),
                },
                "delivery": {
                    "id": str(delivery["id"]),
                    "delivered_at": str(delivery["delivered_at"]),
                    "acknowledged_at": delivery["acknowledged_at"],
                },
                "item": item_d,
            })

    return {"notifications": out}

@app.post("/me/alerts/deliveries/{delivery_id}/ack")
async def ack_delivery(delivery_id: str, user_id: str = Depends(get_user_id_from_auth)):
    async with connection() as conn:
        # ensure it belongs to this user via alert ownership
        row = await conn.fetchrow(
            """
            update deliveries d
            set acknowledged_at=now()
            from alerts a
            where d.id=$1
              and d.alert_id=a.id
              and a.user_id=$2
            returning d.id
            """,
            delivery_id, user_id,
        )
    if not row:
        raise HTTPException(404, "Delivery not found")
    return {"ok": True}