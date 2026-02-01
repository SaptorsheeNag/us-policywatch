# app/ingest_ofac.py
from typing import List, Dict, Any
from .ingest_rss import fetch_rss, map_rss_to_rows
from .db import connection
from .ingest_federal_register import get_or_create_source, upsert_items

# OFAC recent actions RSS (non-SDN sanctions and notices)
OFAC_RSS = "https://home.treasury.gov/news/press-releases/feed"  # placeholder feed
# ^ Swap to the exact OFAC/NS-PLC feed you choose later.

async def ingest_ofac_recent_actions() -> dict:
    """
    Minimal stub: pulls an RSS feed, maps to generic rows, upserts.
    Replace OFAC_RSS with the right feed(s) and improve mapping later.
    """
    parsed = await fetch_rss(OFAC_RSS)
    rows_basic = []
    # map_rss_to_rows expects (parsed_feed, source_id, jurisdiction, agency)
    async with connection() as conn:
        source_id = await get_or_create_source(
            conn, name="Treasury / OFAC", kind="rss", base_url="https://home.treasury.gov/"
        )
        rows_basic = map_rss_to_rows(
            parsed_feed=parsed,
            source_id=source_id,
            jurisdiction="federal",
            agency="Treasury Department, Office of Foreign Assets Control",
        )
        upserted = await upsert_items(conn, rows_basic)
    return {"ingested": len(rows_basic), "upserted": upserted}
