# scripts/resummarize_all_polish.py
import os, asyncio
from dotenv import load_dotenv
load_dotenv()

from app.db import connection
from app.ai_summarizer import ai_polish_summary

# Reuse your extractive helper
from app.summarize import _strip_html_to_text, summarize_extractive, summarize_text, BROWSER_UA_HEADERS
import httpx

SQL = """
select i.external_id, i.title, i.summary, i.url
from items i
join sources s on s.id = i.source_id
where i.summary is not null and length(i.summary) > 0
order by i.id desc
limit $1
"""

async def main(limit=500):
    updated = 0
    async with connection() as conn:
        rows = await conn.fetch(SQL, limit)
        if not rows:
            print("[repolish] nothing to do")
            return

        updates = []
        async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
            for r in rows:
                title, url, prev = r["title"] or "", r["url"] or "", r["summary"] or ""
                # send existing summary through polish (cheap vs refetching pages)
                polished = await ai_polish_summary(prev, title, url)
                if polished and polished != prev:
                    updates.append((polished, r["external_id"]))

        if updates:
            await conn.executemany(
                "update items set summary = $1 where external_id = $2",
                updates,
            )
            updated = len(updates)

    print(f"[repolish] updated {updated} summaries")

if __name__ == "__main__":
    asyncio.run(main())
