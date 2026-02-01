# scripts/resummarize_ca_newsroom.py
import asyncio, re, httpx
from dotenv import load_dotenv
load_dotenv()

from app.db import connection
from app.summarize import (
    summarize_extractive, _strip_html_to_text, _soft_normalize_caps, BROWSER_UA_HEADERS
)
from app.ai_summarizer import ai_polish_summary

SQL = """
    select i.external_id, i.url, i.title
    from items i
    join sources s on s.id = i.source_id
    where s.name = 'California — Newsroom'
    order by i.published_at desc nulls last
    limit $1
"""

async def main(limit=150):
    updates = []
    async with connection() as conn:
        rows = await conn.fetch(SQL, limit)
        if not rows:
            print("[ca-resum] nothing to do")
            return

        async with httpx.AsyncClient(timeout=20.0, headers=BROWSER_UA_HEADERS, follow_redirects=True) as cx:
            for r in rows:
                try:
                    resp = await cx.get(r["url"])
                    if resp.status_code >= 400 or not resp.text:
                        continue
                    html = resp.text

                    # extractive (with your new filters)
                    summary = summarize_extractive(r["title"] or "", r["url"], html, max_sentences=2, max_chars=700)

                    # fallback: first reasonably long paragraph if empty
                    if not summary:
                        text = _strip_html_to_text(html)
                        paras = [p.strip() for p in re.split(r'\n+', text) if len(p.strip()) > 60]
                        if paras:
                            summary = paras[0]
                    if not summary:
                        continue

                    # normalize caps then polish
                    summary = _soft_normalize_caps(summary)
                    summary = await ai_polish_summary(summary, r["title"] or "", r["url"])

                    updates.append((summary, r["external_id"]))
                except Exception:
                    continue

        if updates:
            await conn.executemany(
                "update items set summary = $1 where external_id = $2",
                updates,
            )
            print(f"[ca-resum] updated {len(updates)} summaries")
        else:
            print("[ca-resum] no updates")

if __name__ == "__main__":
    asyncio.run(main())
