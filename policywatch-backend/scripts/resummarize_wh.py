# scripts/resummarize_wh.py
import os, asyncio
from dotenv import load_dotenv
load_dotenv()

from app.db import connection
from app.summarize import resummarize_white_house_batch, force_repolish_white_house_batch

async def main():
    async with connection() as conn:
        # A) fix missing/bad WH summaries (EO boilerplate, crumbs, empty)
        n1 = await resummarize_white_house_batch(conn, limit=1000)
        print(f"[WH][summarize] updated {n1} missing/bad summaries")

        # B) force re-polish recent WH summaries even if they already exist
        n2 = await force_repolish_white_house_batch(conn, limit=1000)
        print(f"[WH][summarize] force-repolished {n2} summaries")

if __name__ == "__main__":
    asyncio.run(main())
