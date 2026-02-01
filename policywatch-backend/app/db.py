import os
import asyncpg
from contextlib import asynccontextmanager
from typing import Optional, List, Tuple, Any

_DB_POOL = None

# Templates (we'll splice in WHERE and then add LIMIT/OFFSET numbering)
LIST_ITEMS_SQL = """
select id, external_id, title, summary, url, jurisdiction, agency, topic, status, published_at
from items
{where}
order by published_at desc nulls last
limit $%d offset $%d
"""

COUNT_ITEMS_SQL = "select count(*) as c from items {where}"


def build_where(
    q: Optional[str],
    topic: Optional[List[str]],
    jurisdiction: Optional[str],
    status: Optional[str],
    date_from: Any,   # pass a timezone-aware datetime from caller (or None)
    date_to: Any,     # pass a timezone-aware datetime from caller (or None)
) -> Tuple[str, List[Any]]:
    """
    Builds a WHERE clause and a params list compatible with asyncpg.

    Notes:
      - topic is text[] in Postgres; we cast the bind to ::text[].
      - date_from/date_to should already be tz-aware datetimes from the caller.
    """
    where: List[str] = []
    params: List[Any] = []

    if q:
        where.append("(title ilike $%d or summary ilike $%d)" % (len(params) + 1, len(params) + 2))
        params.extend([f"%{q}%", f"%{q}%"])

    if topic:
        # Cast bind param to text[] so asyncpg can map a Python list/tuple properly
        where.append("topic && $%d::text[]" % (len(params) + 1))
        params.append(topic)  # list/tuple of strings is fine

    if jurisdiction:
        where.append("jurisdiction = $%d" % (len(params) + 1))
        params.append(jurisdiction)

    if status:
        where.append("status = $%d" % (len(params) + 1))
        params.append(status)

    if date_from:
        where.append("published_at >= $%d" % (len(params) + 1))
        params.append(date_from)

    if date_to:
        where.append("published_at < $%d" % (len(params) + 1))
        params.append(date_to)

    where_sql = (" where " + " and ".join(where)) if where else ""
    return where_sql, params


async def init_pool():
    global _DB_POOL
    if _DB_POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _DB_POOL = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    return _DB_POOL


async def get_pool():
    if _DB_POOL is None:
        await init_pool()
    return _DB_POOL


@asynccontextmanager
async def connection():
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn
