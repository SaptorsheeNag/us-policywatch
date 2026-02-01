from datetime import datetime, timezone
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

FEDERAL_REGISTER_STATUS_MAP = {
    "Rule": "final",
    "Proposed Rule": "proposed",
    "Notice": "notice",
}


def map_fr_status(doc_type: str) -> str:
    return FEDERAL_REGISTER_STATUS_MAP.get(doc_type, (doc_type or "").lower())




def parse_pub_date(d: str):
    # Federal Register gives YYYY-MM-DD; set as midnight UTC
    return datetime.fromisoformat(d).replace(tzinfo=timezone.utc)

def parse_rss_date(s: str):
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        # force tz-aware UTC if missing
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except Exception:
            return None