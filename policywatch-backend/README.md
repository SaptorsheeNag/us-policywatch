# PolicyWatch API (FastAPI)


## Local dev
1. Python 3.11+
2. `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\\Scripts\\activate`)
3. `pip install -r requirements.txt`
4. Copy `.env.example` -> `.env` and fill values
5. Run: `uvicorn app.main:app --reload`
6. Test: `curl http://127.0.0.1:8000/health`


### First ingest (manual test)
```bash
curl -X POST http://127.0.0.1:8000/ingest/federal-register \
-H "Authorization: Bearer $CRON_KEY" \
-H 'Content-Type: application/json' \
-d '{"since_hours": 24}'