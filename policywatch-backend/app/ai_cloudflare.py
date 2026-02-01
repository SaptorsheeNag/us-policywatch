import os, httpx

CF_AI_BASE = "https://api.cloudflare.com/client/v4/accounts/{acct}/ai/run"
MODEL_SUMMARY = "@cf/meta/llama-3.1-8b-instruct"

async def cf_summarize(title: str, abstract: str) -> str:
    CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
    CF_API_TOKEN = os.getenv("CF_API_TOKEN")
    if not (CF_ACCOUNT_ID and CF_API_TOKEN):
        raise RuntimeError("Missing CF_ACCOUNT_ID/CF_API_TOKEN env vars for Cloudflare Workers AI")
    
    prompt = (
        "Summarize in TWO sentences for policy analysts. "
        "Only use facts present in title/abstract.\n"
        f"TITLE: {title}\nABSTRACT: {abstract[:1200]}"
    )
    url = f"{CF_AI_BASE.format(acct=CF_ACCOUNT_ID)}/{MODEL_SUMMARY}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    async with httpx.AsyncClient(timeout=20) as cx:
        r = await cx.post(url, json={"messages":[{"role":"user","content":prompt}]}, headers=headers)
        r.raise_for_status()
        out = r.json()["result"]["response"]
        return out.strip()[:600]

