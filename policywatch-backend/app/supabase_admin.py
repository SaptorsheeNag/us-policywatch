# app/supabase_admin.py
import os
import httpx
from fastapi import HTTPException

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

async def admin_delete_user(user_id: str) -> None:
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")

    url = f"{SUPABASE_URL}/auth/v1/admin/users/{user_id}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.delete(
            url,
            headers={
                "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
                "apikey": SERVICE_ROLE_KEY,
            },
        )

    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=f"Failed to delete user: {resp.status_code} {resp.text}")
