# app/auth.py
import os
from fastapi import Header, HTTPException
from jose import jwt, JWTError

JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")
JWT_ALG = "HS256"

def get_user_id_from_auth(authorization: str | None = Header(default=None)) -> str:
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="SUPABASE_JWT_SECRET not set")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.split(" ", 1)[1].strip()

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG], options={"verify_aud": False})
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    sub = payload.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    return sub
