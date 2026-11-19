from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from ..config import get_settings

router = APIRouter(prefix="/api/auth", tags=["auth"])
bearer = HTTPBearer(auto_error=False)
ALG = "HS256"


class LoginIn(BaseModel):
    username: str
    password: str


def make_token(user: str) -> str:
    s = get_settings()
    exp = datetime.now(timezone.utc) + timedelta(hours=s.jwt_expire_hours)
    return jwt.encode({"sub": user, "exp": exp}, s.jwt_secret, algorithm=ALG)


def verify_token(token: str) -> str:
    try:
        return jwt.decode(token, get_settings().jwt_secret, algorithms=[ALG])["sub"]
    except JWTError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token") from e


async def current_user(cred: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
    if not cred:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    return verify_token(cred.credentials)


@router.post("/login")
async def login(body: LoginIn):
    s = get_settings()
    if body.username != s.admin_username or body.password != s.admin_password:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Wrong username or password")
    return {"token": make_token(body.username), "user": body.username}


@router.get("/me")
async def me(user: str = Depends(current_user)):
    return {"user": user}
