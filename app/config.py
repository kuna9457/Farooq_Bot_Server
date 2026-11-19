"""Environment / infrastructure settings (strategy parameters live in Mongo, see strategy/params.py)."""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- app ---
    app_name: str = "Farooq Trading Bot"
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    admin_username: str = "admin"
    admin_password: str = "admin123"
    jwt_secret: str = "change-me-please-very-secret"
    jwt_expire_hours: int = 24
    encryption_key: str = ""          # Fernet key for broker tokens; auto-derived from jwt_secret if empty

    # --- database ---
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "farooq_bot"
    allow_mock_db: bool = True        # fall back to in-memory mongomock if Mongo is unreachable

    # --- market data ---
    data_provider: str = "yfinance"   # "upstox" | "yfinance"
    ltp_poll_seconds: float = 3.0

    # --- Upstox ---
    upstox_api_key: str = ""
    upstox_api_secret: str = ""
    upstox_redirect_uri: str = "http://localhost:8000/api/broker/upstox/callback"
    upstox_access_token: str = ""     # optional: paste a token directly

    # --- Zerodha Kite ---
    kite_api_key: str = ""
    kite_api_secret: str = ""
    kite_access_token: str = ""

    # --- scheduler ---
    enable_scheduler: bool = True
    frontend_url: str = "http://localhost:5173"   # where broker-login callbacks send the browser back to (Vercel URL)

    @property
    def cors_list(self) -> list[str]:
        """Allowed browser origins: CORS_ORIGINS + FRONTEND_URL, trimmed, no trailing slash."""
        items = [x.strip().rstrip("/") for x in self.cors_origins.split(",")] + [self.frontend_url.strip().rstrip("/")]
        return sorted({x for x in items if x})


@lru_cache
def get_settings() -> Settings:
    return Settings()
