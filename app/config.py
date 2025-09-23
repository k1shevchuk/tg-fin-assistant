from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict  # было: from pydantic import BaseSettings


class Settings(BaseSettings):
    BOT_TOKEN: str
    TZ: str = "Europe/Moscow"
    KEY_RATE_FALLBACK: float | None = Field(default=0.16, ge=0.0)
    IDEAS_MIN_SOURCES: int = Field(default=2, ge=1)
    IDEAS_MAX_AGE_DAYS: int = Field(default=90, ge=1)
    IDEAS_TOPN: int = Field(default=5, ge=1, le=8)
    IDEAS_SCORE_THRESHOLD: float = Field(default=0.6, ge=0.0, le=1.0)
    FRED_API_KEY: str | None = None
    SEC_USER_AGENT: str = Field(default="tg-fin-assistant/1.0")
    TWELVEDATA_API_KEY: str | None = None
    FINNHUB_API_KEY: str | None = None
    HTTP_TIMEOUT_SEC: float = Field(default=5.0, gt=0)
    CACHE_TTL_SEC: int = Field(default=10, ge=1)
    TINKOFF_FILTER_ENABLED: bool = True
    TINKOFF_UNIVERSE_PATH: str = "data/tbank_universe.yml"

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()

