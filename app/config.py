from pydantic_settings import BaseSettings  # было: from pydantic import BaseSettings

class Settings(BaseSettings):
    BOT_TOKEN: str
    TZ: str = "Europe/Moscow"
    class Config: env_file = ".env"

settings = Settings()

