from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_host: str = ''
    db_port: int = 0
    db_name: str = ''
    db_user: str = ''
    db_pass: str = ''

    github_token: str = ''
    ch_url: str = ''
    ch_db: str = ''
    ch_user: str = ''
    ch_password: str = ''

    model_config = SettingsConfigDict(env_file=".env", frozen=True, env_ignore_empty=True)


@lru_cache
def get_settings():
    return Settings()
