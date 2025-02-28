from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncGenerator
import asyncpg
from settings import get_settings


class Db:
    def __init__(self,
                 db_host: str,
                 db_port: int,
                 db_name: str,
                 db_user: str,
                 db_pass: str):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.pool = None

    async def get_pool(self) -> asyncpg.Pool:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass,
                min_size=0,
                max_size=10)
        return self.pool

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            yield conn

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


@lru_cache
def create_db(
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_pass: str):
    return Db(
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_pass=db_pass
    )


def get_db():
    settings = get_settings()
    return create_db(
        db_host=settings.db_host,
        db_port=settings.db_port,
        db_name=settings.db_name,
        db_user=settings.db_user,
        db_pass=settings.db_pass
    )
