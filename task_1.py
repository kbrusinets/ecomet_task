import logging.config
import os
from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator
import asyncpg
import uvicorn
import yaml
from fastapi import APIRouter, FastAPI, Depends, HTTPException
from db import get_db


def setup_logging():
    os.makedirs('logs', exist_ok=True)
    with open('log_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)


async def get_pg_connection() -> AsyncGenerator[asyncpg.Connection]:
    try:
        db = get_db()
        async with db.get_connection() as conn:
            yield conn
    except Exception as e:
        logging.getLogger('my_app').exception(e)
        raise HTTPException(status_code=500, detail=str(e))


async def get_db_version(conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]):
    try:
        return await conn.fetchval("SELECT version()")
    except Exception as e:
        logging.getLogger('my_app').exception(e)
        raise HTTPException(status_code=500, detail=str(e))


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await get_db().close()


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    setup_logging()
    uvicorn.run("task_1:create_app", factory=True, log_config='log_conf.yaml')
