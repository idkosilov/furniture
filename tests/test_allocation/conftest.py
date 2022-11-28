import asyncpg
import pytest_asyncio

from allocation import config


@pytest_asyncio.fixture
async def pg_pool():
    poll = await asyncpg.create_pool(dsn=config.get_postgres_uri())
    return poll


@pytest_asyncio.fixture
async def pg_connection(pg_pool):
    async with pg_pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        yield connection
        await transaction.rollback()
