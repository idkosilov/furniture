import asyncio

import asyncpg
import pytest_asyncio

from allocation import config


@pytest_asyncio.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest_asyncio.fixture()
async def pg_pool():
    poll = await asyncpg.create_pool(dsn=config.get_postgres_uri())
    yield poll
    async with poll.acquire() as connection:
        async with connection.transaction():
            await connection.execute('TRUNCATE TABLE batches CASCADE ')
            await connection.execute('TRUNCATE TABLE order_lines CASCADE ')
            await connection.execute('TRUNCATE TABLE allocations')


@pytest_asyncio.fixture
async def pg_connection(pg_pool):
    async with pg_pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        yield connection
        await transaction.rollback()
