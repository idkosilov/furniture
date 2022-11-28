import datetime

import pytest
from asyncpg.connection import Connection

from allocation.domain import model
from allocation.service_layer import unit_of_work


async def insert_batch(connection: Connection, ref: str, sku: str, qty: str, eta: datetime.datetime):
    await connection.execute(
        "INSERT INTO batches (batch_ref, sku, qty, eta)"
        " VALUES ($1, $2, $3, $4)",
        ref, sku, qty, eta)


async def get_allocated_batch_ref(connection: Connection, order_ref: str, sku: str):
    order_line_id = await connection.fetchval(
        "SELECT id FROM order_lines WHERE order_ref = $1 AND sku = $2",
        order_ref, sku)

    batch_ref = await connection.fetchval(
        "SELECT b.batch_ref FROM allocations a JOIN batches AS b ON a.batch_id = b.id"
        " WHERE order_line_id = $1", order_line_id)
    return batch_ref


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_to_it(pg_pool):
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            await insert_batch(connection, "batch1", "HIPSTER-WORKBENCH", 100, None)

    uow = unit_of_work.PostgresUnitOfWork(pg_pool)

    async with uow:
        batch = await uow.batches.get(reference="batch1")
        line = model.OrderLine("o1", "HIPSTER-WORKBENCH", 10)
        batch.allocate(line)
        await uow.commit()

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "o1", "HIPSTER-WORKBENCH")

    async with pg_pool.acquire() as connection:
        await connection.execute('DELETE FROM allocations')
        await connection.execute('DELETE FROM batches')
        await connection.execute('DELETE FROM order_lines')

    assert batch_ref == "batch1"


@pytest.mark.asyncio
async def test_rolls_back_uncommitted_work_by_default(pg_pool):
    uow = unit_of_work.PostgresUnitOfWork(pg_pool)
    async with uow:
        await insert_batch(uow.connection, "batch1", "MEDIUM-PLINTH", 100, None)

    async with pg_pool.acquire() as connection:
        rows = await connection.fetch('SELECT * FROM "batches"')

    async with pg_pool.acquire() as connection:
        await connection.execute('DELETE FROM allocations')
        await connection.execute('DELETE FROM batches')
        await connection.execute('DELETE FROM order_lines')

    assert rows == []


@pytest.mark.asyncio
async def test_rolls_back_on_error(pg_pool):
    class MyException(Exception):
        pass

    uow = unit_of_work.PostgresUnitOfWork(pg_pool)
    with pytest.raises(MyException):
        async with uow:
            await insert_batch(uow.connection, "batch1", "LARGE-FORK", 100, None)
            raise MyException()

    async with pg_pool.acquire() as connection:
        rows = await connection.fetch('SELECT * FROM "batches"')

    async with pg_pool.acquire() as connection:
        await connection.execute('DELETE FROM allocations')
        await connection.execute('DELETE FROM batches')
        await connection.execute('DELETE FROM order_lines')

    assert rows == []
