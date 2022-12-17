import datetime
import uuid

import pytest
from asyncpg.connection import Connection

from allocation.domain import model
from allocation.service_layer import unit_of_work


def random_suffix():
    return uuid.uuid4().hex[:6]


def random_sku(name=""):
    return f"sku-{name}-{random_suffix()}"


def random_batchref(name=""):
    return f"batch-{name}-{random_suffix()}"


def random_orderid(name=""):
    return f"order-{name}-{random_suffix()}"


async def insert_batch(connection: Connection, ref: str, sku: str, qty: int, eta: datetime.datetime):
    await connection.execute("INSERT INTO products VALUES ($1) ON CONFLICT DO NOTHING", sku)

    batch_id = await connection.fetchval(
        "INSERT INTO batches (batch_ref, sku, qty, eta)"
        " VALUES ($1, $2, $3, $4)"
        "RETURNING batches.id",
        ref, sku, qty, eta)
    return batch_id


async def get_allocated_batch_ref(connection: Connection, order_ref: str, sku: str):
    order_line_id = await connection.fetchval(
        "SELECT id FROM order_lines WHERE order_ref = $1 AND sku = $2",
        order_ref, sku)

    batch_ref = await connection.fetchval(
        "SELECT b.batch_ref FROM allocations a JOIN batches AS b ON a.batch_id = b.id"
        " WHERE order_line_id = $1", order_line_id)
    return batch_ref


async def insert_allocation(connection: Connection, order_ref: str, sku: str, qty: int, batch_id: int):
    await connection.execute(
        """
        WITH iol AS (
            INSERT INTO order_lines (sku, qty, order_ref) 
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING 
            RETURNING order_lines.id
        )
        INSERT INTO allocations (order_line_id, batch_id)
        VALUES ((SELECT * FROM iol), $4)
        """, sku, qty, order_ref, batch_id
    )


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_to_it(pg_pool):
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            await insert_batch(connection, "batch1", "HIPSTER-WORKBENCH", 100, None)

    uow = unit_of_work.PostgresUnitOfWork(pg_pool)

    async with uow:
        product = await uow.products.get(sku="HIPSTER-WORKBENCH")
        line = model.OrderLine("o1", "HIPSTER-WORKBENCH", 10)
        product.allocate(line)
        await uow.commit()

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "o1", "HIPSTER-WORKBENCH")

    assert batch_ref == "batch1"


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_few_lines_to_it_and_deallocate(pg_pool):
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            batch_id = await insert_batch(connection, "batch1", "HIPSTER-WORKBENCH", 100, None)
            await insert_allocation(connection, "order-1", "HIPSTER-WORKBENCH", 10, batch_id)
            await insert_allocation(connection, "order-2", "HIPSTER-WORKBENCH", 23, batch_id)
            await insert_allocation(connection, "order-3", "HIPSTER-WORKBENCH", 43, batch_id)

    uow = unit_of_work.PostgresUnitOfWork(pg_pool)

    async with uow:
        product = await uow.products.get(sku="HIPSTER-WORKBENCH")
        new_line = model.OrderLine("order-4", "HIPSTER-WORKBENCH", 10)
        product.allocate(new_line)
        old_line = model.OrderLine("order-1", "HIPSTER-WORKBENCH", 10)
        product.deallocate(old_line)
        await uow.commit()

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "order-1", "HIPSTER-WORKBENCH")

    assert batch_ref is None

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "order-2", "HIPSTER-WORKBENCH")

    assert batch_ref == "batch1"

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "order-3", "HIPSTER-WORKBENCH")

    assert batch_ref == "batch1"

    async with pg_pool.acquire() as connection:
        batch_ref = await get_allocated_batch_ref(connection, "order-4", "HIPSTER-WORKBENCH")

    assert batch_ref == "batch1"


@pytest.mark.asyncio
async def test_rolls_back_uncommitted_work_by_default(pg_pool):
    uow = unit_of_work.PostgresUnitOfWork(pg_pool)
    async with uow:
        await insert_batch(uow.connection, "batch1", "MEDIUM-PLINTH", 100, None)

    async with pg_pool.acquire() as connection:
        rows = await connection.fetch('SELECT * FROM "batches"')

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

    assert rows == []


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_product_by_batch_ref(pg_pool):
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            await insert_batch(connection, "batch1", "HIPSTER-WORKBENCH", 100, None)

    uow = unit_of_work.PostgresUnitOfWork(pg_pool)

    async with uow:
        product = await uow.products.get_by_batch_ref(batch_ref="batch1")

    assert product.sku == "HIPSTER-WORKBENCH"
