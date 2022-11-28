import datetime
from operator import itemgetter

import pytest

from allocation.adapters import repository
from allocation.domain import model

now = datetime.datetime.now()


async def insert_order_line(pg_connection, order_ref, sku, qty):
    await pg_connection.execute(
        "INSERT INTO order_lines (order_ref, sku, qty)"
        " VALUES ($1, $2, $3)",
        order_ref, sku, qty)

    [order_line_id] = await pg_connection.fetchrow("SELECT id FROM order_lines"
                                                   " WHERE order_ref = $1 AND sku= $2",
                                                   order_ref, sku)

    return order_line_id


async def insert_batch(pg_connection, batch_ref, sku, qty, eta):
    await pg_connection.execute(
        "INSERT INTO batches (batch_ref, sku, qty, eta)"
        " VALUES ($1, $2, $3, $4)",
        batch_ref, sku, qty, eta)

    [batch_ref] = await pg_connection.fetchrow('SELECT id FROM batches'
                                               ' WHERE batch_ref = $1 AND sku = $2',
                                               batch_ref, sku)

    return batch_ref


async def insert_allocation(pg_connection, order_line_id, batch_id):
    await pg_connection.execute(
        "INSERT INTO allocations (order_line_id, batch_id)"
        " VALUES ($1, $2)", order_line_id, batch_id)


@pytest.mark.asyncio
async def test_repository_can_save_a_batch(pg_connection):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, now)

    repo = repository.PostgresRepository(pg_connection)
    await repo.add(batch)

    rows = await pg_connection.fetch('SELECT batch_ref, sku, qty, eta FROM batches')

    assert rows == [("batch1", "RUSTY-SOAPDISH", 100, now)]


@pytest.mark.asyncio
async def test_repository_can_save_a_batch_with_allocations(pg_connection):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, now)
    order_line_1 = model.OrderLine("order-1", "RUSTY-SOAPDISH", 10)
    order_line_2 = model.OrderLine("order-2", "RUSTY-SOAPDISH", 32)

    batch.allocate(order_line_1)
    batch.allocate(order_line_2)

    repo = repository.PostgresRepository(pg_connection)
    await repo.add(batch)

    batches_rows = await pg_connection.fetch('SELECT batch_ref, sku, qty, eta FROM batches')
    order_lines_rows = await pg_connection.fetch('SELECT order_ref, sku, qty FROM order_lines')
    order_lines_rows_sorted = sorted(order_lines_rows, key=itemgetter('order_ref'), reverse=False)

    assert batches_rows == [("batch1", "RUSTY-SOAPDISH", 100, now)]
    assert order_lines_rows_sorted == [("order-1", "RUSTY-SOAPDISH", 10), ("order-2", "RUSTY-SOAPDISH", 32)]


@pytest.mark.asyncio
async def test_repository_can_retrieve_a_batch_with_allocations(pg_connection):
    order_line_1_id = await insert_order_line(pg_connection, "order-1", "SMALL-TABLE", 10)
    order_line_2_id = await insert_order_line(pg_connection, "order-2", "SMALL-TABLE", 10)
    batch_1_id = await insert_batch(pg_connection, "batch-1", "SMALL-TABLE", 100, now)
    await insert_batch(pg_connection, "batch-2", "SMALL-TABLE", 120, now)
    await insert_allocation(pg_connection, order_line_1_id, batch_1_id)
    await insert_allocation(pg_connection, order_line_2_id, batch_1_id)

    repo = repository.PostgresRepository(pg_connection)
    retrieved = await repo.get("batch-1")

    expected = model.Batch("batch-1", "SMALL-TABLE", 100, None)
    assert retrieved == expected
    assert retrieved.stock_keeping_unit == expected.stock_keeping_unit
    assert retrieved.purchased_quantity == expected._purchased_quantity
    assert retrieved.allocations == {model.OrderLine("order-1", "SMALL-TABLE", 10),
                                     model.OrderLine("order-2", "SMALL-TABLE", 10)}
