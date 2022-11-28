from abc import ABC, abstractmethod
from typing import Optional, Set

import asyncpg

from allocation.domain import model


class AbstractRepository(ABC):

    @abstractmethod
    async def get(self, reference: str) -> Optional[model.Batch]:
        ...

    @abstractmethod
    async def add(self, batch: model.Batch) -> None:
        ...


class PostgresRepository(AbstractRepository):

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection
        self._seen: Set[model.Batch] = set()

    async def get(self, reference: str) -> Optional[model.Batch]:
        query = """
                SELECT b.batch_ref AS reference, 
                       b.sku AS stock_keeping_unit, 
                       b.qty AS quantity, 
                       b.eta AS estimated_arrival_time, 
                       array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
                FROM batches b
                JOIN allocations a ON b.id = a.batch_id
                JOIN order_lines ol ON ol.id = a.order_line_id
                WHERE batch_ref = $1
                GROUP BY b.id
                """

        batch_row = await self._connection.fetchrow(query, reference)

        batch = model.Batch(batch_row["reference"],
                            batch_row["stock_keeping_unit"],
                            batch_row["quantity"],
                            batch_row["estimated_arrival_time"])

        for order_line_row in batch_row["allocations"]:
            order_line = model.OrderLine(*order_line_row)
            batch.allocate(order_line)

        self._seen.add(batch)

        return batch

    async def add(self, batch: model.Batch) -> None:
        query = """
                INSERT INTO batches (batch_ref, sku, qty, eta) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT ( batch_ref ) DO NOTHING 
                RETURNING id
                """

        batch_id = await self._connection.fetchrow(query,
                                                   batch.reference,
                                                   batch.stock_keeping_unit,
                                                   batch.purchased_quantity,
                                                   batch.estimated_arrival_time)

        query = """
                INSERT INTO order_lines (order_ref, sku, qty) 
                (
                    SELECT r.order_ref, r.sku, r.qty
                    FROM unnest($1::order_lines[]) as r 
                )
                RETURNING id
                """

        order_lines_ids = await self._connection.fetch(query, [
            (None, order_line.stock_keeping_unit, order_line.quantity, order_line.order_reference)
            for order_line in batch.allocations
        ])

        query = """
                INSERT INTO allocations (order_line_id, batch_id) 
                (
                    SELECT r.order_line_id, r.batch_id
                    FROM unnest($1::allocations[]) as r 
                )
                """

        await self._connection.fetch(query, [
            (None, order_line_id['id'], batch_id['id'])
            for order_line_id in order_lines_ids
        ])

        self._seen.add(batch)
