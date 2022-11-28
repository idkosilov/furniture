from abc import ABC, abstractmethod
from typing import Optional, Set, List

import asyncpg

from allocation.domain import model


class AbstractRepository(ABC):

    @abstractmethod
    async def get(self, reference: str) -> Optional[model.Batch]:
        ...

    @abstractmethod
    async def add(self, batch: model.Batch) -> None:
        ...

    async def list(self) -> List[model.Batch]:
        ...


class PostgresRepository(AbstractRepository):

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection
        self._seen: Set[(int, model.Batch)] = set()

    async def get(self, reference: str) -> Optional[model.Batch]:
        query = """
            SELECT b.id AS id,
                   b.batch_ref AS reference, 
                   b.sku AS stock_keeping_unit, 
                   b.qty AS quantity, 
                   b.eta AS estimated_arrival_time, 
                   array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
            FROM batches b
            LEFT JOIN allocations a ON b.id = a.batch_id
            LEFT JOIN order_lines ol ON ol.id = a.order_line_id
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

        self._seen.add((batch_row["id"], batch))

        return batch

    async def add(self, batch: model.Batch) -> None:
        query = """
            WITH ib AS (
                INSERT INTO batches (batch_ref, sku, qty, eta) 
                VALUES ($1, $2, $3, $4)
                RETURNING id
            ), iol AS (
                INSERT INTO order_lines (order_ref, sku, qty) 
                (
                    SELECT r.order_ref, r.sku, r.qty
                    FROM unnest($5::order_lines[]) as r 
                )
                RETURNING id
            )
            INSERT INTO allocations (order_line_id, batch_id)
            (
                SELECT iol.id, (SELECT * FROM ib)
                FROM iol
            )
            RETURNING batch_id
        """

        batch_row = (batch.reference,
                     batch.stock_keeping_unit,
                     batch.purchased_quantity,
                     batch.estimated_arrival_time)

        order_lines = [(None, order_line.stock_keeping_unit, order_line.quantity,
                        order_line.order_reference)
                       for order_line in batch.allocations]

        batch_id = await self._connection.fetchrow(query, *batch_row, order_lines)

        self._seen.add((batch_id, batch))

    async def list(self) -> List[model.Batch]:
        query = """
            SELECT b.id AS id,
                   b.batch_ref AS reference, 
                   b.sku AS stock_keeping_unit, 
                   b.qty AS quantity, 
                   b.eta AS estimated_arrival_time, 
                   array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
            FROM batches b
            LEFT JOIN allocations a ON b.id = a.batch_id
            LEFT JOIN order_lines ol ON ol.id = a.order_line_id
            GROUP BY b.id
        """
        batch_rows = await self._connection.fetch(query)

        batches = []

        for batch_row in batch_rows:
            batch = model.Batch(batch_row["reference"],
                                batch_row["stock_keeping_unit"],
                                batch_row["quantity"],
                                batch_row["estimated_arrival_time"])

            for order_line_row in batch_row["allocations"]:
                order_line = model.OrderLine(*order_line_row)
                batch.allocate(order_line)

            batches.append(batch)
            self._seen.add((batch_row['id'], batch))

        return batch_rows

    async def save_changes(self) -> None:
        update_batch = """
            WITH ub AS (
                UPDATE batches
                   SET batch_ref = $2, sku = $3, qty = $4, eta = $5
                 WHERE id = $1
                 RETURNING id
            ), iol AS (
                INSERT INTO order_lines (sku, qty, order_ref) 
                (
                    SELECT r.sku, r.qty, r.order_ref
                    FROM unnest($6::order_lines[]) as r 
                )
                ON CONFLICT DO NOTHING 
                RETURNING id
            )
            INSERT INTO allocations (order_line_id, batch_id)
            SELECT iol.id, (SELECT * FROM ub)
            FROM iol
        """

        batches_rows = []

        for b_id, b in self._seen:
            batch_row = [b_id, b.reference, b.stock_keeping_unit, b.purchased_quantity, b.estimated_arrival_time]
            lines_rows = []
            lines_order_refs = []

            for line in b.allocations:
                lines_rows.append(
                    (None, line.stock_keeping_unit, line.quantity, line.order_reference)
                )
                lines_order_refs.append(line.order_reference)

            batches_rows.append([*batch_row, lines_rows])

        await self._connection.executemany(update_batch, batches_rows)
