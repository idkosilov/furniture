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
                   b.batch_ref AS batch_ref, 
                   b.sku AS sku, 
                   b.qty AS qty, 
                   b.eta AS eta, 
                   array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
            FROM batches b
            LEFT JOIN allocations a ON b.id = a.batch_id
            LEFT JOIN order_lines ol ON ol.id = a.order_line_id
            WHERE batch_ref = $1
            GROUP BY b.id
        """
        batch_row = await self._connection.fetchrow(query, reference)

        batch = model.Batch(batch_row["batch_ref"], batch_row["sku"], batch_row["qty"], batch_row["eta"])

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

        batch_row = (batch.ref, batch.sku, batch.purchased_quantity, batch.eta)
        order_lines = [(None, order_line.sku, order_line.qty, order_line.order_ref) for order_line in batch.allocations]

        batch_id = await self._connection.fetchrow(query, *batch_row, order_lines)

        self._seen.add((batch_id, batch))

    async def list(self) -> List[model.Batch]:
        query = """
            SELECT b.id AS id,
                   b.batch_ref AS batch_ref, 
                   b.sku AS sku, 
                   b.qty AS qty, 
                   b.eta AS eta, 
                   array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
            FROM batches b
            LEFT JOIN allocations a ON b.id = a.batch_id
            LEFT JOIN order_lines ol ON ol.id = a.order_line_id
            GROUP BY b.id
        """
        batch_rows = await self._connection.fetch(query)

        batches = []

        for batch_row in batch_rows:
            batch = model.Batch(batch_row["batch_ref"], batch_row["sku"], batch_row["qty"], batch_row["eta"])

            for order_line_row in batch_row["allocations"]:
                order_line = model.OrderLine(*order_line_row)
                batch.allocate(order_line)

            batches.append(batch)
            self._seen.add((batch_row['id'], batch))

        return batch_rows

    async def save_changes(self) -> None:
        delete_order_lines = """
            DELETE FROM order_lines
            WHERE order_ref <> any($1::varchar[])
        """

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
        actual_order_lines = []

        for b_id, b in self._seen:
            batch_row = [b_id, b.ref, b.sku, b.purchased_quantity, b.eta]
            lines_rows = []

            for line in b.allocations:
                lines_rows.append((None, line.sku, line.qty, line.order_ref))
                actual_order_lines.append(line.order_ref)

            batches_rows.append([*batch_row, lines_rows])

        await self._connection.execute(delete_order_lines, actual_order_lines)
        await self._connection.executemany(update_batch, batches_rows)
