from abc import ABC, abstractmethod
from typing import Optional, Set, List

import asyncpg

from allocation.domain import model


class AbstractProductRepository(ABC):

    @abstractmethod
    async def get(self, sku: str) -> Optional[model.Product]:
        ...

    @abstractmethod
    async def add(self, product: model.Product) -> None:
        ...


class PostgresProductRepository(AbstractProductRepository):

    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection
        self._seen: Set[(int, model.Product)] = set()

    async def get(self, sku: str) -> Optional[model.Product]:
        query = """
            WITH find_product AS (
                SELECT sku
                FROM products
                WHERE sku = $1 
            )
            SELECT b.id AS id,
                   b.batch_ref AS batch_ref, 
                   b.sku AS sku, 
                   b.qty AS qty, 
                   b.eta AS eta, 
                   array_agg(row(ol.order_ref, ol.sku, ol.qty)) AS allocations
            FROM batches b
            LEFT JOIN allocations a ON b.id = a.batch_id
            LEFT JOIN order_lines ol ON ol.id = a.order_line_id
            WHERE b.sku = find_product.sku
            GROUP BY b.id                    
        """

        product_batches_rows = await self._connection.fetch(query, sku)

        if len(product_batches_rows) != 0:
            batches = []

            for batch_row in product_batches_rows:
                batch = model.Batch(batch_row["batch_ref"], batch_row["sku"], batch_row["qty"], batch_row["eta"])
                batches.append(batch)

                for order_line_row in batch_row["allocations"]:
                    order_line = model.OrderLine(*order_line_row)
                    batch.allocations.add(order_line)

            product = model.Product(sku, batches)
            self._seen.add(product)

            return product

    async def add(self, product: model.Product) -> None:
        await self._connection.execute("INSERT INTO products VALUES ($1)", product.sku)

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
        """

        allocations_groups = []

        for batch in product.batches:
            allocations_rows = []
            for line in batch.allocations:
                allocations_rows.append((None, line.sku, line.qty, line.order_ref))

            allocation_group = [batch.ref, batch.sku, batch.purchased_quantity, batch.eta, allocations_rows]
            allocations_groups.append(allocation_group)

        await self._connection.executemany(query, allocations_groups)

        self._seen.add(product)

    async def save_changes(self) -> None:
        while len(self._seen) != 0:
            product = self._seen.pop()

