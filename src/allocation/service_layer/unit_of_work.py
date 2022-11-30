from abc import ABC, abstractmethod

from asyncpg import Pool, Connection
from asyncpg.transaction import Transaction, TransactionState
from allocation.adapters import repository
from allocation.service_layer import messagebus


class AbstractUnitOfWork(ABC):
    products: repository.AbstractProductRepository

    async def __aenter__(self) -> "AbstractUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.rollback()

    async def publish_events(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.pop(0)
                await messagebus.handle(event)

    async def commit(self):
        await self._commit()
        await self.publish_events()

    @abstractmethod
    async def _commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError


class PostgresUnitOfWork(AbstractUnitOfWork):
    products: repository.PostgresProductRepository

    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def __aenter__(self) -> "PostgresUnitOfWork":
        self.connection: Connection = await self._pool.acquire()
        self.transaction: Transaction = self.connection.transaction()
        self.products = repository.PostgresProductRepository(self.connection)
        await self.transaction.start()
        return await super().__aenter__()

    async def __aexit__(self, *args) -> None:
        await super().__aexit__(*args)
        await self._pool.release(self.connection)

    async def _commit(self) -> None:
        await self.products.save_changes()
        await self.transaction.commit()

    async def rollback(self) -> None:
        if self.transaction._state is not TransactionState.COMMITTED:
            await self.transaction.rollback()
