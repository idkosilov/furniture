import pytest

from allocation.adapters import repository
from allocation.domain import events
from allocation.service_layer import handlers, unit_of_work


class FakeRepository(repository.AbstractProductRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    async def _add(self, product):
        self._products.add(product)

    async def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass


@pytest.mark.asyncio
async def test_allocate_returns_allocation():
    uow = FakeUnitOfWork()
    event_batch_created = events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None)
    await handlers.add_batch(event_batch_created, uow)
    events_allocation_required = events.AllocationRequired("o1", "COMPLICATED-LAMP", 10)
    result = await handlers.allocate(events_allocation_required, uow)
    assert result == "batch1"


@pytest.mark.asyncio
async def test_allocate_commits():
    uow = FakeUnitOfWork()
    event_batch_created = events.BatchCreated("b1", "OMINOUS-MIRROR", 100, None)
    await handlers.add_batch(event_batch_created, uow)
    event_allocation_required = events.AllocationRequired("o1", "OMINOUS-MIRROR", 10)
    await handlers.allocate(event_allocation_required, uow)
    assert uow.committed


@pytest.mark.asyncio
async def test_add_batch_for_new_product():
    uow = FakeUnitOfWork()
    event_batch_created = events.BatchCreated("b1", "CRUNCHY-ARMCHAIR", 100, None)
    await handlers.add_batch(event_batch_created, uow)
    assert uow.products.get("CRUNCHY-ARMCHAIR") is not None
    assert uow.committed


@pytest.mark.asyncio
async def test_add_batch_for_existing_product():
    uow = FakeUnitOfWork()
    event_batch_created_b1 = events.BatchCreated("b1", "GARISH-RUG", 100, None)
    event_batch_created_b2 = events.BatchCreated("b2", "GARISH-RUG", 99, None)
    await handlers.add_batch(event_batch_created_b1, uow)
    await handlers.add_batch(event_batch_created_b2, uow)
    assert "b2" in [b.ref for b in (await uow.products.get("GARISH-RUG")).batches]


@pytest.mark.asyncio
async def test_allocate_errors_for_invalid_sku():
    uow = FakeUnitOfWork()
    event_batch_created = events.BatchCreated("b1", "AREALSKU", 100, None)
    await handlers.add_batch(event_batch_created, uow)

    with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        events_allocation_required = events.AllocationRequired("o1", "NONEXISTENTSKU", 10)
        await handlers.allocate(events_allocation_required, uow)

