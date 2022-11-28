from typing import Optional, Iterable, List

import pytest

from allocation.adapters import repository
from allocation.domain import model
from allocation.service_layer import services


class FakeRepository(repository.AbstractRepository):

    def __init__(self, batches: Iterable[model.Batch]):
        self._batches = set(batches)

    async def get(self, reference: str) -> Optional[model.Batch]:
        return next(b for b in self._batches if b.reference == reference)

    async def add(self, batch: model.Batch) -> None:
        self._batches.add(batch)

    async def list(self) -> List[model.Batch]:
        return list(self._batches)


class FakeSession:
    committed = False

    async def commit(self):
        self.committed = True


@pytest.mark.asyncio
async def test_commits():
    line = model.OrderLine('o1', 'OMINOUS-MIRROR', 10)
    batch = model.Batch('b1', 'OMINOUS-MIRROR', 100, None)
    repo = FakeRepository([batch])
    session = FakeSession()
    await services.allocate(line, repo, session)
    assert session.committed is True


@pytest.mark.asyncio
async def test_returns_allocation():
    line = model.OrderLine("o1", "COMPLICATED-LAMP", 10)
    batch = model.Batch("b1", "COMPLICATED-LAMP", 100, None)
    repo = FakeRepository([batch])

    result = await services.allocate(line, repo, FakeSession())
    assert result == "b1"


@pytest.mark.asyncio
async def test_error_for_invalid_sku():
    line = model.OrderLine("o1", "NONEXISTENTSKU", 10)
    batch = model.Batch("b1", "AREALSKU", 100, None)
    repo = FakeRepository([batch])

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        await services.allocate(line, repo, FakeSession())
