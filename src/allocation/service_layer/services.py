from typing import Iterable

from allocation.adapters import repository
from allocation.domain import model


class InvalidSku(Exception):
    pass


def is_valid_sku(sku: str, batches: Iterable[model.Batch]):
    return sku in {b.stock_keeping_unit for b in batches}


async def allocate(line: model.OrderLine, repo: repository.AbstractRepository, session) -> str:
    batches = await repo.list()
    if not is_valid_sku(line.stock_keeping_unit, batches):
        raise InvalidSku(f"Invalid sku {line.stock_keeping_unit}")
    batch_ref = model.allocate(line, batches)
    await session.commit()
    return batch_ref
