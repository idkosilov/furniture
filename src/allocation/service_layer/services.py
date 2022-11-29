from datetime import date
from typing import Iterable, Optional

from allocation.domain import model
from allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    pass


def is_valid_sku(sku: str, batches: Iterable[model.Batch]):
    return sku in {b.sku for b in batches}


async def add_batch(ref: str, sku: str, qty: int, eta: Optional[date], uow: unit_of_work.AbstractUnitOfWork):
    async with uow:
        await uow.batches.add(model.Batch(ref, sku, qty, eta))
        await uow.commit()


async def allocate(order_ref: str, sku: str, qty: int, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(order_ref, sku, qty)
    async with uow:
        batches = await uow.batches.list()
        if not is_valid_sku(sku, batches):
            raise InvalidSku(f"Invalid sku {sku}")
        batch_ref = model.allocate(line, batches)
        await uow.commit()
    return batch_ref



