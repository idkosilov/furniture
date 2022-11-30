from datetime import date
from typing import Optional

from allocation.domain import model
from allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    pass


async def add_batch(ref: str, sku: str, qty: int, eta: Optional[date], uow: unit_of_work.AbstractUnitOfWork):
    async with uow:
        product = await uow.products.get(sku=sku)
        if product is None:
            product = model.Product(sku, batches=[])
            await uow.products.add(product)
        product.batches.append(model.Batch(ref, sku, qty, eta))
        await uow.commit()


async def allocate(order_ref: str, sku: str, qty: int, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(order_ref, sku, qty)
    async with uow:
        product = await uow.products.get(sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        batch_ref = product.allocate(line)
        await uow.commit()
        return batch_ref
