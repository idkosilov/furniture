from datetime import date
from typing import Optional

from allocation.adapters import email
from allocation.domain import model, events
from allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    pass


async def add_batch(event: events.BatchCreated, uow: unit_of_work.AbstractUnitOfWork):
    async with uow:
        product = await uow.products.get(sku=event.sku)
        if product is None:
            product = model.Product(event.sku, batches=[])
            await uow.products.add(product)
        product.batches.append(model.Batch(event.ref, event.sku, event.qty, event.eta))
        await uow.commit()


async def allocate(event: events.AllocationRequired, uow: unit_of_work.AbstractUnitOfWork) -> str:
    line = model.OrderLine(event.order_ref, event.sku, event.qty)
    async with uow:
        product = await uow.products.get(event.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        batch_ref = product.allocate(line)
        await uow.commit()
        return batch_ref


async def send_out_of_stock_notification(event: events.OutOfStock, uow: unit_of_work.AbstractUnitOfWork):
    await email.send('stock@made.com', f'Артикула {event.sku} нет в наличии',)
