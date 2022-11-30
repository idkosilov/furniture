from typing import Any

from allocation.domain import events


async def handle(event: events.Event) -> Any:
    for handler in HANDLERS[type(event)]:
        await handler(event)


async def send_out_of_stock_notification(event: events.OutOfStock) -> None:
    await email.send_mail('stock@furniture.com', f'SKU is out of stock {event.sku}')


HANDLERS = {
    events.OutOfStock: [send_out_of_stock_notification, ]
}