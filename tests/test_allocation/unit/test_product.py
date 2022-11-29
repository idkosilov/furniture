import datetime

import pytest

from allocation.domain import model

today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
later = today + datetime.timedelta(days=3)


def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = model.Batch("in-stock-batch", "RETRO-CLOCK", 100, None)
    shipment_batch = model.Batch("shipment-batch", "RETRO-CLOCK", 100, tomorrow)
    product = model.Product("RETRO-CLOCK", [in_stock_batch, shipment_batch])
    line = model.OrderLine("oref", "RETRO-CLOCK", 10)

    product.allocate(line)

    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    earliest = model.Batch("speedy-batch", "MINIMALIST-SPOON", 100, today)
    medium = model.Batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow)
    latest = model.Batch("slow-batch", "MINIMALIST-SPOON", 100, later)
    product = model.Product("MINIMALIST-SPOON", [earliest, medium, latest])
    line = model.OrderLine("order1", "MINIMALIST-SPOON", 10)

    product.allocate(line)

    assert earliest.available_quantity == 90
    assert medium.available_quantity == 100
    assert latest.available_quantity == 100


def test_returns_allocated_batch_ref():
    in_stock_batch = model.Batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None)
    shipment_batch = model.Batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow)
    line = model.OrderLine("oref", "HIGHBROW-POSTER", 10)
    product = model.Product("HIGHBROW-POSTER", [in_stock_batch, shipment_batch])

    allocation = product.allocate(line)

    assert allocation == in_stock_batch.ref


def test_raises_out_of_stock_exception_if_cannot_allocate():
    batch = model.Batch("batch1", "SMALL-FORK", 10, eta=today)
    product = model.Product(sku="SMALL-FORK", batches=[batch])
    product.allocate(model.OrderLine("order1", "SMALL-FORK", 10))

    with pytest.raises(model.OutOfStock, match="SMALL-FORK"):
        product.allocate(model.OrderLine("order2", "SMALL-FORK", 1))
