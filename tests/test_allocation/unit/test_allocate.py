import datetime

from allocation.domain import model

today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
later = today + datetime.timedelta(days=3)


def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = model.Batch("in-stock-batch", "RETRO-CLOCK", 100, None)
    shipment_batch = model.Batch("shipment-batch", "RETRO-CLOCK", 100, tomorrow)
    line = model.OrderLine("oref", "RETRO-CLOCK", 10)

    model.allocate(line, [in_stock_batch, shipment_batch])

    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    earliest = model.Batch("speedy-batch", "MINIMALIST-SPOON", 100, today)
    medium = model.Batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow)
    latest = model.Batch("slow-batch", "MINIMALIST-SPOON", 100, later)
    line = model.OrderLine("order1", "MINIMALIST-SPOON", 10)

    model.allocate(line, [medium, earliest, latest])

    assert earliest.available_quantity == 90
    assert medium.available_quantity == 100
    assert latest.available_quantity == 100


def test_returns_allocated_batch_ref():
    in_stock_batch = model.Batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None)
    shipment_batch = model.Batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow)
    line = model.OrderLine("oref", "HIGHBROW-POSTER", 10)

    allocation = model.allocate(line, [in_stock_batch, shipment_batch])

    assert allocation == in_stock_batch.ref
