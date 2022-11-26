from datetime import date
from allocation.domain import model


def test_allocating_to_a_batch_reduces_the_available_quantity():
    batch = model.Batch("batch-001", "SMALL-TABLE", 20, date.today())
    line = model.OrderLine("order-ref", "SMALL-TABLE", 2)

    batch.allocate(line)

    assert batch.available_quantity == 18


def test_can_allocate_if_available_greater_than_required():
    large_batch = model.Batch("batch-001", "SMALL-TABLE", 20, date.today())
    small_line = model.OrderLine("order-ref", "SMALL-TABLE", 2)

    assert large_batch.can_allocate(small_line)


def test_cannot_allocate_if_available_smaller_than_required():
    small_batch = model.Batch("batch-001", "SMALL-TABLE", 2, date.today())
    large_line = model.OrderLine("order-ref", "SMALL-TABLE", 20)

    assert small_batch.can_allocate(large_line) is False


def test_can_allocate_if_available_equal_to_required():
    batch = model.Batch("batch-001", "SMALL-TABLE", 20, date.today())
    line = model.OrderLine("order-ref", "SMALL-TABLE", 20)

    assert batch.can_allocate(line)


def test_cannot_allocate_if_skus_do_not_match():
    batch = model.Batch("batch-001", "UNCOMFORTABLE-CHAIR", 100, None)
    different_sku_line = model.OrderLine("order-123", "EXPENSIVE-TOASTER", 10)

    assert batch.can_allocate(different_sku_line) is False


def test_allocation_is_idempotent():
    batch = model.Batch("batch-001", "SMALL-TABLE", 20, date.today())
    line = model.OrderLine("order-ref", "SMALL-TABLE", 2)

    batch.allocate(line)
    batch.allocate(line)

    assert batch.available_quantity == 18