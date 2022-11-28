import datetime
from dataclasses import dataclass
from typing import Optional, Set, List


class OutOfStock(Exception):
    pass


@dataclass(unsafe_hash=True)
class OrderLine:
    order_reference: str
    stock_keeping_unit: str
    quantity: int


class Batch:
    def __init__(self,
                 reference: str,
                 stock_keeping_unit: str,
                 quantity: int,
                 estimated_arrival_time: Optional[datetime.date]) -> None:

        self.reference = reference
        self.stock_keeping_unit = stock_keeping_unit
        self.estimated_arrival_time = estimated_arrival_time

        self._purchased_quantity = quantity
        self._allocations: Set[OrderLine] = set()

    def __repr__(self) -> str:
        return f'<Batch {self.reference}>'

    def __eq__(self, other) -> bool:
        if not isinstance(other, Batch):
            return False
        return other.reference == self.reference

    def __hash__(self) -> int:
        return hash(self.reference)

    def __gt__(self, other) -> bool:
        if self.estimated_arrival_time is None:
            return False
        if other.estimated_arrival_time is None:
            return True
        return self.estimated_arrival_time > other.estimated_arrival_time

    def allocate(self, line: OrderLine) -> None:
        if self.can_allocate(line):
            self._allocations.add(line)

    def deallocate(self, line: OrderLine) -> None:
        if line in self._allocations:
            self._allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.quantity for line in self._allocations)

    @property
    def available_quantity(self) -> int:
        return self._purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.stock_keeping_unit == line.stock_keeping_unit and self.available_quantity >= line.quantity

    @property
    def allocations(self):
        return self._allocations

    @property
    def purchased_quantity(self):
        return self._purchased_quantity


def allocate(line: OrderLine, batches: List[Batch]) -> str:
    try:
        batch = next(batch for batch in sorted(batches) if batch.can_allocate(line))
        batch.allocate(line)
        return batch.reference
    except StopIteration:
        raise OutOfStock(f"Out of stock for sku {line.stock_keeping_unit}")
