import datetime
from dataclasses import dataclass
from typing import Optional, Set, List


class OutOfStock(Exception):
    pass


@dataclass(unsafe_hash=True)
class OrderLine:
    order_ref: str
    sku: str
    qty: int


class Batch:
    def __init__(self, batch_ref: str, sku: str, qty: int, eta: Optional[datetime.date]) -> None:
        self.ref = batch_ref
        self.sku = sku
        self.eta = eta

        self._purchased_quantity = qty
        self._allocations: Set[OrderLine] = set()

    def __repr__(self) -> str:
        return f'<Batch {self.ref}>'

    def __eq__(self, other) -> bool:
        if not isinstance(other, Batch):
            return False
        return other.ref == self.ref

    def __hash__(self) -> int:
        return hash(self.ref)

    def __gt__(self, other) -> bool:
        if self.eta is None:
            return False
        if other.eta is None:
            return True
        return self.eta > other.eta

    def allocate(self, line: OrderLine) -> None:
        if self.can_allocate(line):
            self._allocations.add(line)

    def deallocate(self, line: OrderLine) -> None:
        if line in self._allocations:
            self._allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.qty for line in self._allocations)

    @property
    def available_quantity(self) -> int:
        return self._purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.sku == line.sku and self.available_quantity >= line.qty

    @property
    def allocations(self) -> Set[OrderLine]:
        return self._allocations

    @property
    def purchased_quantity(self) -> int:
        return self._purchased_quantity


class Product:

    def __init__(self, sku: str, batches: List[Batch]) -> None:
        self.sku = sku
        self.batches = batches

    def allocate(self, line: OrderLine) -> str:
        try:
            batch = next(batch for batch in sorted(self.batches) if batch.can_allocate(line))
            batch.allocate(line)
            return batch.ref
        except StopIteration:
            raise OutOfStock(f"Out of stock for sku {line.sku}")
