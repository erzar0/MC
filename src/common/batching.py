from itertools import islice
from typing import Generator, Iterable, List, TypeVar

T = TypeVar("T")


def batch_n(iterable: Iterable[T], batch_count: int) -> Generator[List[T], None, None]:
    """Split an iterable into at most `batch_count` batches, as evenly as possible.

    The input is materialized once, so generators are supported. Batches may
    differ in size by up to one element; fewer than `batch_count` batches are
    yielded when the input is too small.

    Args:
        iterable: Any iterable of items (generators are consumed once).
        batch_count: Maximum number of batches to create.

    Yields:
        Lists of items (batches).

    Raises:
        ValueError: If `batch_count` is not positive.
    """
    if batch_count <= 0:
        raise ValueError("Number of batches must be positive")

    items = list(iterable)
    batch_size = len(items) // batch_count + 1

    it = iter(items)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch
