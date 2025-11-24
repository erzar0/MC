from itertools import islice
from typing import Iterable, TypeVar, Generator, List

T = TypeVar('T')

def batch_n(iterable: Iterable[T], batch_count: int) -> Generator[List[T], None, None]:
    """
    Split an iterable into n batches as evenly as possible.

    Args:
        iterable: Any iterable of items.
        n: Number of batches to create.

    Yields:
        Lists of items (batches).
    """
    if batch_count <= 0:
        raise ValueError("Number of batches must be positive")

    batch_size = len(list(iterable)) // batch_count + 1
    
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch

