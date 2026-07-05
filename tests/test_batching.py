import pytest

from src.common.batching import batch_n


def test_batch_list_input():
    batches = list(batch_n(list(range(10)), 3))
    assert [item for batch in batches for item in batch] == list(range(10))
    assert all(len(b) <= 4 for b in batches)


def test_batch_generator_input():
    # Regression: generators used to be consumed by an internal len(list(...))
    # and the function silently yielded nothing.
    batches = list(batch_n((i for i in range(10)), 3))
    assert [item for batch in batches for item in batch] == list(range(10))


def test_batch_uneven_split():
    batches = list(batch_n(list(range(7)), 3))
    assert sum(len(b) for b in batches) == 7
    assert len(batches) <= 3


def test_batch_fewer_items_than_batches():
    batches = list(batch_n([1, 2], 5))
    assert [item for batch in batches for item in batch] == [1, 2]


def test_batch_empty_input():
    assert list(batch_n([], 3)) == []


def test_batch_invalid_count():
    with pytest.raises(ValueError):
        list(batch_n([1, 2, 3], 0))


if __name__ == "__main__":
    pytest.main([__file__])
