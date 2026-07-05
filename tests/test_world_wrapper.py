from pathlib import Path

import pytest

from src.world_wrapper import WorldWrapper


@pytest.fixture
def test_world_path():
    return Path(__file__).parent / "resources" / "test_world"


def test_world_wrapper(test_world_path):
    wrapper = WorldWrapper(test_world_path)
    volume = wrapper.get_region_volume(0, 0)
    assert volume.shape[0] == 512
    assert volume.shape[1] == 512
    assert volume.size > 0


if __name__ == "__main__":
    pytest.main([__file__])
