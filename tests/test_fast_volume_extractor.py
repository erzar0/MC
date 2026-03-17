
import pytest
import numpy as np
from pathlib import Path
from src.world_wrapper import WorldWrapper, BlockStates
from src.fast_volume_extractor import FastVolumeParser

@pytest.fixture
def test_world_path():
    return Path(__file__).parent / "resources" / "test_world"

@pytest.fixture
def test_mca_path():
    return Path(__file__).parent / "resources" / "test_world" / "region" / "r.0.0.mca"

def test_fast_volume_parser(test_mca_path):
    blockstates = BlockStates()
    parser = FastVolumeParser(test_mca_path, blockstates)
    volume = parser.extract_volume(min_y=-64, height=384)
    # New expectation: 6D array (rx, rz, sec, x, y, z)
    assert len(volume.shape) == 6
    assert volume.shape[0] == 32
    assert volume.shape[1] == 32
    assert volume.size > 0
    parser.close()

def test_extractors_comparison(test_world_path, test_mca_path):
    # 1. WorldWrapper setup
    wrapper = WorldWrapper(test_world_path)
    
    # 2. Extract using slow path (Amulet)
    ww_volume_slow = wrapper.get_region_volume(0, 0, use_fast_extractor=False)
    
    # 3. Extract using fast path (Custom Parser)
    ww_volume_fast = wrapper.get_region_volume(0, 0, use_fast_extractor=True)
    
    # 4. Compare shapes and content
    min_h = min(ww_volume_slow.shape[2], ww_volume_fast.shape[2])
    ww_slice_slow = ww_volume_slow[:, :, :min_h]
    ww_slice_fast = ww_volume_fast[:, :, :min_h]
    
    diff = ww_slice_slow != ww_slice_fast
    diff_count = np.sum(diff)
    total_elements = ww_slice_slow.size
    mismatch_percentage = (diff_count / total_elements) * 100
    
    print(f"Mismatch (Slow vs Fast Method): {mismatch_percentage:.6f}% ({diff_count}/{total_elements})")
    
    # Requirement: less than 0.001%
    assert mismatch_percentage < 0.001, f"Mismatch too high: {mismatch_percentage:.6f}%"

if __name__ == "__main__":
    pytest.main([__file__])
