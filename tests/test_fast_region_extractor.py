
import pytest
import numpy as np
from pathlib import Path
from src.world_wrapper import WorldWrapper, BlockStates
from src.fast_region_extractor import FastRegionParser

@pytest.fixture
def test_world_path():
    return Path(__file__).parent / "resources" / "test_world"

@pytest.fixture
def test_mca_path():
    return Path(__file__).parent / "resources" / "test_world" / "region" / "r.0.0.mca"

def test_fast_region_parser(test_mca_path):
    blockstates = BlockStates()
    parser = FastRegionParser(test_mca_path, blockstates)
    volume = parser.extract_volume(min_y=-64, height=384)
    assert volume.shape[0] == 512
    assert volume.shape[1] == 512
    assert volume.size > 0
    parser.close()

def test_extractors_comparison(test_world_path, test_mca_path):
    # 1. WorldWrapper extraction
    wrapper = WorldWrapper(test_world_path)
    ww_volume, _ = wrapper.get_region_volume(0, 0)
    
    # 2. FastRegionParser extraction
    blockstates = BlockStates()
    parser = FastRegionParser(test_mca_path, blockstates)
    fr_volume = parser.extract_volume(min_y=-64, height=384)
    parser.close()
    
    # 3. Compare shapes and content
    min_h = min(ww_volume.shape[2], fr_volume.shape[2])
    ww_slice = ww_volume[:, :, :min_h]
    fr_slice = fr_volume[:, :, :min_h]
    
    diff = ww_slice != fr_slice
    diff_count = np.sum(diff)
    total_elements = ww_slice.size
    mismatch_percentage = (diff_count / total_elements) * 100
    
    print(f"Mismatch: {mismatch_percentage:.6f}% ({diff_count}/{total_elements})")
    
    # Requirement: less than 0.001%
    assert mismatch_percentage < 0.001, f"Mismatch too high: {mismatch_percentage:.6f}%"

if __name__ == "__main__":
    pytest.main([__file__])
