import gzip
import math
import zlib
from pathlib import Path
from typing import Any, Optional

import amulet.api.block
import amulet_nbt
import numpy as np

try:
    from numba import njit, uint16, uint64

    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False

if HAS_NUMBA:

    @njit
    def _unpack_numba(data_u64, bits_per_block, blocks_per_long):
        unpacked = np.empty(4096, dtype=np.uint16)
        mask = np.uint64((1 << bits_per_block) - 1)
        upto = 4096
        for i in range(data_u64.shape[0]):
            val = data_u64[i]
            for j in range(blocks_per_long):
                idx = i * blocks_per_long + j
                if idx < upto:
                    unpacked[idx] = (val >> (j * bits_per_block)) & mask
                else:
                    return unpacked
        return unpacked


SECTOR_BYTES = 4096
CHUNK_HEADER_SIZE = 5

try:
    from .world_wrapper import BlockStates
except ImportError:
    # Handle direct script execution where '.' is not available
    try:
        from world_wrapper import BlockStates
    except ImportError:
        # If it's passed as an argument, we just need the type hint to work
        BlockStates = Any


class FastVolumeParser:
    def __init__(self, region_path: Path, blockstates: BlockStates, translator=None):
        self.region_path = region_path
        self._blockstates = blockstates
        self._block_cache = {}
        self._f = open(self.region_path, "rb")
        self._header = self._f.read(4096)

        if translator is None:
            import PyMCTranslate

            self._translator = PyMCTranslate.new_translation_manager()
            self._version = self._translator.get_version("java", (1, 19, 2))
        else:
            self._translator = translator
            self._version = self._translator.get_version("java", (1, 19, 2))

    def close(self):
        if self._f:
            self._f.close()
            self._f = None

    def get_chunk_nbt(self, cx: int, cz: int) -> Optional[amulet_nbt.NamedTag]:
        idx = (cx % 32) + (cz % 32) * 32
        offset_data = self._header[idx * 4 : idx * 4 + 4]
        if not offset_data:
            return None
        offset = int.from_bytes(offset_data[:3], byteorder="big")
        sector_count = offset_data[3]
        if offset == 0 or sector_count == 0:
            return None
        self._f.seek(offset * SECTOR_BYTES)
        header = self._f.read(CHUNK_HEADER_SIZE)
        length = int.from_bytes(header[:4], byteorder="big")
        compression_type = header[4]
        compressed_data = self._f.read(length - 1)
        if compression_type == 1:
            data = gzip.decompress(compressed_data)
        elif compression_type == 2:
            data = zlib.decompress(compressed_data)
        elif compression_type == 3:
            data = compressed_data
        else:
            return None
        return amulet_nbt.load(data)

    def unpack_block_states(self, compressed_data: np.ndarray, palette_size: int) -> np.ndarray:
        if palette_size <= 1:
            return np.zeros((16, 16, 16), dtype=np.uint16)

        bits_per_block = max(4, math.ceil(math.log2(palette_size)))
        blocks_per_long = 64 // bits_per_block
        data_u64 = compressed_data.view(np.uint64)

        if HAS_NUMBA:
            unpacked_1d = _unpack_numba(data_u64, bits_per_block, blocks_per_long)
        else:
            shifts = np.arange(blocks_per_long, dtype=np.uint64) * bits_per_block
            mask = np.uint64((1 << bits_per_block) - 1)
            unpacked_2d = (data_u64[:, None] >> shifts) & mask
            unpacked_1d = unpacked_2d.flatten()[:4096].astype(np.uint16)

        return unpacked_1d.reshape((16, 16, 16)).transpose(2, 0, 1)

    def parse_block_string(self, block_dict) -> str:
        name_str = block_dict["Name"].py_str
        if ":" in name_str:
            namespace, base_name = name_str.split(":", 1)
        else:
            namespace, base_name = "minecraft", name_str

        properties = {}
        if "Properties" in block_dict:
            for k, v in block_dict["Properties"].items():
                if hasattr(v, "py_str"):
                    properties[k] = amulet_nbt.StringTag(v.py_str)
                elif hasattr(v, "py_data"):
                    properties[k] = amulet_nbt.StringTag(str(v.py_data))
                else:
                    properties[k] = amulet_nbt.StringTag(str(v))

        amulet_block = amulet.api.block.Block(namespace, base_name, properties)
        universal_b, extra_b, _ = self._version.block.to_universal(amulet_block)

        def _get_clean_str(b):
            if b is None:
                return None
            return str(b)

        base_str = _get_clean_str(universal_b)
        extra_str = _get_clean_str(extra_b)

        # Strip BlockEntity data if it exists in extra_str to match WorldWrapper palette
        if extra_str and "BlockEntity" in extra_str:
            extra_str = None

        if extra_str:
            return f"{base_str}{{{extra_str}}}"
        return base_str

    def extract_volume(self, min_y=-64, height=384) -> np.ndarray:
        # Standard Amulet uses (height // 16 + 1) to ensure full coverage
        max_sections = height // 16 + 1
        volume = np.zeros((32, 32, max_sections, 16, 16, 16), dtype=np.uint16)

        marker_id = self._blockstates.get_global_id_by_block('universal_minecraft:sponge[wet="false"]')
        air_id = self._blockstates.get_global_id_by_block("universal_minecraft:air")

        # Consistent with Amulet: if min_y is -64, section -4 maps to index 0
        y_offset_sections = -(min_y // 16)

        for cz in range(32):
            for cx in range(32):
                try:
                    nbt = self.get_chunk_nbt(cx, cz)
                    if nbt is None:
                        continue

                    root = nbt.compound
                    if "sections" in root:
                        sections = root["sections"]
                    elif "Level" in root and "Sections" in root["Level"]:
                        sections = root["Level"]["Sections"]
                    else:
                        continue

                    for section in sections:
                        y = int(section["Y"])
                        sec_idx = y + y_offset_sections
                        if sec_idx < 0 or sec_idx >= max_sections:
                            continue

                        if "block_states" in section:
                            block_states = section["block_states"]
                            if "palette" in block_states:
                                palette_list = block_states["palette"]
                                palette_size = len(palette_list)

                                global_palette = np.zeros(palette_size, dtype=np.uint16)
                                for i, block_dict in enumerate(palette_list):
                                    b_name = block_dict["Name"].py_str
                                    if "minecraft:numerical" in b_name:
                                        global_id = marker_id
                                    else:
                                        # Build a unique key from the NBT dict to avoid slow parse_block_string/to_universal calls
                                        props = block_dict.get("Properties", {})
                                        if props:
                                            # Create a stable string representation for the properties
                                            prop_items = []
                                            for k, v in sorted(props.items()):
                                                v_str = (
                                                    v.py_str
                                                    if hasattr(v, "py_str")
                                                    else str(v.py_data if hasattr(v, "py_data") else v)
                                                )
                                                prop_items.append(f"{k}={v_str}")
                                            cache_key = f"{b_name}[{','.join(prop_items)}]"
                                        else:
                                            cache_key = b_name

                                        if cache_key in self._block_cache:
                                            global_id = self._block_cache[cache_key]
                                        else:
                                            block_str = self.parse_block_string(block_dict)
                                            global_id = self._blockstates.get_global_id_by_block(block_str)
                                            self._block_cache[cache_key] = global_id

                                    global_palette[i] = global_id

                                if "data" in block_states:
                                    data_tag = block_states["data"]
                                    if hasattr(data_tag, "py_ndarray"):
                                        data_array = data_tag.py_ndarray
                                    elif hasattr(data_tag, "py_tuple"):
                                        data_array = np.array(data_tag.py_tuple, dtype=np.int64)
                                    else:
                                        # Fallback for older tags that might only have value or other accessors
                                        data_array = np.array(
                                            data_tag.py_data if hasattr(data_tag, "py_data") else data_tag.value,
                                            dtype=np.int64,
                                        )

                                    unpacked = self.unpack_block_states(data_array, palette_size)
                                    volume[cx, cz, sec_idx] = global_palette[unpacked]
                                else:
                                    # Single-block palette without data means the whole section is that block
                                    volume[cx, cz, sec_idx] = global_palette[0]
                except Exception as e:
                    print(f"FastVolumeParser Error @ {cx},{cz}: {e}")
                    continue

        return volume
