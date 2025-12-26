import amulet
from amulet.api.errors import ChunkDoesNotExist, ChunkLoadError

level = amulet.load_level("data/20250401_April_World_by_McMeddon/20250401 April World 1.2")

try:
    chunk = level.get_chunk(0, 0, "minecraft:overworld")
except ChunkDoesNotExist:
    print("Chunk does not exist")
except ChunkLoadError:
    print("Chunk load error")
else:
    print(chunk)

print(chunk.misc["inhabited_time"])
level.save()

level.close()