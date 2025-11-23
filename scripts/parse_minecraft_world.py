import amulet
import amulet_nbt
from amulet.api.errors import ChunkDoesNotExist, ChunkLoadError

level = amulet.load_level("data/RUNETALE Converged Realms")

try:
    chunk = level.get_chunk(0, 0, "minecraft:overworld")
except ChunkDoesNotExist:
    print("Chunk does not exist")
except ChunkLoadError:
    print("Chunk load error")
else:
    print(chunk)


level.save()

level.close()