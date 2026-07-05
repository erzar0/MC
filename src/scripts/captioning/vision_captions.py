import argparse
import asyncio
import base64
import csv
import io
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from PIL import Image
from tqdm.asyncio import tqdm

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from openai import AsyncOpenAI

from src import config
from src.common.llm_utils import make_async_vllm_client, strip_thinking_tags

try:
    # Registers the JXL codec with PIL as an import side effect
    import pillow_jxl  # noqa: F401
except ImportError:
    pass

CLEANSED_DIR = config.PROCESSED_WORLDS_DIR / "cleansed"
CSV_PATH = config.PIPELINE_DATA_DIR / "pmc_descriptions_cleaned.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
System Role: You are an expert computer vision data annotator generating natural, descriptive captions for text-to-image diffusion model training.

Task: You will be provided with an image of a Minecraft terrain region. Alongside the image, you receive a metadata block containing its id, title, tags, and description. Your job is to synthesize the metadata context with the visual evidence to output a single, comprehensive description of the terrain, structures, and biomes shown.

CRITICAL INSTRUCTION: There is a pink grid overlay with large colored letters and numbers (A1, B2, etc.) baked into the image. YOU MUST COMPLETELY IGNORE THIS OVERLAY. Treat it as invisible.
- NEVER use the word "grid", "overlay", "sections", "alphanumeric", "labels", or "letters".
- NEVER describe the grid lines or the glowing letters as physical objects (e.g. do not call them "glowing barriers", "neon signs", "colored sections").
- If you mention the grid or the letters in your description, you fail the task. Describe ONLY the underlying Minecraft terrain, nature, and buildings.

Step 1: Contextualize via Metadata
Read the provided Title, Tags, and Description for context. CRITICAL: This metadata describes the ENTIRE Minecraft world, NOT necessarily the specific chunk shown in the image.
- You MUST prioritize visual evidence.
- If the metadata mentions cities, skyscrapers, or underground stations, but you only see flat grass or empty terrain, DO NOT INVENT THE CITY. Describe ONLY what is visible in the image.
- Never hallucinate functional or lore details.

Step 2: Visual Extraction Rules
Provide a highly accurate description of what is ACTUALLY visible in the region. Write in MULTIPLE, separate sentences ending in periods. Do NOT use semicolons.

Anti-Hallucination Constraints:
1. The image background outside the terrain is a dark void. DO NOT MISTAKE THE VOID FOR "DARK ASPHALT", "WATER", OR "NIGHT SKY". Ignore the void completely.
2. Do not explicitly list geological layers (like "dirt, stone, and ores") just because they are visible on the cross-section edges.
3. Do NOT mention that the image is an "isometric", "3D", or "cross-section" view. Describe the scene natively.

Empty Regions: If the image is 100% transparent and contains absolutely NO Minecraft terrain (no blocks, no grass, no water, no stone), you MUST output exactly the word "Empty". NOTE: If the image contains even a flat square of water or grass blocks, it is NOT empty.

Features & Structures: Be exhaustive and visually precise about what is actually present. If the image is just a flat, featureless expanse, describe its actual color and texture simply (e.g., "A flat expanse of dark stone"). Do NOT invent buildings, vegetation, or features just to make the description longer.
- If structures are present, detail their architectural styles, exact visible colors, specific materials, and spatial arrangement ("top-left", "foreground"). Do NOT use cardinal directions (North, South, East, West).

Underground: If underground areas are visible, describe them naturally. If not visible, do not mention them.

Detail Level: The description should be rich and accurate ONLY for features that actually exist in the image. If the chunk is featureless, a concise description of the terrain surface is perfectly fine.

Background: YOU MUST NOT describe or mention the background, the void, or the negative space at all in your final output. Do NOT mention the lack of a sky or clouds.

Strict Output Format:
Do not include any conversational filler. Output exactly in this single-line format:
[Region]: [Holistic description or "Empty"]
"""


def encode_image_base64(image_path: Path, save_debug: bool = False) -> str:
    with Image.open(image_path) as img:
        img = img.convert("RGBA")
        img.thumbnail((int(img.width * 0.8), int(img.height * 0.8)))

        if save_debug:
            debug_path = image_path.parent.parent / f"debug_{image_path.stem}.png"
            img.save(debug_path, format="PNG")

        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        return base64.b64encode(buffer.getvalue()).decode("utf-8")


def load_metadata() -> dict:
    metadata = {}
    if not CSV_PATH.exists():
        return metadata

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            metadata[row["id"]] = {
                "title": row.get("title", ""),
                "tags": row.get("tags", ""),
                "description": row.get("description", ""),
            }
    return metadata


async def process_single_image_async(
    client: AsyncOpenAI, image_path: Path, meta: dict, model_name: str, sem: asyncio.Semaphore, delay: float
) -> bool:
    captions_dir = image_path.parent.parent / "captions"
    captions_dir.mkdir(parents=True, exist_ok=True)
    out_path = captions_dir / image_path.with_suffix(".txt").name

    if out_path.exists():
        return True

    async with sem:
        try:
            b64_image = await asyncio.to_thread(encode_image_base64, image_path, False)
        except Exception as e:
            logger.error(f"Failed to process image {image_path}: {e}")
            return False

        title = meta.get("title", "Unknown")
        tags = meta.get("tags", "None")
        desc = meta.get("description", "No physical description provided.")
        context_text = f"Title: {title}\nTags: {tags}\nDescription: {desc}"

        if delay > 0:
            await asyncio.sleep(delay)

        try:
            # This calls the LOCAL vLLM server, NOT an external service.
            response = await client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"Here is the metadata for this terrain region:\n{context_text}\n\nPlease generate the description for the entire region. Remember to output exactly one line starting with [Region]: ",
                            },
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_image}"}},
                        ],
                    },
                ],
                temperature=0.7,
            )
            caption = strip_thinking_tags(response.choices[0].message.content.strip())
            caption = caption.replace("\n", " ")

            if not caption.startswith("[Region]"):
                if caption.startswith(":"):
                    caption = caption[1:].strip()
                caption = f"[Region]: {caption}"

        except Exception as e:
            logger.error(f"Error calling local vLLM for {image_path}: {e}")
            return False

        try:

            def save_file():
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(caption)

            await asyncio.to_thread(save_file)
            logger.info(f"Successfully saved caption to file://{out_path.resolve()}")
            return True
        except Exception as e:
            logger.error(f"Error writing captions to {out_path}: {e}")
            return False


async def main_async():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Generate spatial captions for JXL screenshots using AsyncOpenAI directly to a local vLLM server"
    )
    parser.add_argument("--concurrency", type=int, default=128, help="Concurrent API requests")
    parser.add_argument("--max-images", type=int, default=0, help="Max images to process (0 = all)")
    parser.add_argument("--model", type=str, default="Qwen/Qwen3-VL-8B-Instruct-FP8", help="vLLM model name")
    parser.add_argument("--base-url", type=str, default="http://localhost:8000/v1", help="vLLM server base URL")
    parser.add_argument(
        "--delay", type=float, default=0.0, help="Delay in seconds between API requests to avoid rate limits"
    )
    args = parser.parse_args()

    logger.info("Loading metadata...")
    metadata = load_metadata()
    logger.info(f"Loaded metadata for {len(metadata)} worlds.")

    jxl_files = []
    if CLEANSED_DIR.exists():
        for world_dir in CLEANSED_DIR.iterdir():
            if world_dir.is_dir():
                world_id = world_dir.name
                screenshots_dir = world_dir / "screenshots"
                if screenshots_dir.exists():
                    for f_path in screenshots_dir.glob("*.jxl"):
                        jxl_files.append((f_path, world_id))

    if not jxl_files:
        logger.warning(f"No .jxl files found in {CLEANSED_DIR}/**/screenshots")
        return

    logger.info(f"Found {len(jxl_files)} images total.")

    tasks_to_run = []
    for f_path, wid in jxl_files:
        captions_dir = f_path.parent.parent / "captions"
        out_path = captions_dir / f_path.with_suffix(".txt").name
        if not out_path.exists():
            meta = metadata.get(wid, {})
            tasks_to_run.append((f_path, meta))

    logger.info(f"Need to generate captions for {len(tasks_to_run)} remaining images.")
    if args.max_images > 0:
        tasks_to_run = tasks_to_run[: args.max_images]
        logger.info(f"Limiting to {args.max_images} tasks.")

    if not tasks_to_run:
        logger.info("Done!")
        return

    client = make_async_vllm_client(args.base_url)
    sem = asyncio.Semaphore(args.concurrency)

    coroutines = [
        process_single_image_async(client, f_path, meta, args.model, sem, args.delay) for f_path, meta in tasks_to_run
    ]

    success_count = 0
    for future in tqdm.as_completed(coroutines, total=len(coroutines), desc="Images"):
        result = await future
        if result:
            success_count += 1

    logger.info(f"Successfully generated captions for {success_count}/{len(tasks_to_run)} images.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
