"""
Parses pmc_data_cleansed.csv and uses a local vLLM server (via OpenAI-compatible API)
to generate cleaned titles, tags, and descriptions for each Minecraft map entry.
Each field is processed by a separate, focused LLM call for maximum accuracy.
Outputs a new CSV with columns: id, title, tags, description.

Prerequisites:
    Start vLLM server first:
        vllm serve Qwen/Qwen3-VL-8B-Instruct-FP8 --max-model-len 4096 --gpu-memory-utilization 0.9

Usage:
    python -m src.scripts.cleanse_descriptions [--batch-size 64] [--max-rows 0] [--resume]
"""

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from openai import OpenAI
from tqdm import tqdm

# Support both package import and direct script execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src import config
from src.common.llm_utils import make_vllm_client, strip_thinking_tags

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INPUT_CSV = config.PIPELINE_DATA_DIR / "pmc_data_cleansed.csv"
OUTPUT_CSV = config.PIPELINE_DATA_DIR / "pmc_descriptions_cleaned.csv"

# ---------------------------------------------------------------------------
# Separate system prompts for each field
# ---------------------------------------------------------------------------

TITLE_SYSTEM_PROMPT = """\
You clean Minecraft map titles. Given a raw title, return ONLY the core name of the map.

RULES:
- Strip ALL version numbers (e.g., "v5.2.0", "v1.6").
- Strip ALL bracketed tags (e.g., "[WIP]", "[DOWNLOAD]", "[FREE DOWNLOAD]", "[HARD]").
- Strip ALL generic hype words (e.g., "EPIC", "BEST", "AMAZING").
- Strip ALL platform/edition markers (e.g., "/Modded").
- Strip ALL author credits or suffixes (e.g., "- by Jeracraft", "created by [Name]", "- [Author]").
- Keep the actual name of the map intact.
- Keep subtitles that are part of the identity (e.g., "The immortal lands" in "Aman, The immortal lands").

RESPOND WITH ONLY the cleaned title text. No JSON, no quotes, no explanation."""

TAGS_SYSTEM_PROMPT = """\
You clean Minecraft map tag lists. Given a raw pipe-separated tag list, return a cleaned version.

### STEP 1: SPLIT & NORMALIZE COMPOUND TAGS
Many tags are concatenated words with no spaces. Detect and split them into proper, readable multi-word phrases (e.g., "Survivalrpg" → "Survival RPG", "Ruinedcity" → "Ruined City", "Postapocalyptic" → "Post-Apocalyptic"). Apply this to ALL compound tags.

### STEP 2: FIX CASING & MISSPELLINGS
- Capitalize each word properly. Common abbreviations should be uppercase (e.g., "rpg" → "RPG", "pvp" → "PvP").
- Fix obvious misspellings (e.g., "Superhros" → "Superheroes", "Medival" → "Medieval").

### STEP 3: DEDUPLICATE
Remove near-duplicates, keeping the most descriptive variant.

### STEP 4: REMOVE NOISE (ONLY these specific categories)
- Creator, author, YouTuber, or team names (e.g., "Jeracraft", "Paulzero", "Craftyfoxe", "Theerikcz", "Criand"). If a tag looks like an internet username, DELETE IT.
- Generic spam words that carry zero descriptive value ("Download", "Best", "Cool", "Fun", "New", "Shot")
- Platform names ("Minecraft", "Planetminecraft")
- Software/tool names that refer to how the map was BUILT, not what it IS ("Worldpainter", "Schematics", "MCEdit", "Geyser")
- Tags that are purely structural labels with no theme ("Map", "Build", "Custom", "Pack", "Block", "Set", "Project")
- Version-related tags

### STEP 5: KEEP everything else, including:
- Biomes, terrain, geography
- Structure types and location names
- Themes, aesthetics, franchises, and subject matter (e.g., "Flash", "Star Labs", "Harry Potter", "Fallout")
- Character names and references relevant to the map's theme
- Gameplay genres and features
- Multi-word descriptive labels (e.g., "Land Structure" — keep it, it describes a build type)

### WHEN IN DOUBT: KEEP THE TAG. Only remove if you are certain it carries zero descriptive value about the world.

CRITICAL ANTI-HALLUCINATION RULE: DO NOT inject the examples from this prompt. You must ONLY output tags that actually exist in the user's raw input list (after splitting/fixing).

RESPOND WITH ONLY the cleaned tags separated by " | ". No JSON, no quotes, no explanation. If no relevant tags remain, respond with "Uncategorized"."""

DESCRIPTION_SYSTEM_PROMPT = """\
You extract clean world descriptions from raw Minecraft map descriptions. Your ONLY job is to output what the world physically is, looks like, and what the player does in it.

### CRITICAL TWO-STEP PROCESS:
1. MENTAL DELETION: Before writing anything, mentally delete ALL sentences that contain:
   - Installation instructions or technical requirements (Forge, Fabric, Optifine, Bedrock, version numbers).
   - In-game video settings or setup (e.g., render distance, brightness, command block enablement).
   - Software/Tool names (WorldPainter, Geyser, MCEdit).
   - Server rules, usage rights, or copyright claims (e.g., "free for personal use", "give credit").
   - Disclaimers about bugs, WIP status, or singleplayer/multiplayer compatibility.
   - Credits, puzzle hints/walkthroughs, file sizes, coordinates, URLs, update logs, patch notes, social media links, greetings/sign-offs, or emojis/decorative text.
   - FATAL ERROR PREVENTION: You are STRICTLY FORBIDDEN from including any mention of WorldEdit, required credits, Bukkit/Spigot, or creator inspirations (e.g., "inspired by [Name]"). Delete those sentences entirely.
2. EXTRACTION: Write the final description using ONLY whatever sentences survive the deletion. If nothing survives, output exactly: "No physical description provided."

### WHAT TO INCLUDE:
- Physical geography, landscape, and environment (mountains, biomes, terrain)
- Structures, architecture, and notable locations (cities, castles, rooms, ruins)
- In-game objective or genre ONLY if it defines the overarching world (e.g., survival adventure, post-apocalyptic wasteland)
- Theme, lore, names of objects or atmosphere

### ANTI-HALLUCINATION:
- YOU MUST NOT invent, guess, or infer any details not explicitly in the source text.
- If the valid text is brief, your output must be brief. Do not pad with filler.
- DO NOT include introductory phrases like "This map is..." or "This is a..."
- Multilingual: extract ONLY the English portion.

RESPOND WITH ONLY the cleaned description text. No JSON, no quotes, no explanation."""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Individual field processors
# ---------------------------------------------------------------------------
def clean_title(client: OpenAI, raw_title: str, model_name: str) -> str:
    """Clean a single title via LLM."""
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": TITLE_SYSTEM_PROMPT},
            {"role": "user", "content": raw_title},
        ],
    )
    return strip_thinking_tags(response.choices[0].message.content.strip())


def clean_tags(client: OpenAI, raw_tags: str, model_name: str) -> str:
    """Clean a single tag list via LLM."""
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": TAGS_SYSTEM_PROMPT},
            {"role": "user", "content": raw_tags},
        ],
    )
    return strip_thinking_tags(response.choices[0].message.content.strip())


def clean_description(client: OpenAI, raw_description: str, model_name: str) -> str:
    """Clean a single description via LLM."""
    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": DESCRIPTION_SYSTEM_PROMPT},
            {"role": "user", "content": raw_description},
        ],
    )
    return strip_thinking_tags(response.choices[0].message.content.strip())


# ---------------------------------------------------------------------------
# Row processing
# ---------------------------------------------------------------------------
def process_single_row(client: OpenAI, row: dict, model_name: str) -> dict:
    """Process a single row with 3 separate LLM calls (title, tags, description)."""
    try:
        cleaned_title = clean_title(client, row["title"], model_name)
    except Exception as e:
        print(f"  Error cleaning title for row {row['id']}: {e}")
        cleaned_title = row["title"]

    try:
        cleaned_tags = clean_tags(client, row["tags"], model_name)
    except Exception as e:
        print(f"  Error cleaning tags for row {row['id']}: {e}")
        cleaned_tags = row["tags"]

    try:
        cleaned_desc = clean_description(client, row["description"], model_name)
    except Exception as e:
        print(f"  Error cleaning description for row {row['id']}: {e}")
        cleaned_desc = f"ERROR: {e}"

    return {
        "id": row["id"],
        "title": cleaned_title,
        "tags": cleaned_tags,
        "description": cleaned_desc,
    }


def process_batch(
    client: OpenAI,
    rows: list[dict],
    model_name: str,
    concurrency: int = 16,
) -> list[dict]:
    """
    Process a batch of rows concurrently using ThreadPoolExecutor.
    Each row fires 3 sequential LLM calls (title, tags, description),
    but rows themselves are processed in parallel.
    """
    results = [None] * len(rows)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_idx = {executor.submit(process_single_row, client, row, model_name): i for i, row in enumerate(rows)}

        with tqdm(total=len(rows), desc="  Rows", leave=False) as pbar:
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()
                pbar.update(1)

    return results


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------
def load_input_csv(path: Path, max_rows: int = 0) -> list[dict]:
    """Load the input CSV, extracting only the columns we need."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break
            rows.append(
                {
                    "id": row["id"],
                    "title": row.get("title", ""),
                    "tags": row.get("tags", ""),
                    "description": row.get("description", ""),
                }
            )
    return rows


def load_existing_ids(path: Path) -> set:
    """Load already-processed IDs from the output CSV for resume support."""
    ids = set()
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ids.add(row["id"])
    return ids


def write_results(path: Path, results: list[dict], append: bool = False):
    """Write or append cleaned results to the output CSV."""
    mode = "a" if append else "w"
    write_header = not append or not path.exists()

    with open(path, mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "tags", "description"])
        if write_header:
            writer.writeheader()
        writer.writerows(results)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Cleanse PMC map descriptions using local vLLM server")
    parser.add_argument("--batch-size", type=int, default=64, help="Number of rows per batch (default: 64)")
    parser.add_argument(
        "--concurrency", type=int, default=16, help="Number of concurrent rows to process (default: 16)"
    )
    parser.add_argument("--max-rows", type=int, default=0, help="Max rows to process (0 = all)")
    parser.add_argument(
        "--resume", action="store_true", help="Resume from where we left off (skip already-processed IDs)"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="Qwen/Qwen3-VL-8B-Instruct-FP8",
        help="Model name served by vLLM (default: Qwen/Qwen3-VL-8B-Instruct-FP8)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://localhost:8000/v1",
        help="vLLM server base URL (default: http://localhost:8000/v1)",
    )
    args = parser.parse_args()

    print(f"Model:       {args.model}")
    print(f"Server:      {args.base_url}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Input:       {INPUT_CSV}")
    print(f"Output:      {OUTPUT_CSV}")
    print("Note:        3 LLM calls per row (title, tags, description)")

    # Initialise OpenAI client pointing at local vLLM server
    client = make_vllm_client(args.base_url)

    # Load input
    print("Loading input CSV...")
    rows = load_input_csv(INPUT_CSV, max_rows=args.max_rows)
    print(f"Loaded {len(rows)} rows from input CSV.")

    # Resume support
    if args.resume:
        existing_ids = load_existing_ids(OUTPUT_CSV)
        before = len(rows)
        rows = [r for r in rows if r["id"] not in existing_ids]
        print(f"Resume: skipping {before - len(rows)} already-processed rows. {len(rows)} remaining.")
    else:
        # Start fresh
        if OUTPUT_CSV.exists():
            OUTPUT_CSV.unlink()

    if not rows:
        print("Nothing to process!")
        return

    # Process in batches
    total_processed = 0
    batch_size = args.batch_size
    num_batches = (len(rows) + batch_size - 1) // batch_size
    t_start = time.time()

    for batch_start in tqdm(range(0, len(rows), batch_size), total=num_batches, desc="Batches"):
        batch = rows[batch_start : batch_start + batch_size]

        results = process_batch(client, batch, model_name=args.model, concurrency=args.concurrency)

        write_results(OUTPUT_CSV, results, append=(batch_start > 0 or args.resume))
        total_processed += len(results)

    total_elapsed = time.time() - t_start
    print(
        f"\nDone! Processed {total_processed} rows in {total_elapsed:.1f}s "
        f"({total_processed / total_elapsed:.1f} rows/s)"
    )
    print(f"Output saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
