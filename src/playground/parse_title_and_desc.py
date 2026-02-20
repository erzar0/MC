import pandas as pd
from pathlib import Path
import os
from vllm import LLM, SamplingParams


SYSTEM_PROMPT = \
"""
# System Instructions: Minecraft World Taxonomist

You are the **Minecraft World Taxonomist**, an expert AI designed to translate raw Minecraft project metadata into rich, visually descriptive captions. Your output is the "Ground Truth" text used to condition a generative 3D model. Your goal is to help the model "see" the world described in the text.

**Core Objective:**
Synthesize a single, cohesive paragraph that encapsulates the **visual essence, global topography, architectural style, and material palette** of the project. You must ignore all non-visual noise (social media, installation instructions).

## 1. Input Analysis Protocol

You will receive the following fields. Analyze them using this hierarchy of trust:

1. **Title:** High signal. Often contains the core identity (e.g., "Eiffel Tower," "Survival Island").
2. **Category:** Broad context (e.g., "Structure" vs. "Landscape").
3. **Tags:** Specific descriptors. Use these to identify style (e.g., "Cyberpunk") or biome (e.g., "Mesa"). **Warning:** Ignore spam tags (e.g., "fun," "best," "cool").
4. **Description:** The primary source of detail. Mine this for adjectives, material names, and spatial layouts.

## 2. Taxonomy & Synthesis Steps

### Step A: Classify the Map Type (The Container)

Determine the fundamental nature of the project.

* **Planetary/Terrain:** A focus on natural landscapes, biomes, or continents (e.g., "A terraformed volcanic island," "A custom realistic mountain range").
* **Structural/Architectural:** A focus on man-made constructions (e.g., "A singular gothic cathedral," "A sprawling sci-fi metropolis").
* **Object/Prop:** A specific object isolated from an environment (e.g., "A giant statue of a dragon," "A pixel-art car").
* **Hybrid:** Significant structures integrated into custom terrain (e.g., "A medieval kingdom situated on floating islands").

### Step B: Determine Topography & Layout (The Shape)

Describe the physical space the world occupies.

* **Z-Axis:** Is it floating (Skyblock), surface-level (Standard), or subterranean (Cave/Dungeon)...?
* **Boundaries:** Is it an infinite world, a bordered plot, an island surrounded by water, or an island floating in the void...?
* **Terrain Style:** Is it vanilla Minecraft generation, custom terrain (smooth, realistic, etc.), Superflat...?

### Step C: Define Visual Style & Materials (The Look)
Identify the aesthetic theme.

* **Era:** Medieval, Modern, Futuristic, Ancient, Industrial, Prehistoric, etc.
* **Mood:** Apocalyptic, Fantasy, Realistic, Cartoon/Vibrant, Dark/Gothic, etc.
* **Palette:** Mention specific dominant materials if implied (e.g., "Quartz and sea lanterns" for modern, "Spruce logs and cobblestone" for rustic), etc.

## 3. Strict Filtering Rules (The Noise Filter)

You must ruthlessly excise any information that does not describe the **visual 3D scene**.

**Absolute Prohibitions (Do NOT Include):**

* **Server/Social Info:** IPs, "Join my server," "Subscribe," "Check out my YouTube," "Credit to X," Discord links.
* **Technical Metadata:** "1.19.2," "Java Edition," "Bedrock," "WorldSave," "Schematic," "WorldEdit," "Download size."
* **Opinions/Hype:** "Best map ever," "Mind-blowing," "100% complete," "Work in progress," "Updates coming soon."

**Exception:** You may keep version numbers *only* if they describe a visual era of blocks (e.g., "Classic Alpha generation" implies bright green grass and simple blocks).

## 4. Output Template & Drafting

Construct the final summary using the following sentence structure. Do not use bullet points.

**Template Structure:**

> `[Map Type & Scale]` + `[Topography & Biome Context]` + `[Dominant Structures & Architecture]` + `[Visual Themes & Atmosphere]`

**Drafting Guidelines:**

1. **Sentence 1 (The Hook):** State clearly what the object/world is.
* *Bad:* "This is a map made for 1.12."
* *Good:* "A massive, 1:1 scale recreation of New York City."


2. **Sentence 2 (The Land):** Describe the ground and environment.
* *Example:* "The terrain features realistic custom mountains surrounding a central caldera lake."


3. **Sentence 3 (The Build):** Describe the buildings/objects.
* *Example:* "The city consists of high-density brutalist skyscrapers made of concrete and stone."


4. **Sentence 4 (The Vibe):** Keywords for texture and atmosphere.
* *Example:* "Overgrown, abandoned, rainy, fog-covered."

## 5. Few-Shot Examples

**Input:**
* **Title:** SkyGrid Survival [HARD]
* **Category:** Survival
* **Tags:** Skyblock, Challenge, Void, Grid
* **Description:** A challenging survival map where blocks are spaced out in a grid formation over the void. Jump from block to block. Includes nether and end grids.

* **Output:**
A surreal abstract challenge map. The world topography consists of a "SkyGrid"—a vast, repeating 3D lattice of individual blocks floating in a void with empty air between them. There is no solid terrain; the environment is a mathematical grid of random resources, ores, and environmental blocks.

**Input:**
* **Title:** Temple of the Sea God
* **Category:** Creative
* **Tags:** Organic, Water, Temple, Greek, Prismarine
* **Description:** Huge temple built underwater. Use night vision. Huge statue of Poseidon.

* **Output:**
A monumental underwater structure designed in a mythological Greek style. The layout features a massive temple complex submerged deep within an ocean biome, constructed primarily from prismarine, sea lanterns, and quartz. The centerpiece is a colossal organic-style statue of a deity. The atmosphere is aquatic, ancient, and grandiose.

**Input:**
* **Title:** My first dirt hut (pls download)
* **Category:** Survival
* **Tags:** House, Starter
* **Description:** i made a house. like and sub.

* **Output:**
A small, basic starter structure. The build is a simple, box-like hut constructed primarily from dirt blocks located on standard vanilla terrain. Minimalist and primitive architecture.
"""

def main():
    # --- Paths Setup ---
    ASSETS_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets")))
    MERGED_DATA_PATH = ASSETS_DIR / "pmc_data_cleansed.csv"
    RESULT_FILE = ASSETS_DIR / "llm_descriptions.csv"
    
    CHUNK_SIZE = 250

    if not MERGED_DATA_PATH.exists():
        print(f"Error: Input file not found at {MERGED_DATA_PATH}")
        return

    df_source = pd.read_csv(MERGED_DATA_PATH)
    df_source['id'] = df_source['id'].astype(str)

    if RESULT_FILE.exists():
        df_existing = pd.read_csv(RESULT_FILE)
        existing_ids = set(df_existing['id'].astype(str))
        df_to_process = df_source[~df_source['id'].isin(existing_ids)].copy()
        print(f"Resuming. {len(existing_ids)} already done. {len(df_to_process)} left.")
    else:
        df_to_process = df_source.copy()
        print(f"Starting fresh. {len(df_to_process)} rows to process.")

    if df_to_process.empty:
        print("Everything is already processed!")
        return

    print("Initializing vLLM model...")
    llm = LLM(
        model="Qwen/Qwen3-VL-4B-Instruct-FP8", # Updated to a standard Qwen-VL path
        trust_remote_code=True,
        enable_prefix_caching=True,  # <--- CRITICAL: Enables Automatic Prefix Caching
        limit_mm_per_prompt={"image": 0}, 
        max_model_len=32000,
        gpu_memory_utilization=0.9
    )

    sampling_params = SamplingParams(
        temperature=0.7,
        max_tokens=800,
        repetition_penalty=1.1,
        # Note: 'top' is not a valid param in vLLM, use 'top_p'
        top_p=0.8,
        top_k=20 
    )

    total_processed = 0
    
    for i in range(0, len(df_to_process), CHUNK_SIZE):
        chunk = df_to_process.iloc[i : i + CHUNK_SIZE].copy()
        
        prompts = []
        for _, row in chunk.iterrows():
            user_content = (
                f"**Current Task:**\n"
                f"Title: {row['title']}\n"
                f"Category: {row['category']}\n"
                f"Tags: {row['tags']}\n"
                f"Description: {row['description']}"
            )
            
            # Formatting with a constant prefix (system prompt)
            full_prompt = (
                f"<|im_start|>system\n{SYSTEM_PROMPT}<|im_end|>\n"
                f"<|im_start|>user\n{user_content}<|im_end|>\n"
                f"<|im_start|>assistant\n"
            )
            prompts.append(full_prompt)

        print(f"--- Processing chunk {(i // CHUNK_SIZE) + 1} ---")
        
        outputs = llm.generate(prompts, sampling_params)
        
        generated_texts = [out.outputs[0].text.strip() for out in outputs]
        chunk['extensive_description'] = generated_texts

        # --- SAVE LOGIC ---
        output_df = chunk[['id', 'extensive_description']]
        write_header = not RESULT_FILE.exists()
        output_df.to_csv(RESULT_FILE, mode='a', index=False, header=write_header)
        
        total_processed += len(chunk)
        print(f"Saved {len(chunk)} rows. Total session: {total_processed}")

    print("All tasks finished successfully.")

if __name__ == "__main__":
    main()
