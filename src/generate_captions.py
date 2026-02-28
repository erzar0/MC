import os
import glob
try:
    from vllm import LLM, SamplingParams
    from PIL import Image
    VLLM_AVAILABLE = True
except ImportError:
    VLLM_AVAILABLE = False
    print("Warning: vLLM or PIL not installed. process_screenshots_folder will fall back to single-image OpenAI processing.")

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

def generate_caption(img_path: str):
    """Fallback capability if vLLM isn't available."""
    print(f"Warning: OpenAI fallback for generate_caption is not fully implemented for {img_path}.")
    return "A Minecraft scene."

def process_screenshots_folder(screenshot_dir: str, output_dir: str):
    """
    Reads all screenshots in a directory, generates captions using local vLLM (if available), 
    and saves them as text files in the output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    screenshot_paths = glob.glob(os.path.join(screenshot_dir, "*.png"))
    if not screenshot_paths:
        print(f"No PNG screenshots found in {screenshot_dir}")
        return
        
    to_process = []
    for img_path in screenshot_paths:
        base_name = os.path.splitext(os.path.basename(img_path))[0]
        out_path = os.path.join(output_dir, f"{base_name}.txt")
        
        if not os.path.exists(out_path):
            to_process.append((img_path, out_path, base_name))
        else:
            print(f"Caption already exists for {base_name}, skipping.")
            
    if not to_process:
        print("All captions are already generated!")
        return

    if VLLM_AVAILABLE:
        print(f"Initializing vLLM model for {len(to_process)} images...")
        llm = LLM(
            model="Qwen/Qwen3-VL-4B-Instruct-FP8",
            trust_remote_code=True,
            enable_prefix_caching=True,
            limit_mm_per_prompt={"image": 1},
            max_model_len=8192,
            gpu_memory_utilization=0.9
        )

        sampling_params = SamplingParams(
            temperature=0.7,
            max_tokens=800,
            repetition_penalty=1.1,
            top_p=0.8,
            top_k=20 
        )

        print("Preparing inputs for vLLM...")
        inputs = []
        for img_path, out_path, base_name in to_process:
            image = Image.open(img_path).convert("RGB")
            
            # Use explicit standard Qwen-VL template
            full_prompt = (
                f"<|im_start|>system\n{SYSTEM_PROMPT}<|im_end|>\n"
                f"<|im_start|>user\n<|vision_start|><|image_pad|><|vision_end|>\nDescribe this Minecraft structure in detail.\n"
                f"Please strictly follow the output template and taxonomy rules provided in the system prompt.<|im_end|>\n"
                f"<|im_start|>assistant\n"
            )
            
            inputs.append({
                "prompt": full_prompt,
                "multi_modal_data": {"image": image}
            })
            
        print(f"Running vLLM inference on {len(inputs)} images...")
        outputs = llm.generate(inputs, sampling_params)
        
        for i, out in enumerate(outputs):
            _, out_path, base_name = to_process[i]
            caption = out.outputs[0].text.strip()
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(caption)
            print(f"Saved caption to {out_path}")
            
    else:
        # Fallback to OpenAI sequential calls
        print(f"Processing {len(to_process)} images sequentially with OpenAI API fallback...")
        for img_path, out_path, base_name in to_process:
            print(f"Generating caption for {base_name}...")
            caption = generate_caption(img_path)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(caption)
            print(f"Saved caption to {out_path}")

if __name__ == "__main__":
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))
        
    from src import config
    
    world_name = config.DEFAULT_WORLD_NAME
    cleansed_dir = config.PROCESSED_WORLDS_DIR / "cleansed" / world_name
    screenshots_dir = cleansed_dir / "screenshots"
    captions_dir = cleansed_dir / "captions"
    
    print(f"Processing screenshots from: {screenshots_dir}")
    print(f"Saving captions to: {captions_dir}")
    process_screenshots_folder(str(screenshots_dir), str(captions_dir))
