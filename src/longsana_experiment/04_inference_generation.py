import numpy as np
import torch
import sys
import os

# Add playground directory to path to import the custom pipeline logic
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'playground')))
from sana_test import SanaVideoCustomPipeline

def generate_castle(prompt: str, height: int = 100, model_path: str = "path/to/fine_tuned_longsana"):
    """
    Stage 4: Uses the trained LongSANA model to generate a castle 
    (as a video) and transposes it back to the Minecraft spatial grid format.
    """
    print(f"\\n[INFERENCE] Generating castle for prompt: '{prompt}'")
    
    # --- Integration with diffusers (using the custom pipeline from sana_test) ---
    print(f"Loading model from {model_path}...")
    pipe = SanaVideoCustomPipeline.from_pretrained(model_path, torch_dtype=torch.bfloat16)
    pipe.vae.to(torch.float32)
    pipe.text_encoder.to(torch.bfloat16)
    pipe.to("cuda")
    
    # 1. Generate the raw video frames (Shape: [H, 256, 256, 3])
    print(f"Running generation for {height} steps (frames)...")
    video = pipe(
        prompt=prompt,
        height=256,
        width=256,
        frames=height,
        guidance_scale=6,
        use_resolution_binning=False,  # Important: Keep exact dimensions
        num_inference_steps=30,
        generator=torch.Generator(device="cuda").manual_seed(42),
        output_type="np.array" # Request numpy output instead of mp4 export
    ).frames[0]
    
    # If the output is float [0, 1], scale to uint8 [0, 255]
    if video.dtype == np.float32 or video.dtype == np.float64:
        video = (video * 255).astype(np.uint8)
        
    print(f"Generated video shape: {video.shape}")
        
    # --- Spatial Transpose ---
    # Transpose back from Video format to Spatial format: (Y=Time, X=Height, Z=Width, 3) -> (X, Z, Y, 3)
    # Assumes output shape is (Frames, H, W, Channels)
    spatial_cube = np.transpose(video, (1, 2, 0, 3))
    
    print(f"Generated Spatial cube shape: {spatial_cube.shape}")
    return spatial_cube

if __name__ == "__main__":
    # Example usage:
    # prompt = "A sprawling medieval castle built with stone bricks, 90 blocks tall."
    # generated_castle_rgb = generate_castle(prompt, height=90, model_path="Efficient-Large-Model/SANA-Video_2B_480p_diffusers")
    pass
