# LongSANA Pipeline Architecture

This document details the data flow and model architecture implemented in the `tiny_diffusion_prototype.ipynb`.

## Data Flow Diagram

The following diagram illustrates the transformation of raw Minecraft voxel data into a generated 3D structure.

```mermaid
graph TD
    subgraph "1. Data Preparation"
        A[Raw .b2frame Voxel Volume] -->|Crop 64x64x16| B(Processed Volume)
        C[Text Caption .txt] -->|T5 Tokenizer| D(Text Tokens)
        B -->|Color Mapping| E(RGB Volume NumPy)
        E -->|Permute & Norm| F[Video Tensor B,C,T,H,W]
    end

    subgraph "2. Latent Encoding"
        F -->|VAE Encoder| G[Latent Space B,4,16,16,16]
        D -->|T5 Encoder Model| H(Text Embeddings)
    end

    subgraph "3. Training Phase (Forward/Backward)"
        G -->|Forward Diffusion| I[Noisy Latents]
        I -->|Noise Predictor DiT| J[Predicted Noise]
        H -.->|Conditioning| J
        J -->|MSE Loss| K[Optimizer Update]
    end

    subgraph "4. Generation Phase (Sampling)"
        L[Pure Gaussian Noise] -->|Iterative Denoising DiT| M[Clean Latents Estimate]
        H -.->|Prompt Conditioning| M
        M -->|VAE Decoder| N[Reconstructed Video Tensor]
    end

    subgraph "5. Output"
        N -->|Spatial Reshape| O[Predicted RGB Volume]
        O -->|2D Slices| P[Matplotlib 2D Plot]
        O -->|Voxel Renderer| Q[Matplotlib 3D Plot]
    end
```

## Step-by-Step Breakdown

### 1. Data Preparation

- **Voxel Volume:** Loaded from `.b2frame` (Blosc2) files.
- **Cropping:** Aggressively reduced to `64x64x16` to fit local VRAM (Blackwell compatible).
- **Color Mapping:** Converts raw block IDs to normalized RGB values.
- **Formatting:** Reshapes data into `(B, C, T, H, W)`, effectively treating the vertical axis as the "time" dimension
  for video diffusion layers.

### 2. Latent Encoding (VAE)

- Uses a **3D Variational Autoencoder** (Trainable) to compress the spatial dimensions.
- The high-resolution Grid is reduced to a compact latent representation, lowering the computational cost for the
  Diffusion Transformer.

### 3. Text Conditioning

- **T5 Encoder:** Processes the caption (e.g., "A simple stone castle tower") into high-dimensional semantic embeddings.
- These embeddings guide the denoising process to ensure the output matches the user's intent.

### 4. Diffusion Transformer (DiT)

- A **3D Convolutional Block** acts as the noise predictor.
- It learns to estimate the noise added to a latent sample at a specific timestep, conditioned on the text.

### 5. 3D Visualization

- Final output is decoded via the VAE Decoder.
- A custom **Voxel Renderer** uses Matplotlib's `voxels` method to display the 3D structure, allowing for physical
  verification of the generated geometry.

---

## Design vs. Implementation Comparison

The `tiny_diffusion_prototype.ipynb` serves as a functional proof-of-concept for the full LongSANA architecture
described in `diagram.puml` and `training_plan.md`.

### Feature Parity Table

| Feature             | Real Design (LongSANA)            | Tiny Notebook (Prototype)             |
|:--------------------|:----------------------------------|:--------------------------------------|
| **Model Scaling**   | 2B+ parameters (LongSANA DiT)     | ~10M parameters (TinyDiT)             |
| **Text Encoder**    | Gemma-2B / T5-XXL                 | T5-Small (`google-t5/t5-small`)       |
| **VAE Compression** | High-fidelity SANA VAE            | Narrow `TinyVAE` (Conv3D)             |
| **Resolution**      | 256x256xH (Standard Footprint)    | 64x64x16 (Aggressive Crop)            |
| **Data Source**     | Multi-source (PlanetMinecraft)    | Local `tmp/processed_worlds/cleansed` |
| **Hardware Target** | Multi-GPU Server Clusters         | Single Local Blackwell (RTX 50xx)     |
| **Output Type**     | Discrete Block IDs (cKDTree Snap) | Continuous RGB Voxels (Matplotlib)    |

### Key Differences & Strategy

1. **Dimensionality Reduction:** The real model works at `256x256` which is 16x more spatial data than the `64x64`
   prototype. The tiny implementation uses aggressive cropping to maintain sub-second iteration times on local hardware.
2. **Latent Depth:** While the real LongSANA uses a deep Diffusion Transformer, the prototype uses simplified 3D
   Convolutional blocks to simulate the denoising process without the memory overhead of large self-attention blocks.
3. **Workflow Parity:** Both implementations follow the **Train -> Save -> Load -> Validate** workflow, ensuring that
   the logic verified in the notebook is directly transferable to the high-scale training scripts.
