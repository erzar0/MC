# LongSANA Text-to-Minecraft Training Plan

## Goal
To train a diffusion model (LongSANA) capable of reading a text prompt (e.g., "A huge gothic castle") and generating a 3D Minecraft voxel grid (256x256xH) by treating the vertical axis (Height/Y) as the temporal axis (Time/T) in a video.

---

## Stage 1: Data Preparation

To train a robust model, you will need a large dataset (e.g., 5,000 to 20,000) of unique Minecraft structures. 

1. **Sourcing Data:**
   * Scrape `.mca` region files, schematics, and worlds from sites like PlanetMinecraft containing castles, houses, and towers.
2. **Extraction (`region_extractor.py`):**
   * Use your `WorldWrapper` to extract the `rgb_cube` associated with each structure.
   * Standardize the footprint to `256x256` blocks (X, Z axes).
   * Standardize the height bounding box (e.g., crop `H` to the highest block in the structure so the sky isn't empty).
3. **Format as Video Tensors:**
   * Transpose the data from `(X, Z, Y, 3)` to `(Y, X, Z, 3)`. Now, each "frame" of this pseudo-video is a horizontal cross-section of the castle progressing from the foundation to the roof.
   * Save the arrays as `.npy` / `.mp4` or blosc2 (lossless compression) for the dataloader.
4. **VLM Captioning (The "Text" in Text-to-Video):**
   * Render isometric images of the castles using `pyvista`/`mcmap`.
   * Pass these images into a Vision-Language Model (VLM) like LLaVA or GPT-4V with the prompt: *"Describe this Minecraft structure in detail."*
   * Save the resulting text alongside the `.npy` cube.

---

## Stage 2: Choosing the Base Model

Training a diffusion model from scratch is too expensive. We will fine-tune the open-source **SANA** model (or **LongSANA**).

1. **Pre-trained Weights:** We initialize the LongSANA DiT (Diffusion Transformer) with pre-trained weights that already understand geometric shapes, colors, and layout from natural video data. 
2. **Text Encoder:** LongSANA relies on the **Gemma-2B** or **T5-XXL** text encoder. This encoder is kept *frozen* during training. It simply reads your generated VLM captions and converts them into text embeddings.
3. **VAE (Variational Autoencoder):** SANA uses an internal 3D VAE to compress video pixels into a latent space. You will use SANA's standard VAE to compress your Minecraft RGB frames before they hit the Diffusion Transformer.

---

## Stage 3: The Training Loop

1. **Input Encoding:**
   * A batch of `(Time=H, Height=256, Width=256, C=3)` Minecraft slices goes in.
   * The SANA VAE compresses this into a smaller continuous *latent video*.
2. **Adding Noise (Forward Process):**
   * The training script gradually adds Gaussian noise to the latent video according to a timestep $t$.
3. **Denoising (Backward Process):**
   * The noisy latent video, along with the text embeddings from Gemma-2B, is fed into the **LongSANA Diffusion Transformer**.
   * *LongSANA Magic:* Thanks to the Constant-Memory KV Cache, the Transformer looks at the entire vertical height of the castle simultaneously without running out of GPU memory.
   * The network predicts the noise that was added to the latent space.
4. **Loss Calculation:**
   * Mean Squared Error (MSE) is calculated between the predicted noise and the actual noise. 
   * Gradients are backpropagated to update the weights of the LongSANA Transformer.

---

## Stage 4: Inference (Generation)

Once the model loss converges (i.e., the model stops generating blurry blocks and starts generating coherent geometric walls and roofs), it is ready for inference.

1. **Text Prompt:** User inputs *"A large sand-castle with 4 towers"*.
2. **Setup:** Starting with pure Gaussian noise of shape `(H, 256, 256)`, the LongSANA model iteratively removes the noise over 30-50 steps, conditioned by the text.
3. **Decoding:** The generated latent representation is passed through the VAE Decoder returning the raw RGB tensor `(H, 256, 256, 3)`.
4. **Color Snapping (Post-Processing):** Use `scipy.spatial.cKDTree` as shown in your pipeline script to snap the raw continuous RGB generated pixels back to the discrete Minecraft Block ID Palette using your pre-defined `lut`.
5. **Final Output:** You now have a voxel grid containing valid block IDs that can be injected into a `.mca` file and loaded in game!
