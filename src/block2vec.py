"""Block2Vec: Learning block embeddings from Minecraft volumes using Skip-Gram with Negative Sampling.

Architecture:
    - Triton kernel for fused SGNS gradient updates (bypasses PyTorch autograd)
    - Deterministic full-volume iteration with C/Fortran order randomization
    - Word2Vec-style subsampling to reduce frequency bias from common blocks
"""

import logging
import os
from pathlib import Path
from typing import Optional

import blosc2
import numpy as np
import torch
import torch.nn as nn
import triton
import triton.language as tl
from tqdm import tqdm

logger = logging.getLogger(__name__)


@triton.jit
def _sgns_kernel(
    center_embeddings_ptr,
    context_embeddings_ptr,
    center_ids_ptr,
    context_ids_ptr,
    negative_ids_ptr,
    learning_rate,
    n_elements,
    n_negatives,
    embedding_dim: tl.constexpr,
    BLOCK_SIZE_N: tl.constexpr,
):
    """Fused Skip-Gram with Negative Sampling update.

    For each (center, context) pair:
      1. Compute positive gradient:  g = (σ(v_c · u_ctx) - 1) · lr
      2. Compute negative gradients: g = σ(v_c · u_neg) · lr  (for each negative)
      3. Accumulate all gradients for v_c, update u_ctx and u_neg in-place
    """
    pid = tl.program_id(0)
    element_offsets = pid * BLOCK_SIZE_N + tl.arange(0, BLOCK_SIZE_N)
    element_mask = element_offsets < n_elements
    dim_offsets = tl.arange(0, embedding_dim)

    # Load IDs and compute embedding memory offsets
    center_ids = tl.load(center_ids_ptr + element_offsets, mask=element_mask, other=0)
    context_ids = tl.load(context_ids_ptr + element_offsets, mask=element_mask, other=0)
    center_emb_offsets = center_ids[:, None] * embedding_dim + dim_offsets[None, :]
    context_emb_offsets = context_ids[:, None] * embedding_dim + dim_offsets[None, :]

    # Load embeddings
    center_vecs = tl.load(center_embeddings_ptr + center_emb_offsets, mask=element_mask[:, None], other=0.0)
    pos_context_vecs = tl.load(context_embeddings_ptr + context_emb_offsets, mask=element_mask[:, None], other=0.0)

    # Positive pair: maximize dot product → gradient = (σ(dot) - 1) · lr
    pos_dot = tl.sum(center_vecs * pos_context_vecs, axis=1)
    pos_grad = (tl.sigmoid(pos_dot) - 1.0) * learning_rate

    center_grad_accum = pos_grad[:, None] * pos_context_vecs
    tl.store(
        context_embeddings_ptr + context_emb_offsets,
        pos_context_vecs - pos_grad[:, None] * center_vecs,
        mask=element_mask[:, None],
    )

    # Negative pairs: minimize dot product → gradient = σ(dot) · lr
    for neg_idx in range(n_negatives):
        neg_ids = tl.load(negative_ids_ptr + element_offsets * n_negatives + neg_idx, mask=element_mask, other=0)
        neg_emb_offsets = neg_ids[:, None] * embedding_dim + dim_offsets[None, :]
        neg_vecs = tl.load(context_embeddings_ptr + neg_emb_offsets, mask=element_mask[:, None], other=0.0)

        neg_dot = tl.sum(center_vecs * neg_vecs, axis=1)
        neg_grad = tl.sigmoid(neg_dot) * learning_rate

        center_grad_accum += neg_grad[:, None] * neg_vecs
        tl.store(
            context_embeddings_ptr + neg_emb_offsets,
            neg_vecs - neg_grad[:, None] * center_vecs,
            mask=element_mask[:, None],
        )

    # Apply accumulated gradient to center vector
    tl.store(
        center_embeddings_ptr + center_emb_offsets,
        center_vecs - center_grad_accum,
        mask=element_mask[:, None],
    )


class Block2Vec(nn.Module):
    """Skip-Gram embedding model for Minecraft block IDs.

    Uses separate input (center) and output (context) embedding matrices,
    trained via a fused Triton SGNS kernel that bypasses PyTorch autograd.
    """

    def __init__(self, vocab_size: int, embedding_dim: int = 128, n_negatives: int = 5):
        super().__init__()
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.n_negatives = n_negatives

        # Registered as buffers (not Parameters) since Triton writes directly to memory
        self.register_buffer("embedding_in", torch.randn(vocab_size, embedding_dim) * 0.05)
        self.register_buffer("embedding_out", torch.zeros(vocab_size, embedding_dim))

    def train_step(self, center_ids, context_ids, negative_ids, learning_rate: float):
        n_elements = center_ids.numel()
        if n_elements == 0:
            return

        def grid(meta):
            return (triton.cdiv(n_elements, meta["BLOCK_SIZE_N"]),)

        _sgns_kernel[grid](
            self.embedding_in,
            self.embedding_out,
            center_ids,
            context_ids,
            negative_ids,
            learning_rate,
            n_elements,
            self.n_negatives,
            embedding_dim=self.embedding_dim,
            BLOCK_SIZE_N=128,
        )

    def get_embeddings(self) -> np.ndarray:
        return self.embedding_in.detach().cpu().numpy()

    def most_similar(self, block_id: int, top_n: int = 10, id_to_name: dict = None):
        emb = self.embedding_in[block_id].unsqueeze(0)
        norms = torch.norm(self.embedding_in, p=2, dim=1, keepdim=True)
        norm_emb = emb / (torch.norm(emb) + 1e-12)
        norm_all = self.embedding_in / (norms + 1e-12)

        if not torch.isfinite(norm_emb).all():
            return [("NaN Embeddings - Training Failed", 0.0)]

        sims = torch.mm(norm_emb, norm_all.t()).squeeze(0)
        top_indices = torch.topk(sims, min(top_n + 1, self.vocab_size)).indices[1:].cpu().tolist()
        return [(id_to_name.get(idx, str(idx)) if id_to_name else str(idx), sims[idx].item()) for idx in top_indices]


# Cached neighbor offset tensors (module-level singletons)
_FACE6_OFFSETS = None
_CUBE26_OFFSETS = None


def _get_offsets(neighbor_mode: str) -> torch.Tensor:
    """Return cached neighbor offset tensor on GPU."""
    global _FACE6_OFFSETS, _CUBE26_OFFSETS

    if neighbor_mode == "face6":
        if _FACE6_OFFSETS is None:
            _FACE6_OFFSETS = torch.tensor(
                [
                    [-1, 0, 0],
                    [1, 0, 0],
                    [0, -1, 0],
                    [0, 1, 0],
                    [0, 0, -1],
                    [0, 0, 1],
                ],
                device="cuda",
                dtype=torch.int32,
            )
        return _FACE6_OFFSETS
    elif neighbor_mode == "cube26":
        if _CUBE26_OFFSETS is None:
            offsets = [
                [dx, dy, dz]
                for dx in [-1, 0, 1]
                for dy in [-1, 0, 1]
                for dz in [-1, 0, 1]
                if not (dx == 0 and dy == 0 and dz == 0)
            ]
            _CUBE26_OFFSETS = torch.tensor(offsets, device="cuda", dtype=torch.int32)
        return _CUBE26_OFFSETS
    else:
        raise ValueError(f"Unknown neighbor_mode: {neighbor_mode!r}. Use 'face6' or 'cube26'.")


def _compute_subsample_probs(volume: np.ndarray, threshold: float = 1e-2) -> np.ndarray:
    """Word2Vec-style subsampling: P(keep) = min(1, sqrt(t / f(w))). O(n) via bincount."""
    counts = np.bincount(volume.ravel())
    freqs = counts / volume.size

    keep_probs = np.ones(len(counts), dtype=np.float32)
    nonzero = freqs > 0
    keep_probs[nonzero] = np.minimum(1.0, np.sqrt(threshold / freqs[nonzero]))
    return keep_probs


class SpatialMinecraftDataset:
    """Deterministic full-volume iterator for Block2Vec training.

    Iterates over ALL non-boundary blocks in a region as centers, paired with
    their 3D spatial neighbors as context. Applies Word2Vec subsampling to
    suppress high-frequency blocks (air, stone, dirt).

    Memory layout is randomized per-region (50% C order, 50% Fortran order)
    to vary the spatial locality of contiguous batches.
    """

    def __init__(
        self,
        region_volume: np.ndarray,
        neg_buffer_gpu: torch.Tensor,
        neg_idx: int,
        subsample_threshold: float = 1e-2,
        neighbor_mode: str = "face6",
        batch_size: int = 4096,
    ):
        self.shape = region_volume.shape
        self.total_size = region_volume.size
        self.neg_idx = neg_idx
        self.batch_size = batch_size

        # Volume on GPU
        self.gpu_volume = torch.from_numpy(region_volume.astype(np.int32)).cuda()

        # Shared negative buffer (already on GPU)
        self.neg_buffer = neg_buffer_gpu
        self.neg_buffer_size = neg_buffer_gpu.size(0)

        # Subsampling probabilities
        keep_probs = _compute_subsample_probs(region_volume, threshold=subsample_threshold)
        self.keep_probs = torch.from_numpy(keep_probs.astype(np.float32)).cuda()

        # Neighbor offsets
        self.offsets = _get_offsets(neighbor_mode)
        self.n_neighbors = self.offsets.shape[0]

        # Build valid center indices and memory-order strides
        self._build_valid_centers()

    def _build_valid_centers(self):
        """Flatten volume in random C/Fortran order, extract non-boundary indices."""
        sx, sz, sy = self.shape
        mask = np.zeros(self.shape, dtype=bool)
        mask[1 : sx - 1, 1 : sz - 1, 1 : sy - 1] = True

        # Randomize memory traversal order for batch locality variation
        self._use_fortran = np.random.random() < 0.5
        if self._use_fortran:
            valid_flat = np.flatnonzero(mask.ravel(order="F"))
            self._stride_y = sx * sz
            self._stride_z = sx
        else:
            valid_flat = np.flatnonzero(mask.ravel(order="C"))
            self._stride_x = sz * sy
            self._stride_z = sy

        self.valid_centers = torch.from_numpy(valid_flat.astype(np.int64)).cuda()
        self.n_valid = self.valid_centers.numel()

    def _unravel(self, flat_indices: torch.Tensor):
        """Convert flat indices → (x, z, y) coordinates on GPU."""
        if self._use_fortran:
            y = flat_indices // self._stride_y
            remainder = flat_indices % self._stride_y
            z = remainder // self._stride_z
            x = remainder % self._stride_z
        else:
            x = flat_indices // self._stride_x
            remainder = flat_indices % self._stride_x
            z = remainder // self._stride_z
            y = remainder % self._stride_z
        return x.int(), z.int(), y.int()

    def num_batches(self) -> int:
        return max(1, (self.n_valid + self.batch_size - 1) // self.batch_size)

    def get_batch(self, batch_idx: int, n_negatives: int):
        """Return (center_ids, context_ids, negative_ids) for one batch.

        1. Slice contiguous block of valid center indices
        2. Subsample centers by block frequency
        3. Gather 3D neighbors as context
        4. Subsample context by block frequency
        5. Draw negatives from pre-computed buffer
        """
        start = batch_idx * self.batch_size
        end = min(start + self.batch_size, self.n_valid)
        if start >= self.n_valid:
            return self._empty_batch(n_negatives)

        # Unravel flat indices → 3D coordinates
        cx, cz, cy = self._unravel(self.valid_centers[start:end])
        center_ids = self.gpu_volume[cx.long(), cz.long(), cy.long()]

        # Subsample centers
        keep_mask = torch.rand(center_ids.numel(), device="cuda") < self.keep_probs[center_ids]
        if not keep_mask.any():
            return self._empty_batch(n_negatives)

        cx, cz, cy = cx[keep_mask], cz[keep_mask], cy[keep_mask]
        center_ids = center_ids[keep_mask]

        # Gather spatial neighbors (no clamp needed — centers are ≥1 voxel from edges)
        # Loop avoids allocating (B, N, 3) intermediate; peak mem: (B, N) vs (B, N, 3)
        n_centers = cx.shape[0]
        context_ids_2d = torch.empty((n_centers, self.n_neighbors), device="cuda", dtype=self.gpu_volume.dtype)
        for k in range(self.n_neighbors):
            context_ids_2d[:, k] = self.gpu_volume[
                (cx + self.offsets[k, 0]).long(),
                (cz + self.offsets[k, 1]).long(),
                (cy + self.offsets[k, 2]).long(),
            ]

        # Subsample context
        ctx_keep = torch.rand_like(self.keep_probs[context_ids_2d]) < self.keep_probs[context_ids_2d]
        center_expanded = center_ids.unsqueeze(1).expand_as(context_ids_2d)
        valid = ctx_keep.flatten()
        final_centers = center_expanded.flatten()[valid].contiguous()
        final_contexts = context_ids_2d.flatten()[valid].contiguous()

        if final_centers.numel() == 0:
            return self._empty_batch(n_negatives)

        # Draw negatives from circular buffer (slice-based, no index tensor)
        total_pairs = final_centers.numel()
        needed = total_pairs * n_negatives
        negatives = torch.empty(needed, device="cuda", dtype=self.neg_buffer.dtype)
        remaining, pos, idx = needed, 0, self.neg_idx
        while remaining > 0:
            avail = min(remaining, self.neg_buffer_size - idx)
            negatives[pos : pos + avail] = self.neg_buffer[idx : idx + avail]
            pos += avail
            remaining -= avail
            idx = 0
        negatives = negatives.view(total_pairs, n_negatives)
        self.neg_idx = (self.neg_idx + needed) % self.neg_buffer_size

        return final_centers, final_contexts, negatives

    @staticmethod
    def _empty_batch(n_negatives):
        empty = torch.zeros(0, device="cuda", dtype=torch.int32)
        return empty, empty, torch.zeros((0, n_negatives), device="cuda", dtype=torch.int32)


# ============================================================================
# Training
# ============================================================================

# Fallback vocabulary size when block_states.txt is missing
DEFAULT_VOCAB_SIZE = 65536


def get_vocab_size(block_states_path: Optional[str] = None) -> int:
    """Read vocabulary size from block_states.txt."""
    path = (
        Path(block_states_path) if block_states_path else Path(__file__).parent.parent / "assets" / "block_states.txt"
    )
    if path.exists():
        with open(path, "r") as f:
            return sum(1 for line in f if line.strip())
    logger.warning(f"block_states.txt not found at {path}, using default vocab_size={DEFAULT_VOCAB_SIZE}")
    return DEFAULT_VOCAB_SIZE


def _linear_lr_schedule(progress: float, initial_lr: float, min_lr: float) -> float:
    """Word2Vec-style linearly decaying learning rate, clamped at min_lr."""
    return max(min_lr, initial_lr * (1.0 - progress))


def _save_embedding_checkpoint(model: "Block2Vec", save_dir: str, volumes_processed: int) -> None:
    ckpt_path = Path(save_dir) / f"block_embeddings_ckpt_{volumes_processed}.npy"
    np.save(ckpt_path, model.get_embeddings())


def train_block2vec_from_volumes(
    volumes_dir: str,
    embedding_dim: int = 128,
    epochs: int = 10,
    batch_size: int = 4096,
    negative_buffer_path: Optional[str] = None,
    initial_lr: float = 0.025,
    min_lr: float = 0.0001,
    subsample_t: float = 1e-2,
    neighbor_mode: str = "cube26",
    save_dir: Optional[str] = None,
    save_every: int = 100,
    block_states_path: Optional[str] = None,
    seed: Optional[int] = None,
):
    if seed is not None:
        np.random.seed(seed)
        torch.manual_seed(seed)

    # Discover volumes
    volumes_path = Path(volumes_dir)
    b2frames = list(volumes_path.rglob("*.b2frame"))
    if not b2frames:
        logger.error(f"No .b2frame files found in {volumes_dir}!")
        return

    # Load negative buffer
    if not negative_buffer_path or not os.path.exists(negative_buffer_path):
        raise ValueError(f"Negative sampling buffer path required! Got: {negative_buffer_path}")
    logger.info(f"Loading negative sampling buffer from {negative_buffer_path}...")
    neg_buffer_gpu = torch.load(negative_buffer_path, weights_only=True).cuda()

    # Initialize model
    vocab_size = get_vocab_size(block_states_path)
    logger.info(f"Vocab size: {vocab_size} | Volumes: {len(b2frames)} | Neighbor mode: {neighbor_mode}")
    model = Block2Vec(vocab_size=vocab_size, embedding_dim=embedding_dim).cuda()

    # Training state
    num_volumes = len(b2frames)
    total_volume_steps = epochs * num_volumes
    total_words_processed = 0
    neg_idx = 0
    lr = initial_lr

    if save_dir:
        Path(save_dir).mkdir(parents=True, exist_ok=True)

    vbar = tqdm(total=total_volume_steps, desc="Training")
    volumes_processed = 0

    for epoch in range(epochs):
        np.random.shuffle(b2frames)

        for b2_file in b2frames:
            vbar.set_description(f"Epoch {epoch + 1}/{epochs} | {b2_file.name}")

            try:
                with open(b2_file, "rb") as f:
                    volume = blosc2.unpack_array2(f.read())

                dataset = SpatialMinecraftDataset(
                    volume,
                    neg_buffer_gpu=neg_buffer_gpu,
                    neg_idx=neg_idx,
                    subsample_threshold=subsample_t,
                    neighbor_mode=neighbor_mode,
                    batch_size=batch_size,
                )
                num_batches = dataset.num_batches()

                pbar = tqdm(range(num_batches), desc=f"  Batches ({dataset.n_valid} centers)", leave=False)
                for i in pbar:
                    progress = (vbar.n + (i / num_batches)) / (total_volume_steps + 1)
                    lr = _linear_lr_schedule(progress, initial_lr, min_lr)

                    c_ids, ctx_ids, neg_ids = dataset.get_batch(i, model.n_negatives)
                    model.train_step(c_ids, ctx_ids, neg_ids, lr)
                    total_words_processed += c_ids.numel()

                    pbar.set_postfix({"lr": f"{lr:.6f}", "pairs": f"{c_ids.numel()}"})

                neg_idx = dataset.neg_idx
                vbar.set_postfix({"lr": f"{lr:.6f}", "total_pairs": f"{total_words_processed / 1e6:.1f}M"})
            except Exception as e:
                logger.error(f"Error processing volume {b2_file.name}: {e}")

            volumes_processed += 1
            vbar.update(1)

            if save_dir and volumes_processed % save_every == 0:
                _save_embedding_checkpoint(model, save_dir, volumes_processed)

    return model


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train Block2Vec embeddings from Minecraft volumes")
    parser.add_argument(
        "--volumes", type=str, default=str(Path(__file__).parent.parent / "tmp/processed_worlds/cleansed")
    )
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch_size", type=int, default=2**23)
    parser.add_argument("--neg_buffer", type=str, required=True, help="Path to pre-sampled negative IDs (.pt)")
    parser.add_argument("--lr", type=float, default=0.025, help="Initial learning rate")
    parser.add_argument("--min_lr", type=float, default=0.00001, help="Minimum learning rate")
    parser.add_argument("--subsample_t", type=float, default=1e-5, help="Subsampling threshold")
    parser.add_argument("--neighbor_mode", type=str, default="cube26", choices=["face6", "cube26"])
    parser.add_argument("--save_dir", type=str, default="tmp/checkpoints")
    parser.add_argument("--save_every", type=int, default=1000, help="Checkpoint every N volumes")
    parser.add_argument("--block_states", type=str, default=None, help="Path to block_states.txt")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility (default: unseeded)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if not Path(args.volumes).exists():
        print(f"Volumes directory {args.volumes} not found.")
        raise SystemExit(1)

    model = train_block2vec_from_volumes(
        args.volumes,
        embedding_dim=args.dim,
        epochs=args.epochs,
        batch_size=args.batch_size,
        negative_buffer_path=args.neg_buffer,
        initial_lr=args.lr,
        min_lr=args.min_lr,
        subsample_t=args.subsample_t,
        neighbor_mode=args.neighbor_mode,
        save_dir=args.save_dir,
        save_every=args.save_every,
        block_states_path=args.block_states,
        seed=args.seed,
    )

    if model:
        embeddings = model.get_embeddings()
        np.save("tmp/block_embeddings.npy", embeddings)
        print("Embeddings saved to tmp/block_embeddings.npy")

        # Quick similarity test
        states_path = (
            Path(args.block_states)
            if args.block_states
            else Path(__file__).parent.parent / "assets" / "block_states.txt"
        )
        id_to_name = {}
        if states_path.exists():
            with open(states_path, "r") as f:
                id_to_name = {i: line.strip() for i, line in enumerate(f)}

        for name, tid in {"dirt": 4, "stone": 3, "grass_block": 5, "iron_ore": 9}.items():
            if tid < model.vocab_size:
                print(f"\nSimilarity test for ID {tid}: {name}")
                for sim_name, score in model.most_similar(tid, id_to_name=id_to_name):
                    print(f"  {sim_name.replace('universal_minecraft:', '')}: {score:.4f}")
