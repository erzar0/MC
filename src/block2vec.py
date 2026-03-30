import torch
import os
import torch.nn as nn
import triton
import triton.language as tl
import numpy as np
import blosc2
from typing import Optional, Tuple
from pathlib import Path
from tqdm import tqdm

# Efficient Triton kernel for fused Skip-Gram with Negative Sampling (SGNS) update
@triton.jit
def block2vec_sgns_kernel(
    center_embeddings_ptr,    # Pointer to embedding_in (v_c)
    context_embeddings_ptr,   # Pointer to embedding_out (u_ctx / u_neg)
    center_ids_ptr,           # Pointer to center block IDs
    context_ids_ptr,          # Pointer to positive context block IDs
    negative_ids_ptr,         # Pointer to negative context block IDs
    learning_rate,            # Learning rate (alpha)
    n_elements,               # Total number of pairs in this batch
    n_negatives,              # Number of negative samples per positive sample
    embedding_dim: tl.constexpr, # Dimensionality of the embeddings
    BLOCK_SIZE_N: tl.constexpr,  # Number of pairs processed per thread block
):
    pid = tl.program_id(0)
    
    # Calculate global element indices for this thread block
    element_offsets = pid * BLOCK_SIZE_N + tl.arange(0, BLOCK_SIZE_N)
    element_mask = element_offsets < n_elements

    # Offsets for embedding dimensions
    dim_offsets = tl.arange(0, embedding_dim)

    # Load center and positive context IDs
    center_ids = tl.load(center_ids_ptr + element_offsets, mask=element_mask, other=0)
    context_ids = tl.load(context_ids_ptr + element_offsets, mask=element_mask, other=0)

    # Calculate memory offsets for embeddings
    center_emb_offsets = center_ids[:, None] * embedding_dim + dim_offsets[None, :]
    context_emb_offsets = context_ids[:, None] * embedding_dim + dim_offsets[None, :]
    
    # Load center (v_c) and positive context (u_ctx) embeddings
    center_vecs = tl.load(center_embeddings_ptr + center_emb_offsets, mask=element_mask[:, None], other=0.0)
    pos_context_vecs = tl.load(context_embeddings_ptr + context_emb_offsets, mask=element_mask[:, None], other=0.0)

    # Positive sample interaction: maximize dot product
    pos_dot_product = tl.sum(center_vecs * pos_context_vecs, axis=1)
    pos_prob = tl.sigmoid(pos_dot_product)
    
    # Gradient of log(sigmoid(dot)) wrt dot product = (1 - sigmoid(dot))
    # We negate it because we want to maximize, and SGD is subtraction: g_pos = (sigmoid - 1) * lr
    pos_grad_coeff = (pos_prob - 1.0) * learning_rate

    # Accumulate the gradient for the center vector (v_c)
    center_grad_accum = pos_grad_coeff[:, None] * pos_context_vecs
    
    # Update positive context vector (u_ctx) immediately
    tl.store(context_embeddings_ptr + context_emb_offsets, pos_context_vecs - pos_grad_coeff[:, None] * center_vecs, mask=element_mask[:, None])

    # Negative sample interactions
    for neg_idx in range(n_negatives):
        # Load negative sample ID
        neg_ids = tl.load(negative_ids_ptr + element_offsets * n_negatives + neg_idx, mask=element_mask, other=0)
        neg_emb_offsets = neg_ids[:, None] * embedding_dim + dim_offsets[None, :]
        
        # Load negative context (u_neg) embedding
        neg_context_vecs = tl.load(context_embeddings_ptr + neg_emb_offsets, mask=element_mask[:, None], other=0.0)
        
        # Negative sample interaction: minimize dot product
        neg_dot_product = tl.sum(center_vecs * neg_context_vecs, axis=1)
        neg_prob = tl.sigmoid(neg_dot_product)
        
        # g_neg = sigmoid(dot) * lr
        neg_grad_coeff = neg_prob * learning_rate

        # Accumulate the gradient for the center vector (v_c)
        center_grad_accum += neg_grad_coeff[:, None] * neg_context_vecs
        
        # Update negative context vector (u_neg) immediately
        tl.store(context_embeddings_ptr + neg_emb_offsets, neg_context_vecs - neg_grad_coeff[:, None] * center_vecs, mask=element_mask[:, None])

    # Apply all accumulated gradients to the center vector (v_c)
    tl.store(center_embeddings_ptr + center_emb_offsets, center_vecs - center_grad_accum, mask=element_mask[:, None])

class Block2Vec(nn.Module):
    def __init__(self, vocab_size: int, embedding_dim: int = 128, n_negatives: int = 5):
        super().__init__()
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.n_negatives = n_negatives
        
        # embedding_in corresponds to the center block vectors (v_c)
        self.embedding_in = nn.Parameter(torch.randn(vocab_size, embedding_dim) * 0.05)
        # embedding_out corresponds to the context/negative block vectors (u_ctx / u_neg)
        self.embedding_out = nn.Parameter(torch.zeros(vocab_size, embedding_dim))
        
    def train_step(self, center_ids, context_ids, negative_ids, learning_rate: float):
        n_elements = center_ids.numel()
        if n_elements == 0:
            return
        grid = lambda meta: (triton.cdiv(n_elements, meta['BLOCK_SIZE_N']),)
        
        block2vec_sgns_kernel[grid](
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

    def get_embeddings(self):
        return self.embedding_in.detach().cpu().numpy()

    def most_similar(self, block_id: int, top_n: int = 10, id_to_name: dict = None):
        emb = self.embedding_in[block_id].unsqueeze(0)
        norms = torch.norm(self.embedding_in, p=2, dim=1, keepdim=True)
        norm_emb = emb / (torch.norm(emb) + 1e-12)
        norm_all = self.embedding_in / (norms + 1e-12)
        
        # Check if vectors are finite before performing similarity
        if not torch.isfinite(norm_emb).all():
            return [("NaN Embeddings - Training Failed", 0.0)]
            
        sims = torch.mm(norm_emb, norm_all.t()).squeeze(0)
        top_indices = torch.topk(sims, min(top_n + 1, self.vocab_size)).indices[1:].cpu().tolist()
        
        results = []
        for idx in top_indices:
            name = id_to_name.get(idx, str(idx)) if id_to_name else str(idx)
            results.append((name, sims[idx].item()))
        return results


def compute_subsample_probs(volume: np.ndarray, threshold: float = 1e-5) -> np.ndarray:
    """Compute per-ID keep probabilities using Word2Vec-style subsampling.
    
    P(keep) = min(1, sqrt(t / f(w_i)))
    
    where t is the threshold and f(w_i) is the frequency of block ID w_i.
    
    Returns:
        Array of shape (max_id+1,) with keep probabilities for each block ID.
    """
    ids, counts = np.unique(volume, return_counts=True)
    total = volume.size
    freqs = np.zeros(ids.max() + 1, dtype=np.float64)
    freqs[ids] = counts / total
    
    keep_probs = np.ones_like(freqs)
    nonzero = freqs > 0
    keep_probs[nonzero] = np.minimum(1.0, np.sqrt(threshold / freqs[nonzero]))
    
    return keep_probs


# Pre-compute neighbor offset tensors (module-level, created once)
_FACE6_OFFSETS = None
_CUBE26_OFFSETS = None

def _get_offsets(neighbor_mode: str) -> torch.Tensor:
    """Return cached neighbor offset tensor on GPU."""
    global _FACE6_OFFSETS, _CUBE26_OFFSETS
    
    if neighbor_mode == 'face6':
        if _FACE6_OFFSETS is None:
            _FACE6_OFFSETS = torch.tensor([
                [-1, 0, 0], [1, 0, 0],
                [0, -1, 0], [0, 1, 0],
                [0, 0, -1], [0, 0, 1],
            ], device='cuda', dtype=torch.int32)
        return _FACE6_OFFSETS
    elif neighbor_mode == 'cube26':
        if _CUBE26_OFFSETS is None:
            offsets = []
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    for dz in [-1, 0, 1]:
                        if dx == 0 and dy == 0 and dz == 0:
                            continue
                        offsets.append([dx, dy, dz])
            _CUBE26_OFFSETS = torch.tensor(offsets, device='cuda', dtype=torch.int32)
        return _CUBE26_OFFSETS
    else:
        raise ValueError(f"Unknown neighbor_mode: {neighbor_mode}. Use 'face6' or 'cube26'.")


class SpatialMinecraftDataset:
    """A dataset that samples 3D spatial neighbors from Minecraft volumes for Block2Vec training.
    
    Instead of flattening to a 1D tape (which loses spatial structure), this class:
    1. Randomly samples center block positions from the 3D volume
    2. Gathers their actual 3D face-adjacent (6) or cube-adjacent (26) neighbors
    3. Applies Word2Vec-style high-frequency subsampling to discard Air/Stone/Dirt
    """
    def __init__(
        self,
        region_volume: np.ndarray,
        neg_buffer_gpu: torch.Tensor,
        neg_idx: int,
        subsample_threshold: float = 1e-5,
        neighbor_mode: str = 'face6',
        batch_size: int = 4096,
    ):
        self.shape = region_volume.shape
        self.total_size = region_volume.size
        self.neg_idx = neg_idx
        
        # Move volume to GPU as int32 for indexing
        self.gpu_volume = torch.from_numpy(region_volume.astype(np.int32)).cuda()
        
        # Reference to shared negative buffer (already on GPU, no transfer)
        self.neg_buffer = neg_buffer_gpu
        self.neg_buffer_size = self.neg_buffer.size(0)
        
        # Compute subsampling keep probabilities
        keep_probs = compute_subsample_probs(region_volume, threshold=subsample_threshold)
        self.keep_probs = torch.from_numpy(keep_probs.astype(np.float32)).cuda()
        
        # Cached neighbor offsets (shared across all datasets)
        self.offsets = _get_offsets(neighbor_mode)
        self.n_neighbors = self.offsets.shape[0]
        
        # Pre-allocate reusable coordinate buffers (oversample 3x to handle subsampling)
        oversample = batch_size * 3
        self._cx_buf = torch.empty(oversample, device='cuda', dtype=torch.int32)
        self._cz_buf = torch.empty(oversample, device='cuda', dtype=torch.int32)
        self._cy_buf = torch.empty(oversample, device='cuda', dtype=torch.int32)
        self._rand_buf = torch.empty(oversample, device='cuda', dtype=torch.float32)
        
    def sample_batch(self, batch_size: int, n_negatives: int):
        """Sample a batch of (center, context, negative) triplets using 3D spatial neighbors.
        
        1. Randomly sample center positions from the 3D volume
        2. Apply subsampling to discard high-frequency centers  
        3. For surviving centers, gather 3D neighbors as context
        4. Apply subsampling to discard high-frequency context blocks
        5. Sample negatives from the pre-computed buffer
        """
        sx, sz, sy = self.shape
        n_candidates = batch_size * 3
        
        # Fill pre-allocated buffers in-place (avoids allocation)
        torch.randint(0, sx, (n_candidates,), out=self._cx_buf)
        torch.randint(0, sz, (n_candidates,), out=self._cz_buf)
        torch.randint(0, sy, (n_candidates,), out=self._cy_buf)
        
        # Get center block IDs via advanced indexing
        center_ids = self.gpu_volume[self._cx_buf.long(), self._cz_buf.long(), self._cy_buf.long()]
        
        # Apply subsampling: keep centers with probability keep_probs[block_id]
        keep_prob = self.keep_probs[center_ids]
        torch.rand(n_candidates, out=self._rand_buf)
        keep_mask = self._rand_buf < keep_prob
        
        # Filter to kept centers (take at most batch_size)
        kept_indices = torch.where(keep_mask)[0][:batch_size]
        actual_batch_size = kept_indices.numel()
        
        if actual_batch_size == 0:
            empty = torch.zeros(0, device='cuda', dtype=torch.int32)
            return empty, empty, torch.zeros((0, n_negatives), device='cuda', dtype=torch.int32)
        
        cx = self._cx_buf[kept_indices]
        cz = self._cz_buf[kept_indices]
        cy = self._cy_buf[kept_indices]
        center_ids = center_ids[kept_indices]
        
        # Compute neighbor positions: (B, N, 3)
        positions = torch.stack([cx, cz, cy], dim=1)  # (B, 3)
        neighbor_pos = positions.unsqueeze(1) + self.offsets.unsqueeze(0)  # (B, N, 3)
        
        # Clamp to volume bounds
        neighbor_pos[:, :, 0].clamp_(0, sx - 1)
        neighbor_pos[:, :, 1].clamp_(0, sz - 1)
        neighbor_pos[:, :, 2].clamp_(0, sy - 1)
        
        # Gather neighbor block IDs
        context_ids_2d = self.gpu_volume[
            neighbor_pos[:, :, 0].long(),
            neighbor_pos[:, :, 1].long(),
            neighbor_pos[:, :, 2].long(),
        ]  # (B, N)
        
        # Apply subsampling to context blocks
        ctx_keep_prob = self.keep_probs[context_ids_2d]
        ctx_keep_mask = torch.rand_like(ctx_keep_prob) < ctx_keep_prob
        
        # Flatten and filter valid (center, context) pairs
        center_expanded = center_ids.unsqueeze(1).expand_as(context_ids_2d)
        valid_pairs = ctx_keep_mask.flatten()
        final_center_ids = center_expanded.flatten()[valid_pairs].contiguous()
        final_context_ids = context_ids_2d.flatten()[valid_pairs].contiguous()
        
        total_pairs = final_center_ids.numel()
        
        if total_pairs == 0:
            empty = torch.zeros(0, device='cuda', dtype=torch.int32)
            return empty, empty, torch.zeros((0, n_negatives), device='cuda', dtype=torch.int32)
        
        # Sample negatives from the pre-computed buffer
        needed_neg = total_pairs * n_negatives
        neg_idx_range = torch.arange(self.neg_idx, self.neg_idx + needed_neg, device='cuda') % self.neg_buffer_size
        negative_ids = self.neg_buffer[neg_idx_range].view(total_pairs, n_negatives).contiguous()
        self.neg_idx = (self.neg_idx + needed_neg) % self.neg_buffer_size
        
        return final_center_ids, final_context_ids, negative_ids


def get_vocab_size(block_states_path: Optional[str] = None) -> int:
    """Read the actual vocab size from block_states.txt."""
    if block_states_path is None:
        block_states_path = Path(__file__).parent.parent / "assets" / "block_states.txt"
    else:
        block_states_path = Path(block_states_path)
    
    if block_states_path.exists():
        with open(block_states_path, "r") as f:
            count = sum(1 for line in f if line.strip())
        return count
    
    print(f"Warning: block_states.txt not found at {block_states_path}, using default vocab_size=65536")
    return 65536


def train_block2vec_from_volumes(
    volumes_dir: str, 
    embedding_dim: int = 128, 
    epochs: int = 10, 
    batch_size: int = 4096, 
    negative_buffer_path: Optional[str] = None,
    initial_lr: float = 0.025,
    min_lr: float = 0.0001,
    subsample_t: float = 1e-5,
    neighbor_mode: str = 'cube26',
    save_dir: Optional[str] = None,
    save_every: int = 100,
    block_states_path: Optional[str] = None,
):
    volumes_path = Path(volumes_dir)
    b2frames = list(volumes_path.rglob("*.b2frame"))
    
    if not b2frames:
        print(f"No .b2frame files found in {volumes_dir}!")
        return
    
    if not negative_buffer_path or not os.path.exists(negative_buffer_path):
        raise ValueError(f"Negative sampling buffer path required! Got: {negative_buffer_path}")
    
    print(f"Loading negative sampling buffer from {negative_buffer_path}...")
    negative_buffer = torch.load(negative_buffer_path, weights_only=True)

    # Determine vocab size from block_states.txt
    vocab_size = get_vocab_size(block_states_path)
    print(f"Vocab size: {vocab_size} (from block_states.txt)")

    print(f"Found {len(b2frames)} volumes. Initializing model...")
    print(f"  Neighbor mode: {neighbor_mode}")
    print(f"  Subsampling threshold: {subsample_t}")
    model = Block2Vec(vocab_size=vocab_size, embedding_dim=embedding_dim).cuda()
    lr = initial_lr
    
    num_volumes = len(b2frames)
    total_volume_steps = epochs * num_volumes
    total_words_processed = 0
    
    # Move negative buffer to GPU once (shared across all volumes)
    neg_buffer_gpu = negative_buffer.cuda()
    neg_idx = 0  # Persistent negative buffer index across volumes
    
    # Setup checkpoint directory
    if save_dir:
        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)
    
    vbar = tqdm(total=total_volume_steps, desc="Training")
    volumes_processed = 0
    
    for epoch in range(epochs):
        # Shuffle volumes each epoch
        np.random.shuffle(b2frames)
        
        for b2_file in b2frames:
            vbar.set_description(f"Epoch {epoch+1}/{epochs} | {b2_file.name}")
            
            try:
                # Load volume from blosc2 frame
                with open(b2_file, "rb") as f:
                    volume = blosc2.unpack_array2(f.read())
                
                # Pass shared GPU neg buffer + persistent neg_idx
                dataset = SpatialMinecraftDataset(
                    volume,
                    neg_buffer_gpu=neg_buffer_gpu,
                    neg_idx=neg_idx,
                    subsample_threshold=subsample_t,
                    neighbor_mode=neighbor_mode,
                    batch_size=batch_size,
                )
                
                num_batches = max(1, dataset.total_size // batch_size)
                
                pbar = tqdm(range(num_batches), desc="  Batches", leave=False)
                for i in pbar:
                    # Linear learning rate scheduling (Word2Vec "progress" variable)
                    progress = (vbar.n + (i / num_batches)) / (total_volume_steps + 1)
                    lr = max(min_lr, initial_lr * (1.0 - progress))
                    
                    c_ids, ctx_ids, neg_ids = dataset.sample_batch(batch_size, model.n_negatives)
                    model.train_step(c_ids, ctx_ids, neg_ids, lr)
                    total_words_processed += c_ids.numel()
                    
                    pbar.set_postfix({"lr": f"{lr:.6f}", "pairs": f"{c_ids.numel()}"})
                
                # Carry forward the neg_idx for the next volume
                neg_idx = dataset.neg_idx
                
                vbar.set_postfix({"lr": f"{lr:.6f}", "total_pairs": f"{total_words_processed/1e6:.1f}M"})
            except Exception as e:
                print(f"\nError processing volume {b2_file.name}: {e}")
            
            volumes_processed += 1
            vbar.update(1)
            
            # Periodic checkpoint saving
            if save_dir and volumes_processed % save_every == 0:
                ckpt_path = Path(save_dir) / f"block_embeddings_ckpt_{volumes_processed}.npy"
                np.save(ckpt_path, model.get_embeddings())
                tqdm.write(f"  Checkpoint saved: {ckpt_path}")
                
    return model

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--volumes", type=str, default="/home/kyre/repos/minecraft-world-generator/tmp/processed_worlds/cleansed/")
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--epochs", type=int, default=1)   
    parser.add_argument("--batch_size", type=int, default=2**23)
    parser.add_argument("--neg_buffer", type=str, required=True, help="Path to pre-sampled negative IDs (.pt file)")
    parser.add_argument("--lr", type=float, default=0.025, help="Initial learning rate")
    parser.add_argument("--min_lr", type=float, default=0.0001, help="Minimum learning rate")
    parser.add_argument("--subsample_t", type=float, default=1e-5, help="Subsampling threshold (higher = more aggressive)")
    parser.add_argument("--neighbor_mode", type=str, default="cube26", choices=["face6", "cube26"], help="3D neighbor sampling mode")
    parser.add_argument("--save_dir", type=str, default="tmp/checkpoints", help="Directory to save checkpoints")
    parser.add_argument("--save_every", type=int, default=10, help="Save checkpoint every N volumes")
    parser.add_argument("--block_states", type=str, default=None, help="Path to block_states.txt")
    args = parser.parse_args()

    if Path(args.volumes).exists():
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
        )
        if model:
            embeddings = model.get_embeddings()
            np.save("tmp/block_embeddings.npy", embeddings)
            print("Embeddings saved to tmp/block_embeddings.npy")
            
            # Simple test similarity
            block_states_path = Path(args.block_states) if args.block_states else Path(__file__).parent.parent / "assets" / "block_states.txt"
            id_to_name = {}
            if block_states_path.exists():
                with open(block_states_path, "r") as f:
                    for i, line in enumerate(f):
                        id_to_name[i] = line.strip()
            
            test_ids = {"dirt": 4, "stone": 3, "grass_block": 5, "iron_ore": 9}
            for name, tid in test_ids.items():
                if tid < model.vocab_size:
                    print(f"\nSimilarity test for ID {tid}: {name}")
                    sims = model.most_similar(tid, id_to_name=id_to_name)
                    for sim_name, score in sims:
                        display_name = sim_name.replace("universal_minecraft:", "")
                        print(f"  {display_name}: {score:.4f}")
    else:
        print(f"Volumes directory {args.volumes} not found.")
