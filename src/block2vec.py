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

class MinecraftDataset:
    """
    A dataset that samples sequential pairs from flattened Minecraft volumes for Block2Vec training.
    """
    def __init__(self, region_volume: np.ndarray, negative_buffer: torch.Tensor, window_size: int = 2, flatten_order: str = 'C'):
        # Flatten the volume to treat it as a sequential tape of blocks
        self.total_size = region_volume.size
        self.window_size = window_size
        self.current_idx = 0
        
        # Flatten the volume and strictly keep as uint16
        # 'C' order = Y-fast (vertical), 'F' order = X-fast (horizontal)
        flat_vol_np = region_volume.flatten(order=flatten_order).astype(np.uint16)
        # Move to GPU as int32 tensor (PyTorch requirement for indexing)
        self.flat_volume = torch.from_numpy(flat_vol_np.astype(np.int32)).cuda()
        
        # Negative sampling (strictly pre-sampled frequency-based)
        self.shuffled_volume = negative_buffer.cuda()
        self.buffer_size = self.shuffled_volume.size(0)
        self.shuffled_idx = 0
        
        # Pre-compute window offsets as a tensor
        self.offsets = torch.tensor(
            [i for i in range(-self.window_size, self.window_size + 1) if i != 0], 
            device='cuda', 
            dtype=torch.int32
        )

    def sample_batch(self, batch_size: int, n_negatives: int):
        # 1. Center Blocks
        center_idx_range = torch.arange(self.current_idx, self.current_idx + batch_size, device='cuda') % self.total_size
        center_ids = self.flat_volume[center_idx_range]
        
        # 2. Context Blocks
        # Offsets + Center Indices with modulo bounds checking natively on GPU
        ctx_idx_ranges = (center_idx_range.unsqueeze(0) + self.offsets.unsqueeze(1)) % self.total_size
        final_context_ids = self.flat_volume[ctx_idx_ranges].flatten().contiguous()
        final_center_ids = center_ids.repeat(len(self.offsets)).contiguous()
        
        self.current_idx = (self.current_idx + batch_size) % self.total_size
        
        # 3. Negative Samples
        total_pairs = batch_size * len(self.offsets)
        needed_neg = total_pairs * n_negatives
        neg_idx_range = torch.arange(self.shuffled_idx, self.shuffled_idx + needed_neg, device='cuda') % self.buffer_size
        negative_ids = self.shuffled_volume[neg_idx_range].view(total_pairs, n_negatives).contiguous()
        
        self.shuffled_idx = (self.shuffled_idx + needed_neg) % self.buffer_size
        
        return final_center_ids, final_context_ids, negative_ids

def train_block2vec_from_volumes(volumes_dir: str, embedding_dim: int = 128, epochs: int = 10, batch_size: int = 4096, window_size: int = 2, negative_buffer_path: Optional[str] = None):
    volumes_path = Path(volumes_dir)
    b2frames = list(volumes_path.rglob("*.b2frame"))
    
    if not b2frames:
        print(f"No .b2frame files found in {volumes_dir}!")
        return
    
    if not negative_buffer_path or not os.path.exists(negative_buffer_path):
        raise ValueError(f"Negative sampling buffer path required! Got: {negative_buffer_path}")
    
    print(f"Loading negative sampling buffer from {negative_buffer_path}...")
    negative_buffer = torch.load(negative_buffer_path)

    print(f"Found {len(b2frames)} volumes. Initializing model...")
    model = Block2Vec(vocab_size=65536, embedding_dim=embedding_dim).cuda()
    initial_lr = 0.025
    num_volumes = len(b2frames)
    
    total_volume_steps = epochs * num_volumes
    vbar = tqdm(total=total_volume_steps, desc="Training")
    
    for epoch in range(epochs):
        # Shuffle volumes each epoch
        np.random.shuffle(b2frames)
        
        for b2_file in b2frames:
            # Exponential decay based only on the number of volumes processed across all epochs
            progress = vbar.n / total_volume_steps
            lr = max(0.0001, initial_lr * (0.01 ** progress))
            
            # Randomly alternate between 'C' (Vertical/Y-fast) and 'F' (Horizontal/X-fast) flattening
            order = np.random.choice(['C', 'F'])
            vbar.set_description(f"Epoch {epoch+1}/{epochs} | {b2_file.name} [{order}]")
            vbar.set_postfix({"lr": f"{lr:.6f}"})
            
            try:
                # Load volume from blosc2 frame
                with open(b2_file, "rb") as f:
                    volume = blosc2.unpack_array2(f.read())
                
                dataset = MinecraftDataset(volume, window_size=window_size, negative_buffer=negative_buffer, flatten_order=order)
                num_batches = dataset.total_size // batch_size
                
                if num_batches > 0:
                    # Provide inner visual feedback for current volume progress
                    pbar = tqdm(range(num_batches), desc="  Batches", leave=False)
                    for _ in pbar:
                        c_ids, ctx_ids, neg_ids = dataset.sample_batch(batch_size, model.n_negatives)
                        model.train_step(c_ids, ctx_ids, neg_ids, lr)
                        pbar.set_postfix({"lr": f"{lr:.6f}"})
            except Exception as e:
                print(f"\nError processing volume {b2_file.name}: {e}")
            
            vbar.update(1)
                
    return model

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--volumes", type=str, default="/home/kyre/repos/minecraft-world-generator/tmp/processed_worlds/cleansed/")
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--window", type=int, default=2)
    parser.add_argument("--epochs", type=int, default=1)   
    parser.add_argument("--batch_size", type=int, default=4096)
    parser.add_argument("--neg_buffer", type=str, required=True, help="Path to pre-sampled negative IDs (.pt file)")
    args = parser.parse_args()

    if Path(args.volumes).exists():
        model = train_block2vec_from_volumes(
            args.volumes, 
            embedding_dim=args.dim, 
            epochs=args.epochs,
            window_size=args.window,
            batch_size=args.batch_size,
            negative_buffer_path=args.neg_buffer
        )
        if model:
            embeddings = model.get_embeddings()
            np.save("tmp/block_embeddings.npy", embeddings)
            print("Embeddings saved to tmp/block_embeddings.npy")
            
            # Simple test similarity
            print("\nSimilarity test for ID 11: dirt")
            sims = model.most_similar(11)
            for name, score in sims:
                print(f"  {name}: {score:.4f}")
    else:
        print(f"Volumes directory {args.volumes} not found.")
