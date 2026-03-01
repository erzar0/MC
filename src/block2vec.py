import torch
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
        
        # Gradient of log(sigmoid(-dot)) wrt dot product = -sigmoid(dot)
        # We negate it for gradient descent: g_neg = sigmoid(dot) * lr
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
        norm_emb = emb / (torch.norm(emb) + 1e-9)
        norm_all = self.embedding_in / (norms + 1e-9)
        
        sims = torch.mm(norm_emb, norm_all.t()).squeeze(0)
        top_indices = torch.topk(sims, top_n + 1).indices[1:].cpu().tolist()
        
        results = []
        for idx in top_indices:
            name = id_to_name.get(idx, str(idx)) if id_to_name else str(idx)
            results.append((name, sims[idx].item()))
        return results

class MinecraftDataset:
    """
    A dataset that samples 3D context from Minecraft regions for Block2Vec training.
    """
    def __init__(self, region_volume: np.ndarray, window_size: int = 2):
        self.volume = region_volume  # Shape: (X, Y, Z)
        self.window_size = window_size
        self.dim_x, self.dim_y, self.dim_z = region_volume.shape
        
        # Calculate block ID frequencies
        unique_ids, counts = np.unique(region_volume, return_counts=True)
        self.vocab_counts = dict(zip(unique_ids, counts))
        # Keep vocab size large enough for any potential block ID
        self.vocab_size = max(unique_ids) + 1 if len(unique_ids) > 0 else 65536
        
        # Word2Vec negative sampling distribution (frequency ^ 0.75)
        neg_sample_probs = np.zeros(self.vocab_size)
        for block_id, count in self.vocab_counts.items():
            if block_id < self.vocab_size:
                neg_sample_probs[block_id] = count ** 0.75
        neg_sample_probs /= neg_sample_probs.sum()
        self.neg_sample_table = neg_sample_probs

    def sample_batch(self, batch_size: int, n_negatives: int):
        ws = self.window_size
        
        # Randomly select center block coordinates, avoiding the boundaries
        center_x = np.random.randint(ws, self.dim_x - ws, batch_size)
        center_y = np.random.randint(ws, self.dim_y - ws, batch_size)
        center_z = np.random.randint(ws, self.dim_z - ws, batch_size)
        
        center_ids = self.volume[center_x, center_y, center_z]
        
        # Randomly determine context offset within the window size
        offset_x = np.random.randint(-ws, ws + 1, batch_size)
        offset_y = np.random.randint(-ws, ws + 1, batch_size)
        offset_z = np.random.randint(-ws, ws + 1, batch_size)
        
        # Ensure we don't pick the center block as its own context
        zero_offset_mask = (offset_x == 0) & (offset_y == 0) & (offset_z == 0)
        offset_x[zero_offset_mask] = 1 
        
        context_ids = self.volume[center_x + offset_x, center_y + offset_y, center_z + offset_z]
        
        # Sample negative context blocks based on the negative sampling distribution
        negative_ids = np.random.choice(
            self.vocab_size, 
            size=(batch_size, n_negatives), 
            p=self.neg_sample_table
        )
        
        return (
            torch.from_numpy(center_ids.astype(np.int32)).cuda(),
            torch.from_numpy(context_ids.astype(np.int32)).cuda(),
            torch.from_numpy(negative_ids.astype(np.int32)).cuda()
        )

def train_block2vec_from_volumes(volumes_dir: str, embedding_dim: int = 128, epochs: int = 10, batch_size: int = 4096):
    volumes_path = Path(volumes_dir)
    b2frames = list(volumes_path.glob("*.b2frame"))
    
    if not b2frames:
        print(f"No .b2frame files found in {volumes_dir}!")
        return
    
    print(f"Found {len(b2frames)} volumes. Initializing model...")
    model = Block2Vec(vocab_size=65536, embedding_dim=embedding_dim).cuda()
    lr = 0.025
    
    for epoch in range(epochs):
        print(f"Epoch {epoch+1}/{epochs}")
        total_batches_per_volume = 500
        
        # Shuffle volumes each epoch
        np.random.shuffle(b2frames)
        
        for b2_file in b2frames:
            try:
                # Load volume from blosc2 frame
                with open(b2_file, "rb") as f:
                    data = f.read()
                volume = blosc2.unpack_array2(data)
                
                dataset = MinecraftDataset(volume)
                
                pbar = tqdm(range(total_batches_per_volume), desc=f"Volume {b2_file.name}")
                for _ in pbar:
                    c_ids, ctx_ids, neg_ids = dataset.sample_batch(batch_size, model.n_negatives)
                    model.train_step(c_ids, ctx_ids, neg_ids, lr)
                    
                    # Decay learning rate
                    lr = max(0.0001, lr * 0.99999)
                    pbar.set_postfix({"lr": f"{lr:.6f}"})
            except Exception as e:
                print(f"Error processing volume {b2_file}: {e}")
                
    return model

def train_block2vec(world_path: str, embedding_dim: int = 128, epochs: int = 10, batch_size: int = 1024):
    from src.world_wrapper import WorldWrapper
    from pathlib import Path
    
    wrapper = WorldWrapper(Path(world_path))
    regions = wrapper.mca_coords()
    
    if not regions:
        print("No regions found in world!")
        return
    
    model = Block2Vec(vocab_size=65536, embedding_dim=embedding_dim).cuda()
    lr = 0.025
    
    for epoch in range(epochs):
        print(f"Epoch {epoch+1}/{epochs}")
        total_batches = 1000  
        
        for rx, rz in regions:
            try:
                volume = wrapper.get_region_volume(rx, rz)
                dataset = MinecraftDataset(volume)
                
                pbar = tqdm(range(total_batches), desc=f"Region ({rx}, {rz})")
                for _ in pbar:
                    c_ids, ctx_ids, neg_ids = dataset.sample_batch(batch_size, model.n_negatives)
                    model.train_step(c_ids, ctx_ids, neg_ids, lr)
                    lr = max(0.0001, lr * 0.99999)
                    pbar.set_postfix({"lr": f"{lr:.6f}"})
            except Exception as e:
                print(f"Error processing region {rx}, {rz}: {e}")
                
    return model

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--volumes", type=str, default="/home/kyre/repos/minecraft-world-generator/tmp/processed_worlds/cleansed/greenfield/volumes")
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--epochs", type=int, default=5)
    args = parser.parse_args()

    if Path(args.volumes).exists():
        model = train_block2vec_from_volumes(args.volumes, embedding_dim=args.dim, epochs=args.epochs)
        if model:
            embeddings = model.get_embeddings()
            np.save("block_embeddings.npy", embeddings)
            print("Embeddings saved to block_embeddings.npy")
            
            # Simple test similarity
            # Assuming 1 is stone, 2 is grass, etc. but it depends on the world mapping
            # Just showing it works
            print("\nSimilarity test for ID 1:")
            sims = model.most_similar(1)
            for name, score in sims:
                print(f"  {name}: {score:.4f}")
    else:
        print(f"Volumes directory {args.volumes} not found.")
