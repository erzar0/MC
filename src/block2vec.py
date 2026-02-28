import torch
import torch.nn as nn
import triton
import triton.language as tl
import numpy as np
import blosc2
from typing import Optional, Tuple
from pathlib import Path
from tqdm import tqdm

# Efficient Triton kernel for fused SGNS update
@triton.jit
def block2vec_sgns_kernel(
    embedding_in_ptr,
    embedding_out_ptr,
    center_ids_ptr,
    context_ids_ptr,
    negative_ids_ptr,
    learning_rate,
    n_elements,
    n_negatives,
    embedding_dim: tl.constexpr,
    BLOCK_SIZE_N: tl.constexpr,
):
    pid = tl.program_id(0)
    offsets_n = pid * BLOCK_SIZE_N + tl.arange(0, BLOCK_SIZE_N)
    mask_n = offsets_n < n_elements

    offsets_d = tl.arange(0, embedding_dim)

    c_ids = tl.load(center_ids_ptr + offsets_n, mask=mask_n, other=0)
    ctx_ids = tl.load(context_ids_ptr + offsets_n, mask=mask_n, other=0)

    c_offs = c_ids[:, None] * embedding_dim + offsets_d[None, :]
    ctx_offs = ctx_ids[:, None] * embedding_dim + offsets_d[None, :]
    
    v_c = tl.load(embedding_in_ptr + c_offs, mask=mask_n[:, None], other=0.0)
    u_ctx = tl.load(embedding_out_ptr + ctx_offs, mask=mask_n[:, None], other=0.0)

    pos_dot = tl.sum(v_c * u_ctx, axis=1)
    pos_sigm = tl.sigmoid(pos_dot)
    g_pos = (pos_sigm - 1.0) * learning_rate

    grad_vc = g_pos[:, None] * u_ctx
    tl.store(embedding_out_ptr + ctx_offs, u_ctx - g_pos[:, None] * v_c, mask=mask_n[:, None])

    for k in range(n_negatives):
        neg_ids = tl.load(negative_ids_ptr + offsets_n * n_negatives + k, mask=mask_n, other=0)
        neg_offs = neg_ids[:, None] * embedding_dim + offsets_d[None, :]
        
        u_neg = tl.load(embedding_out_ptr + neg_offs, mask=mask_n[:, None], other=0.0)
        
        neg_dot = tl.sum(v_c * u_neg, axis=1)
        neg_sigm = tl.sigmoid(neg_dot)
        g_neg = neg_sigm * learning_rate

        grad_vc += g_neg[:, None] * u_neg
        tl.store(embedding_out_ptr + neg_offs, u_neg - g_neg[:, None] * v_c, mask=mask_n[:, None])

    tl.store(embedding_in_ptr + c_offs, v_c - grad_vc, mask=mask_n[:, None])

class Block2Vec(nn.Module):
    def __init__(self, vocab_size: int, embedding_dim: int = 128, n_negatives: int = 5):
        super().__init__()
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.n_negatives = n_negatives
        
        self.embedding_in = nn.Parameter(torch.randn(vocab_size, embedding_dim) * 0.05)
        self.embedding_out = nn.Parameter(torch.zeros(vocab_size, embedding_dim))
        
    def train_step(self, center_ids, context_ids, negative_ids, lr: float):
        n_elements = center_ids.numel()
        grid = lambda meta: (triton.cdiv(n_elements, meta['BLOCK_SIZE_N']),)
        
        block2vec_sgns_kernel[grid](
            self.embedding_in,
            self.embedding_out,
            center_ids,
            context_ids,
            negative_ids,
            lr,
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
        
        unique, counts = np.unique(region_volume, return_counts=True)
        self.vocab_counts = dict(zip(unique, counts))
        self.vocab_size = max(unique) + 1 if len(unique) > 0 else 65536
        
        probs = np.zeros(self.vocab_size)
        for i, c in self.vocab_counts.items():
            if i < self.vocab_size:
                probs[i] = c ** 0.75
        probs /= probs.sum()
        self.neg_table = probs

    def sample_batch(self, batch_size: int, n_negatives: int):
        ws = self.window_size
        xs = np.random.randint(ws, self.dim_x - ws, batch_size)
        ys = np.random.randint(ws, self.dim_y - ws, batch_size)
        zs = np.random.randint(ws, self.dim_z - ws, batch_size)
        
        center_ids = self.volume[xs, ys, zs]
        
        dx = np.random.randint(-ws, ws + 1, batch_size)
        dy = np.random.randint(-ws, ws + 1, batch_size)
        dz = np.random.randint(-ws, ws + 1, batch_size)
        
        mask_zeros = (dx == 0) & (dy == 0) & (dz == 0)
        dx[mask_zeros] = 1 
        
        context_ids = self.volume[xs + dx, ys + dy, zs + dz]
        
        negative_ids = np.random.choice(
            self.vocab_size, 
            size=(batch_size, n_negatives), 
            p=self.neg_table
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
