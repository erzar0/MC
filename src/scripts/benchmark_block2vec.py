import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import torch
import time
import numpy as np
from tqdm import tqdm
from block2vec import Block2Vec, SpatialMinecraftDataset

def benchmark_volume_processing():
    embedding_dim = 128
    vocab_size = 8320
    n_negatives = 5
    warmup_steps = 20
    
    VOLUME_SHAPE = (512, 256, 512)
    TOTAL_VOLUME_SIZE = VOLUME_SHAPE[0] * VOLUME_SHAPE[1] * VOLUME_SHAPE[2]
    
    batch_sizes = [2 ** i for i in range(16, 29)]
    
    print(f"--- Volume Processing Speed Test (3D Spatial Sampling) ---")
    print(f"Target: One Full Standard Region ({TOTAL_VOLUME_SIZE/1e6:.1f}M blocks)")
    print(f"GPU: {torch.cuda.get_device_name(0)}\n")
    
    ids = np.arange(1, 1000)
    probs = 1 / ids
    probs /= probs.sum()
    dummy_volume = np.random.choice(ids, size=TOTAL_VOLUME_SIZE, p=probs).astype(np.uint16).reshape(VOLUME_SHAPE)
    
    buffer_size = 100_000_000
    neg_buffer_gpu = torch.from_numpy(np.random.choice(ids, size=buffer_size, p=probs).astype(np.int32)).cuda()
    
    model = Block2Vec(vocab_size=vocab_size, embedding_dim=embedding_dim, n_negatives=n_negatives).cuda()
    
    lr = 0.025
    results = []

    print(f"{'Batch Size':>10} | {'Num Batches':>12} | {'Time/Volume':>15} | {'Throughput':>15} ")
    print("-" * 65)

    for bs in batch_sizes:
        dataset = SpatialMinecraftDataset(
            dummy_volume,
            neg_buffer_gpu=neg_buffer_gpu,
            neg_idx=0,
            subsample_threshold=1e-5,
            neighbor_mode='face6',
            batch_size=bs,
        )
        
        num_batches = dataset.num_batches()
        if num_batches == 0:
            continue

        # Warmup
        for i in tqdm(range(min(warmup_steps, num_batches)), desc=f"  Warmup bs={bs}", leave=False):
            c_ids, ctx_ids, neg_ids = dataset.get_batch(i, n_negatives)
            model.train_step(c_ids, ctx_ids, neg_ids, lr)
        
        # Re-create dataset to reset state for a clean benchmark run
        dataset = SpatialMinecraftDataset(
            dummy_volume,
            neg_buffer_gpu=neg_buffer_gpu,
            neg_idx=0,
            subsample_threshold=1e-5,
            neighbor_mode='face6',
            batch_size=bs,
        )
        num_batches = dataset.num_batches()
        
        torch.cuda.synchronize()
        start_time = time.perf_counter()
        
        total_pairs = 0
        for i in tqdm(range(num_batches), desc=f"  Bench  bs={bs}", leave=False):
            c_ids, ctx_ids, neg_ids = dataset.get_batch(i, n_negatives)
            model.train_step(c_ids, ctx_ids, neg_ids, lr)
            total_pairs += c_ids.numel()
            
        torch.cuda.synchronize()
        end_time = time.perf_counter()
        
        duration = end_time - start_time
        pairs_per_sec = total_pairs / duration if duration > 0 else 0
        
        print(f"{bs:10d} | {num_batches:12d} | {duration:13.2f}s | {pairs_per_sec/1e6:13.1f}M/s")
        results.append((bs, duration))

    if results:
        best_bs, best_duration = min(results, key=lambda x: x[1])
        print("-" * 65)
        print(f"Optimal Batch Size: {best_bs}")
        print(f"Fastest region processing time: {best_duration:.2f} seconds.")

if __name__ == "__main__":
    with torch.cuda.device(0):
        benchmark_volume_processing()
