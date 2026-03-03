import torch
import time
import numpy as np
from src.block2vec import Block2Vec, MinecraftDataset

def benchmark_volume_processing():
    # Benchmark parameters
    embedding_dim = 128
    vocab_size = 65536
    n_negatives = 5
    window_size = 2
    warmup_steps = 20
    
    # Standard Minecraft Region Volume (512 x 256 x 512 = 67.1M blocks)
    # We will measure how long it takes to process this entire "tape" once.
    TOTAL_VOLUME_SIZE = 512 * 256 * 512
    
    # Batch sizes to test (powers of 2)
    # Note: batch_size here is 'center blocks per batch'
    batch_sizes = [2 ** i for i in range(10, 20)]
    
    print(f"--- Volume Processing Speed Test ---")
    print(f"Target: One Full Standard Region ({TOTAL_VOLUME_SIZE/1e6:.1f}M blocks)")
    print(f"GPU: {torch.cuda.get_device_name(0)}\n")
    
    # Create fixed synthetic data with Power Law distribution
    ids = np.arange(1, 1000)
    probs = 1 / ids
    probs /= probs.sum()
    dummy_volume = np.random.choice(ids, size=TOTAL_VOLUME_SIZE, p=probs).astype(np.uint16)
    
    # Synthetic negative buffer (100M)
    buffer_size = 100_000_000
    dummy_negative_buffer = torch.from_numpy(np.random.choice(ids, size=buffer_size, p=probs).astype(np.int32)).cuda()
    
    model = Block2Vec(vocab_size=vocab_size, embedding_dim=embedding_dim, n_negatives=n_negatives).cuda()
    dataset = MinecraftDataset(dummy_volume, dummy_negative_buffer, window_size=window_size)
    
    lr = 0.025
    results = []

    print(f"{'Batch Size':>10} | {'Num Batches':>12} | {'Time/Volume':>15} | {'Throughput':>15}")
    print("-" * 65)

    for bs in batch_sizes:
        num_batches = TOTAL_VOLUME_SIZE // bs
        if num_batches == 0:
            continue

        # Warmup
        for _ in range(warmup_steps):
            c_ids, ctx_ids, neg_ids = dataset.sample_batch(bs, n_negatives)
            model.train_step(c_ids, ctx_ids, neg_ids, lr)
        
        torch.cuda.synchronize()
        start_time = time.perf_counter()
        
        # Process the entire virtual volume
        total_pairs = 0
        for _ in range(num_batches):
            c_ids, ctx_ids, neg_ids = dataset.sample_batch(bs, n_negatives)
            model.train_step(c_ids, ctx_ids, neg_ids, lr)
            total_pairs += c_ids.numel()
            
        torch.cuda.synchronize()
        end_time = time.perf_counter()
        
        duration = end_time - start_time
        pairs_per_sec = total_pairs / duration
        
        print(f"{bs:10d} | {num_batches:12d} | {duration:13.2f}s | {pairs_per_sec/1e6:13.1f}M/s")
        results.append((bs, duration))

    best_bs, best_duration = min(results, key=lambda x: x[1])
    print("-" * 65)
    print(f"Optimal Batch Size: {best_bs}")
    print(f"Fastest region processing time: {best_duration:.2f} seconds.")

if __name__ == "__main__":
    with torch.cuda.device(0):
        benchmark_volume_processing()
