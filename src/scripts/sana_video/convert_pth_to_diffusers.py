"""Converts a SANA-Video 2B 480p .pth checkpoint (train_ivjoint.py output) to diffusers format.

Key remap based on diffusers' scripts/convert_sana_video_to_diffusers.py. The
output directory can be passed to inference.py / generate_world.py via
--transformer_path.

Usage:
    python src/scripts/sana_video/convert_pth_to_diffusers.py \
        --pth tmp/sana_video_ivjoint/checkpoints/latest.pth \
        --output tmp/sana_video_ivjoint/transformer_diffusers [--dtype bf16]
"""

import argparse

import torch
from diffusers import SanaVideoTransformer3DModel

NUM_LAYERS = 20

TRANSFORMER_KWARGS = {
    "in_channels": 16,
    "out_channels": 16,
    "num_attention_heads": 20,
    "attention_head_dim": 112,
    "num_layers": NUM_LAYERS,
    "num_cross_attention_heads": 20,
    "cross_attention_head_dim": 112,
    "cross_attention_dim": 2240,
    "caption_channels": 2304,
    "mlp_ratio": 3.0,
    "attention_bias": False,
    "sample_size": 30,
    "patch_size": (1, 2, 2),
    "norm_elementwise_affine": False,
    "norm_eps": 1e-6,
    "qk_norm": "rms_norm_across_heads",
    "rope_max_seq_len": 1024,
}

DTYPE = {"fp32": torch.float32, "fp16": torch.float16, "bf16": torch.bfloat16}


def convert_state_dict(state_dict: dict) -> dict:
    """Remaps upstream Sana .pth keys to diffusers SanaVideoTransformer3DModel keys."""
    sd = dict(state_dict)  # copy so pops don't mutate the caller's dict
    out = {}

    out["patch_embedding.weight"] = sd.pop("x_embedder.proj.weight")
    out["patch_embedding.bias"] = sd.pop("x_embedder.proj.bias")

    out["caption_projection.linear_1.weight"] = sd.pop("y_embedder.y_proj.fc1.weight")
    out["caption_projection.linear_1.bias"] = sd.pop("y_embedder.y_proj.fc1.bias")
    out["caption_projection.linear_2.weight"] = sd.pop("y_embedder.y_proj.fc2.weight")
    out["caption_projection.linear_2.bias"] = sd.pop("y_embedder.y_proj.fc2.bias")

    out["time_embed.emb.timestep_embedder.linear_1.weight"] = sd.pop("t_embedder.mlp.0.weight")
    out["time_embed.emb.timestep_embedder.linear_1.bias"] = sd.pop("t_embedder.mlp.0.bias")
    out["time_embed.emb.timestep_embedder.linear_2.weight"] = sd.pop("t_embedder.mlp.2.weight")
    out["time_embed.emb.timestep_embedder.linear_2.bias"] = sd.pop("t_embedder.mlp.2.bias")

    out["time_embed.linear.weight"] = sd.pop("t_block.1.weight")
    out["time_embed.linear.bias"] = sd.pop("t_block.1.bias")

    out["caption_norm.weight"] = sd.pop("attention_y_norm.weight")

    for depth in range(NUM_LAYERS):
        out[f"transformer_blocks.{depth}.scale_shift_table"] = sd.pop(f"blocks.{depth}.scale_shift_table")

        # Self attention: fused qkv -> separate projections
        q, k, v = torch.chunk(sd.pop(f"blocks.{depth}.attn.qkv.weight"), 3, dim=0)
        out[f"transformer_blocks.{depth}.attn1.to_q.weight"] = q
        out[f"transformer_blocks.{depth}.attn1.to_k.weight"] = k
        out[f"transformer_blocks.{depth}.attn1.to_v.weight"] = v
        out[f"transformer_blocks.{depth}.attn1.norm_q.weight"] = sd.pop(f"blocks.{depth}.attn.q_norm.weight")
        out[f"transformer_blocks.{depth}.attn1.norm_k.weight"] = sd.pop(f"blocks.{depth}.attn.k_norm.weight")
        out[f"transformer_blocks.{depth}.attn1.to_out.0.weight"] = sd.pop(f"blocks.{depth}.attn.proj.weight")
        out[f"transformer_blocks.{depth}.attn1.to_out.0.bias"] = sd.pop(f"blocks.{depth}.attn.proj.bias")

        # Feed-forward (GLUMBConvTemp)
        out[f"transformer_blocks.{depth}.ff.conv_inverted.weight"] = sd.pop(
            f"blocks.{depth}.mlp.inverted_conv.conv.weight"
        )
        out[f"transformer_blocks.{depth}.ff.conv_inverted.bias"] = sd.pop(f"blocks.{depth}.mlp.inverted_conv.conv.bias")
        out[f"transformer_blocks.{depth}.ff.conv_depth.weight"] = sd.pop(f"blocks.{depth}.mlp.depth_conv.conv.weight")
        out[f"transformer_blocks.{depth}.ff.conv_depth.bias"] = sd.pop(f"blocks.{depth}.mlp.depth_conv.conv.bias")
        out[f"transformer_blocks.{depth}.ff.conv_point.weight"] = sd.pop(f"blocks.{depth}.mlp.point_conv.conv.weight")
        out[f"transformer_blocks.{depth}.ff.conv_temp.weight"] = sd.pop(f"blocks.{depth}.mlp.t_conv.weight")

        # Cross-attention: fused kv -> separate projections
        out[f"transformer_blocks.{depth}.attn2.to_q.weight"] = sd.pop(f"blocks.{depth}.cross_attn.q_linear.weight")
        out[f"transformer_blocks.{depth}.attn2.to_q.bias"] = sd.pop(f"blocks.{depth}.cross_attn.q_linear.bias")
        k, v = torch.chunk(sd.pop(f"blocks.{depth}.cross_attn.kv_linear.weight"), 2, dim=0)
        k_bias, v_bias = torch.chunk(sd.pop(f"blocks.{depth}.cross_attn.kv_linear.bias"), 2, dim=0)
        out[f"transformer_blocks.{depth}.attn2.to_k.weight"] = k
        out[f"transformer_blocks.{depth}.attn2.to_k.bias"] = k_bias
        out[f"transformer_blocks.{depth}.attn2.to_v.weight"] = v
        out[f"transformer_blocks.{depth}.attn2.to_v.bias"] = v_bias
        out[f"transformer_blocks.{depth}.attn2.norm_q.weight"] = sd.pop(f"blocks.{depth}.cross_attn.q_norm.weight")
        out[f"transformer_blocks.{depth}.attn2.norm_k.weight"] = sd.pop(f"blocks.{depth}.cross_attn.k_norm.weight")
        out[f"transformer_blocks.{depth}.attn2.to_out.0.weight"] = sd.pop(f"blocks.{depth}.cross_attn.proj.weight")
        out[f"transformer_blocks.{depth}.attn2.to_out.0.bias"] = sd.pop(f"blocks.{depth}.cross_attn.proj.bias")

    out["proj_out.weight"] = sd.pop("final_layer.linear.weight")
    out["proj_out.bias"] = sd.pop("final_layer.linear.bias")
    out["scale_shift_table"] = sd.pop("final_layer.scale_shift_table")

    # Keys not carried over to diffusers
    for key in ("y_embedder.y_embedding", "pos_embed", "logvar_linear.weight", "logvar_linear.bias"):
        sd.pop(key, None)

    if sd:
        raise ValueError(f"Unconverted keys left in state dict: {sorted(sd.keys())}")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pth", required=True, help="Path to the .pth checkpoint (train_ivjoint.py output)")
    parser.add_argument("--output", required=True, help="Output directory for the diffusers transformer")
    parser.add_argument("--dtype", default="bf16", choices=list(DTYPE), help="Output weight dtype")
    args = parser.parse_args()

    print(f"Loading {args.pth}...")
    ckpt = torch.load(args.pth, map_location="cpu", weights_only=False)
    state_dict = ckpt.get("state_dict", ckpt)

    converted = convert_state_dict(state_dict)

    transformer = SanaVideoTransformer3DModel(**TRANSFORMER_KWARGS)
    transformer.load_state_dict(converted, strict=True, assign=True)
    transformer = transformer.to(DTYPE[args.dtype])
    transformer.save_pretrained(args.output, safe_serialization=True, max_shard_size="5GB")
    print(f"Saved diffusers transformer to {args.output}")


if __name__ == "__main__":
    main()
