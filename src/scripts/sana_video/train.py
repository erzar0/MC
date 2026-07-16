"""Fine-tune pretrained SANA-Video 2B on Minecraft voxel pseudo-videos.

Supports two training modes on top of the same pretrained checkpoint:
  - lora: PEFT LoRA adapters on the transformer attention layers (cheap, default)
  - full: full fine-tuning of all transformer weights (needs much more VRAM)

Usage:
    accelerate launch src/scripts/train_sana_video.py \\
        --manifest tmp/sana_video_manifest.jsonl \\
        --mode lora \\
        --output_dir tmp/sana_video_ft
"""

import argparse
import inspect
import math
import os
import sys
from pathlib import Path

import torch
import torch.nn as nn
from accelerate import Accelerator
from accelerate.utils import set_seed
from diffusers import SanaVideoPipeline
from diffusers.optimization import get_scheduler
from diffusers.training_utils import cast_training_params
from peft import LoraConfig
from peft.utils import get_peft_model_state_dict
from torch.utils.data import DataLoader
from tqdm.auto import tqdm

# Support both `python -m src.scripts.train_sana_video` and direct execution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
try:
    from src.scripts.sana_video.dataset import BucketBatchSampler, MinecraftVideoDataset
except ImportError:
    from dataset import BucketBatchSampler, MinecraftVideoDataset  # pyrefly: ignore  # direct-script-execution fallback


def parse_args():
    parser = argparse.ArgumentParser(description="Fine-tune SANA-Video on Minecraft 3D voxel volumes")
    parser.add_argument(
        "--manifest", type=str, default="tmp/sana_video_manifest.jsonl", help="Path to JSONL dataset manifest"
    )
    parser.add_argument(
        "--pretrained_model",
        type=str,
        default="Efficient-Large-Model/SANA-Video_2B_480p_diffusers",
        help="Base SANA-Video model ID",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["lora", "full"],
        default="lora",
        help="Fine-tuning mode: LoRA adapters or full transformer weights",
    )
    parser.add_argument("--output_dir", type=str, default="tmp/sana_video_ft", help="Directory to save checkpoints")
    parser.add_argument("--spatial_crop_size", type=int, default=128, help="Spatial resolution to crop volumes to")
    parser.add_argument(
        "--max_frames",
        type=int,
        default=385,
        help="Frame/Y-layer count per sample (must be 4n+1 for the Wan VAE); 385 covers a full 384-tall region",
    )
    parser.add_argument("--lora_rank", type=int, default=8, help="LoRA attention rank (lora mode only)")
    parser.add_argument(
        "--learning_rate", type=float, default=None, help="Learning rate (default: 2e-4 for lora, 5e-5 for full)"
    )
    parser.add_argument("--epochs", type=int, default=3, help="Number of training epochs")
    parser.add_argument("--batch_size", type=int, default=1, help="Batch size per device (keep 1 to fit VRAM)")
    parser.add_argument(
        "--bucket_step",
        type=int,
        default=4,
        help="Height-bucket granularity (multiple of 4). Smaller pads less air but makes more buckets",
    )
    parser.add_argument("--gradient_accumulation_steps", type=int, default=4, help="Gradient accumulation steps")
    parser.add_argument("--num_workers", type=int, default=2, help="DataLoader workers")
    parser.add_argument("--save_every_epochs", type=int, default=1, help="Save checkpoint every N epochs")
    parser.add_argument(
        "--save_every_steps",
        type=int,
        default=500,
        help="Also save a checkpoint every N samples seen (0 disables step-based saving)",
    )
    parser.add_argument(
        "--flow_shift",
        type=float,
        default=3.0,
        help="Training flow shift for sigma sampling (upstream trains at 3.0; inference uses the scheduler's 8.0)",
    )
    parser.add_argument(
        "--weighting_scheme",
        type=str,
        choices=["logit_normal", "uniform"],
        default="logit_normal",
        help="Distribution for the pre-shift timestep u: logit-normal (upstream default) or uniform",
    )
    parser.add_argument("--logit_mean", type=float, default=0.0, help="Mean of the logit-normal u distribution")
    parser.add_argument("--logit_std", type=float, default=1.0, help="Std of the logit-normal u distribution")
    parser.add_argument("--gradient_clip", type=float, default=0.1, help="Max gradient norm (upstream trains with 0.1)")
    parser.add_argument(
        "--no_chi",
        action="store_true",
        help="Encode captions without the complex-human-instruction prefix (upstream trains with it)",
    )
    parser.add_argument(
        "--use_8bit_adam",
        action="store_true",
        help="Use bitsandbytes 8-bit AdamW to shrink optimizer state (for low-VRAM runs)",
    )
    parser.add_argument("--warmup_steps", type=int, default=500, help="LR warmup steps (constant-with-warmup schedule)")
    parser.add_argument(
        "--caption_dropout",
        type=float,
        default=0.1,
        help="Probability of replacing a caption with the empty string (enables classifier-free guidance)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--report_to",
        type=str,
        default="none",
        choices=["none", "wandb"],
        help="Experiment tracker for metrics logging (wandb requires WANDB_API_KEY / prior login)",
    )
    parser.add_argument("--wandb_project", type=str, default="minecraft-sana-video", help="wandb project name")
    parser.add_argument("--run_name", type=str, default=None, help="wandb run name (defaults to a timestamped name)")
    parser.add_argument(
        "--resume_from_checkpoint",
        type=str,
        default=None,
        help="Path to a checkpoint directory (step or epoch) to resume training from",
    )
    return parser.parse_args()


def save_checkpoint(args, accelerator, transformer, tag):
    """Saves a checkpoint (LoRA adapter or full transformer) named ``checkpoint-{tag}``.

    Writes an empty ``.complete`` marker file once all weights are flushed, so an
    external checkpoint uploader (see ``deploy/remote_train.sh``) can tell the
    checkpoint is fully written before syncing it to Google Drive.
    """
    unwrapped = accelerator.unwrap_model(transformer)
    save_path = os.path.join(args.output_dir, f"checkpoint-{tag}")

    if args.mode == "lora":
        lora_state_dict = get_peft_model_state_dict(unwrapped)
        SanaVideoPipeline.save_lora_weights(save_path, transformer_lora_layers=lora_state_dict)
        print(f"Saved LoRA adapter weights to {save_path}")
    else:
        unwrapped.save_pretrained(save_path)
        print(f"Saved full transformer weights to {save_path}")

    Path(save_path, ".complete").touch()
    return save_path


def main():
    args = parse_args()
    set_seed(args.seed)

    # 1. Initialize Accelerator (bf16 mixed precision to save memory)
    accelerator = Accelerator(
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        mixed_precision="bf16",
        log_with="wandb" if args.report_to == "wandb" else None,
    )
    device = accelerator.device

    if args.learning_rate is None:
        if args.mode == "lora":
            args.learning_rate = 2e-4
        else:
            # Upstream auto-scales the base full-FT lr (5e-5) by
            # sqrt(effective_batch / 256) (auto_lr rule=sqrt, base 256).
            effective_bs = args.batch_size * args.gradient_accumulation_steps * accelerator.num_processes
            args.learning_rate = 5e-5 * math.sqrt(effective_bs / 256)
            print(f"Auto-scaled lr to {args.learning_rate:.2e} (sqrt rule, effective batch {effective_bs})")

    # Initialize the experiment tracker on the main process only. Hyperparameters
    # are logged as the run config; per-step loss/lr are logged in the loop below.
    if args.report_to == "wandb":
        accelerator.init_trackers(
            args.wandb_project,
            config=vars(args),
            init_kwargs={"wandb": {"name": args.run_name}},
        )
    print(
        f"Mode: {args.mode} | LR: {args.learning_rate} | Device: {device} | Mixed precision: {accelerator.mixed_precision}"
    )

    # 2. Load pretrained pipeline and split out components
    print(f"Loading pretrained SANA-Video model from {args.pretrained_model}...")
    pipe = SanaVideoPipeline.from_pretrained(args.pretrained_model, torch_dtype=torch.bfloat16)

    # Resume transformer/LoRA weights if specified
    if args.resume_from_checkpoint:
        print(f"Resuming weights from checkpoint: {args.resume_from_checkpoint}")
        if args.mode == "lora":
            # For LoRA, load the adapter weights (this automatically initializes the adapter modules)
            pipe.load_lora_weights(args.resume_from_checkpoint)
        else:
            # For Full, load the custom transformer checkpoint directly
            transformer_cls = type(pipe.transformer)
            pipe.transformer = transformer_cls.from_pretrained(args.resume_from_checkpoint, torch_dtype=torch.bfloat16)

    # Upstream SANA-Video trains with a milder flow shift (3.0) than inference
    # (the scheduler's 8.0): training must cover low/mid noise levels, while the
    # inference schedule deliberately over-samples high noise.
    flow_shift = args.flow_shift
    print(f"Flow-matching sigma sampling: {args.weighting_scheme} u, flow_shift={flow_shift}")

    transformer = pipe.transformer
    vae = pipe.vae
    text_encoder = pipe.text_encoder

    # Freeze VAE and text encoder — only the transformer is trained
    vae.requires_grad_(False)
    text_encoder.requires_grad_(False)
    # VAE runs in float32 for precise encoding (matches the SANA-Video pipeline)
    vae.to(device, dtype=torch.float32)
    if hasattr(vae, "enable_tiling"):
        vae.enable_tiling()
    text_encoder.to(device, dtype=torch.bfloat16)

    # The pretrained model's text conditioning was learned with the complex-
    # human-instruction (CHI) prefix on every caption (upstream sets chi_prompt
    # during training); encode training captions the same way. The pipeline's
    # __call__ default is the exact list used upstream.
    chi_instruction = None
    if not args.no_chi:
        chi_instruction = inspect.signature(SanaVideoPipeline.__call__).parameters["complex_human_instruction"].default
        print("Encoding captions with the complex-human-instruction prefix (pass --no_chi to disable).")

    # Caption dropout swaps in this null embedding — a plain empty string
    # WITHOUT the CHI prefix, matching upstream's null embed and the diffusers
    # negative-prompt path.
    with torch.no_grad():
        null_embeds, null_mask, _, _ = pipe.encode_prompt(
            prompt=[""],
            do_classifier_free_guidance=False,
            device=device,
            clean_caption=False,
            complex_human_instruction=None,
        )

    # 3. Configure trainable weights per mode
    if args.mode == "lora":
        if not args.resume_from_checkpoint:
            print("Attaching new LoRA adapters to the transformer attention layers...")
            transformer.requires_grad_(False)
            lora_config = LoraConfig(
                r=args.lora_rank,
                lora_alpha=args.lora_rank,
                target_modules=["to_q", "to_k", "to_v", "to_out.0"],
                init_lora_weights="gaussian",
            )
            transformer.add_adapter(lora_config)
        else:
            print("Using loaded LoRA adapters from checkpoint...")
        # Keep LoRA params in fp32 for stable mixed-precision training
        cast_training_params(transformer, dtype=torch.float32)
    else:
        print("Full fine-tuning: all transformer weights are trainable.")
        # Mixed-precision training requires fp32 master weights
        transformer.to(dtype=torch.float32)
        transformer.requires_grad_(True)

    trainable_params = [p for p in transformer.parameters() if p.requires_grad]
    num_trainable = sum(p.numel() for p in trainable_params)
    print(f"Trainable parameters: {num_trainable / 1e6:.2f}M")

    transformer.enable_gradient_checkpointing()

    # 4. Dataset and dataloader
    # Regions are bucketed by content height: BucketBatchSampler batches
    # equal-height regions together so each batch is a clean fixed-shape tensor
    # with different batches using different frame counts (see dataset.py).
    print("Preparing Minecraft voxel pseudo-video dataset...")
    dataset = MinecraftVideoDataset(
        manifest_path=args.manifest,
        spatial_crop_size=args.spatial_crop_size,
        max_frames=args.max_frames,
        bucket_step=args.bucket_step,
    )
    batch_sampler = BucketBatchSampler(
        dataset.frame_counts,
        batch_size=args.batch_size,
        shuffle=True,
        drop_last=False,
        seed=args.seed,
    )
    dataloader = DataLoader(
        dataset,
        batch_sampler=batch_sampler,
        num_workers=args.num_workers,
        pin_memory=True,
    )

    # 5. Optimizer (standard AdamW like upstream; 8-bit only on request)
    if args.use_8bit_adam:
        import bitsandbytes as bnb

        optimizer_cls = bnb.optim.AdamW8bit
        print("Using 8-bit AdamW optimizer.")
    else:
        optimizer_cls = torch.optim.AdamW
    optimizer = optimizer_cls(
        trainable_params,
        lr=args.learning_rate,
        betas=(0.9, 0.999),
        eps=1e-10,
        weight_decay=0.0,
    )
    lr_scheduler = get_scheduler("constant_with_warmup", optimizer, num_warmup_steps=args.warmup_steps)

    # 6. Prepare with Accelerator
    transformer, optimizer, dataloader, lr_scheduler = accelerator.prepare(
        transformer, optimizer, dataloader, lr_scheduler
    )

    os.makedirs(args.output_dir, exist_ok=True)
    global_step = 0
    samples_seen = 0
    last_ckpt_milestone = 0
    print(f"Start training: {args.epochs} epochs | Steps per epoch: {len(dataloader)}")

    for epoch in range(args.epochs):
        transformer.train()
        epoch_loss = 0.0

        progress_bar = tqdm(
            dataloader,
            desc=f"Epoch {epoch + 1}/{args.epochs}",
            disable=not accelerator.is_local_main_process,
        )

        for pixel_values, prompts in progress_bar:
            with accelerator.accumulate(transformer):
                # pixel_values shape: (B, C, F, H, W) in [-1, 1]
                B = pixel_values.shape[0]

                # A. Encode text prompts (CHI-prefixed, matching pretraining).
                # Caption dropout: swap some samples to the null (empty, CHI-free)
                # embedding so the model learns an unconditional distribution —
                # required for classifier-free guidance at inference (guidance
                # extrapolates conditional vs unconditional).
                with torch.no_grad():
                    prompt_embeds, prompt_attention_mask, _, _ = pipe.encode_prompt(
                        prompt=list(prompts),
                        do_classifier_free_guidance=False,
                        device=device,
                        clean_caption=False,
                        complex_human_instruction=chi_instruction,
                    )
                drop = torch.rand(B, device=device) < args.caption_dropout
                if drop.any():
                    prompt_embeds[drop] = null_embeds.to(prompt_embeds.dtype)
                    prompt_attention_mask[drop] = null_mask

                # B. Encode pseudo-videos to latents (fp32 VAE)
                with torch.no_grad():
                    video_input = pixel_values.to(device=device, dtype=torch.float32)
                    latents = vae.encode(video_input, return_dict=False)[0].sample()
                    latents_mean = torch.tensor(vae.config.latents_mean).view(1, -1, 1, 1, 1).to(device, torch.float32)
                    latents_std = torch.tensor(vae.config.latents_std).view(1, -1, 1, 1, 1).to(device, torch.float32)
                    latents = (latents - latents_mean) / latents_std
                    latents = latents.to(dtype=torch.bfloat16)

                # C. Flow-matching in the diffusers convention: sigma=1 is pure
                # noise, sigma=0 is clean data, and the model predicts the
                # velocity (noise - data). The pretrained SANA-Video transformer
                # and the DPMSolver flow scheduler both assume this orientation;
                # flipping it trains against the pretrained prior.
                # u follows the upstream logit-normal(0,1) density (SD3-style,
                # concentrating supervision at mid noise levels), then sigma is
                # shift-transformed (sigma = shift*u / (1 + (shift-1)*u)) with
                # the training flow shift.
                if args.weighting_scheme == "logit_normal":
                    u = torch.sigmoid(torch.normal(args.logit_mean, args.logit_std, (B,), device=device))
                else:
                    u = torch.rand((B,), device=device, dtype=torch.float32)
                sigma = (flow_shift * u / (1.0 + (flow_shift - 1.0) * u)).to(torch.bfloat16)
                sigma_expanded = sigma.view(-1, 1, 1, 1, 1)
                noise = torch.randn_like(latents)
                noisy_latents = (1.0 - sigma_expanded) * latents + sigma_expanded * noise
                target = noise - latents

                # D. Forward pass (scheduler timesteps live in [0, 1000], sigma*1000)
                timestep = sigma.float() * 1000.0
                model_pred = transformer(
                    noisy_latents,
                    encoder_hidden_states=prompt_embeds.to(dtype=torch.bfloat16),
                    encoder_attention_mask=prompt_attention_mask,
                    timestep=timestep,
                    return_dict=False,
                )[0]

                # E. MSE loss against the flow-matching velocity target
                loss = nn.functional.mse_loss(model_pred.float(), target.float(), reduction="mean")

                accelerator.backward(loss)
                if accelerator.sync_gradients:
                    accelerator.clip_grad_norm_(trainable_params, args.gradient_clip)
                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()

            loss_val = loss.item()
            epoch_loss += loss_val
            global_step += 1
            samples_seen += B
            progress_bar.set_postfix({"loss": f"{loss_val:.4f}", "step": global_step, "samples": samples_seen})
            accelerator.log(
                {
                    "train/loss": loss_val,
                    "train/lr": optimizer.param_groups[0]["lr"],
                    "train/epoch": epoch + 1,
                    "train/samples_seen": samples_seen,
                },
                step=global_step,
            )

            # Step-based checkpointing: save each time samples_seen crosses a
            # multiple of save_every_steps (robust to batch size and resumes).
            if args.save_every_steps and samples_seen // args.save_every_steps > last_ckpt_milestone:
                last_ckpt_milestone = samples_seen // args.save_every_steps
                accelerator.wait_for_everyone()
                if accelerator.is_main_process:
                    save_checkpoint(args, accelerator, transformer, f"step-{samples_seen}")

        avg_loss = epoch_loss / max(len(dataloader), 1)
        print(f"Epoch {epoch + 1} Avg Loss: {avg_loss:.4f}")
        accelerator.log({"train/epoch_avg_loss": avg_loss}, step=global_step)

        if (epoch + 1) % args.save_every_epochs == 0 or (epoch + 1) == args.epochs:
            accelerator.wait_for_everyone()
            if accelerator.is_main_process:
                save_checkpoint(args, accelerator, transformer, f"epoch-{epoch + 1}")

    accelerator.end_training()
    print("Training complete!")


if __name__ == "__main__":
    main()
