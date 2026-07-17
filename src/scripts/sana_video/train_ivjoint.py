# Ported from Sana/train_video_scripts/train_video_ivjoint.py (Apache-2.0,
# Copyright 2024 NVIDIA CORPORATION & AFFILIATES) and adapted to train on
# Minecraft region pseudo-videos:
#   - dataset is MinecraftRegionVideoDataset (region_dataset.py) instead of
#     the zip-based SanaZipDataset
#   - single-GPU / DDP only: FSDP, multi-scale sampling, image joint-training,
#     model-growth and T5/Qwen text encoders are stripped (gemma + CHI kept)
# Everything else (Scheduler, vae_encode, .pth checkpointing, log_validation)
# is the upstream code path, driven by configs/sana_video_minecraft.yaml.
#
# Usage:
#   python src/scripts/sana_video/train_ivjoint.py --config configs/sana_video_minecraft.yaml

import datetime
import gc
import hashlib
import os
import os.path as osp
import sys
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")  # ignore warning

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "Sana"))

# Allow a plain `python train_ivjoint.py` launch (no torchrun): the upstream
# code reads these env vars unconditionally, and setting RANK/WORLD_SIZE makes
# Accelerate init torch.distributed, which needs the rendezvous vars too.
os.environ.setdefault("WORLD_SIZE", "1")
os.environ.setdefault("RANK", "0")
os.environ.setdefault("LOCAL_RANK", "0")
os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
os.environ.setdefault("MASTER_PORT", "29500")

import imageio
import pyrallis
import torch
from accelerate import Accelerator, InitProcessGroupKwargs
from diffusion import DPMS, Scheduler
from diffusion.data.builder import build_dataloader
from diffusion.model.builder import build_model, get_tokenizer_and_text_encoder, get_vae, vae_decode, vae_encode
from diffusion.model.respace import compute_density_for_timestep_sampling
from diffusion.model.utils import get_weight_dtype
from diffusion.utils.checkpoint import load_checkpoint, save_checkpoint
from diffusion.utils.config import SanaVideoConfig, model_video_init_config
from diffusion.utils.dist_utils import get_rank, get_world_size
from diffusion.utils.logger import LogBuffer, get_root_logger
from diffusion.utils.lr_scheduler import build_lr_scheduler
from diffusion.utils.misc import DebugUnderflowOverflow, init_random_seed, set_random_seed
from diffusion.utils.optimizer import auto_scale_lr, build_optimizer
from termcolor import colored

from src.scripts.sana_video.dataset import BucketBatchSampler
from src.scripts.sana_video.region_dataset import MinecraftRegionVideoDataset

os.environ["TOKENIZERS_PARALLELISM"] = "false"

# The vendored Sana code calls torch.load without weights_only, which torch>=2.6
# defaults to True; upstream .pth checkpoints carry numpy objects and fail that
# check. All checkpoints here are trusted (our own or the official HF release).
_torch_load = torch.load


def _torch_load_trusted(*args, **kwargs):
    kwargs.setdefault("weights_only", False)
    return _torch_load(*args, **kwargs)


torch.load = _torch_load_trusted


@torch.inference_mode()
def log_validation(accelerator, config, model, logger, step, device, vae=None, init_noise=None):
    torch.cuda.empty_cache()
    vis_sampler = config.scheduler.vis_sampler
    model = accelerator.unwrap_model(model).eval()
    hw = torch.tensor([[video_height, video_width]], dtype=torch.float, device=device).repeat(1, 1)
    ar = torch.tensor([[1.0]], device=device).repeat(1, 1)
    null_y = torch.load(null_embed_path, map_location="cpu")
    null_y = null_y["uncond_prompt_embeds"].to(device)
    cfg_scale = 4.5

    # Create sampling noise:
    logger.info("Running validation... ")
    video_logs = []

    def run_sampling(init_z=None, label_suffix="", vae=None, sampler="dpm-solver"):
        latents = []
        current_video_logs = []
        for prompt in validation_prompts:
            z = (
                torch.randn(1, config.vae.vae_latent_dim, latent_temp, latent_height, latent_width, device=device)
                if init_z is None
                else init_z
            )
            logger.info(f"Loading embedding for prompt from: {config.train.valid_prompt_embed_root}")
            embed = torch.load(
                osp.join(config.train.valid_prompt_embed_root, f"{prompt[:50]}_{valid_prompt_embed_suffix}"),
                map_location="cpu",
            )
            caption_embs, emb_masks = embed["caption_embeds"].to(device), embed["emb_mask"].to(device)
            model_kwargs = dict(data_info={"img_hw": hw, "aspect_ratio": ar}, mask=emb_masks)

            with torch.autocast(device_type=device.type, dtype=torch.bfloat16):
                if sampler == "flow_dpm-solver":
                    dpm_solver = DPMS(
                        model.forward_with_dpmsolver,
                        condition=caption_embs,
                        uncondition=null_y,
                        cfg_scale=cfg_scale,
                        model_type="flow",
                        model_kwargs=model_kwargs,
                        schedule="FLOW",
                    )
                    denoised = dpm_solver.sample(
                        z,
                        steps=50,
                        order=2,
                        skip_type="time_uniform_flow",
                        method="multistep",
                        flow_shift=(
                            config.scheduler.inference_flow_shift
                            if config.scheduler.inference_flow_shift is not None
                            else config.scheduler.flow_shift
                        ),
                    )
                else:
                    raise ValueError(f"{sampler} not implemented")

            latents.append(denoised)
        torch.cuda.empty_cache()
        if vae is None:
            vae = get_vae(
                config.vae.vae_type, config.vae.vae_pretrained, accelerator.device, dtype=vae_dtype, config=config.vae
            )
        for prompt, latent in zip(validation_prompts, latents, strict=True):
            latent = latent.to(vae_dtype)
            samples = vae_decode(config.vae.vae_type, vae, latent)
            video = (
                torch.clamp(127.5 * samples[0] + 127.5, 0, 255).permute(1, 0, 2, 3).to("cpu", dtype=torch.uint8).numpy()
            )  # C,T,H,W -> T,C,H,W
            current_video_logs.append({"validation_prompt": prompt + label_suffix, "videos": video})

        return current_video_logs

    # First run with original noise
    video_logs += run_sampling(init_z=None, label_suffix="", vae=vae, sampler=vis_sampler)

    # Second run with init_noise if provided
    if init_noise is not None:
        torch.cuda.empty_cache()
        gc.collect()
        init_noise = torch.clone(init_noise).to(device)
        video_logs += run_sampling(init_z=init_noise, label_suffix=" w/ init noise", vae=vae, sampler=vis_sampler)

    for tracker in accelerator.trackers:
        if tracker.name == "wandb":
            import wandb

            wandb_items = []
            for log_item in video_logs:
                wandb_items.append(
                    wandb.Video(log_item["videos"], caption=log_item["validation_prompt"], fps=16, format="mp4")
                )
            tracker.log({"validation": wandb_items})
        else:
            logger.warn(f"Video logging not implemented for {tracker.name}")

    def concatenate_videos(video_data, videos_per_row=3):
        videos = [torch.from_numpy(log["videos"]).to(torch.uint8) for log in video_data]  # T,C,H,W

        num_videos = len(videos)
        num_rows = (num_videos + videos_per_row - 1) // videos_per_row
        num_frames, num_channels, height, width = videos[0].shape
        total_width = width * min(videos_per_row, num_videos)
        total_height = height * num_rows

        grid_video = torch.zeros((num_frames, num_channels, total_height, total_width), dtype=videos[0].dtype)

        for i, video in enumerate(videos):
            row = i // videos_per_row
            col = i % videos_per_row

            y_offset = row * height
            x_offset = col * width

            h, w = video.shape[2:]

            grid_video[:, :, y_offset : y_offset + h, x_offset : x_offset + w] = video

        return grid_video

    if config.train.local_save_vis:
        file_format = "mp4"
        local_vis_save_path = osp.join(config.work_dir, "log_vis")
        os.umask(0o000)
        os.makedirs(local_vis_save_path, exist_ok=True)
        concatenated_video = concatenate_videos(video_logs, videos_per_row=5)
        save_path = (
            osp.join(local_vis_save_path, f"vis_{step}.{file_format}")
            if init_noise is None
            else osp.join(local_vis_save_path, f"vis_{step}_w_init.{file_format}")
        )
        save_video = concatenated_video.permute(0, 2, 3, 1)
        writer = imageio.v2.get_writer(save_path, fps=16, format="FFMPEG", codec="libx264", quality=8)
        for frame in save_video.numpy():
            writer.append_data(frame)
        writer.close()

    model.train()
    del vae
    torch.cuda.empty_cache()
    return video_logs


def train(
    config,
    args,
    accelerator,
    model,
    optimizer,
    lr_scheduler,
    train_dataloader,
    train_diffusion,
    logger,
):
    if getattr(config.train, "debug_nan", False):
        DebugUnderflowOverflow(model, max_frames_to_save=100)
        logger.info("NaN debugger registered. Start to detect overflow during training.")
    log_buffer = LogBuffer()

    global_step = start_step
    video_step = start_video_step
    loss_nan_timer = 0

    # Now you train the model. Buckets reshuffle every epoch, so resuming
    # restarts the current epoch from its beginning (no in-epoch skip).
    for epoch in range(start_epoch + 1, config.train.num_epochs + 1):
        time_start, last_tic = time.time(), time.time()
        data_time_start = time.time()
        data_time_all = 0
        lm_time_all = 0
        vae_time_all = 0
        model_time_all = 0

        video_dataloader_iter = iter(train_dataloader)

        for step in range(train_dataloader_len):
            try:
                batch = next(video_dataloader_iter)
            except StopIteration:
                logger.info("Reset video dataloader iterator")
                video_dataloader_iter = iter(train_dataloader)
                batch = next(video_dataloader_iter)
            video_step += 1

            accelerator.wait_for_everyone()
            data_time_all += time.time() - data_time_start
            vae_time_start = time.time()
            data_info = batch[3]

            with torch.no_grad():
                z = vae_encode(
                    config.vae.vae_type,
                    vae,
                    batch[0].permute(0, 2, 1, 3, 4).to(vae_dtype),
                    device=accelerator.device,
                    cache_key=data_info["cache_key"],
                    if_cache=config.vae.if_cache,
                    data_info=data_info,
                )  # B,F,C,H,W -> B,C,F,H,W

            accelerator.wait_for_everyone()
            vae_time_all += time.time() - vae_time_start

            clean_images = z

            lm_time_start = time.time()
            if "gemma" in config.text_encoder.text_encoder_name:
                with torch.no_grad():
                    if not config.text_encoder.chi_prompt:
                        max_length_all = config.text_encoder.model_max_length
                        prompt = batch[1]
                    else:
                        chi_prompt = "\n".join(config.text_encoder.chi_prompt)
                        prompt = [chi_prompt + i for i in batch[1]]
                        num_sys_prompt_tokens = len(tokenizer.encode(chi_prompt))
                        max_length_all = (
                            num_sys_prompt_tokens + config.text_encoder.model_max_length - 2
                        )  # magic number 2: [bos], [_]
                    txt_tokens = tokenizer(
                        prompt,
                        padding="max_length",
                        max_length=max_length_all,
                        truncation=True,
                        return_tensors="pt",
                    ).to(accelerator.device)
                    select_index = [0] + list(
                        range(-config.text_encoder.model_max_length + 1, 0)
                    )  # first bos and end N-1
                    y = text_encoder(txt_tokens.input_ids, attention_mask=txt_tokens.attention_mask)[0][:, None][
                        :, :, select_index
                    ]
                    y_mask = txt_tokens.attention_mask[:, None, None][:, :, :, select_index]
            else:
                raise ValueError(f"Only gemma text encoders are supported, got {config.text_encoder.text_encoder_name}")

            # Sample a random timestep for each sample
            bs = clean_images.shape[0]
            timesteps = torch.randint(
                0, config.scheduler.train_sampling_steps, (bs,), device=clean_images.device
            ).long()
            if config.scheduler.weighting_scheme in ["logit_normal", "mode"]:
                # adapting from diffusers.training_utils
                u = compute_density_for_timestep_sampling(
                    weighting_scheme=config.scheduler.weighting_scheme,
                    batch_size=bs,
                    logit_mean=config.scheduler.logit_mean,
                    logit_std=config.scheduler.logit_std,
                    mode_scale=config.scheduler.mode_scale,
                )
                timesteps = (u * config.scheduler.train_sampling_steps).long().to(clean_images.device)
            grad_norm = None
            accelerator.wait_for_everyone()
            lm_time_all += time.time() - lm_time_start
            model_time_start = time.time()
            with accelerator.accumulate(model):
                # Predict the noise residual
                optimizer.zero_grad()
                loss_term = train_diffusion.training_losses(
                    model, clean_images, timesteps, model_kwargs=dict(y=y, mask=y_mask, data_info=data_info)
                )
                loss = loss_term["loss"].mean()

                accelerator.backward(loss)

                if accelerator.sync_gradients:
                    grad_norm = accelerator.clip_grad_norm_(model.parameters(), config.train.gradient_clip)

                optimizer.step()
                lr_scheduler.step()
                accelerator.wait_for_everyone()
                model_time_all += time.time() - model_time_start

            if torch.any(torch.isnan(loss)):
                loss_nan_timer += 1
            lr = lr_scheduler.get_last_lr()[0]
            logs = {args.loss_report_name: accelerator.gather(loss).mean().item()}
            if grad_norm is not None:
                logs.update(grad_norm=accelerator.gather(grad_norm).mean().item())
            log_buffer.update(logs)
            if (global_step + 1) % config.train.log_interval == 0 or (step + 1) == 1:
                accelerator.wait_for_everyone()
                if args.debug:
                    print(f"Rank {rank}: current_batch_id: {batch[4]}")

                t = (time.time() - last_tic) / config.train.log_interval
                t_d = data_time_all / config.train.log_interval
                t_m = model_time_all / config.train.log_interval
                t_lm = lm_time_all / config.train.log_interval
                t_vae = vae_time_all / config.train.log_interval
                avg_time = (time.time() - time_start) / (step + 1)
                eta = str(datetime.timedelta(seconds=int(avg_time * (total_steps - global_step - 1))))
                eta_epoch = str(datetime.timedelta(seconds=int(avg_time * (train_dataloader_len - step - 1))))
                log_buffer.average()

                info = (
                    f"Epoch: {epoch} | Global Step: {global_step + 1} / {train_dataloader_len}, "
                    f"Video Step: {video_step} | id: {batch[4][-1]}, "
                    f"total_eta: {eta}, epoch_eta:{eta_epoch}, time: all:{t:.3f}, model:{t_m:.3f}, data:{t_d:.3f}, "
                    f"lm:{t_lm:.3f}, vae:{t_vae:.3f}, lr:{lr:.3e}, Cap: {batch[5][0]}, "
                )
                info += (
                    f"s:({model.module.f}, {model.module.h}, {model.module.w}), "
                    if hasattr(model, "module")
                    else f"s:({model.f}, {model.h}, {model.w}), "
                )

                info += ", ".join([f"{k}:{v:.4f}" for k, v in log_buffer.output.items()])
                last_tic = time.time()
                log_buffer.clear()
                data_time_all = 0
                model_time_all = 0
                lm_time_all = 0
                vae_time_all = 0
                if accelerator.is_main_process:
                    logger.info(info)

            logs.update(lr=lr)
            if accelerator.is_main_process:
                accelerator.log(logs, step=global_step)

            global_step += 1

            if loss_nan_timer > 20:
                raise ValueError("Loss is NaN too much times. Break here.")
            if (
                global_step % config.train.save_model_steps == 0
                or (time.time() - training_start_time) / 3600 > config.train.early_stop_hours
            ):
                torch.cuda.synchronize()
                accelerator.wait_for_everyone()
                if accelerator.is_main_process:
                    os.umask(0o000)
                    save_checkpoint(
                        work_dir=osp.join(config.work_dir, "checkpoints"),
                        epoch=epoch,
                        model=accelerator.unwrap_model(model),
                        optimizer=optimizer,
                        lr_scheduler=lr_scheduler,
                        step=global_step,
                        saved_info={"video_step": video_step, "image_step": 0},
                        generator=generator,
                        add_symlink=True,
                    )

                if (time.time() - training_start_time) / 3600 > config.train.early_stop_hours:
                    logger.info(f"Stopping training at epoch {epoch}, step {global_step} due to time limit.")
                    return

            if config.train.visualize and (global_step % config.train.eval_sampling_steps == 0 or (step + 1) == 1):
                accelerator.wait_for_everyone()
                if accelerator.is_main_process:
                    log_validation(
                        accelerator=accelerator,
                        config=config,
                        model=model,
                        logger=logger,
                        step=global_step,
                        device=accelerator.device,
                        vae=vae,
                        init_noise=validation_noise,
                    )

            data_time_start = time.time()

        if epoch % config.train.save_model_epochs == 0 or epoch == config.train.num_epochs and not config.debug:
            accelerator.wait_for_everyone()
            torch.cuda.synchronize()
            if accelerator.is_main_process:
                os.umask(0o000)
                save_checkpoint(
                    osp.join(config.work_dir, "checkpoints"),
                    epoch=epoch,
                    step=global_step,
                    saved_info={"video_step": video_step, "image_step": 0},
                    model=accelerator.unwrap_model(model),
                    optimizer=optimizer,
                    lr_scheduler=lr_scheduler,
                    generator=generator,
                    add_symlink=True,
                )

        if epoch > config.train.num_epochs:
            logger.info(f"Stopping training at epoch {epoch}, step {global_step} due to num_epochs limit.")
            return


@pyrallis.wrap()
def main(cfg: SanaVideoConfig) -> None:
    global train_dataloader_len, start_epoch, start_step, start_video_step, vae, generator, num_replicas, rank
    global training_start_time, validation_noise, text_encoder, tokenizer
    global max_length, validation_prompts, valid_prompt_embed_suffix, null_embed_path
    global total_steps, vae_dtype
    global video_width, video_height, num_frames, latent_temp, latent_height, latent_width

    config = cfg
    args = cfg

    assert not config.train.use_fsdp, "FSDP was stripped from this port; use the upstream script for FSDP."
    assert not config.model.multi_scale, "multi-scale sampling was stripped from this port."

    training_start_time = time.time()
    load_from = True

    # model.resume_from defaults to a dict with checkpoint=None (no resume);
    # a string (or --resume_from) names a checkpoint path or "latest".
    resume_ckpt = args.resume_from or (
        config.model.resume_from
        if isinstance(config.model.resume_from, str)
        else (config.model.resume_from or {}).get("checkpoint")
    )
    if resume_ckpt:
        load_from = False
        config.model.resume_from = dict(
            checkpoint=resume_ckpt,
            load_ema=False,
            resume_optimizer=True,
            resume_lr_scheduler=config.train.resume_lr_scheduler,
        )
    else:
        config.model.resume_from = None

    os.umask(0o000)
    os.makedirs(config.work_dir, exist_ok=True)

    init_handler = InitProcessGroupKwargs()
    init_handler.timeout = datetime.timedelta(seconds=5400)  # change timeout to avoid a strange NCCL bug

    # Initialize accelerator and tensorboard logging
    accelerator = Accelerator(
        mixed_precision=config.model.mixed_precision,
        gradient_accumulation_steps=config.train.gradient_accumulation_steps,
        log_with=args.report_to,
        project_dir=osp.join(config.work_dir, "logs"),
        kwargs_handlers=[init_handler],
    )

    log_name = "train_log.log"
    logger = get_root_logger(osp.join(config.work_dir, log_name))
    logger.info(accelerator.state)

    config.train.seed = init_random_seed(getattr(config.train, "seed", None))
    set_random_seed(config.train.seed + int(os.environ["LOCAL_RANK"]))
    generator = torch.Generator(device="cpu").manual_seed(config.train.seed)

    if accelerator.is_main_process:
        pyrallis.dump(config, open(osp.join(config.work_dir, "config.yaml"), "w"), sort_keys=False, indent=4)
        if args.report_to == "wandb":
            import wandb
            # Generate a timestamp to make the run name/id unique on each launch
            run_timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
            run_name = f"{args.name}-{run_timestamp}"
            wandb.init(project=args.tracker_project_name, name=run_name, id=run_name)

    config.global_world_size = get_world_size()
    logger.info(f"Config: \n{config}")
    logger.info(f"World_size: {config.global_world_size}, seed: {config.train.seed}")
    # scheduler
    pred_sigma = getattr(config.scheduler, "pred_sigma", True)
    learn_sigma = getattr(config.scheduler, "learn_sigma", True) and pred_sigma

    # VAE
    vae_dtype = get_weight_dtype(config.vae.weight_dtype)
    vae = get_vae(
        config.vae.vae_type, config.vae.vae_pretrained, accelerator.device, dtype=vae_dtype, config=config.vae
    )

    logger.info(f"vae type: {config.vae.vae_type}, path: {config.vae.vae_pretrained}, weight_dtype: {vae_dtype}")

    # Text encoder
    max_length = config.text_encoder.model_max_length
    tokenizer, text_encoder = get_tokenizer_and_text_encoder(
        name=config.text_encoder.text_encoder_name, device=accelerator.device
    )
    text_embed_dim = text_encoder.config.hidden_size

    if config.text_encoder.chi_prompt:
        chi_prompt = "\n".join(config.text_encoder.chi_prompt)
        logger.info(f"Complex Human Instruct: {chi_prompt}")

    os.makedirs(config.train.null_embed_root, exist_ok=True)
    null_embed_path = osp.join(
        config.train.null_embed_root,
        f"null_embed_diffusers_{config.text_encoder.text_encoder_name}_{max_length}token_{text_embed_dim}.pth",
    )

    # 2. build scheduler
    train_diffusion = Scheduler(
        str(config.scheduler.train_sampling_steps),
        noise_schedule=config.scheduler.noise_schedule,
        predict_flow_v=config.scheduler.predict_flow_v,
        learn_sigma=learn_sigma,
        pred_sigma=pred_sigma,
        snr=config.train.snr_loss,
        flow_shift=config.scheduler.flow_shift,
    )
    predict_info = (
        f"flow-prediction: {config.scheduler.predict_flow_v}, noise schedule: {config.scheduler.noise_schedule}"
    )
    if "flow" in config.scheduler.noise_schedule:
        predict_info += f", flow shift: {config.scheduler.flow_shift}"
        if config.scheduler.inference_flow_shift is not None:
            predict_info += f", inference flow shift: {config.scheduler.inference_flow_shift}"
    if config.scheduler.weighting_scheme in ["logit_normal", "mode"]:
        predict_info += (
            f", flow weighting: {config.scheduler.weighting_scheme}, "
            f"logit-mean: {config.scheduler.logit_mean}, logit-std: {config.scheduler.logit_std}"
        )
    logger.info(predict_info)

    # 3. build dataloader (Minecraft region manifest instead of upstream zips)
    num_replicas = get_world_size()
    rank = get_rank()

    manifest_path = (
        next(iter(config.data.data_dir.values())) if isinstance(config.data.data_dir, dict) else config.data.data_dir
    )
    dataset = MinecraftRegionVideoDataset(
        manifest_path,
        num_frames=config.data.num_frames,
        image_size=config.data.image_size,
        max_length=max_length,
    )
    # Variable region heights: BucketBatchSampler groups equal-frame-count
    # samples so every batch is a fixed-shape tensor without air padding.
    # ponytail: single-process only — it does not shard across ranks.
    assert num_replicas == 1, "BucketBatchSampler does not shard across ranks; run single-GPU."
    batch_sampler = BucketBatchSampler(
        dataset.frame_counts, batch_size=config.train.train_batch_size, seed=config.train.seed
    )
    train_dataloader = build_dataloader(
        dataset,
        num_workers=config.train.num_workers,
        batch_sampler=batch_sampler,
        dataloader_type="video",
    )
    train_dataloader_len = len(train_dataloader)
    bucket_counts = sorted(set(dataset.frame_counts))
    logger.info(f"Height buckets (frames): {bucket_counts[0]}..{bucket_counts[-1]} ({len(bucket_counts)} buckets)")
    logger.info(f"Video set DataLoader length: {train_dataloader_len}")

    # prepare input for visualization during training
    video_height = video_width = config.data.image_size
    num_frames = config.data.num_frames
    latent_width = int(video_width) // config.vae.vae_stride[2]
    latent_height = int(video_height) // config.vae.vae_stride[1]
    latent_temp = int(num_frames - 1) // config.vae.vae_stride[0] + 1

    validation_noise = (
        torch.randn(
            1, config.vae.vae_latent_dim, latent_temp, latent_height, latent_width, device="cpu", generator=generator
        )
        if getattr(config.train, "deterministic_validation", False)
        else None
    )

    # 4. preparing embeddings for visualization. We put it here for saving GPU memory
    if config.train.visualize and len(config.train.validation_prompts):
        valid_prompt_embed_suffix = f"{max_length}token_{config.text_encoder.text_encoder_name}_{text_embed_dim}.pth"
        validation_prompts = config.train.validation_prompts
        skip = True
        if config.text_encoder.chi_prompt:
            uuid_sys_prompt = hashlib.sha256(chi_prompt.encode()).hexdigest()
        else:
            uuid_sys_prompt = hashlib.sha256(b"").hexdigest()
        config.train.valid_prompt_embed_root = osp.join(
            config.train.valid_prompt_embed_root,
            f"{uuid_sys_prompt}_{config.task}_{latent_height}x{latent_width}_{config.vae.vae_type}_{config.model.image_latent_mode}",
        )
        Path(config.train.valid_prompt_embed_root).mkdir(parents=True, exist_ok=True)

        if config.text_encoder.chi_prompt:
            # Save system prompt to a file
            system_prompt_file = osp.join(config.train.valid_prompt_embed_root, "system_prompt.txt")
            with open(system_prompt_file, "w", encoding="utf-8") as f:
                f.write(chi_prompt)

        for prompt in validation_prompts:
            prompt_embed_path = osp.join(
                config.train.valid_prompt_embed_root, f"{prompt[:50]}_{valid_prompt_embed_suffix}"
            )
            if not (osp.exists(prompt_embed_path) and osp.exists(null_embed_path)):
                skip = False
                logger.info(f"Preparing Visualization prompt embeddings at: {config.train.valid_prompt_embed_root}")
                break
        if accelerator.is_main_process and not skip:
            for prompt in validation_prompts:
                prompt_embed_path = osp.join(
                    config.train.valid_prompt_embed_root, f"{prompt[:50]}_{valid_prompt_embed_suffix}"
                )
                if "gemma" in config.text_encoder.text_encoder_name:
                    if not config.text_encoder.chi_prompt:
                        max_length_all = config.text_encoder.model_max_length
                    else:
                        chi_prompt = "\n".join(config.text_encoder.chi_prompt)
                        prompt = chi_prompt + prompt
                        num_sys_prompt_tokens = len(tokenizer.encode(chi_prompt))
                        max_length_all = (
                            num_sys_prompt_tokens + config.text_encoder.model_max_length - 2
                        )  # magic number 2: [bos], [_]

                    txt_tokens = tokenizer(
                        prompt,
                        max_length=max_length_all,
                        padding="max_length",
                        truncation=True,
                        return_tensors="pt",
                    ).to(accelerator.device)
                    select_index = [0] + list(range(-config.text_encoder.model_max_length + 1, 0))
                    caption_emb = text_encoder(txt_tokens.input_ids, attention_mask=txt_tokens.attention_mask)[0][
                        :, select_index
                    ]
                    caption_emb_mask = txt_tokens.attention_mask[:, select_index]
                else:
                    raise ValueError(f"{config.text_encoder.text_encoder_name} is not supported!!")

                save_dict = {"caption_embeds": caption_emb, "emb_mask": caption_emb_mask}
                torch.save(save_dict, prompt_embed_path)

            null_tokens = tokenizer(
                "", max_length=max_length, padding="max_length", truncation=True, return_tensors="pt"
            ).to(accelerator.device)
            null_token_emb = text_encoder(null_tokens.input_ids, attention_mask=null_tokens.attention_mask)[0]
            torch.save(
                {"uncond_prompt_embeds": null_token_emb, "uncond_prompt_embeds_mask": null_tokens.attention_mask},
                null_embed_path,
            )
            del null_token_emb
            del null_tokens
            torch.cuda.empty_cache()

    # load_checkpoint patches y_embedder.y_embedding from this file whenever
    # load_from is set, so it must exist even with visualize=false (upstream
    # only generates it inside the visualization block above).
    if accelerator.is_main_process and not osp.exists(null_embed_path):
        null_tokens = tokenizer(
            "", max_length=max_length, padding="max_length", truncation=True, return_tensors="pt"
        ).to(accelerator.device)
        null_token_emb = text_encoder(null_tokens.input_ids, attention_mask=null_tokens.attention_mask)[0]
        torch.save(
            {"uncond_prompt_embeds": null_token_emb, "uncond_prompt_embeds_mask": null_tokens.attention_mask},
            null_embed_path,
        )
        del null_token_emb
        del null_tokens
        torch.cuda.empty_cache()

    # 5. build models
    os.environ["AUTOCAST_LINEAR_ATTN"] = "true" if config.model.autocast_linear_attn else "false"
    image_size = config.model.image_size
    latent_size = int(image_size) // config.vae.vae_stride[-1]
    model_kwargs = model_video_init_config(config, latent_size=latent_size)
    model = build_model(
        config.model.model,
        config.train.grad_checkpointing,
        getattr(config.model, "fp32_attention", False),
        null_embed_path=null_embed_path,
        **model_kwargs,
    ).train()

    logger.info(
        colored(
            f"{model.__class__.__name__}:{config.model.model}, "
            f"Model Parameters: {sum(p.numel() for p in model.parameters()) / 1e6:.2f}M",
            "green",
            attrs=["bold"],
        )
    )

    # 5-1. load model
    if args.load_from is not None:
        config.model.load_from = args.load_from
    if config.model.load_from is not None and load_from:
        load_result = load_checkpoint(
            checkpoint=config.model.load_from,
            model=model,
            null_embed_path=null_embed_path,
        )

        _, missing, unexpected, _, _ = load_result
        logger.warning(colored(f"Missing keys: {missing}", "red"))
        logger.warning(colored(f"Unexpected keys: {unexpected}", "red"))

    # 6. build optimizer and lr scheduler
    lr_scale_ratio = 1
    if getattr(config.train, "auto_lr", None):
        lr_scale_ratio = auto_scale_lr(
            config.train.train_batch_size * get_world_size() * config.train.gradient_accumulation_steps,
            config.train.optimizer,
            **config.train.auto_lr,
        )
    optimizer = build_optimizer(model, config.train.optimizer)


    lr_scheduler = build_lr_scheduler(config.train, optimizer, train_dataloader, lr_scale_ratio)
    logger.warning(
        f"{colored('Basic Training Settings: ', 'green', attrs=['bold'])}"
        f"lr: {config.train.optimizer['lr']:.5f}, bs: {config.train.train_batch_size}, gc: {config.train.grad_checkpointing}, "
        f"gc_accum_step: {config.train.gradient_accumulation_steps}."
    )
    logger.info(
        f"{colored('Model Settings: ', 'green', attrs=['bold'])}"
        f"qk norm: {config.model.qk_norm}, fp32 attn: {config.model.fp32_attention}, attn type: {config.model.attn_type}, linear_head_dim: {config.model.linear_head_dim}, ffn type: {config.model.ffn_type}, "
        f"text encoder: {config.text_encoder.text_encoder_name}, captions: {config.data.caption_proportion}, precision: {config.model.mixed_precision}."
    )

    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())

    if accelerator.is_main_process:
        tracker_config = dict(vars(config))
        try:
            accelerator.init_trackers(args.tracker_project_name, tracker_config)
        except Exception:
            accelerator.init_trackers(f"tb_{timestamp}")

    start_epoch = 0
    start_step = 0
    start_video_step = 0
    total_steps = train_dataloader_len * config.train.num_epochs

    # 7. Resume training
    if config.model.resume_from is not None and config.model.resume_from["checkpoint"] is not None:
        loaded_video_step = None
        fresh_start = False
        ckpt_path = osp.join(config.work_dir, "checkpoints")
        check_flag = osp.exists(ckpt_path) and len(os.listdir(ckpt_path)) != 0
        remove_state_dict_keys = config.model.remove_state_dict_keys

        if config.model.resume_from["checkpoint"] == "latest":
            if check_flag:
                remove_state_dict_keys = None
                config.model.resume_from["resume_optimizer"] = True
                config.model.resume_from["resume_lr_scheduler"] = True
                checkpoints = os.listdir(ckpt_path)
                if "latest.pth" in checkpoints and osp.exists(osp.join(ckpt_path, "latest.pth")):
                    config.model.resume_from["checkpoint"] = osp.realpath(osp.join(ckpt_path, "latest.pth"))
                else:
                    checkpoints = [i for i in checkpoints if i.startswith("epoch_")]
                    checkpoints = sorted(checkpoints, key=lambda x: int(x.replace(".pth", "").split("_")[3]))
                    config.model.resume_from["checkpoint"] = osp.join(ckpt_path, checkpoints[-1])
            else:
                # Fresh start: load_from is a weights-only init, so the
                # epoch/step encoded in its filename must not be adopted.
                fresh_start = True
                config.model.resume_from["resume_optimizer"] = config.train.load_from_optimizer
                config.model.resume_from["resume_lr_scheduler"] = config.train.load_from_lr_scheduler
                config.model.resume_from["checkpoint"] = config.model.load_from

        if config.model.resume_from["checkpoint"] is not None:
            load_result = load_checkpoint(
                **config.model.resume_from,
                model=model,
                optimizer=optimizer,
                lr_scheduler=lr_scheduler,
                null_embed_path=null_embed_path,
                remove_state_dict_keys=remove_state_dict_keys,
            )

            epoch, missing, unexpected, rng_state, saved_info = load_result
            loaded_video_step = saved_info.get("video_step", None)

            logger.warning(colored(f"Missing keys: {missing}", "red"))
            logger.warning(colored(f"Unexpected keys: {unexpected}", "red"))

            if not fresh_start:
                path = osp.basename(config.model.resume_from["checkpoint"])
                try:
                    start_epoch = int(path.replace(".pth", "").split("_")[1]) - 1
                    start_step = int(path.replace(".pth", "").split("_")[3])
                except (IndexError, ValueError):
                    pass

        if loaded_video_step is not None and not fresh_start:
            start_video_step = loaded_video_step
            logger.info(f"Loaded video_step: {start_video_step} from checkpoint")
        else:
            start_video_step = start_step
            logger.info(f"No video_step in checkpoint, using global_step as video_step: {start_video_step}")

    # 8. Prepare everything
    model = accelerator.prepare(model)
    optimizer, lr_scheduler = accelerator.prepare(optimizer, lr_scheduler)

    set_random_seed((start_step + 1) // config.train.save_model_steps + int(os.environ["LOCAL_RANK"]))
    logger.info(f"Set seed: {(start_step + 1) // config.train.save_model_steps + int(os.environ['LOCAL_RANK'])}")

    # Start Training
    train(
        config=config,
        args=args,
        accelerator=accelerator,
        model=model,
        optimizer=optimizer,
        lr_scheduler=lr_scheduler,
        train_dataloader=train_dataloader,
        train_diffusion=train_diffusion,
        logger=logger,
    )


if __name__ == "__main__":
    main()
