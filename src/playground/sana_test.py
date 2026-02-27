import torch
from typing import Union, List, Optional, Dict, Any, Tuple, Callable
import warnings
from diffusers import SanaVideoPipeline
from diffusers import AutoencoderKLWan
from diffusers.utils import export_to_video
from diffusers.pipelines.sana_video.pipeline_sana_video import *
import PIL.Image as Image
import numpy as np


model_id = "Efficient-Large-Model/SANA-Video_2B_480p_diffusers"



# pipe: SanaVideoPipeline = SanaVideoPipeline.from_pretrained(model_id, torch_dtype=torch.bfloat16)
# pipe.vae.to(torch.float32)
# pipe.text_encoder.to(torch.bfloat16)
# pipe.to("cuda")
# motion_score = 30

# prompt = "happy turtle"
# negative_prompt = "A chaotic sequence with misshapen, deformed limbs in heavy motion blur, sudden disappearance, jump cuts, jerky movements, rapid shot changes, frames out of sync, inconsistent character shapes, temporal artifacts, jitter, and ghosting effects, creating a disorienting visual experience."
# motion_prompt = f" motion score: {motion_score}."
# prompt = prompt + motion_prompt

# video = pipe(
#     prompt=prompt,
#     negative_prompt=negative_prompt,
#     height=512,
#     width=512,
#     frames=16,
#     guidance_scale=6,
#     use_resolution_binning=False,  # Add this line
#     num_inference_steps=30,
#     generator=torch.Generator(device="cuda").manual_seed(42),
# ).frames[0]

# export_to_video(video, "sana_video.mp4", fps=16)


class SanaVideoCustomPipeline(SanaVideoPipeline):
    @torch.no_grad()
    @replace_example_docstring(EXAMPLE_DOC_STRING)
    def __call__(
        self,
        prompt: Union[str, List[str]] = None,
        negative_prompt: str = "",
        num_inference_steps: int = 50,
        timesteps: List[int] = None,
        sigmas: List[float] = None,
        guidance_scale: float = 6.0,
        num_videos_per_prompt: Optional[int] = 1,
        height: int = 480,
        width: int = 832,
        frames: int = 81,
        eta: float = 0.0,
        generator: Optional[Union[torch.Generator, List[torch.Generator]]] = None,
        latents: Optional[torch.Tensor] = None,
        prompt_embeds: Optional[torch.Tensor] = None,
        prompt_attention_mask: Optional[torch.Tensor] = None,
        negative_prompt_embeds: Optional[torch.Tensor] = None,
        negative_prompt_attention_mask: Optional[torch.Tensor] = None,
        output_type: Optional[str] = "pil",
        return_dict: bool = True,
        clean_caption: bool = False,
        use_resolution_binning: bool = True,
        attention_kwargs: Optional[Dict[str, Any]] = None,
        callback_on_step_end: Optional[Callable[[int, int, Dict], None]] = None,
        callback_on_step_end_tensor_inputs: List[str] = ["latents"],
        max_sequence_length: int = 300,
        video: Optional[torch.Tensor] = None,
        mask_video: Optional[torch.Tensor] = None,
        inpainting_coeff: Optional[float] = 0, # 0 means no inpainting, 1 means full inpainting
        complex_human_instruction: List[str] = [
            "Given a user prompt, generate an 'Enhanced prompt' that provides detailed visual descriptions suitable for video generation. Evaluate the level of detail in the user prompt:",
            "- If the prompt is simple, focus on adding specifics about colors, shapes, sizes, textures, motion, and temporal relationships to create vivid and dynamic scenes.",
            "- If the prompt is already detailed, refine and enhance the existing details slightly without overcomplicating.",
            "Here are examples of how to transform or refine prompts:",
            "- User Prompt: A cat sleeping -> Enhanced: A small, fluffy white cat slowly settling into a curled position, peacefully falling asleep on a warm sunny windowsill, with gentle sunlight filtering through surrounding pots of blooming red flowers.",
            "- User Prompt: A busy city street -> Enhanced: A bustling city street scene at dusk, featuring glowing street lamps gradually lighting up, a diverse crowd of people in colorful clothing walking past, and a double-decker bus smoothly passing by towering glass skyscrapers.",
            "Please generate only the enhanced description for the prompt below and avoid including any additional commentary or evaluations:",
            "User Prompt: ",
        ],
    ) -> Union[SanaVideoPipelineOutput, Tuple]:
        """
        Function invoked when calling the pipeline for generation.

        Args:
            prompt (`str` or `List[str]`, *optional*):
                The prompt or prompts to guide the video generation. If not defined, one has to pass `prompt_embeds`.
                instead.
            negative_prompt (`str` or `List[str]`, *optional*):
                The prompt or prompts not to guide the video generation. If not defined, one has to pass
                `negative_prompt_embeds` instead. Ignored when not using guidance (i.e., ignored if `guidance_scale` is
                less than `1`).
            num_inference_steps (`int`, *optional*, defaults to 50):
                The number of denoising steps. More denoising steps usually lead to a higher quality video at the
                expense of slower inference.
            timesteps (`List[int]`, *optional*):
                Custom timesteps to use for the denoising process with schedulers which support a `timesteps` argument
                in their `set_timesteps` method. If not defined, the default behavior when `num_inference_steps` is
                passed will be used. Must be in descending order.
            sigmas (`List[float]`, *optional*):
                Custom sigmas to use for the denoising process with schedulers which support a `sigmas` argument in
                their `set_timesteps` method. If not defined, the default behavior when `num_inference_steps` is passed
                will be used.
            guidance_scale (`float`, *optional*, defaults to 4.5):
                Guidance scale as defined in [Classifier-Free Diffusion
                Guidance](https://huggingface.co/papers/2207.12598). `guidance_scale` is defined as `w` of equation 2.
                of [Imagen Paper](https://huggingface.co/papers/2205.11487). Guidance scale is enabled by setting
                `guidance_scale > 1`. Higher guidance scale encourages to generate videos that are closely linked to
                the text `prompt`, usually at the expense of lower video quality.
            num_videos_per_prompt (`int`, *optional*, defaults to 1):
                The number of videos to generate per prompt.
            height (`int`, *optional*, defaults to 480):
                The height in pixels of the generated video.
            width (`int`, *optional*, defaults to 832):
                The width in pixels of the generated video.
            frames (`int`, *optional*, defaults to 81):
                The number of frames in the generated video.
            eta (`float`, *optional*, defaults to 0.0):
                Corresponds to parameter eta (η) in the DDIM paper: https://huggingface.co/papers/2010.02502. Only
                applies to [`schedulers.DDIMScheduler`], will be ignored for others.
            generator (`torch.Generator` or `List[torch.Generator]`, *optional*):
                One or a list of [torch generator(s)](https://pytorch.org/docs/stable/generated/torch.Generator.html)
                to make generation deterministic.
            latents (`torch.Tensor`, *optional*):
                Pre-generated noisy latents, sampled from a Gaussian distribution, to be used as inputs for video
                generation. Can be used to tweak the same generation with different prompts. If not provided, a latents
                tensor will be generated by sampling using the supplied random `generator`.
            prompt_embeds (`torch.Tensor`, *optional*):
                Pre-generated text embeddings. Can be used to easily tweak text inputs, *e.g.* prompt weighting. If not
                provided, text embeddings will be generated from `prompt` input argument.
            prompt_attention_mask (`torch.Tensor`, *optional*): Pre-generated attention mask for text embeddings.
            negative_prompt_embeds (`torch.Tensor`, *optional*):
                Pre-generated negative text embeddings. For PixArt-Sigma this negative prompt should be "". If not
                provided, negative_prompt_embeds will be generated from `negative_prompt` input argument.
            negative_prompt_attention_mask (`torch.Tensor`, *optional*):
                Pre-generated attention mask for negative text embeddings.
            output_type (`str`, *optional*, defaults to `"pil"`):
                The output format of the generated video. Choose between mp4 or `np.array`.
            return_dict (`bool`, *optional*, defaults to `True`):
                Whether or not to return a [`SanaVideoPipelineOutput`] instead of a plain tuple.
            attention_kwargs:
                A kwargs dictionary that if specified is passed along to the `AttentionProcessor` as defined under
                `self.processor` in
                [diffusers.models.attention_processor](https://github.com/huggingface/diffusers/blob/main/src/diffusers/models/attention_processor.py).
            clean_caption (`bool`, *optional*, defaults to `True`):
                Whether or not to clean the caption before creating embeddings. Requires `beautifulsoup4` and `ftfy` to
                be installed. If the dependencies are not installed, the embeddings will be created from the raw
                prompt.
            use_resolution_binning (`bool` defaults to `True`):
                If set to `True`, the requested height and width are first mapped to the closest resolutions using
                `ASPECT_RATIO_480_BIN` or `ASPECT_RATIO_720_BIN`. After the produced latents are decoded into videos,
                they are resized back to the requested resolution. Useful for generating non-square videos.
            callback_on_step_end (`Callable`, *optional*):
                A function that calls at the end of each denoising steps during the inference. The function is called
                with the following arguments: `callback_on_step_end(self: DiffusionPipeline, step: int, timestep: int,
                callback_kwargs: Dict)`. `callback_kwargs` will include a list of all tensors as specified by
                `callback_on_step_end_tensor_inputs`.
            callback_on_step_end_tensor_inputs (`List`, *optional*):
                The list of tensor inputs for the `callback_on_step_end` function. The tensors specified in the list
                will be passed as `callback_kwargs` argument. You will only be able to include variables listed in the
                `._callback_tensor_inputs` attribute of your pipeline class.
            max_sequence_length (`int` defaults to `300`):
                Maximum sequence length to use with the `prompt`.
            complex_human_instruction (`List[str]`, *optional*):
                Instructions for complex human attention:
                https://github.com/NVlabs/Sana/blob/main/configs/sana_app_config/Sana_1600M_app.yaml#L55.

        Examples:

        Returns:
            [`~pipelines.sana_video.pipeline_output.SanaVideoPipelineOutput`] or `tuple`:
                If `return_dict` is `True`, [`~pipelines.sana_video.pipeline_output.SanaVideoPipelineOutput`] is
                returned, otherwise a `tuple` is returned where the first element is a list with the generated videos
        """

        if isinstance(callback_on_step_end, (PipelineCallback, MultiPipelineCallbacks)):
            callback_on_step_end_tensor_inputs = callback_on_step_end.tensor_inputs

        # 1. Check inputs. Raise error if not correct
        if use_resolution_binning:
            if self.transformer.config.sample_size == 30:
                aspect_ratio_bin = ASPECT_RATIO_480_BIN
            elif self.transformer.config.sample_size == 22:
                aspect_ratio_bin = ASPECT_RATIO_720_BIN
            else:
                raise ValueError("Invalid sample size")
            orig_height, orig_width = height, width
            height, width = self.video_processor.classify_height_width_bin(height, width, ratios=aspect_ratio_bin)

        self.check_inputs(
            prompt,
            height,
            width,
            callback_on_step_end_tensor_inputs,
            negative_prompt,
            prompt_embeds,
            negative_prompt_embeds,
            prompt_attention_mask,
            negative_prompt_attention_mask,
        )

        self._guidance_scale = guidance_scale
        self._attention_kwargs = attention_kwargs
        self._interrupt = False

        # 2. Default height and width to transformer
        if prompt is not None and isinstance(prompt, str):
            batch_size = 1
        elif prompt is not None and isinstance(prompt, list):
            batch_size = len(prompt)
        else:
            batch_size = prompt_embeds.shape[0]

        device = self._execution_device
        lora_scale = self.attention_kwargs.get("scale", None) if self.attention_kwargs is not None else None

        # 3. Encode input prompt
        (
            prompt_embeds,
            prompt_attention_mask,
            negative_prompt_embeds,
            negative_prompt_attention_mask,
        ) = self.encode_prompt(
            prompt,
            self.do_classifier_free_guidance,
            negative_prompt=negative_prompt,
            num_videos_per_prompt=num_videos_per_prompt,
            device=device,
            prompt_embeds=prompt_embeds,
            negative_prompt_embeds=negative_prompt_embeds,
            prompt_attention_mask=prompt_attention_mask,
            negative_prompt_attention_mask=negative_prompt_attention_mask,
            clean_caption=clean_caption,
            max_sequence_length=max_sequence_length,
            complex_human_instruction=complex_human_instruction,
            lora_scale=lora_scale,
        )
        if self.do_classifier_free_guidance:
            prompt_embeds = torch.cat([negative_prompt_embeds, prompt_embeds], dim=0)
            prompt_attention_mask = torch.cat([negative_prompt_attention_mask, prompt_attention_mask], dim=0)

        # 4. Prepare timesteps
        timesteps, num_inference_steps = retrieve_timesteps(
            self.scheduler, num_inference_steps, device, timesteps, sigmas
        )

        # 5. Prepare latents.
        latent_channels = self.transformer.config.in_channels
        latents = self.prepare_latents(
            batch_size * num_videos_per_prompt,
            latent_channels,
            height,
            width,
            frames,
            torch.float32,
            device,
            generator,
            latents,
        )

        # 6. Prepare extra step kwargs. TODO: Logic should ideally just be moved out of the pipeline
        extra_step_kwargs = self.prepare_extra_step_kwargs(generator, eta)

        # --- ADDED: INPAINTING PREPARATION ---
        init_latents = None
        mask = None
        if video is not None and mask_video is not None and inpainting_coeff > 0:
            # 1. Preprocess and Encode the reference video
            # Assuming video input is [B, C, F, H, W]
            video_input = video.to(device=device, dtype=self.vae.dtype)
            
            # Encode and scale based on VAE config
            latents_mean = torch.tensor(self.vae.config.latents_mean).view(1, -1, 1, 1, 1).to(device, self.vae.dtype)
            latents_std = torch.tensor(self.vae.config.latents_std).view(1, -1, 1, 1, 1).to(device, self.vae.dtype)
            
            init_latents = self.vae.encode(video_input, return_dict=False)[0].sample()
            init_latents = (init_latents - latents_mean) / latents_std

            # 2. Prepare the mask
            # Resize mask to latent dimensions [F_lat, H_lat, W_lat]
            mask = torch.nn.functional.interpolate(
                mask_video.to(device=device, dtype=torch.float32), 
                size=init_latents.shape[-3:], 
                mode="nearest"
            )
            # Ensure the mask is used as a blending coefficient
            mask = mask * inpainting_coeff
            
            # 3. Prepare fixed background noise
            background_noise = torch.randn_like(init_latents)
        # --------------------------------------

        # 7. Denoising loop
        num_warmup_steps = max(len(timesteps) - num_inference_steps * self.scheduler.order, 0)
        self._num_timesteps = len(timesteps)

        transformer_dtype = self.transformer.dtype
        with self.progress_bar(total=num_inference_steps) as progress_bar:
            for i, t in enumerate(timesteps):
                if self.interrupt:
                    continue

                latent_model_input = torch.cat([latents] * 2) if self.do_classifier_free_guidance else latents

                # broadcast to batch dimension in a way that's compatible with ONNX/Core ML
                timestep = t.expand(latent_model_input.shape[0])

                # predict noise model_output
                noise_pred = self.transformer(
                    latent_model_input.to(dtype=transformer_dtype),
                    encoder_hidden_states=prompt_embeds.to(dtype=transformer_dtype),
                    encoder_attention_mask=prompt_attention_mask,
                    timestep=timestep,
                    return_dict=False,
                    attention_kwargs=self.attention_kwargs,
                )[0]
                noise_pred = noise_pred.float()

                # perform guidance
                if self.do_classifier_free_guidance:
                    noise_pred_uncond, noise_pred_text = noise_pred.chunk(2)
                    noise_pred = noise_pred_uncond + guidance_scale * (noise_pred_text - noise_pred_uncond)

                # learned sigma
                if self.transformer.config.out_channels // 2 == latent_channels:
                    noise_pred = noise_pred.chunk(2, dim=1)[0]

                # compute previous image: x_t -> x_t-1
                latents = self.scheduler.step(noise_pred, t, latents, **extra_step_kwargs, return_dict=False)[0]

                # --- ADDED: INPAINTING RE-INJECTION ---
                if init_latents is not None and mask is not None:
                    # Determine noise level for the background. 
                    # We should match the noise level of the NEXT step's latents.
                    if i < len(timesteps) - 1:
                        noise_timestep = timesteps[i+1]
                        init_latents_proper = self.scheduler.add_noise(init_latents, background_noise, noise_timestep.view(-1))
                    else:
                        init_latents_proper = init_latents
                    
                    # Blend: Generated where mask is 1, Original where mask is 0
                    latents = (latents * mask) + (init_latents_proper * (1.0 - mask))
                # --------------------------------------

                if callback_on_step_end is not None:
                    callback_kwargs = {}
                    for k in callback_on_step_end_tensor_inputs:
                        callback_kwargs[k] = locals()[k]
                    callback_outputs = callback_on_step_end(self, i, t, callback_kwargs)

                    latents = callback_outputs.pop("latents", latents)
                    prompt_embeds = callback_outputs.pop("prompt_embeds", prompt_embeds)
                    negative_prompt_embeds = callback_outputs.pop("negative_prompt_embeds", negative_prompt_embeds)

                # call the callback, if provided
                if i == len(timesteps) - 1 or ((i + 1) > num_warmup_steps and (i + 1) % self.scheduler.order == 0):
                    progress_bar.update()

                if XLA_AVAILABLE:
                    xm.mark_step()

        # Final blend to ensure unmasked areas are perfectly preserved
        if init_latents is not None and mask is not None:
            latents = (latents * mask) + (init_latents * (1.0 - mask))

        if output_type == "latent":
            video = latents
        else:
            latents = latents.to(self.vae.dtype)
            torch_accelerator_module = getattr(torch, get_device(), torch.cuda)
            oom_error = (
                torch.OutOfMemoryError
                if is_torch_version(">=", "2.5.0")
                else torch_accelerator_module.OutOfMemoryError
            )
            latents_mean = (
                torch.tensor(self.vae.config.latents_mean)
                .view(1, self.vae.config.z_dim, 1, 1, 1)
                .to(latents.device, latents.dtype)
            )
            latents_std = 1.0 / torch.tensor(self.vae.config.latents_std).view(1, self.vae.config.z_dim, 1, 1, 1).to(
                latents.device, latents.dtype
            )
            latents = latents / latents_std + latents_mean
            try:
                video = self.vae.decode(latents, return_dict=False)[0]
            except oom_error as e:
                warnings.warn(
                    f"{e}. \n"
                    f"Try to use VAE tiling for large images. For example: \n"
                    f"pipe.vae.enable_tiling(tile_sample_min_width=512, tile_sample_min_height=512)"
                )

            if use_resolution_binning:
                video = self.video_processor.resize_and_crop_tensor(video, orig_width, orig_height)

            video = self.video_processor.postprocess_video(video, output_type=output_type)

        # Offload all models
        self.maybe_free_model_hooks()

        if not return_dict:
            return (video,)

        return SanaVideoPipelineOutput(frames=video)


image = Image.open("image.png").convert("RGB")
image = np.array(image)
print(image.shape)
image_video = np.stack([image] * 16)
print(image_video.shape)
image_video = image_video.transpose(3, 0, 1, 2)
image_video = (image_video / 127.5) - 1.0
print(image_video.shape)
image_video = torch.from_numpy(image_video).to(torch.float32).unsqueeze(0)
print(image_video.shape)

mask_video = np.zeros((16, 512, 512))
mask_video[:, 256:] = 1
mask_video = torch.from_numpy(mask_video).to(torch.float32).unsqueeze(0).unsqueeze(0)


pipe: SanaVideoPipeline = SanaVideoCustomPipeline.from_pretrained(model_id, torch_dtype=torch.bfloat16)
pipe.vae.to(torch.float32)
pipe.text_encoder.to(torch.bfloat16)
pipe.to("cuda")
motion_score = 30

prompt = "nicki minaj"
negative_prompt = ""
motion_prompt = f" motion score: {motion_score}."
prompt = prompt + motion_prompt


video = pipe(
    prompt=prompt,
    negative_prompt=negative_prompt,
    height=512,
    width=512,
    frames=16,
    guidance_scale=6,
    use_resolution_binning=False,  # Add this line
    num_inference_steps=30,
    generator=torch.Generator(device="cuda").manual_seed(42),
    video=image_video,
    mask_video=mask_video,
    inpainting_coeff=0.99,

).frames[0]

export_to_video(video, "sana_video.mp4", fps=16)