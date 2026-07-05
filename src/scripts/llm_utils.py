"""Shared helpers for talking to the local vLLM (OpenAI-compatible) server.

Used by `cleanse_descriptions.py` and `generate_vision_captions.py`.
"""

from openai import AsyncOpenAI, OpenAI

DEFAULT_VLLM_BASE_URL = "http://localhost:8000/v1"

# vLLM ignores the API key, but the OpenAI client requires a non-empty one
_DUMMY_API_KEY = "token-is-ignored"


def make_vllm_client(base_url: str = DEFAULT_VLLM_BASE_URL) -> OpenAI:
    """Creates a synchronous OpenAI client pointed at the local vLLM server."""
    return OpenAI(api_key=_DUMMY_API_KEY, base_url=base_url)


def make_async_vllm_client(base_url: str = DEFAULT_VLLM_BASE_URL) -> AsyncOpenAI:
    """Creates an async OpenAI client pointed at the local vLLM server."""
    return AsyncOpenAI(api_key=_DUMMY_API_KEY, base_url=base_url)


def strip_thinking_tags(text: str) -> str:
    """Removes a leading `<think>...</think>` block from reasoning-model output."""
    text = text.strip()
    if "<think>" in text:
        think_end = text.find("</think>")
        if think_end != -1:
            text = text[think_end + len("</think>") :].strip()
    return text
