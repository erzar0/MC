# vllm serve Qwen/Qwen3-VL-4B-Instruct-FP8 --max-model-len 32768 --gpu-memory-utilization 0.9
import base64
from openai import OpenAI

# Initialize the client pointing to your local vLLM server
client = OpenAI(
    api_key="token-is-ignored", 
    base_url="http://localhost:8000/v1"
)

# Function to encode local image to base64
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

image_path = "/home/kyre/repos/minecraft-world-generator/Screenshot 2026-02-06 231819.png"
base64_image = encode_image(image_path)

# Construct the messages payload
messages = [
    {
        "role": "user",
        "content": [
            {
                "type": "text", 
                "text": "What animal is on the image?"
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{base64_image}"
                },
            },
        ],
    }
]

for i in range(10):
    # Execute the request
    response = client.chat.completions.create(
        model="Qwen/Qwen3-VL-4B-Instruct-FP8", # e.g., "llava-hf/llava-1.5-7b-hf"
        messages=messages,
    )

    print(response.choices[0].message.content)