#!/bin/bash

HF_DEBUG=1 HF_HUB_DOWNLOAD_TIMEOUT=120 HF_HUB_DOWNLOAD_NUM_THREADS=1 HF_ENDPOINT=http://localhost:3000 \
 uv run hf download  Qwen/Qwen3-Embedding-0.6B

uv run modelscope download --model Qwen/Qwen3-Embedding-0.6B --endpoint http://localhost:3000/ms --cache_dir loc2