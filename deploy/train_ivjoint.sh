#!/bin/bash
#
# train_ivjoint.sh — launcher for the ported upstream SANA-Video trainer,
# mirroring Sana/train_video_scripts/train_video_ivjoint.sh:
#
#   bash deploy/train_ivjoint.sh \
#       configs/sana_video_minecraft.yaml \
#       --data.data_dir="{minecraft: tmp/sana_video_manifest.jsonl}" \
#       --train.train_batch_size=1 \
#       --work_dir=output/sana_video \
#       --train.num_workers=10 \
#       --train.visualize=true
#
# A *.yaml positional selects the config (default: configs/sana_video_minecraft.yaml);
# everything else is passed through as pyrallis overrides. Single GPU only
# (the height-bucket sampler does not shard across ranks).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PYTHON="${PYTHON:-.venv/bin/python}"
config=""
other_args=()

while [[ $# -gt 0 ]]; do
    case $1 in
        *.yaml)
            config=$1
            shift
            ;;
        *)
            other_args+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$config" ]]; then
    config="configs/sana_video_minecraft.yaml"
    echo "No yaml file specified. Set to --config_path=$config"
fi

export DISABLE_XFORMERS=1

"$PYTHON" src/scripts/sana_video/train_ivjoint.py \
    --config_path="$config" \
    "${other_args[@]}"
