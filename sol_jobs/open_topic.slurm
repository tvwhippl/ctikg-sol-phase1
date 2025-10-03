#!/bin/bash
#SBATCH -J ctikg-open-topic
#SBATCH -c 4
#SBATCH --mem=8G
#SBATCH -t 02:00:00
#SBATCH -o logs/%x-%j.out

set -euo pipefail

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make topic-setup
make topic-gen TOPIC="${TOPIC}"
make topic-pull SOURCES="${SOURCES:-configs/sources/common.json}"
make topic-select
make topic-scrape WINNERS="${WINNERS:-25}" CONCURRENCY="${CONCURRENCY:-4}" THROTTLE_SEC="${THROTTLE_SEC:-1}" IGNORE_ROBOTS="${IGNORE_ROBOTS:-1}"
make topic-chunk
make topic-verify
