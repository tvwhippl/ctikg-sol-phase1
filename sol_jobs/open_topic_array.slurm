#!/bin/bash
#SBATCH -J ctikg-open-topic-array
#SBATCH -c 4
#SBATCH --mem=8G
#SBATCH -t 04:00:00
#SBATCH -o logs/%x-%A_%a.out
#SBATCH --array=1-1

set -euo pipefail

# ---- User inputs ----
# A text file with one topic YAML path per line.
TOPICS_LIST="${TOPICS_LIST:-configs/categories/_generated/topics.list}"

# Link queue (build once with `make topic-pull`)
QUEUE_CSV="${QUEUE_CSV:-data/Links_Queue_sorted_flags.csv}"

# Optional: parent directory for outputs
RUNS_ROOT="${RUNS_ROOT:-runs/slurm-${SLURM_JOB_ID}}"

# ---- Resolve the YAML for this array task ----
TOPIC_YAML=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$TOPICS_LIST" | tr -d '\r')
if [[ -z "${TOPIC_YAML}" ]]; then
  echo "[FATAL] No topic YAML found for task ${SLURM_ARRAY_TASK_ID} in ${TOPICS_LIST}" >&2
  exit 2
fi

echo "[INFO] task=${SLURM_ARRAY_TASK_ID} topic_yaml=${TOPIC_YAML}"

# ---- Environment ----
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

mkdir -p logs "$RUNS_ROOT"

# ---- Run ----
python scripts/run_open_topic.py \
  --topic-yaml "$TOPIC_YAML" \
  --queue "$QUEUE_CSV" \
  --run-dir "$RUNS_ROOT/${SLURM_ARRAY_TASK_ID}" \
  --concurrency "${CONCURRENCY:-4}" \
  --throttle-sec "${THROTTLE_SEC:-1}" \
  --min-qsim "${MIN_QSIM:-0.10}"
