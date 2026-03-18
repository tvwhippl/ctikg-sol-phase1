#!/bin/bash
#SBATCH -J ot_ranked_offsets
#SBATCH -o logs/ot_ranked_%A_%a.out
#SBATCH -e logs/ot_ranked_%A_%a.err
#SBATCH -t 01:00:00
#SBATCH -n 1
#SBATCH -c 1
#SBATCH --mem=4G
#SBATCH --array=0-3

set -euo pipefail

cd "$HOME/ctikg-sol-phase1"
source .venv/bin/activate

# ---- Inputs (override via sbatch --export=ALL,VAR=...) ----
TOPIC="${TOPIC:-Remote Code Execution}"
STAGE_DIR="${STAGE_DIR:-runs/_stage/Remote_Code_Execution}"
SCRAPE_MAX="${SCRAPE_MAX:-3}"
STRIDE="${STRIDE:-3}"   # typically = SCRAPE_MAX
CONCURRENCY="${CONCURRENCY:-1}"
THROTTLE_SEC="${THROTTLE_SEC:-1}"
IGNORE_ROBOTS="${IGNORE_ROBOTS:-1}"

OFFSET=$(( SLURM_ARRAY_TASK_ID * STRIDE ))

# Keep cache off on shared FS (avoid sqlite contention)
export SCRAPE_CACHE=0

SAFE_TOPIC="${SAFE_TOPIC:-$(basename "$STAGE_DIR")}"
RUN_ROOT="runs/${SAFE_TOPIC}/slurm-${SLURM_JOB_ID}"
RUN_DIR="${RUN_ROOT}/offset_${OFFSET}"

echo "[INFO] job=${SLURM_JOB_ID} task=${SLURM_ARRAY_TASK_ID} topic=${TOPIC} offset=${OFFSET} scrape_max=${SCRAPE_MAX}"
echo "[INFO] stage_dir=${STAGE_DIR}"
echo "[INFO] run_dir=${RUN_DIR}"

mkdir -p "${RUN_DIR}"/{config,queue,selection,scrape,artifacts,exports,data}

# Copy audit inputs into the run dir (small + safe)
cp -f "${STAGE_DIR}/config/topic.yaml" "${RUN_DIR}/config/topic.yaml"
cp -f "${STAGE_DIR}/selection/ranked.csv" "${RUN_DIR}/selection/ranked.csv"
cp -f "${STAGE_DIR}/queue/Links_Queue_sorted_flags.csv" "${RUN_DIR}/queue/Links_Queue_sorted_flags.csv"

# Slice ranked.csv -> selected.csv for this offset
python - <<PY
import csv, sys
from pathlib import Path

ranked = Path("${RUN_DIR}/selection/ranked.csv")
out = Path("${RUN_DIR}/selection/selected.csv")

offset = int("${OFFSET}")
maxn = int("${SCRAPE_MAX}")

rows = list(csv.reader(ranked.open(newline="", encoding="utf-8")))
hdr, data = rows[0], rows[1:]
sl = data[offset:offset+maxn]

out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(hdr)
    w.writerows(sl)

print(f"[INFO] selected_rows={len(sl)} offset={offset} max={maxn} out={out}")
if len(sl) == 0:
    print("[INFO] nothing to do for this offset; exiting 0")
PY

# If selection empty, exit cleanly (shouldn't happen if array sized correctly)
if [ "$(wc -l < "${RUN_DIR}/selection/selected.csv")" -le 1 ]; then
  echo "[INFO] empty selection (offset beyond ranked size). exiting 0"
  exit 0
fi

# Scrape -> export -> verify
python scripts/scrape_selected.py \
  --in "${RUN_DIR}/selection/selected.csv" \
  --out "${RUN_DIR}/scrape/scrape_log.csv" \
  --jsonl "${RUN_DIR}/scrape/scraped_corpus.jsonl" \
  --artifacts "${RUN_DIR}/artifacts" \
  --max_per_category "${SCRAPE_MAX}" \
  --concurrency "${CONCURRENCY}" \
  --throttle_sec "${THROTTLE_SEC}" \
  $( [ "${IGNORE_ROBOTS}" = "1" ] && echo "--ignore_robots" ) \
  --no-cache \
  --stats-json "${RUN_DIR}/scrape/scrape_stats.json"

python scripts/export_ctikg_input.py \
  --in_jsonl "${RUN_DIR}/scrape/scraped_corpus.jsonl" \
  --out_csv "${RUN_DIR}/exports/ctikg_input.csv" \
  --out_docs "${RUN_DIR}/data/ctikg_docs_meta.json" \
  --log_csv "${RUN_DIR}/scrape/scrape_log.csv"

python scripts/verify_export.py \
  --corpus "${RUN_DIR}/scrape/scraped_corpus.jsonl" \
  --csv "${RUN_DIR}/exports/ctikg_input.csv"

# Write manifest (no LLM call happened here, but we still record provider/model for provenance)
python scripts/write_run_manifest.py \
  --run-dir "${RUN_DIR}" \
  --topic "${TOPIC}" \
  --provider "openai" \
  --model "${LLM_MODEL}" \
  --scrape-max "${SCRAPE_MAX}" \
  --offset "${OFFSET}" \
  --concurrency "${CONCURRENCY}" \
  --throttle-sec "${THROTTLE_SEC}" \
  --ignore-robots "${IGNORE_ROBOTS}" \
  --cache-enabled 0 \
  --cache-db "" \
  --cache-ttl-days 0 \
  --rescue-enabled 0 \
  --rescue-max-add 0 \
  --rescue-min-qsim 0.0 \
  --run-started-at-utc "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --run-finished-at-utc "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --dur-gen-yaml-sec -1 --dur-queue-sec -1 --dur-select-sec -1 \
  --dur-scrape-sec -1 --dur-export-sec -1 --dur-verify-sec -1 \
  --verify-status pass

echo "[OK] run_dir=${RUN_DIR}"
