#!/usr/bin/env bash
set -euo pipefail

# Smoke test for the v1 one-command open-topic pipeline.
# - Uses PROVIDER=dry-run (no LLM network calls)
# - Still fetches the link queue + scrapes a tiny number of pages

if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

OUT_FILE="$(mktemp -t open_topic_smoke.XXXXXX)"

make open-topic \
  TOPIC="Remote Code Execution" \
  PROVIDER=dry-run MODEL=ignored \
  SCRAPE_MAX=3 CONCURRENCY=1 THROTTLE_SEC=1 IGNORE_ROBOTS=1 \
  | tee "$OUT_FILE"

RUN_DIR="$(grep -E '^\[OK\] run_dir=' "$OUT_FILE" | tail -n 1 | sed 's/^\[OK\] run_dir=//')"

if [ -z "$RUN_DIR" ]; then
  echo "[FAIL] Could not parse RUN_DIR from make output" >&2
  exit 2
fi

test -s "$RUN_DIR/exports/ctikg_input.csv"
test -s "$RUN_DIR/data/ctikg_docs_meta.json"
test -s "$RUN_DIR/selection/ranked.csv"

echo "[smoke] verifying run: $RUN_DIR"
make verify RUN_DIR="$RUN_DIR"

echo "SMOKE OPEN-TOPIC PASSED: $RUN_DIR"
