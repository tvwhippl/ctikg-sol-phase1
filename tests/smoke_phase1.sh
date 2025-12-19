#!/usr/bin/env bash
set -euo pipefail
source .venv/bin/activate

rm -f data/Links_Queue.csv data/Links_Queue_sorted_flags.csv \
      results/scraped_corpus.jsonl results/scrape_log.csv

make topic-pull SOURCES=configs/sources/common.json
make topic-select
make topic-scrape WINNERS=3 CONCURRENCY=1 THROTTLE_SEC=1 IGNORE_ROBOTS=1

python scripts/export_ctikg_input.py \
  --in_jsonl results/scraped_corpus.jsonl \
  --out_csv exports/ctikg_input.csv \
  --out_docs data/ctikg_docs_meta.json

test -s exports/ctikg_input.csv
echo "SMOKE TEST PASSED"