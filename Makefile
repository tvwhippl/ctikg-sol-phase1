SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
PY ?= python3

# Use the pipeline we maintain in open_topic.mk
include open_topic.mk

# ---- Optional: legacy helpers, renamed so they don't collide ----
.PHONY: legacy-scrape legacy-export-ctikg

legacy-scrape:
	$(PY) scripts/scrape_selected.py \
		--in "data/Links_Queue_with_selected.csv" \
		--out "results/scrape_log.csv" \
		--jsonl "results/scraped_corpus.jsonl" \
		--artifacts artifacts --max_per_category 140 --concurrency 4

legacy-export-ctikg:
	$(PY) scripts/export_ctikg_input.py \
		--in_jsonl "results/scraped_corpus.jsonl" \
		--out_csv "exports/ctikg_input.csv" \
		--out_docs "data/ctikg_docs_meta.json"
