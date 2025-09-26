PY=python3
QUEUE=data/Links_Queue_with_selected.csv
SCRAPE_LOG=results/scrape_log.csv
CORPUS=results/scraped_corpus.jsonl

scrape:
	$(PY) scripts/scrape_selected.py --in $(QUEUE) --out $(SCRAPE_LOG) --jsonl $(CORPUS) --artifacts artifacts --max_per_category 140 --concurrency 4

verify-scrape:
	$(PY) - <<'PY'
import json, os, pandas as pd
log = pd.read_csv("results/scrape_log.csv")
print("Statuses:\n", log['status'].value_counts())
print("Corpus lines:", sum(1 for _ in open("results/scraped_corpus.jsonl","r",encoding="utf-8")))
PY

export-ctikg:
	$(PY) scripts/export_ctikg_input.py --in_jsonl results/scraped_corpus.jsonl --out_csv data/ctikg_input.csv --out_docs data/ctikg_docs_meta.jsonl

.PHONY: scrape verify-scrape export-ctikg

# -------- Scrape / Chunk / Export (Phase B) --------
CATEGORY ?=
RATE ?= 1.0
TIMEOUT ?= 12

scrape:
	python3 scripts/scrape_selected.py \
		--winners data/Links_Queue_with_selected.csv \
		$(if $(CATEGORY),--category "$(CATEGORY)",) \
		--outdir content --rate $(RATE) --timeout $(TIMEOUT)

chunk:
	python3 scripts/chunk_articles.py \
		$(if $(CATEGORY),--category "$(CATEGORY)",) \
		--indir content/text --outdir chunks

export-ctikg:
	python3 scripts/export_for_ctikg.py --chunks_dir chunks --out exports

# One-shot pipeline including your existing rank/merge/flags/select
quickstart:
	$(MAKE) all
	$(MAKE) scrape $(if $(CATEGORY),CATEGORY="$(CATEGORY)",)
	$(MAKE) chunk $(if $(CATEGORY),CATEGORY="$(CATEGORY)",)
	$(MAKE) export-ctikg
