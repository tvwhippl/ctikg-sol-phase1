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

# -------- Open-topic pipeline (LLM-assisted) --------
TOPIC ?=
SOURCES ?= configs/sources/common.json
WINNERS ?= 120
RATE ?= 1.0
TIMEOUT ?= 12

# Generate a category YAML using a local LLM (Ollama by default).
topic-gen:
	@[ -n "$(TOPIC)" ] || (echo "Set TOPIC=\"...\""; exit 2)
	python3 scripts/gen_category_from_llm.py --topic "$(TOPIC)" --provider $${LLM_PROVIDER:-ollama} --model $${LLM_MODEL:-llama3.1}

# Pull feeds (neutral bundle), update queue, add flags
topic-pull:
	python3 scripts/pre_rank_links_v3.py \
		--sources $(SOURCES) \
		--categories configs/Category_Keywords_Expanded.json \
		--out batch_topic.csv \
		--limit_per_feed 600 --half_life_days 9999 --verbose
	python3 scripts/merge_dedupe.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv ; \
	mv data/Links_Queue_master.csv data/Links_Queue.csv ; \
	python3 scripts/make_helper_flags.py data/Links_Queue.csv

# Select winners with the generated YAML (picks the most recent file)
topic-select:
	@export CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	echo "Selecting with $$CAT" && \
	python3 scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category $$CAT

topic-scrape:
	@export CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	export CATNAME=$$(python3 - <<'PY'\nimport yaml,sys,re\np=sys.argv[1]\nd=yaml.safe_load(open(p))\nprint(re.sub(r\"[^A-Za-z0-9]+\",\"_\",d['name']).strip('_'))\nPY $$CAT) && \
	python3 scripts/scrape_selected.py \
	  --winners data/Selected_$${CATNAME}.csv \
	  --category "$${CATNAME}" --outdir content --rate $(RATE) --timeout $(TIMEOUT)

topic-chunk:
	@export CAT=$$(ls -t configs/categories/_generated/*.yaml | head -n1) && \
	export CATNAME=$$(python3 - <<'PY'\nimport yaml,sys,re\np=sys.argv[1]\nd=yaml.safe_load(open(p))\nprint(d['name'])\nPY $$CAT) && \
	python3 scripts/chunk_articles.py --category "$${CATNAME}" --indir content/text --outdir chunks

topic-export:
	python3 scripts/export_for_ctikg.py --chunks_dir chunks --out exports

# One-shot
topic: topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export
