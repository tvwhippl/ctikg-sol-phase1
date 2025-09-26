SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

PY ?= python3

# Baseline I/O
QUEUE      ?= data/Links_Queue_with_selected.csv
SCRAPE_LOG ?= results/scrape_log.csv
CORPUS     ?= results/scraped_corpus.jsonl

# Open-topic vars
TOPIC   ?=
SOURCES ?= configs/sources/common.json
RATE    ?= 1.0
TIMEOUT ?= 12

.PHONY: scrape export-ctikg topic topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export

# ===== Baseline scrape/export =====
scrape:
	$(PY) scripts/scrape_selected.py --in "$(QUEUE)" --out "$(SCRAPE_LOG)" --jsonl "$(CORPUS)" --artifacts artifacts --max_per_category 140 --concurrency 4

export-ctikg:
	$(PY) scripts/export_ctikg_input.py --in_jsonl "$(CORPUS)" --out_csv data/ctikg_input.csv --out_docs data/ctikg_docs_meta.jsonl

# ===== Open-topic pipeline =====
topic-gen:
	: "${TOPIC:?Set TOPIC=\"...\"}"
	$(PY) scripts/gen_category_from_llm.py --topic "$(TOPIC)" --provider "$${LLM_PROVIDER:-ollama}" --model "$${LLM_MODEL:-llama3.1}"

topic-pull:
	$(PY) scripts/pre_rank_links_v3.py --sources "$(SOURCES)" \
	  --categories configs/Category_Keywords_Expanded.json \
	  --out batch_topic.csv --limit_per_feed 600 --half_life_days 9999 --verbose
	$(PY) scripts/merge_dedupe.py data/Links_Queue_master.csv data/Links_Queue.csv batch_topic.csv
	mv data/Links_Queue_master.csv data/Links_Queue.csv
	$(PY) scripts/make_helper_flags.py data/Links_Queue.csv

topic-select:
	CAT="$(shell ls -t configs/categories/_generated/*.yaml 2>/dev/null | head -n1)"; \
	[ -n "$$CAT" ] || { echo "No generated category YAML found in configs/categories/_generated/"; exit 2; }; \
	echo "Selecting with $$CAT"; \
	$(PY) scripts/category_select.py --in data/Links_Queue_sorted_flags.csv --category "$$CAT"

topic-scrape:
	CAT="$(shell ls -t configs/categories/_generated/*.yaml 2>/dev/null | head -n1)"; \
	[ -n "$$CAT" ] || { echo "No generated category YAML found"; exit 2; }; \
	CATNAME="$$(python3 -c "import yaml,sys,re; d=yaml.safe_load(open(sys.argv[1])); print(re.sub(r'[^A-Za-z0-9]+','_',d['name']).strip('_'))" "$$CAT")"; \
	$(PY) scripts/scrape_selected.py --winners "data/Selected_$${CATNAME}.csv" \
	  --category "$${CATNAME}" --outdir content --rate $(RATE) --timeout $(TIMEOUT)

topic-chunk:
	CAT="$(shell ls -t configs/categories/_generated/*.yaml 2>/dev/null | head -n1)"; \
	[ -n "$$CAT" ] || { echo "No generated category YAML found"; exit 2; }; \
	CATNAME="$$(python3 -c "import yaml,sys; print(yaml.safe_load(open(sys.argv[1]))['name'])" "$$CAT")"; \
	$(PY) scripts/chunk_articles.py --category "$${CATNAME}" --indir content/text --outdir chunks

topic-export:
	$(PY) scripts/export_ctikg_input.py --chunks_dir chunks --out exports

# One-shot
topic: topic-gen topic-pull topic-select topic-scrape topic-chunk topic-export

include open_topic.mk
